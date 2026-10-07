#include "consensus/worker.h"
#include "consensus/snapshot_channel.h"
#include <deque>
#include <chrono>
#include <fstream>
#include <sys/resource.h>
#include "journal/posix.h"
#include <iostream>
#include <map>
#include <ctime>

using namespace cluster::consensus;
namespace {
int checks = 0, failures = 0;
bool admission_benchmark = false;
void check(bool value, std::string_view message) { ++checks; if (!value) { ++failures; std::cerr << "FAIL: " << message << '\n'; } }
template <class F> bool throws(F&& function) { try { function(); return false; } catch (const std::exception&) { return true; } }
FixedConfiguration configuration() { FixedConfiguration result; result.local = 1; result.voters = {1}; result.cluster[0] = 'C'; result.configuration[0] = 'V'; return result; }
Limits limits() { Limits result; result.command_bytes = 1024; result.rpc_bytes = 2048; result.log_bytes = 4096; result.log_entries = 32; result.control_entries = 4; result.rpc_entries = 4; return result; }
kronuz::journal::AdmissionLimits admission() { return {{4000, 100}, {2000, 3}, {1000, 4}, 5, 3}; }
class FaultIO final : public kronuz::journal::IO {
public:
	explicit FaultIO(const std::filesystem::path& directory) : backend(directory) {}
	bool fail_sync = false; unsigned syncs_until_failure = 0;
	bool fail_read = false, read_failure_after = false, corrupt_payload_read = false;
	std::size_t artifact_read_chunk = 65536;
	unsigned payload_reads = 0, artifact_opens = 0;
	unsigned scans = 0, artifacts = 0, directory_syncs = 0;
	auto acquire_owner(bool create) -> std::unique_ptr<kronuz::journal::OwnerLock> override { return backend.acquire_owner(create); }
	auto open_existing(std::string_view name) -> std::unique_ptr<kronuz::journal::File> override {
		auto file = backend.open_existing(name);
		if (name.starts_with("artifact-")) { ++artifact_opens; return std::make_unique<ReadFile>(*this, std::move(file)); }
		return file;
	}
	auto create_exclusive(std::string_view name) -> std::unique_ptr<kronuz::journal::File> override { auto file = backend.create_exclusive(name); if (name.starts_with("artifact-")) { ++artifacts; } return file; }
	void replace(std::string_view source, std::string_view destination) override { backend.replace(source, destination); }
	void remove(std::string_view name) override { backend.remove(name); }
	auto scan_directory() -> std::unique_ptr<kronuz::journal::DirectoryCursor> override { ++scans; return backend.scan_directory(); }
	auto entry_footprint(std::string_view name) -> std::optional<kronuz::journal::EntryFootprint> override { return backend.entry_footprint(name); }
	auto open_reclaim_candidate(std::string_view name) -> std::unique_ptr<kronuz::journal::File> override { return backend.open_reclaim_candidate(name); }
	void sync_directory() override { if (fail_sync || (syncs_until_failure && --syncs_until_failure == 0)) { fail_sync = false; throw std::runtime_error("injected worker directory barrier failure"); } backend.sync_directory(); ++directory_syncs; }
private:
	class ReadFile final : public kronuz::journal::File {
	public:
		ReadFile(FaultIO& owner, std::unique_ptr<kronuz::journal::File> file) : owner_(owner), file_(std::move(file)) {}
		std::uint64_t size() override { return file_->size(); }
		std::size_t read_at(std::uint64_t offset, std::span<char> bytes) override {
			if (offset >= kronuz::journal::detail::artifact_header_size) { ++owner_.payload_reads; }
			bool fail = std::exchange(owner_.fail_read, false);
			if (fail && !owner_.read_failure_after) { throw std::runtime_error("injected read failure before effect"); }
			auto count = file_->read_at(offset, bytes.first(std::min(bytes.size(), owner_.artifact_read_chunk)));
			if (owner_.corrupt_payload_read && offset >= kronuz::journal::detail::artifact_header_size && count) { bytes[0] ^= 1; }
			if (fail) { throw std::runtime_error("injected read failure after effect"); } return count;
		}
		std::size_t write_at(std::uint64_t offset, std::string_view bytes) override { return file_->write_at(offset, bytes); }
		void truncate(std::uint64_t size) override { file_->truncate(size); }
		void sync() override { file_->sync(); }
	private: FaultIO& owner_; std::unique_ptr<kronuz::journal::File> file_;
	};
	kronuz::journal::PosixIO backend;
};
void terminal_original_settlement() {
 auto directory = std::filesystem::current_path() / ".scratch" / ("worker-terminal-" + std::to_string(::getpid()));
 std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
 struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
 auto io = std::make_shared<kronuz::journal::PosixIO>(directory);
 {
  WorkerLimits scheduling; scheduling.async_appends = true;
  Worker worker(io, configuration(), admission(), limits(), {}, scheduling);
  Identity id{}; id[0] = 'S'; worker.create(id);
  while (worker.run_one({0, 100}) != TurnResult::IOPending) {}
  auto job = worker.take_io_operation();
  check(job && !job->done(), "terminal fixture owns an accepted initialization original");
  auto frontier = worker.storage_frontier(); auto debt = worker.accounting()->outstanding;
  job->submitted(); auto original = kronuz::journal::detail::execute_primitive(job->io(), job->request());
  worker.close(); worker.close();
  check(!worker.ready() && !worker.drained() && worker.try_submit(Start{}) == SubmitResult::Busy,
   "terminal close rejects protocol admission while accepted IO remains owned");
  for (unsigned turn = 0; turn < 20; ++turn) {
   check(worker.run_one({turn, 100}) == TurnResult::IOPending && worker.owns_io_operation(job) &&
    worker.storage_frontier().sequence == frontier.sequence && worker.accounting()->outstanding == debt,
    "terminal waits preserve original ownership and accounting without fabricated completion");
  }
  check(job->complete(std::move(original)), "terminal accepts original submitted completion");
  kronuz::journal::drive_synchronously(*job);
  for (unsigned turn = 0; !worker.drained() && turn < 100; ++turn) { worker.run_one({turn, 100}); }
  check(worker.drained() && !worker.owns_io_operation(job) && !worker.fenced(),
   "terminal drain follows original durable barriers and retained retirement");
 }
 Worker recovered(io, configuration(), admission(), limits()); recovered.recover([](auto&) {});
 for (unsigned turn = 0; !recovered.ready() && turn < 100; ++turn) { recovered.run_one({turn, 100}); }
 check(recovered.ready(), "accepted initialization survives terminal close and reopening");
 recovered.close(); while (!recovered.drained()) { recovered.run_one({0, 100}); }
}

void terminal_checkpoint_originals() {
 for (bool publishing : {false, true}) {
  auto directory = std::filesystem::current_path() / ".scratch" /
   ("worker-terminal-checkpoint-" + std::to_string(publishing) + "-" + std::to_string(::getpid()));
  std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
  struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
  auto io = std::make_shared<kronuz::journal::PosixIO>(directory);
  auto capacity = kronuz::journal::AdmissionLimits{{256 * 1024, 512}, {16 * 1024, 3}, {64 * 1024, 4}, 8, 3};
  bool held = false;
  {
   Worker worker(io, configuration(), capacity, limits(), {}, {16, 128, false, true});
   Identity id{}; id[0] = 'P'; worker.create(id);
   auto pump = [&] {
    worker.run_one({100, 100});
    if (auto job = worker.take_io_operation()) { kronuz::journal::drive_synchronously(*job); }
    for (auto &action : worker.take_actions()) {
     if (auto range = std::get_if<Committed>(&action)) { worker.applied({range->entries.back().index}); }
    }
   };
   for (unsigned turn = 0; turn < 500 && !worker.ready(); ++turn) { pump(); }
   worker.try_submit(Start{}); worker.try_submit(Tick{100, 100});
   for (unsigned turn = 0; turn < 500 && worker.applied_index() < 1; ++turn) { pump(); }
   auto checkpoint = worker.reserve_checkpoint(16); check(bool(checkpoint), "terminal checkpoint owns replacement capacity");
   if (!checkpoint) { continue; }
   worker.attach_capture(*checkpoint, {70, 1, worker.term()}); worker.offer_application_chunk(*checkpoint, "image", true);
   for (unsigned turn = 0; turn < 1000 && !held; ++turn) {
    worker.run_one({100, 100});
    if (auto job = worker.take_io_operation()) {
     auto target = publishing ? bool(std::dynamic_pointer_cast<kronuz::journal::StorePublication>(job)) :
      bool(std::dynamic_pointer_cast<kronuz::journal::StoreArtifactOperation>(job));
     if (target) {
      held = true; auto debt = worker.accounting()->outstanding; auto frontier = worker.storage_frontier();
      job->submitted(); auto original = kronuz::journal::detail::execute_primitive(job->io(), job->request());
      worker.close();
      for (unsigned wait = 0; wait < 20; ++wait) {
       check(worker.run_one({1000, 100}) == TurnResult::IOPending && !worker.drained() &&
        worker.owns_io_operation(job) && worker.accounting()->outstanding == debt &&
        worker.storage_frontier().generation == frontier.generation,
        "terminal checkpoint retains original barriers, pins and quota until completion");
      }
      check(job->complete(std::move(original)), "terminal checkpoint reaps original completion");
     }
     kronuz::journal::drive_synchronously(*job);
    }
    if (!held) { worker.take_actions(); }
   }
   check(held, "terminal fixture reaches accepted preparation or publication");
   for (unsigned turn = 0; turn < 1000 && !worker.drained(); ++turn) { worker.run_one({100, 100}); }
   check(worker.drained() && !worker.fenced() && worker.take_actions().empty(),
    "terminal checkpoint drains without fabricating publication or installation acknowledgments");
   check(bool(worker.storage_frontier().checkpoint) == publishing,
    "terminal close preserves only actually published checkpoints");
  }
  bool restored = false;
  Worker recovered(io, configuration(), capacity, limits());
  recovered.recover([&](auto &reader) {
   std::array<char, 5> bytes{};
   check(reader.descriptor().length == bytes.size() && reader.read_at(0, bytes) == bytes.size() &&
    std::string_view(bytes.data(), bytes.size()) == "image", "terminal publication recovers exact application bytes");
   restored = true;
  });
  check(restored == publishing && !recovered.fenced(), "terminal preparation and publication reopen at their actual durable boundaries");
  recovered.close(); for (unsigned turn = 0; turn < 1000 && !recovered.drained(); ++turn) { recovered.run_one({0, 100}); }
  check(recovered.drained(), "recovered terminal fixture retires all retained history");
 }
}

void terminal_output_and_dispatch_failure() {
 for (bool dispatch_failure : {false, true}) {
  auto directory = std::filesystem::current_path() / ".scratch" /
   ("worker-terminal-output-" + std::to_string(dispatch_failure) + "-" + std::to_string(::getpid()));
  std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
  struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
  auto io = std::make_shared<kronuz::journal::PosixIO>(directory);
  Worker worker(io, configuration(), admission(), limits(), {}, {16,128,true});
  Identity id{}; id[0]='F'; worker.create(id);
  for (unsigned turn=0; turn<100 && !worker.ready(); ++turn) {
   worker.run_one({100,100});
   if (auto job=worker.take_io_operation()) {
    if (dispatch_failure) {
     auto original=job;
     worker.close(); worker.storage_operation_failed(job,"injected unsubmitted dispatch failure");
     check(worker.fenced() && !original->done() && !original->in_flight() && !worker.owns_io_operation(original),
      "unsubmitted driver failure abandons locally without fabricating operation completion");
     break;
    }
    kronuz::journal::drive_synchronously(*job);
   }
  }
  if (!dispatch_failure) {
   worker.try_submit(Start{}); worker.try_submit(Tick{100,100});
   for (unsigned turn=0; turn<100; ++turn) {
    auto result=worker.run_one({100,100});
    if (auto original=worker.take_io_operation()) { kronuz::journal::drive_synchronously(*original); }
    if (result==TurnResult::Blocked) { break; }
   }
   auto applied=worker.applied_index(); worker.close();
   check(worker.take_actions().empty(), "terminal output remains owned instead of releasing retained protocol effects");
   worker.applied({1}); worker.application_failed(1,"late application failure");
   check(worker.applied_index()==applied && !worker.fenced(), "late application callbacks cannot restart or fence unrelated terminal work");
  }
  for(unsigned turn=0;turn<1000 && !worker.drained();++turn) {
   worker.run_one({100,100});
   if(auto original=worker.take_io_operation()) { kronuz::journal::drive_synchronously(*original); }
   check(worker.take_actions().empty(), "closed worker never hands protocol work to the host during retirement");
  }
  check(worker.drained(), "terminal output and dispatch-failure ownership drain without an actor waiting forever");
 }
}

void completion_appends() {
	for (bool fail : {false, true}) {
		auto directory = std::filesystem::current_path() / ".scratch" / ("worker-completion-" + std::to_string(fail) + "-" + std::to_string(::getpid()));
		std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
		struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
		auto io = std::make_shared<kronuz::journal::PosixIO>(directory);
		check(throws([&] { Worker rejected(*io, configuration(), admission(), limits(), {}, {16, 128, true}); }), "asynchronous worker rejects borrowed backend before IO");
		Worker worker(io, configuration(), admission(), limits(), {}, {16, 128, true});
		kronuz::journal::Identity identity{}; identity[0] = 'A'; worker.create(identity);
		for (unsigned turn = 0; turn < 10; ++turn) {
			// Bootstrap inventory is bounded; claim only after initialization is queued.
			if (worker.run_one({100, 100}) == TurnResult::IOPending) { break; }
		}
		auto job = worker.take_io_operation();
		check(job && !worker.ready() && !worker.take_io_operation(), "initialization job has one external owner claim and gates readiness");
		if (!job) { return; }
		auto frontier = worker.storage_frontier(); auto outstanding = worker.accounting()->outstanding;
		job->submitted(); auto completion = kronuz::journal::detail::execute_primitive(job->io(), job->request());
		check(worker.run_one({1000, 100}) == TurnResult::IOPending && worker.storage_frontier().sequence == frontier.sequence && worker.accounting()->outstanding == outstanding, "held initialization primitive retains frontier and admission");
		if (fail) {
			worker.storage_operation_failed(job, "injected driver failure after submission");
			check(worker.fenced() && job->in_flight() && worker.accounting()->tainted, "driver failure fences without fabricating completion or refund");
			check(job->complete(std::move(completion)) && job->done(), "original submitted primitive is reaped after worker fencing");
			continue;
		}
		check(job->complete(std::move(completion)), "owner accepts original initialization completion");
		kronuz::journal::drive_synchronously(*job);
		check(!worker.ready() && worker.run_one({1000, 100}) == TurnResult::Stored && worker.ready(), "readiness follows final barrier and owner settlement");
		worker.storage_operation_failed(job, "stale driver failure");
		check(!worker.fenced(), "retired operation cannot fence current worker");
		worker.try_submit(Start{}); worker.try_submit(Tick{1000, 100});
		unsigned durable_jobs = 0; bool proposed = false; std::vector<std::string> commands;
		for (unsigned turn = 0; turn < 200 && commands.empty(); ++turn) {
			auto result = worker.run_one({1000 + turn, 100});
			if (result == TurnResult::IOPending) {
				auto next = worker.take_io_operation();
				check(next && !worker.take_io_operation(), "each consensus append is dispatched exactly once");
				if (!next) { break; }
				auto committed = worker.committed();
				next->submitted(); auto held = kronuz::journal::detail::execute_primitive(next->io(), next->request());
				check(worker.run_one({2000 + turn, 100}) == TurnResult::IOPending && worker.committed() == committed, "pending consensus IO advances clock without releasing commit");
				check(next->complete(std::move(held)), "matching consensus primitive completes");
				kronuz::journal::drive_synchronously(*next); ++durable_jobs;
			}
			for (const auto& action : worker.take_actions()) {
				check(!std::holds_alternative<Persist>(action), "completion worker retains persistence internally");
				if (auto batch = std::get_if<Committed>(&action)) {
					for (const auto& entry : batch->entries) { if (entry.kind == EntryKind::Command) { commands.push_back(entry.payload); } }
					worker.applied({batch->entries.back().index});
				}
			}
			if (!proposed && worker.role() == Role::Leader && worker.committed() >= 1) {
				proposed = worker.try_submit(Propose{11, "completion-command"}) == SubmitResult::Accepted;
			}
		}
		check(proposed && durable_jobs >= 5 && commands == std::vector<std::string>{"completion-command"} && !worker.fenced(), "election and command application require their complete durable append chain");
	}
}
void completion_storage() {
	for (int cancel_at : {-1, 0, 1, 2}) {
		auto directory = std::filesystem::current_path() / ".scratch" / ("worker-all-io-" + std::to_string(cancel_at) + "-" + std::to_string(::getpid()));
		std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
		struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
		auto io = std::make_shared<kronuz::journal::PosixIO>(directory);
		Worker worker(io, configuration(), {{256 * 1024, 512}, {16 * 1024, 3}, {64 * 1024, 4}, 8, 3}, limits(), {}, {16, 128, false, true});
		kronuz::journal::Identity identity{}; identity[0] = 'Q'; worker.create(identity);
		auto pump = [&] {
			worker.run_one({100, 100});
			if (auto job = worker.take_io_operation()) { kronuz::journal::drive_synchronously(*job); }
			for (auto& action : worker.take_actions()) { if (auto batch = std::get_if<Committed>(&action)) { worker.applied({batch->entries.back().index}); } }
		};
		for (unsigned turn = 0; turn < 500 && !worker.ready(); ++turn) { pump(); }
		check(worker.ready(), "full completion profile initializes through owned IO");
		worker.try_submit(Start{}); worker.try_submit(Tick{100, 100});
		for (unsigned turn = 0; turn < 500 && worker.applied_index() < 1; ++turn) { pump(); }
		check(worker.role() == Role::Leader && worker.applied_index() == 1, "full completion profile elects and applies its durable barrier");
		auto id = worker.reserve_checkpoint(16); check(bool(id), "full completion checkpoint reserves capacity"); if (!id) { continue; }
		worker.attach_capture(*id, {70, 1, worker.term()}); worker.offer_application_chunk(*id, "image", true);
		int artifact_jobs = 0; bool canceled = false; unsigned published = 0;
		for (unsigned turn = 0; turn < 500 && worker.checkpoint_active(*id); ++turn) {
			worker.run_one({100, 100});
			if (auto job = worker.take_io_operation()) {
				bool target = std::dynamic_pointer_cast<kronuz::journal::StoreArtifactOperation>(job) && artifact_jobs++ == cancel_at;
				if (target) {
					job->submitted(); auto held = kronuz::journal::detail::execute_primitive(job->io(), job->request()); auto outstanding = worker.accounting()->outstanding;
					check(worker.cancel_checkpoint(*id) == CancelResult::Pending && worker.cancel_checkpoint(*id) == CancelResult::Pending, "accepted checkpoint quantum latches idempotent pending cancellation");
					check(worker.checkpoint_active(*id) && worker.accounting()->outstanding == outstanding && !worker.reserve_checkpoint(16), "pending cancellation retains capacity and denies replacement reuse");
					check(worker.run_one({1000, 100}) == TurnResult::IOPending && !worker.fenced(), "canceling accepted disk IO preserves protocol clock without fencing");
					check(job->complete(std::move(held)), "pending cancellation reaps the original completion"); canceled = true;
				}
				kronuz::journal::drive_synchronously(*job);
			}
			for (auto& action : worker.take_actions()) { published += std::holds_alternative<CheckpointPublished>(action); }
		}
		for (unsigned turn = 0; turn < 100 && !canceled && !published; ++turn) { worker.run_one({100, 100}); if (auto job = worker.take_io_operation()) { kronuz::journal::drive_synchronously(*job); } for (auto& action : worker.take_actions()) { published += std::holds_alternative<CheckpointPublished>(action); } }
		check(!worker.fenced() && !worker.checkpoint_active(*id) && worker.cancel_checkpoint(*id) == CancelResult::Stale, "settled full completion checkpoint disposes its identity exactly once");
		check(cancel_at < 0 ? published == 1 && worker.base_index() == 1 : canceled && published == 0 && worker.base_index() == 0, "full completion publication or cancellation preserves its exact Core boundary");
	}
}
void retired_completion_worker() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("worker-retired-completion-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	auto io = std::make_shared<kronuz::journal::PosixIO>(directory);
	auto worker = std::make_unique<Worker>(io, configuration(), admission(), limits(), Timing{}, WorkerLimits{16, 128, true});
	kronuz::journal::Identity identity{}; identity[0] = 'R'; worker->create(identity);
	for (unsigned turn = 0; turn < 10; ++turn) { if (worker->run_one({100, 100}) == TurnResult::IOPending) { break; } }
	auto operation = worker->take_io_operation(); check(bool(operation), "retired worker hands initialization to its driver");
	if (!operation) { return; }
	operation->submitted(); auto original = kronuz::journal::detail::execute_primitive(operation->io(), operation->request());
	worker.reset(); io.reset();
	check(operation->complete(std::move(original)), "accepted original completion survives Worker and backend facade retirement");
	kronuz::journal::drive_synchronously(*operation); operation.reset();
	kronuz::journal::PosixIO reopened(directory); Worker recovered(reopened, configuration(), admission(), limits());
	recovered.recover([](auto&) { throw std::runtime_error("unexpected retired checkpoint"); });
	for (unsigned turn = 0; turn < 10 && !recovered.ready(); ++turn) { recovered.run_one({100, 100}); }
	check(recovered.ready() && !recovered.fenced(), "retired Worker operation completes typed durable initialization and releases lock");
}
void real_worker() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("consensus-worker-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	std::vector<std::string> commands;
	{
		kronuz::journal::PosixIO io(directory); Worker worker(io, configuration(), admission(), limits(), {}, {2, 1});
		kronuz::journal::Identity identity{}; identity[0] = 'W'; worker.create(identity);
		for (unsigned turn = 0; turn < 10 && !worker.ready(); ++turn) { worker.run_one({100, 100}); }
		check(worker.ready(), "worker completes bounded bootstrap inventory and initialization");
		check(worker.try_submit(Start{}) == SubmitResult::Accepted, "worker starts protocol after durable initialization");
		check(worker.try_submit(Tick{100, 100}) == SubmitResult::Accepted, "worker preowns complete election pack");
		bool blocked = false;
		for (unsigned turn = 0; turn < 10 && !blocked; ++turn) { blocked = worker.run_one({100, 100}) == TurnResult::Blocked; }
		check(blocked && worker.committed() == 0 && !worker.fenced(), "output pressure retains the next durable election completion");
		check(worker.try_submit(Read{99}) == SubmitResult::Busy, "undrained output blocks external protocol admission");
		Event retained_busy = Propose{99, std::string(512, 'b')};
		auto busy_data = std::get<Propose>(retained_busy).command.data();
		check(worker.try_submit_retained(retained_busy) == SubmitResult::Busy &&
			std::get<Propose>(retained_busy).command.data() == busy_data &&
			std::get<Propose>(retained_busy).command == std::string(512, 'b'),
			"Busy retained admission leaves the caller event and allocation intact");
		if (admission_benchmark) {
			constexpr unsigned iterations = 100000;
			std::cout << "profile,payload_bytes,attempts,cpu_seconds\n";
			for (unsigned sample = 0; sample != 6; ++sample) {
				const bool retained = (sample % 2) != 0;
				bool busy = true;
				const auto start = std::clock();
				for (unsigned attempt = 0; attempt != iterations; ++attempt) {
					const auto result = retained ? worker.try_submit_retained(retained_busy) : worker.try_submit(retained_busy);
					busy = busy && result == SubmitResult::Busy;
				}
				const auto elapsed = double(std::clock() - start) / CLOCKS_PER_SEC;
				check(busy, "paired admission benchmark preserves Busy state");
				std::cout << (retained ? "retained" : "by_value") << ",512," << iterations << ',' << elapsed << '\n';
			}
		}
		unsigned maintenance = 0;
		bool hold_noop = true;
		auto drain = [&] {
			for (const auto& action : worker.take_actions()) {
				check(!std::holds_alternative<Persist>(action), "internal persistence never escapes worker");
				if (auto batch = std::get_if<Committed>(&action)) {
					for (const auto& entry : batch->entries) { if (entry.kind == EntryKind::Command) { commands.push_back(entry.payload); } }
					if (hold_noop && batch->entries.back().index == 1) { hold_noop = false; }
					else { worker.applied({batch->entries.back().index}); }
				}
			}
		};
		for (unsigned turn = 0; turn < 100 && worker.committed() < 1; ++turn) {
			if (worker.run_one({100, 100}) == TurnResult::Maintenance) { ++maintenance; } drain();
		}
		check(worker.role() == Role::Leader && worker.committed() == 1 && worker.applied_index() == 0 && !worker.fenced(), "three-write election completes through real durable storage");
		// Drain due maintenance before exercising denied pre-event admission.
		while (worker.try_submit(Read{1}) == SubmitResult::Busy) { worker.run_one({100, 100}); drain(); }
		drain();
		auto before = worker.accounting()->used;
		auto proposal = worker.try_submit(Propose{2, std::string(1024, 'x')});
		if (proposal == SubmitResult::Busy) { worker.run_one({100, 100}); proposal = worker.try_submit(Propose{2, std::string(1024, 'x')}); }
		check(proposal == SubmitResult::Pressure && worker.term() == 1 && worker.committed() == 1 && worker.accounting()->used == before, "denied pre-event reservation leaves durable and Core state unchanged");
		Event retained_pressure = Propose{2, std::string(1024, 'p')};
		auto pressure_data = std::get<Propose>(retained_pressure).command.data();
		check(worker.try_submit_retained(retained_pressure) == SubmitResult::Pressure &&
			std::get<Propose>(retained_pressure).command.data() == pressure_data &&
			std::get<Propose>(retained_pressure).command == std::string(1024, 'p') && worker.accounting()->used == before,
			"Pressure retained admission preserves event bytes and refunds partial reservations");
		Event retained_accepted = Propose{3, "state"};
		check(worker.try_submit_retained(retained_accepted) == SubmitResult::Accepted, "small retained event transfers its commit continuation");
		worker.applied({1});
		check(worker.applied_index() == 1, "application completion survives busy command persistence");
		for (unsigned turn = 0; turn < 100 && worker.applied_index() < 2; ++turn) { worker.run_one({100, 100}); drain(); }
		check(commands == std::vector<std::string>{"state"} && worker.applied_index() == 2 && !worker.fenced(), "worker reliably delivers and applies a committed command");
		check(maintenance > 0, "mandatory reclamation runs during election foreground traffic");
		unsigned accepted = 0, slices = 0;
		for (unsigned turn = 0; turn < 100 && accepted < 32; ++turn) {
			auto result = worker.try_submit(Read{100 + accepted});
			if (result == SubmitResult::Accepted) { ++accepted; drain(); }
			else if (worker.run_one({100, 100}) == TurnResult::Maintenance) { ++slices; }
		}
		check(accepted == 32 && slices >= 15, "continuous foreground reads cannot bypass mandatory bounded reclamation turns");
	}
	{
		kronuz::journal::PosixIO io(directory); Worker worker(io, configuration(), admission(), limits(), {}, {2, 1});
		worker.recover([](auto&) { throw std::runtime_error("unexpected checkpoint"); });
		for (unsigned turn = 0; turn < 10 && !worker.ready(); ++turn) { worker.run_one({100, 100}); }
		check(worker.ready() && worker.committed() == 2, "worker owns typed recovery before protocol admission");
		worker.try_submit(Start{}); auto actions = worker.take_actions();
		bool restored = false;
		for (const auto& action : actions) { if (auto batch = std::get_if<Committed>(&action)) { restored = batch->entries.back().payload == "state"; } }
		check(restored, "restarted worker replays exact durable committed command");
	}
}
void partial_control_pack() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("worker-control-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	kronuz::journal::AdmissionLimits scarce{{1343, 100}, {1179, 3}, {0, 0}, 5, 3};
	{
		kronuz::journal::PosixIO io(directory); Worker worker(io, configuration(), scarce, limits());
		kronuz::journal::Identity identity{}; identity[0] = 'P'; worker.create(identity);
		while (!worker.ready()) { worker.run_one({100, 100}); }
		worker.try_submit(Start{}); worker.try_submit(Tick{100, 100});
		for (unsigned turn = 0; turn < 30 && worker.committed() < 1; ++turn) { worker.run_one({100, 100}); worker.take_actions(); }
		check(worker.role() == Role::Leader && worker.accounting()->control_available.logical_bytes < 1179, "election consumes protection when no unprotected bytes remain");
		auto used = worker.accounting()->used;
		check(worker.try_submit(Tick{100, 100}) == SubmitResult::Accepted && worker.accounting()->used == used, "leader tick needs no campaign pack or storage growth under pressure");
	}
	{
		kronuz::journal::PosixIO io(directory); Worker worker(io, configuration(), scarce, limits());
		worker.recover([](auto&) {}); while (!worker.ready()) { worker.run_one({100, 100}); }
		worker.try_submit(Start{}); worker.take_actions(); auto used = worker.accounting()->used;
		check(worker.try_submit(Tick{100, 100}) == SubmitResult::Pressure && worker.term() == 1, "partial campaign pack denial leaves recovered follower term unchanged");
		check(worker.accounting()->tickets == 0 && worker.accounting()->outstanding == kronuz::journal::StorageResources{} && worker.accounting()->used == used, "partial pack acquisition refunds every pre-IO reservation");
	}
}
void failures_and_multi_action_output() {
	for (bool inject : {false, true}) {
		auto directory = std::filesystem::current_path() / ".scratch" / ((inject ? "worker-failure-" : "worker-output-") + std::to_string(::getpid()));
		std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
		struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
		FaultIO io(directory); auto fixed = configuration(); if (!inject) { fixed.voters = {1, 2, 3}; }
		Worker worker(io, fixed, admission(), limits()); kronuz::journal::Identity identity{}; identity[0] = 'F';
		worker.create(identity); while (!worker.ready()) { worker.run_one({100, 100}); }
		worker.try_submit(Start{}); worker.try_submit(Tick{100, 100});
		check(worker.run_one({100, 100}) == TurnResult::Stored && worker.run_one({100, 100}) == TurnResult::Completed, "worker establishes candidate output through matching durable completion");
		if (inject) {
			io.fail_sync = true;
			check(worker.run_one({100, 100}) == TurnResult::Fenced && worker.role() == Role::Fenced && !worker.ready(), "storage failure fences Core and Store while output is occupied");
			auto retained = worker.take_actions(); auto terminal = worker.take_actions();
			check(retained.size() == 1 && std::holds_alternative<RoleChanged>(retained[0]) && terminal.size() == 1 && std::holds_alternative<Fenced>(terminal[0]), "terminal notice survives behind retained output without overwrite");
			check(worker.take_actions().empty() && worker.accounting()->tainted, "terminal notice drains exactly once and uncertain accounting stays fenced");
		} else {
			check(worker.try_submit(Read{1}) == SubmitResult::Busy && worker.run_one({100, 100}) == TurnResult::Blocked, "multi-action output blocks protocol steps under pressure");
			auto actions = worker.take_actions(); std::size_t sends = 0, changes = 0;
			for (const auto& action : actions) { sends += std::holds_alternative<Send>(action); changes += std::holds_alternative<RoleChanged>(action); }
			check(actions.size() == 3 && sends == 2 && changes == 1 && !worker.fenced(), "one output batch retains role change and every voter request");
		}
	}
}
void interrupted_initialization() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("worker-bootstrap-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	{
		kronuz::journal::PosixIO io(directory); Worker worker(io, configuration(), admission(), limits());
		kronuz::journal::Identity identity{}; identity[0] = 'B'; worker.create(identity);
		check(!worker.ready(), "create can return before queued typed initialization is durable");
	}
	{
		kronuz::journal::PosixIO io(directory); Worker worker(io, configuration(), admission(), limits());
		worker.recover([](auto&) {});
		for (unsigned turn = 0; turn < 10 && !worker.ready(); ++turn) { worker.run_one({100, 100}); }
		check(worker.ready() && worker.try_submit(Start{}) == SubmitResult::Accepted, "verified empty bootstrap resumes typed initialization after restart");
		worker.try_submit(Tick{100, 100});
		Index delivered = 0;
		for (unsigned turn = 0; turn < 30 && !delivered; ++turn) {
			worker.run_one({100, 100});
			for (const auto& action : worker.take_actions()) { if (auto batch = std::get_if<Committed>(&action)) { delivered = batch->entries.back().index; } }
		}
		worker.application_failed(99, "stale"); check(!worker.fenced(), "stale application failures do not fence another delivery");
		check(worker.try_submit(Propose{1, "pending"}) == SubmitResult::Accepted, "later persistence may overlap earlier application delivery");
		worker.application_failed(delivered, "injected application failure");
		check(worker.fenced() && worker.role() == Role::Fenced && worker.accounting()->tainted, "matching application failure fences Core and Store during persistence");
		check(worker.take_actions().size() == 1 && worker.take_actions().empty(), "application failure retains one terminal notice");
	}
}
void checkpoint_crash_and_progress() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("worker-checkpoint-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	auto quota = kronuz::journal::AdmissionLimits{{256 * 1024, 512}, {16 * 1024, 3}, {128 * 1024, 4}, 8, 3};
	std::vector<std::string> expected_suffix;
	Index committed = 0;
	{
		FaultIO io(directory); Worker worker(io, configuration(), quota, limits(), {}, {1, 128});
		kronuz::journal::Identity identity{}; identity[0] = 'S'; worker.create(identity);
		while (!worker.ready()) { worker.run_one({100, 100}); }
		std::map<RequestId, std::string> requested; std::map<Index, std::string> placed;
		Index held_application = 0; bool published_notice = false;
		auto drain = [&] {
			for (const auto& action : worker.take_actions()) {
				if (auto proposal = std::get_if<ProposalPlaced>(&action)) { placed[proposal->index] = requested.at(proposal->request); }
				if (auto batch = std::get_if<Committed>(&action)) {
					if (batch->entries.back().index == 3) { held_application = 3; }
					else { worker.applied({batch->entries.back().index}); }
				}
				published_notice |= std::holds_alternative<CheckpointPublished>(action);
			}
		};
		auto pump = [&] { worker.run_one({100, 100}); drain(); };
		auto submit = [&](Event event) {
			for (unsigned turn = 0; turn < 100; ++turn) { auto result = worker.try_submit(event); if (result != SubmitResult::Busy) { check(result == SubmitResult::Accepted, "checkpoint fixture admits foreground work"); return; } pump(); }
			check(false, "checkpoint fixture admission stalled");
		};
		submit(Start{}); submit(Tick{100, 100});
		for (unsigned turn = 0; turn < 100 && worker.applied_index() < 1; ++turn) { pump(); }
		requested[1] = "before"; submit(Propose{1, requested[1]});
		for (unsigned turn = 0; turn < 100 && worker.applied_index() < 2; ++turn) { pump(); }
		check(worker.applied_index() == 2, "checkpoint capture starts from nonempty applied state");
		auto id = worker.reserve_checkpoint(64 * 1024); check(id.has_value(), "whole replacement is reserved before immutable capture"); if (!id) { return; }
		worker.attach_capture(*id, {90, 2, 1});
		check(worker.offer_application_chunk(*id, "before", true) == SubmitResult::Accepted, "bounded final capture chunk is accepted");
		unsigned scans_at_freeze = 0; bool applied_during_freeze = false; unsigned proposals = 0;
		for (unsigned turn = 0; turn < 300 && !worker.storage_frontier().checkpoint; ++turn) {
			auto request = RequestId{10} + proposals; requested[request] = "after-" + std::to_string(proposals);
			if (worker.try_submit(Propose{request, requested[request]}) == SubmitResult::Accepted) { ++proposals; }
			pump();
			if (io.artifacts == 2 && !applied_during_freeze) {
				scans_at_freeze = io.scans;
				if (held_application) { worker.applied({held_application}); applied_during_freeze = worker.applied_index() == held_application; }
			}
		}
		check(worker.storage_frontier().checkpoint.has_value() && proposals > 0 && !worker.fenced(), "continuous proposals cannot prevent checkpoint cutover and publication");
		check(applied_during_freeze && worker.applied_index() >= 3, "application completion is preserved during frozen bundle preparation");
		check(io.scans >= scans_at_freeze + 3, "reclamation continues through frozen bundle preparation and publication");
		check(worker.cancel_checkpoint(*id) == CancelResult::TooLate, "publication cannot be canceled after Core owns persistence");
		check(worker.base_index() == 0 && !published_notice, "durable publication does not invent its Core completion");
		committed = worker.committed();
		for (const auto& [index, command] : placed) { if (index > 2 && index <= committed) { expected_suffix.push_back(command); } }
		// Close after Store publication, before delivering its held Persisted.
	}
	{
		FaultIO io(directory); Worker worker(io, configuration(), quota, limits());
		std::string image;
		worker.recover([&](auto& reader) { image.resize(static_cast<std::size_t>(reader.descriptor().length)); reader.read_at(0, std::span<char>(image.data(), image.size())); });
		while (!worker.ready()) { worker.run_one({100, 100}); }
		check(image == "before" && worker.base_index() == 2 && worker.committed() == committed, "lost publication completion restores exact application image and committed boundary");
		worker.try_submit(Start{}); std::vector<std::string> suffix;
		for (unsigned turn = 0; turn < 100 && worker.applied_index() < committed; ++turn) {
			for (const auto& action : worker.take_actions()) { if (auto batch = std::get_if<Committed>(&action)) { for (const auto& entry : batch->entries) { if (entry.kind == EntryKind::Command) { suffix.push_back(entry.payload); } } worker.applied({batch->entries.back().index}); } }
			worker.run_one({100, 100});
		}
		check(!expected_suffix.empty() && suffix == expected_suffix && worker.applied_index() == committed, "recovery applies only the committed suffix above the immutable capture");
	}
}
void preparation_preserves_protocol_timers() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("worker-timers-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	FaultIO io(directory); auto fixed = configuration(); fixed.voters = {1, 2, 3};
	Worker worker(io, fixed, {{256 * 1024, 512}, {16 * 1024, 3}, {64 * 1024, 4}, 8, 3}, limits(), {}, {4, 128});
	kronuz::journal::Identity identity{}; identity[0] = 'T'; worker.create(identity); while (!worker.ready()) { worker.run_one({100, 100}); }
	std::map<NodeId, Receive> responses; unsigned sends = 0;
	auto drain = [&] {
		for (const auto& action : worker.take_actions()) {
			if (auto batch = std::get_if<Committed>(&action)) { worker.applied({batch->entries.back().index}); }
			if (auto send = std::get_if<Send>(&action)) {
				if (auto vote = std::get_if<VoteRequest>(&send->message)) { responses.insert_or_assign(send->peer, Receive{send->peer, VoteResponse{vote->term, true}}); }
				if (auto append = std::get_if<AppendRequest>(&send->message)) {
					++sends; auto matched = append->previous + append->entries.size();
					responses.insert_or_assign(send->peer, Receive{send->peer, AppendResponse{append->term, append->rpc, true, matched, matched + 1, append->read_probe}});
				}
			}
		}
	};
	auto respond = [&] { if (!responses.empty()) { auto next = responses.begin(); if (worker.try_submit(next->second) == SubmitResult::Accepted) { responses.erase(next); } } };
	worker.try_submit(Start{}); worker.try_submit(Tick{100, 100});
	for (unsigned turn = 0; turn < 100 && worker.applied_index() < 1; ++turn) { respond(); worker.run_one({100, 100}); drain(); }
	check(worker.role() == Role::Leader && worker.applied_index() == 1, "timer fixture elects a three-voter leader with correlated peer replies");
	responses.clear(); sends = 0; auto scans = io.scans; auto used = worker.accounting()->used;
	auto id = worker.reserve_checkpoint(4096); if (!id) { check(false, "timer fixture replacement unavailable"); return; }
	worker.attach_capture(*id, {20, 1, 1}); unsigned chunks = 0;
	for (unsigned turn = 0; turn < 60; ++turn) {
		if (worker.offer_application_chunk(*id, "part") == SubmitResult::Accepted) { ++chunks; }
		respond(); worker.run_one({200 + 20 * turn, 100}); drain();
	}
	check(sends >= 4 && !worker.fenced(), "continuously runnable preparation preserves heartbeat and retry timer service before sealing");
	check(chunks >= 4 && io.scans >= scans + 4, "timer service cannot starve preparation or reclamation");
	check(worker.cancel_checkpoint(*id) == CancelResult::Canceled && worker.accounting()->used.logical_bytes > used.logical_bytes, "unfinished streamed capture cancels with actual staged bytes charged");
}
void maintenance_preserves_protocol_timers() {
	for (bool drain_before : {false, true}) {
	auto directory = std::filesystem::current_path() / ".scratch" / ("worker-maintenance-timers-" + std::to_string(::getpid()) + "-" + std::to_string(drain_before));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	FaultIO io(directory); auto fixed = configuration(); fixed.voters = {1, 2, 3};
	auto bounded_reads = limits(); bounded_reads.reads = 1;
	Worker worker(io, fixed, {{256 * 1024, 512}, {16 * 1024, 3}, {64 * 1024, 4}, 8, 3}, bounded_reads, {}, {1, 128});
	kronuz::journal::Identity identity{}; identity[0] = 'T'; worker.create(identity); while (!worker.ready()) { worker.run_one({100, 100}); }
	std::map<NodeId, Receive> responses; unsigned sends = 0;
	auto drain = [&] {
		for (const auto& action : worker.take_actions()) {
			if (auto batch = std::get_if<Committed>(&action)) { worker.applied({batch->entries.back().index}); }
			if (auto send = std::get_if<Send>(&action)) {
				if (auto vote = std::get_if<VoteRequest>(&send->message)) { responses.insert_or_assign(send->peer, Receive{send->peer, VoteResponse{vote->term, true}}); }
				if (auto append = std::get_if<AppendRequest>(&send->message)) {
					++sends; auto matched = append->previous + append->entries.size();
					responses.insert_or_assign(send->peer, Receive{send->peer, AppendResponse{append->term, append->rpc, true, matched, matched + 1, append->read_probe}});
				}
			}
		}
	};
	auto respond = [&] { if (!responses.empty()) { auto next = responses.begin(); if (worker.try_submit(next->second) == SubmitResult::Accepted) { responses.erase(next); } } };
	worker.try_submit(Start{}); worker.try_submit(Tick{100, 100});
	for (unsigned turn = 0; turn < 100 && worker.applied_index() < 1; ++turn) { respond(); worker.run_one({100, 100}); drain(); }
	check(worker.role() == Role::Leader && worker.applied_index() == 1, "timer fixture elects a three-voter leader with correlated peer replies");
	responses.clear(); sends = 0; unsigned reads = 0; auto scans = io.scans;
	for (unsigned turn = 0; turn < 80; ++turn) {
		if (worker.try_submit(Read{100 + reads}) == SubmitResult::Accepted) { ++reads; }
		if (drain_before) { drain(); }
		worker.run_one({200 + 20 * turn, 100}); drain();
	}
	check(sends >= 4 && reads >= 8 && io.scans >= scans + 4 && !worker.fenced(), "mandatory maintenance preserves timer retries and ordinary admission under saturated reads");
	}
}
void start_single(Worker& worker, char storage) {
	kronuz::journal::Identity identity{}; identity[0] = storage; worker.create(identity);
	for (unsigned turn = 0; turn < 100 && !worker.ready(); ++turn) { worker.run_one({100, 100}); }
	worker.try_submit(Start{}); worker.try_submit(Tick{100, 100});
	for (unsigned turn = 0; turn < 100 && worker.applied_index() < 1; ++turn) {
		worker.run_one({100, 100});
		for (const auto& action : worker.take_actions()) { if (auto batch = std::get_if<Committed>(&action)) { worker.applied({batch->entries.back().index}); } }
	}
	check(worker.ready() && worker.applied_index() == 1, "checkpoint fixture starts a durable applied leader");
}
void checkpoint_cancellation_and_completion() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("worker-cancel-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	FaultIO io(directory); Worker worker(io, configuration(), {{256 * 1024, 512}, {16 * 1024, 3}, {64 * 1024, 4}, 8, 3}, limits());
	start_single(worker, 'C');
	auto id = worker.reserve_checkpoint(16); if (!id) { check(false, "cancellation fixture reservation"); return; }
	check(worker.cancel_checkpoint(*id) == CancelResult::Canceled && worker.cancel_checkpoint(*id) == CancelResult::Stale, "unstarted cancellation refunds once and invalidates its session token");
	check(throws([&] { worker.attach_capture(*id, {1, 1, 1}); }), "stale checkpoint token cannot attach a new capture");
	id = worker.reserve_checkpoint(16); worker.attach_capture(*id, {2, 1, 1});
	{
		Worker other(io, configuration(), {{256 * 1024, 512}, {16 * 1024, 3}, {64 * 1024, 4}, 8, 3}, limits());
		check(throws([&] { other.attach_capture(*id, {2, 1, 1}); }) && other.cancel_checkpoint(*id) == CancelResult::Stale, "foreign checkpoint token rejects before unopened storage changes");
	}
	check(throws([&] { worker.attach_capture(*id, {3, 1, 1}); }), "capture metadata attaches exactly once");
	check(throws([&] { worker.offer_application_chunk(*id, std::string(65537, 'x')); }) && throws([&] { worker.offer_application_chunk(*id, std::string(17, 'x')); }), "chunk and aggregate capture bounds reject before staging");
	check(worker.offer_application_chunk(*id, "part") == SubmitResult::Accepted && worker.offer_application_chunk(*id, "next") == SubmitResult::Busy, "one copied chunk mailbox applies bounded backpressure");
	bool final = false;
	for (unsigned turn = 0; turn < 100 && !final; ++turn) { worker.run_one({100, 100}); worker.take_actions(); final = worker.offer_application_chunk(*id, "", true) == SubmitResult::Accepted; }
	check(final && throws([&] { worker.offer_application_chunk(*id, "late"); }), "final marker closes the capture stream immediately");
	auto before = worker.accounting()->used;
	check(worker.cancel_checkpoint(*id) == CancelResult::Canceled && worker.accounting()->used.logical_bytes == before.logical_bytes + 72, "partial streaming cancellation charges the written header and payload");
	for (unsigned turn = 0; turn < 100 && worker.accounting()->used != before; ++turn) { worker.run_one({100, 100}); worker.take_actions(); }
	check(worker.accounting()->used == before, "durable GC alone credits canceled artifact bytes");
	id = worker.reserve_checkpoint(16); worker.attach_capture(*id, {4, 1, 1}); worker.offer_application_chunk(*id, "image", true);
	auto synced = io.directory_syncs;
	for (unsigned turn = 0; turn < 100 && io.directory_syncs == synced; ++turn) { worker.run_one({100, 100}); worker.take_actions(); }
	check(io.directory_syncs > synced && worker.cancel_checkpoint(*id) == CancelResult::Canceled, "sealed capture remains cancelable before Core checkpoint persistence");
	id = worker.reserve_checkpoint(16); worker.attach_capture(*id, {5, 1, 2}); worker.offer_application_chunk(*id, "invalid", true);
	bool rejected = false;
	for (unsigned turn = 0; turn < 100 && !rejected; ++turn) {
		worker.run_one({100, 100}); for (const auto& action : worker.take_actions()) { if (auto reject = std::get_if<Reject>(&action)) { rejected |= reject->request == 5 && reject->reason == RejectReason::InvalidCheckpoint; } }
	}
	check(rejected && worker.cancel_checkpoint(*id) == CancelResult::Stale && !worker.fenced(), "invalid Core capture cancels its replacement and clears cutover debt");
	id = worker.reserve_checkpoint(16); worker.attach_capture(*id, {6, 1, 1}); worker.offer_application_chunk(*id, "", true);
	unsigned published = 0;
	for (unsigned turn = 0; turn < 100 && !published; ++turn) {
		worker.run_one({100, 100}); for (const auto& action : worker.take_actions()) { published += std::holds_alternative<CheckpointPublished>(action); }
	}
	check(published == 1 && worker.base_index() == 1 && worker.cancel_checkpoint(*id) == CancelResult::Stale, "matching persistence completion publishes and compacts exactly once");
	SubmitResult read = SubmitResult::Busy;
	for (unsigned turn = 0; turn < 100 && read == SubmitResult::Busy; ++turn) { read = worker.try_submit(Read{30}); if (read == SubmitResult::Busy) { worker.run_one({100, 100}); worker.take_actions(); } }
	check(read == SubmitResult::Accepted && !worker.fenced(), "completed checkpoint releases debt and allows new ordinary work");
	SubmitResult proposal = SubmitResult::Busy;
	for (unsigned turn = 0; turn < 100 && proposal == SubmitResult::Busy; ++turn) {
		worker.run_one({100, 100}); worker.take_actions(); proposal = worker.try_submit(Propose{31, "after checkpoint"});
	}
	bool committed = false;
	for (unsigned turn = 0; turn < 100 && worker.applied_index() < 2; ++turn) {
		worker.run_one({100, 100});
		for (const auto& action : worker.take_actions()) {
			if (auto batch = std::get_if<Committed>(&action)) {
				committed |= batch->entries.back().payload == "after checkpoint";
				worker.applied({batch->entries.back().index});
			}
		}
	}
	check(proposal == SubmitResult::Accepted && committed && worker.committed() == 2 && worker.applied_index() == 2 && !worker.fenced(), "first proposal in the replacement generation persists, commits, and applies");
}
void checkpoint_faults() {
	for (bool publication : {false, true}) {
		auto directory = std::filesystem::current_path() / ".scratch" / ("worker-cp-fault-" + std::to_string(::getpid()) + "-" + std::to_string(publication));
		std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
		struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
		auto quota = kronuz::journal::AdmissionLimits{{256 * 1024, 512}, {16 * 1024, 3}, {64 * 1024, 4}, 8, 3};
		{
			FaultIO io(directory); Worker worker(io, configuration(), quota, limits()); start_single(worker, 'F');
			auto id = worker.reserve_checkpoint(16); worker.attach_capture(*id, {1, 1, 1}); worker.offer_application_chunk(*id, "image", true);
			auto synced = io.directory_syncs;
			if (!publication) { worker.try_submit(Read{20}); io.fail_sync = true; }
			for (unsigned turn = 0; turn < 100 && !worker.fenced(); ++turn) {
				worker.run_one({100, 100});
				if (publication && io.directory_syncs >= synced + 2) { io.fail_sync = true; }
			}
			check(worker.fenced() && worker.role() == Role::Fenced && worker.accounting()->tainted, "uncertain preparation or publication fences both owned modules");
			if (!publication) { auto held = worker.take_actions(); check(held.size() == 1 && std::holds_alternative<ReadReady>(held[0]), "preparation failure retains occupied foreground output"); }
			auto terminal = worker.take_actions(); check(terminal.size() == 1 && std::holds_alternative<Fenced>(terminal[0]), "checkpoint failure retains a terminal notice");
		}
		{
			FaultIO io(directory); Worker worker(io, configuration(), quota, limits()); std::string image;
			worker.recover([&](auto& reader) { image.resize(static_cast<std::size_t>(reader.descriptor().length)); reader.read_at(0, std::span<char>(image.data(), image.size())); });
			for (unsigned turn = 0; turn < 100 && !worker.ready(); ++turn) { worker.run_one({100, 100}); }
			check(worker.ready() && worker.committed() == 1 && (worker.base_index() == 0 || (worker.base_index() == 1 && image == "image")), "fresh recovery retains acknowledged state after uncertain checkpoint IO");
		}
	}
}
struct SnapshotFixture {
	struct Directory {
		std::filesystem::path path;
		explicit Directory(std::string name) : path(std::filesystem::current_path() / ".scratch" / ("worker-snapshot-" + name + "-" + std::to_string(::getpid()))) {
			std::filesystem::create_directories(path.parent_path());
			if (!std::filesystem::create_directory(path)) { throw std::runtime_error("snapshot fixture directory exists"); } ::chmod(path.c_str(), 0700);
		}
		~Directory() { std::filesystem::remove_all(path); }
	};
	static FixedConfiguration fixed() { auto result = configuration(); result.voters = {1, 2, 3}; return result; }
	static kronuz::journal::AdmissionLimits quota() { return {{512 * 1024, 4096}, {16 * 1024, 3}, {128 * 1024, 4}, 8, 3}; }
	Directory directory; std::shared_ptr<FaultIO> owned_io; FaultIO& io; Worker worker; bool unexpected_install = false; std::string application_state;
	explicit SnapshotFixture(std::string name, std::optional<SnapshotLimits> snapshots = SnapshotLimits{7, 100000}, kronuz::journal::AdmissionLimits capacity = quota(), bool start = true, bool async = false)
		: directory(std::move(name)), owned_io(std::make_shared<FaultIO>(directory.path)), io(*owned_io), worker(owned_io, fixed(), capacity, limits(), {}, {4, 1, false, async}, snapshots) {
		kronuz::journal::Identity identity{}; identity[0] = 'S'; worker.create(identity);
		for (unsigned turn = 0; turn < 100 && !worker.ready(); ++turn) { worker.run_one({0, 100}); if (auto job = worker.take_io_operation()) { kronuz::journal::drive_synchronously(*job); } }
		if (start) { worker.try_submit(Start{}); drain(); }
	}
	void drain() {
		for (const auto& action : worker.take_actions()) {
			unexpected_install |= std::holds_alternative<PersistInstall>(action) || std::holds_alternative<ActivateInstall>(action) || std::holds_alternative<InstallCompleted>(action) || std::holds_alternative<CheckpointPublished>(action);
			if (auto range = std::get_if<Committed>(&action)) {
				for (const auto& entry : range->entries) { if (entry.kind == EntryKind::Command) { application_state += "/" + entry.payload; } }
				worker.applied({range->entries.back().index});
			}
		}
	}
	std::uint64_t staged_bytes() const {
		std::uint64_t total = 0;
		for (const auto& entry : std::filesystem::directory_iterator(directory.path)) { if (entry.path().filename().string().starts_with("artifact-")) { total += entry.file_size(); } }
		return total;
	}
	void pump() { worker.run_one({0, 100}); if (auto job = worker.take_io_operation()) { kronuz::journal::drive_synchronously(*job); } drain(); }
	SnapshotContext context(std::string_view payload) {
		auto configuration = fixed(); return {2, 1, 13, 17, {configuration.cluster, configuration.configuration, 7, 1, 1, payload.size(), kronuz::journal::crc32c(payload)}};
	}
	void send(const SnapshotId& id, std::string_view payload) {
		std::uint64_t offset = 0; bool final = false;
		for (unsigned turn = 0; turn < 1000 && !final; ++turn) {
			auto count = std::min<std::size_t>(payload.size() - offset, 65536);
			auto result = worker.offer_snapshot_chunk(id, offset, payload.substr(static_cast<std::size_t>(offset), count), offset + count == payload.size());
			if (result.result == SubmitResult::Accepted) { offset = result.next_offset; final = offset == payload.size(); }
			else { pump(); }
		}
		check(final, "snapshot reception accepts the complete exact input stream");
	}
};
void completion_channel_cancellation() {
	SnapshotFixture fixture("completion-channel-cancel", SnapshotLimits{7, 100000}, SnapshotFixture::quota(), true, true);
	auto& worker = fixture.worker; SnapshotChannel channel(worker);
	Identity local{}, remote{}; local[0] = 'l'; remote[0] = 'r'; TrustedSession session{2, local, remote}; channel.register_session(session);
	auto fixed = worker.configuration(); TransferEnvelope envelope{fixed.cluster, fixed.configuration, remote, local, 2, 1, {1, 17, {1, 1}}};
	auto begin = encode_transfer(TransferKind::Begin, envelope, encode_snapshot(fixture.context("data").descriptor, *worker.snapshot_policy()));
	channel.receive(session, std::span<const char>(begin.data(), begin.size())); auto id = channel.incoming_snapshot();
	check(bool(id), "completion channel owns incoming Begin"); if (!id) { return; }
	std::shared_ptr<kronuz::journal::IOOperation> pending;
	for (unsigned turn = 0; turn < 100 && !pending; ++turn) {
		worker.run_one({0, 100});
		if (auto job = worker.take_io_operation()) { if (std::dynamic_pointer_cast<kronuz::journal::StoreArtifactOperation>(job)) { pending = job; } else { kronuz::journal::drive_synchronously(*job); } }
		fixture.drain();
	}
	check(bool(pending), "completion channel reaches owned application creation"); if (!pending) { return; }
	pending->submitted(); auto original = kronuz::journal::detail::execute_primitive(pending->io(), pending->request());
	auto accepted = channel.outbound(); check(bool(accepted), "cancel fixture retains Accepted output pressure"); auto output_token = accepted->token;
	auto cancel = encode_transfer(TransferKind::Cancel, envelope, {}); channel.receive(session, std::span<const char>(cancel.data(), cancel.size()));
	check(worker.snapshot_canceling(*id) && !channel.drained() && !worker.pending_snapshot_result(), "channel retains pending receiver before original completion");
	auto repeated = channel.receive(session, std::span<const char>(begin.data(), begin.size()));
	check(repeated.result == TransferReceive::Accepted && repeated.consumed == begin.size() && channel.outbound()->token == output_token, "canceling duplicate Begin creates no new acceptance under output pressure");
	auto chunk = encode_transfer(TransferKind::Chunk, envelope, transfer_chunk_body(90, 0, "data", true));
	auto discarded = channel.receive(session, std::span<const char>(chunk.data(), chunk.size()));
	check(discarded.result == TransferReceive::Accepted && discarded.consumed == chunk.size() && channel.outbound()->token == output_token, "canceling receiver consumes already-sent data without credit or mailbox reuse");
	check(worker.run_one({1000, 100}) == TurnResult::IOPending && worker.busy(), "held maintenance IO permits a fully funded election persistence request");
	channel.session_closed(session); check(!channel.drained(), "disconnect retains canceling receiver through original IO");
	check(pending->complete(std::move(original)), "disconnected receiver reaps original accepted creation"); kronuz::journal::drive_synchronously(*pending);
	worker.run_one({1000, 100}); channel.run_one({1000, 100});
	check(channel.drained() && !worker.pending_snapshot_result() && !worker.fenced(), "disconnected cancellation drains exact terminal result after accounting settles");
	check(worker.run_one({1000, 100}) == TurnResult::IOPending, "funded Raft append precedes the next maintenance quantum");
	auto append = worker.take_io_operation(); check(bool(std::dynamic_pointer_cast<kronuz::journal::StoreAppend>(append)), "maintenance reaping resumes its pre-funded consensus append"); if (append) { kronuz::journal::drive_synchronously(*append); worker.run_one({1000, 100}); }
}
void terminal_reclamation_original() {
	SnapshotFixture fixture("terminal-reclaim", SnapshotLimits{7, 100000}, SnapshotFixture::quota(), true, true);
	auto& worker = fixture.worker;
	bool held = false;
	for (unsigned turn = 0; turn < 10000 && !held; ++turn) {
		worker.try_submit(Read{turn + 1});
		worker.run_one({0, 100});
		if (auto job = worker.take_io_operation()) {
			if (!std::dynamic_pointer_cast<kronuz::journal::StoreReclaim>(job)) { kronuz::journal::drive_synchronously(*job); }
			else {
				job->submitted(); auto outcome = kronuz::journal::detail::execute_primitive(job->io(), job->request());
				auto debt = worker.accounting()->outstanding;
				worker.close();
				for (unsigned wait = 0; wait < 20; ++wait) {
					worker.run_one({0, 100});
					check(!worker.drained() && !job->done() && worker.owns_io_operation(job) && worker.accounting()->outstanding == debt,
						"terminal reclamation retains its accepted original and quota");
				}
				check(job->complete(std::move(outcome)), "terminal reclamation observes its authentic completion");
				kronuz::journal::drive_synchronously(*job);
				for (unsigned wait = 0; wait < 10000 && !worker.drained(); ++wait) { worker.run_one({0, 100}); }
				check(worker.drained() && !worker.fenced(), "terminal reclamation settles before bounded protocol retirement");
				held = true;
			}
		}
		fixture.drain();
	}
	check(held, "terminal regression reaches an accepted reclamation original");
}
void terminal_snapshot_readback() {
	for (unsigned kind : {0u, 1u}) {
		SnapshotFixture fixture("terminal-readback-" + std::to_string(kind), SnapshotLimits{7, 100000}, SnapshotFixture::quota(), true, true);
		auto& worker = fixture.worker;
		std::string payload(65553, 't');
		auto id = worker.reserve_snapshot(fixture.context(payload));
		check(bool(id), "terminal verification reserves incoming snapshot");
		if (!id) { continue; }
		fixture.send(*id, payload);
		bool held = false;
		for (unsigned turn = 0; turn < 10000 && !held; ++turn) {
			worker.run_one({0, 100});
			if (auto job = worker.take_io_operation()) {
				bool target = kind == 0 ? bool(std::dynamic_pointer_cast<kronuz::journal::ArtifactOpen>(job)) : bool(std::dynamic_pointer_cast<kronuz::journal::ArtifactRead>(job));
				if (!target) { kronuz::journal::drive_synchronously(*job); }
				else {
					job->submitted(); auto outcome = kronuz::journal::detail::execute_primitive(job->io(), job->request());
					auto debt = worker.accounting()->outstanding;
					worker.close();
					for (unsigned wait = 0; wait < 20; ++wait) {
						worker.run_one({0, 100});
						check(!worker.drained() && !job->done() && worker.accounting()->outstanding == debt && !worker.validation_chunk(), "terminal verification retains original completion and admission debt");
					}
					check(job->complete(std::move(outcome)), "terminal verification reaps its authentic accepted completion");
					kronuz::journal::drive_synchronously(*job);
					for (unsigned wait = 0; wait < 10000 && !worker.drained(); ++wait) { worker.run_one({0, 100}); }
					check(worker.drained() && !worker.fenced() && !worker.snapshot_activation() && worker.take_actions().empty(), "terminal verification drains without install or synthesized acknowledgement");
					held = true;
				}
			}
		}
		check(held, "terminal verification reaches the requested original operation");
	}
}
void completion_snapshot_readback() {
	for (int cancel_kind : {-1, 0, 1}) {
		SnapshotFixture fixture("completion-read-" + std::to_string(cancel_kind), SnapshotLimits{7, 100000}, SnapshotFixture::quota(), true, true);
		auto& worker = fixture.worker; fixture.io.artifact_read_chunk = 1023;
		std::string payload(65553, 'r'); auto id = worker.reserve_snapshot(fixture.context(payload)); check(bool(id), "completion snapshot reserves incoming state"); if (!id) { continue; } fixture.send(*id, payload);
		std::string candidate; bool canceled = false; Token eof = 0;
		for (unsigned turn = 0; turn < 10000 && !canceled && !eof && !worker.fenced(); ++turn) {
			worker.run_one({0, 100});
			if (auto job = worker.take_io_operation()) {
				bool target = cancel_kind == 0 ? bool(std::dynamic_pointer_cast<kronuz::journal::ArtifactOpen>(job)) : cancel_kind == 1 && bool(std::dynamic_pointer_cast<kronuz::journal::ArtifactRead>(job));
				if (target) {
					job->submitted(); auto held = kronuz::journal::detail::execute_primitive(job->io(), job->request()); auto outstanding = worker.accounting()->outstanding;
					check(worker.cancel_snapshot(*id) == CancelResult::Pending && worker.cancel_snapshot(*id) == CancelResult::Pending, "accepted verification IO latches one cancellation reason");
					check(!worker.pending_snapshot_result() && !worker.validation_chunk() && !worker.snapshot_validated(*id) && worker.request_install(*id) == SubmitResult::Busy && worker.accounting()->outstanding == outstanding, "canceling verification exposes no terminal result, validation or admission refund");
					check(worker.run_one({1000, 100}) == TurnResult::IOPending && !worker.fenced(), "incoming cancellation waits for original reaping without blocking clock progress");
					check(job->complete(std::move(held)), "original verification completion survives cancellation"); kronuz::journal::drive_synchronously(*job); worker.run_one({1000, 100});
					auto result = worker.take_snapshot_result(); check(result && result->reason == SnapshotReason::Canceled && !worker.take_snapshot_result() && worker.cancel_snapshot(*id) == CancelResult::Stale, "verification settlement emits one cancellation result"); canceled = true;
				} else { kronuz::journal::drive_synchronously(*job); }
			}
			fixture.drain();
			if (auto view = worker.validation_chunk()) { if (view->verified_eof) { eof = view->chunk; } else { candidate.append(view->bytes.data(), view->bytes.size()); } check(worker.consume_validation(*id, view->chunk) == ValidationAck::Accepted, "completion readback consumes exact owned validation views"); }
		}
		check(!worker.fenced() && (cancel_kind < 0 ? eof && candidate == payload : canceled), "partial completion reads verify exact payload or dispose cancellation safely");
		if (eof) { check(worker.validation_succeeded(*id, eof) == ValidationAck::Accepted && worker.snapshot_validated(*id), "completion readback reaches semantically validated EOF"); worker.cancel_snapshot(*id); worker.take_snapshot_result(); }
	}
}
void snapshot_reception_and_validation() {
	for (std::size_t length : {std::size_t{0}, std::size_t{9}, std::size_t{65553}}) {
		SnapshotFixture fixture("validation-" + std::to_string(length)); auto& worker = fixture.worker; fixture.io.artifact_read_chunk = 3;
		std::string payload(length, 'x'); auto before = worker.storage_frontier(); auto id = worker.reserve_snapshot(fixture.context(payload));
		check(id.has_value() && !worker.reserve_checkpoint(16) && !worker.reserve_snapshot(fixture.context(payload)), "local and incoming capture share one reserved replacement slot");
		if (!id) { continue; } fixture.send(*id, payload);
		check(throws([&] { worker.offer_snapshot_chunk(*id, payload.size(), "", true); }), "input closes immediately after the accepted final marker");
		std::string candidate; Token previous = 0, eof = 0; bool held = false;
		for (unsigned turn = 0; turn < 150000 && !eof; ++turn) {
			fixture.pump(); auto view = worker.validation_chunk(); if (!view) { continue; }
			check(view->chunk > previous && view->offset == candidate.size(), "validation views have exact offsets and fresh monotonic tokens");
			check(worker.validation_succeeded(*id, view->chunk) == ValidationAck::NotReady, "data or unconsumed EOF cannot declare semantic success");
			if (!view->verified_eof) {
				check(!view->bytes.empty() && view->bytes.size() <= 65536, "candidate data is bounded and distinct from verified EOF");
				if (!held) {
					auto token = view->chunk; auto data = std::string(view->bytes.data(), view->bytes.size()); auto reads = fixture.io.payload_reads; auto scans = fixture.io.scans; unsigned admitted = 0;
					for (unsigned wait = 0; wait < 12; ++wait) { admitted += worker.try_submit(Read{100 + wait}) == SubmitResult::Accepted; fixture.pump(); }
					check(admitted >= 3 && fixture.io.scans > scans, "held data view permits ordinary admission and bounded reclamation");
					auto same = worker.validation_chunk();
					check(same && same->chunk == token && std::string(same->bytes.data(), same->bytes.size()) == data && fixture.io.payload_reads == reads, "held validation view stays stable and does not occupy ordinary consensus output"); held = true;
				}
				candidate.append(view->bytes.data(), view->bytes.size());
			} else { check(view->bytes.empty() && candidate == payload, "verified EOF follows complete matching candidate data"); eof = view->chunk; }
			previous = view->chunk;
			check(worker.consume_validation(*id, view->chunk + 1000000) == ValidationAck::Stale && worker.consume_validation(*id, view->chunk) == ValidationAck::Accepted && worker.consume_validation(*id, view->chunk) == ValidationAck::Stale, "only an exact unconsumed validation token advances the mailbox");
		}
		check(eof && worker.validation_succeeded(*id, eof + 1) == ValidationAck::Stale && worker.validation_succeeded(*id, eof) == ValidationAck::Accepted && worker.validation_succeeded(*id, eof) == ValidationAck::Stale, "semantic success requires exactly the consumed verified EOF");
		unsigned validated_admission = 0; auto validated_scans = fixture.io.scans;
		for (unsigned turn = 0; turn < 20; ++turn) { validated_admission += worker.try_submit(Read{200 + turn}) == SubmitResult::Accepted; fixture.pump(); }
		check(validated_admission >= 3 && fixture.io.scans > validated_scans, "validated unpublished candidate permits ordinary traffic and reclamation");
		auto after = worker.storage_frontier();
		check(worker.snapshot_validated(*id) && !worker.take_snapshot_result() && !worker.fenced() && !worker.busy() && worker.term() == 0 && worker.base_index() == 0 && after.generation == before.generation && after.sequence == before.sequence && !after.checkpoint && !fixture.unexpected_install, "validated candidate changes no incoming Core state, generation, activation or ordinary admission");
		check(worker.cancel_snapshot(*id) == CancelResult::Canceled && !worker.reserve_checkpoint(16) && !worker.reserve_snapshot(fixture.context(payload)), "cancelable validated candidate retains terminal-result backpressure on both replacement APIs");
		unsigned accepted = 0;
		for (unsigned turn = 0; turn < 20; ++turn) { accepted += worker.try_submit(Read{300 + turn}) == SubmitResult::Accepted; fixture.pump(); }
		check(accepted >= 3 && !worker.reserve_checkpoint(16) && !worker.reserve_snapshot(fixture.context(payload)), "held snapshot result blocks only replacement work while ordinary consensus continues");
		auto result = worker.take_snapshot_result(); check(result && result->reason == SnapshotReason::Canceled && result->context.transfer == 17 && !worker.take_snapshot_result(), "snapshot cancellation result is correlated and delivered exactly once");
		check(worker.consume_validation(*id, eof) == ValidationAck::Stale && worker.validation_succeeded(*id, eof) == ValidationAck::Stale && worker.cancel_snapshot(*id) == CancelResult::Stale, "completed snapshot tokens cannot mutate another operation");
		auto next = worker.reserve_snapshot(fixture.context("next")); check(next.has_value(), "drained result permits a new replacement reservation");
		check(worker.consume_validation(*id, eof) == ValidationAck::Stale && worker.validation_succeeded(*id, eof) == ValidationAck::Stale && !worker.snapshot_validated(*next), "old-operation callbacks leave the new incoming candidate untouched");
		worker.cancel_snapshot(*next); worker.take_snapshot_result();
	}
}
void snapshot_policy_and_rejection() {
	SnapshotFixture fixture("policy"); auto& worker = fixture.worker; auto context = fixture.context("abc"); auto used = worker.accounting()->used;
	auto local = worker.reserve_checkpoint(16); check(local && !worker.reserve_snapshot(context), "reserved local checkpoint excludes inbound replacement"); if (local) { worker.cancel_checkpoint(*local); }
	for (unsigned fault = 0; fault < 9; ++fault) {
		auto invalid = context;
		switch (fault) { case 0: invalid.request = 0; break; case 1: invalid.transfer = 0; break; case 2: invalid.authenticated_peer = 1; break; case 3: invalid.authenticated_peer = 4; break; case 4: invalid.leader_term = 0; break; case 5: invalid.descriptor.configuration[0] ^= 1; break; case 6: invalid.descriptor.application_format = 8; break; case 7: invalid.descriptor.application_bytes = 100001; break; case 8: invalid.descriptor.term = 2; break; }
		check(throws([&] { worker.reserve_snapshot(invalid); }) && !worker.fenced() && worker.accounting()->used == used && !worker.take_snapshot_result(), "malformed snapshot policy/context reserves nothing and queues no result");
	}
	auto id = worker.reserve_snapshot(context); if (!id) { check(false, "policy fixture reservation"); return; }
	check(worker.snapshot_context(*id) && worker.snapshot_context(*id)->descriptor == context.descriptor, "matching incoming capability exposes its immutable validation boundary");
	check(throws([&] { worker.offer_snapshot_chunk(*id, 1, "a"); }) && throws([&] { worker.offer_snapshot_chunk(*id, 0, ""); }) && throws([&] { worker.offer_snapshot_chunk(*id, 0, "a", true); }) && throws([&] { worker.offer_snapshot_chunk(*id, 0, "abcd", true); }) && throws([&] { worker.offer_snapshot_chunk(*id, 0, std::string(65537, 'x')); }) && !worker.fenced(), "invalid offsets, zero chunks, early final and excess sizes leave reception unchanged");
	auto first = worker.offer_snapshot_chunk(*id, 0, "a"); auto busy = worker.offer_snapshot_chunk(*id, 1, "b");
	check(first.result == SubmitResult::Accepted && first.next_offset == 1 && busy.result == SubmitResult::Busy && busy.next_offset == 1 && throws([&] { worker.offer_snapshot_chunk(*id, 0, "a"); }), "busy input accepts no bytes and duplicate offsets reject");
	for (unsigned turn = 0; turn < 100 && worker.offer_snapshot_chunk(*id, 1, "bc").result == SubmitResult::Busy; ++turn) { fixture.pump(); }
	SnapshotOffer final{SubmitResult::Busy, 3};
	for (unsigned turn = 0; turn < 100 && final.result == SubmitResult::Busy; ++turn) { fixture.pump(); final = worker.offer_snapshot_chunk(*id, 3, "", true); }
	check(final.result == SubmitResult::Accepted, "empty final marker closes a fully offered nonempty image");
	SnapshotFixture other("foreign"); auto other_id = other.worker.reserve_snapshot(other.context("abc"));
	check(other_id && worker.consume_validation(*other_id, 1) == ValidationAck::Stale && worker.validation_succeeded(*other_id, 1) == ValidationAck::Stale && worker.cancel_snapshot(*other_id) == CancelResult::Stale && throws([&] { worker.offer_snapshot_chunk(*other_id, 0, "a"); }), "foreign session callbacks cannot change an incoming candidate");
	check(other_id && !worker.snapshot_context(*other_id), "foreign snapshot context capability exposes no metadata");
	check(worker.reject_snapshot_validation(*id) == CancelResult::Canceled, "semantic rejection cancels healthy staged storage");
	check(!worker.snapshot_context(*id), "canceled snapshot context capability exposes no metadata");
	auto rejected = worker.take_snapshot_result(); check(rejected && rejected->reason == SnapshotReason::InvalidApplication && !worker.fenced(), "semantic rejection is a correlated healthy result");
	context.descriptor.application_crc32c ^= 1; id = worker.reserve_snapshot(context); fixture.send(*id, "abc");
	for (unsigned turn = 0; turn < 100 && worker.accounting()->tickets; ++turn) { fixture.pump(); }
	rejected = worker.take_snapshot_result(); check(rejected && rejected->reason == SnapshotReason::InvalidImage && !worker.fenced() && !worker.validation_chunk() && fixture.io.payload_reads == 0, "remote checksum mismatch rejects before readback without fencing storage");
	SnapshotFixture disabled("disabled", std::nullopt); check(throws([&] { disabled.worker.reserve_snapshot(context); }) && !disabled.worker.fenced(), "existing callers keep reception disabled until configured");
	auto capacity = SnapshotFixture::quota(); capacity.replacement_pool = {}; SnapshotFixture pressure("pressure", SnapshotLimits{7, 100000}, capacity);
	check(!pressure.worker.reserve_snapshot(pressure.context("abc")) && !pressure.worker.take_snapshot_result() && !pressure.worker.fenced(), "replacement pressure creates no incoming operation or terminal result");
}
}
void snapshot_cancellation_phases() {
	for (unsigned phase = 0; phase < 8; ++phase) {
		SnapshotFixture fixture("cancel-" + std::to_string(phase)); auto& worker = fixture.worker;
		auto baseline = worker.accounting()->used; std::string payload(17, 'c'); auto id = worker.reserve_snapshot(fixture.context(payload));
		if (!id) { check(false, "cancellation fixture reserves replacement"); continue; }
		Token eof = 0; auto barriers = fixture.io.directory_syncs;
		if (phase == 1) { for (unsigned turn = 0; turn < 20 && !fixture.io.artifacts; ++turn) { fixture.pump(); } }
		if (phase == 2) {
			worker.offer_snapshot_chunk(*id, 0, "part");
			for (unsigned turn = 0; turn < 10; ++turn) { fixture.pump(); }
		}
		if (phase >= 3) {
			fixture.send(*id, payload);
			for (unsigned turn = 0; turn < 200; ++turn) {
				fixture.pump(); auto view = worker.validation_chunk();
				if (phase == 3 && fixture.io.directory_syncs >= barriers + 1 && !fixture.io.artifact_opens) { break; }
				if (!view) { continue; }
				if (phase == 4 && !view->verified_eof) { break; }
				if (view->verified_eof) {
					eof = view->chunk;
					if (phase >= 6) { worker.consume_validation(*id, eof); }
					if (phase == 7) { worker.validation_succeeded(*id, eof); }
					break;
				}
				worker.consume_validation(*id, view->chunk);
			}
		}
		check(phase == 0 || (phase == 1 ? fixture.io.artifacts > 0 : phase == 2 ? fixture.staged_bytes() == kronuz::journal::detail::artifact_header_size + 4 : phase == 3 ? fixture.io.directory_syncs >= barriers + 1 && !fixture.io.artifact_opens : phase == 4 ? worker.validation_chunk() && !worker.validation_chunk()->verified_eof : phase == 5 ? worker.validation_chunk() && worker.validation_chunk()->verified_eof : phase == 6 ? eof && !worker.validation_chunk() : worker.snapshot_validated(*id)), "cancellation reaches its explicit targeted phase witness");
		check(worker.cancel_snapshot(*id) == CancelResult::Canceled && !worker.validation_chunk() && !worker.fenced(), "every unpublished receive/validation phase cancels without a storage fence");
		check(worker.consume_validation(*id, eof) == ValidationAck::Stale && worker.validation_succeeded(*id, eof) == ValidationAck::Stale, "cancellation invalidates outstanding data and semantic callbacks");
		check(!worker.reserve_checkpoint(8), "held cancellation result retains replacement exclusion");
		worker.take_snapshot_result();
		for (unsigned turn = 0; turn < 300 && worker.accounting()->used != baseline; ++turn) { fixture.pump(); }
		check(worker.accounting()->used == baseline, "cancel closes validation pins and reclaims exact staged byte/name charges");
		auto local = worker.reserve_checkpoint(8); check(bool(local), "incoming cancellation releases the shared local checkpoint slot");
		if (local) { check(worker.cancel_checkpoint(*local) == CancelResult::Canceled, "local checkpoint cancellation remains valid after every incoming phase"); }
	}
}
void snapshot_read_faults() {
	for (unsigned fault = 0; fault < 5; ++fault) {
		SnapshotFixture fixture("read-fault-" + std::to_string(fault)); auto& worker = fixture.worker;
		auto id = worker.reserve_snapshot(fixture.context("payload")); if (!id) { check(false, "fault fixture reserves replacement"); continue; }
		auto barriers = fixture.io.directory_syncs;
		fixture.send(*id, "payload");
		for (unsigned turn = 0; turn < 100 && !fixture.io.artifact_opens; ++turn) {
			if (fault < 2 && fixture.io.directory_syncs >= barriers + 1) { break; }
			fixture.pump();
		}
		// Admission is idle here; retain its output before arming the fault.
		check(worker.try_submit(Read{71}) == SubmitResult::Accepted, "read fault starts with accepted ordinary output retained");
		if (fault == 4) { fixture.io.corrupt_payload_read = true; }
		else { fixture.io.fail_read = true; fixture.io.read_failure_after = fault % 2; }
		for (unsigned turn = 0; turn < 100 && !worker.fenced(); ++turn) {
			worker.run_one({0, 100});
			if (auto view = worker.validation_chunk()) {
				check(!view->verified_eof, "corrupt readback never exposes verified EOF"); worker.consume_validation(*id, view->chunk);
			}
		}
		check(worker.fenced() && !worker.validation_chunk() && !worker.take_snapshot_result() && worker.consume_validation(*id, 1) == ValidationAck::Fenced, "metadata/payload failures and final checksum corruption fence all receiver capabilities");
		bool notice = false; for (const auto& action : worker.take_actions()) { notice |= std::holds_alternative<Fenced>(action); }
		worker.run_one({0, 100}); for (const auto& action : worker.take_actions()) { notice |= std::holds_alternative<Fenced>(action); }
		check(notice && !fixture.unexpected_install, "storage fault retains a global fence notice without incoming publication");
	}
}

void snapshot_restart_cleanup() {
	for (bool sealed : {false, true}) {
		SnapshotFixture::Directory directory("restart-" + std::to_string(sealed));
		kronuz::journal::StorageResources baseline{};
		{
			FaultIO io(directory.path); Worker worker(io, SnapshotFixture::fixed(), SnapshotFixture::quota(), limits(), {}, {4, 1}, SnapshotLimits{7, 100000});
			kronuz::journal::Identity identity{}; identity[0] = 'S'; worker.create(identity);
			while (!worker.ready()) { worker.run_one({0, 100}); }
			baseline = worker.accounting()->used;
			auto fixed = SnapshotFixture::fixed(); std::string payload(17, 'r');
			SnapshotContext context{2, 1, 13, 17, {fixed.cluster, fixed.configuration, 7, 1, 1, payload.size(), kronuz::journal::crc32c(payload)}};
			auto id = worker.reserve_snapshot(context); if (!id) { check(false, "restart fixture reserves replacement"); continue; }
			worker.offer_snapshot_chunk(*id, 0, sealed ? payload : std::string("partial"), sealed);
			for (unsigned turn = 0; turn < 50; ++turn) { worker.run_one({0, 100}); worker.take_actions(); }
			std::uint64_t staged = 0; for (const auto& entry : std::filesystem::directory_iterator(directory.path)) { if (entry.path().filename().string().starts_with("artifact-")) { staged += entry.file_size(); } }
			check(staged == kronuz::journal::detail::artifact_header_size + (sealed ? payload.size() : 7) && (!sealed || worker.validation_chunk()), "restart fixture leaves actual unpublished artifact bytes and optionally a held verifier view");
		}
		{
			FaultIO io(directory.path); Worker worker(io, SnapshotFixture::fixed(), SnapshotFixture::quota(), limits(), {}, {4, 1}, SnapshotLimits{7, 100000});
			bool restored = false; worker.recover([&](auto&) { restored = true; });
			for (unsigned turn = 0; turn < 300; ++turn) { worker.run_one({0, 100}); worker.take_actions(); }
			check(worker.ready() && !worker.fenced() && !restored && worker.base_index() == 0 && worker.committed() == 0 && worker.storage_frontier().sequence == 1 && !worker.storage_frontier().checkpoint, "reopen preserves the old durable frontier without activating an abandoned candidate");
			check(worker.accounting()->used == baseline && !worker.validation_chunk() && !worker.take_snapshot_result(), "restart inventory and bounded reclamation remove abandoned receive artifacts with exact accounting");
		}
	}
}

void snapshot_protocol_progress() {
	SnapshotFixture fixture("protocol-progress"); auto& worker = fixture.worker; fixture.io.artifact_read_chunk = 1024;
	std::map<NodeId, Receive> responses; unsigned sends = 0;
	auto drain = [&] {
		for (const auto& action : worker.take_actions()) {
			if (auto range = std::get_if<Committed>(&action)) { worker.applied({range->entries.back().index}); }
			if (auto send = std::get_if<Send>(&action)) {
				if (auto vote = std::get_if<VoteRequest>(&send->message)) { responses.insert_or_assign(send->peer, Receive{send->peer, VoteResponse{vote->term, true}}); }
				if (auto append = std::get_if<AppendRequest>(&send->message)) {
					++sends; auto matched = append->previous + append->entries.size();
					responses.insert_or_assign(send->peer, Receive{send->peer, AppendResponse{append->term, append->rpc, true, matched, matched + 1, append->read_probe}});
				}
			}
		}
	};
	auto respond = [&] { if (!responses.empty()) { auto next = responses.begin(); if (worker.try_submit(next->second) == SubmitResult::Accepted) { responses.erase(next); } } };
	for (unsigned turn = 0; turn < 200 && worker.applied_index() < 1; ++turn) { respond(); worker.run_one({100, 100}); drain(); }
	check(worker.role() == Role::Leader && worker.applied_index() == 1, "incoming progress fixture elects a durable three-voter leader");
	std::string payload(90000, 'p'); auto context = fixture.context(payload); context.leader_term = worker.term();
	auto id = worker.reserve_snapshot(context); if (!id) { check(false, "protocol progress reserves incoming image"); return; }
	auto scan = fixture.io.scans; sends = 0; std::uint64_t offset = 0; Token eof = 0;
	for (unsigned turn = 0; turn < 2000 && !eof; ++turn) {
		if (offset < payload.size()) {
			auto count = std::min<std::size_t>(1024, payload.size() - offset);
			auto result = worker.offer_snapshot_chunk(*id, offset, std::string_view(payload).substr(offset, count), offset + count == payload.size());
			if (result.result == SubmitResult::Accepted) { offset = result.next_offset; }
		}
		respond(); worker.run_one({200 + 20 * turn, 100}); drain();
		if (auto view = worker.validation_chunk(); view && turn % 2 == 0) {
			if (view->verified_eof) { eof = view->chunk; }
			worker.consume_validation(*id, view->chunk);
		}
	}
	check(offset == payload.size() && eof && sends >= 4 && fixture.io.scans >= scan + 4 && !worker.fenced(), "continuous receive and incremental validation preserve protocol timers and reclamation progress");
	check(eof && worker.validation_succeeded(*id, eof) == ValidationAck::Accepted && worker.role() == Role::Leader && worker.base_index() == 0, "semantic validation neither changes leader role nor installs the boundary");
	worker.cancel_snapshot(*id); worker.take_snapshot_result();
}

Token validate_incoming(SnapshotFixture& fixture, const SnapshotId& id, std::string_view payload) {
	fixture.send(id, payload); Token eof = 0;
	for (unsigned turn = 0; turn < 200 && !eof; ++turn) {
		fixture.pump(); if (auto view = fixture.worker.validation_chunk()) {
			if (view->verified_eof) { eof = view->chunk; }
			fixture.worker.consume_validation(id, view->chunk);
		}
	}
	check(eof && fixture.worker.validation_succeeded(id, eof) == ValidationAck::Accepted, "installation fixture reaches consumed integrity EOF and semantic validation"); return eof;
}
void terminal_published_activation() {
	SnapshotFixture fixture("terminal-published-activation", SnapshotLimits{7, 100000}, SnapshotFixture::quota(), true, true);
	auto& worker = fixture.worker;
	auto id = worker.reserve_snapshot(fixture.context("image"));
	check(bool(id), "terminal activation reserves incoming image");
	if (!id) { return; }
	validate_incoming(fixture, *id, "image");
	check(worker.request_install(*id) == SubmitResult::Accepted, "terminal activation admits validated image");
	for (unsigned turn = 0; turn < 1000 && !worker.snapshot_activation(); ++turn) { fixture.pump(); }
	auto activation = worker.snapshot_activation();
	check(bool(activation) && bool(worker.storage_frontier().checkpoint), "terminal activation waits after actual manifest publication");
	if (!activation) { return; }
	worker.close();
	check(worker.snapshot_activated(*id, activation->token) == ValidationAck::NotReady &&
		worker.snapshot_activation_failed(*id, activation->token) == ValidationAck::NotReady,
		"terminal activation ignores delayed success and failure consumers");
	for (unsigned turn = 0; turn < 10000 && !worker.drained(); ++turn) { worker.run_one({0, 100}); }
	check(worker.drained() && !worker.fenced() && !worker.accounting()->tainted && worker.take_actions().empty(),
		"published terminal image drains without fabricated activation or storage taint");
}
void snapshot_installation() {
	for (unsigned callback_case = 0; callback_case < 3; ++callback_case) {
		bool failure = callback_case != 0;
		SnapshotFixture fixture("install-" + std::to_string(callback_case)); auto& worker = fixture.worker;
		auto id = worker.reserve_snapshot(fixture.context("image")); if (!id) { check(false, "installation reserves incoming image"); continue; }
		validate_incoming(fixture, *id, "image"); auto before = worker.storage_frontier();
		check(worker.snapshot_activated(*id, 1) == ValidationAck::NotReady && worker.request_install(*id) == SubmitResult::Accepted && worker.request_install(*id) == SubmitResult::Accepted, "installation admission is explicit and matching retries own no extra permit");
		std::optional<SnapshotActivation> activation;
		for (unsigned turn = 0; turn < 200 && !activation && !worker.fenced(); ++turn) { worker.run_one({0, 100}); activation = worker.snapshot_activation(); }
		check(activation && worker.busy() && worker.base_index() == 1 && worker.committed() == 1 && worker.applied_index() == 0 && worker.storage_frontier().generation > before.generation && worker.storage_frontier().sequence == before.sequence && !worker.take_snapshot_result(), "publication retains busy state and exact receiver sequence until application activation");
		if (!activation) { continue; }
		check(worker.cancel_snapshot(*id) == CancelResult::TooLate && worker.snapshot_activated(*id, activation->token + 1) == ValidationAck::Stale, "published installation rejects cancellation and wrong activation tokens");
		if (!failure) {
			SnapshotFixture foreign("install-foreign"); auto foreign_id = foreign.worker.reserve_snapshot(foreign.context("image"));
			check(foreign_id && worker.snapshot_activated(*foreign_id, activation->token) == ValidationAck::Stale && worker.snapshot_activation_failed(*foreign_id, activation->token) == ValidationAck::Stale && worker.snapshot_activation() && !worker.fenced(), "foreign-session activation callbacks cannot consume or fence the current request");
		}
		if (failure) {
			if (callback_case == 2) { fixture.application_state = "image"; check(worker.snapshot_activated(*id, activation->token) == ValidationAck::Accepted, "failure fixture holds a queued activation success under output pressure"); }
			check(worker.snapshot_activation_failed(*id, activation->token) == ValidationAck::Accepted && worker.fenced() && !worker.take_snapshot_result(), "matching activation failure fences immediately under held ordinary output"); continue;
		}
		fixture.application_state = "image"; // Atomic application switch precedes its reliable acknowledgment.
		check(worker.snapshot_activated(*id, activation->token) == ValidationAck::Accepted && worker.snapshot_activated(*id, activation->token) == ValidationAck::Stale && !worker.snapshot_activation(), "activation completion has a reliable independent slot and rejects duplicates");
		auto scans = fixture.io.scans;
		for (unsigned turn = 0; turn < 10; ++turn) { worker.run_one({10000, 100}); }
		check(worker.busy() && !worker.take_snapshot_result() && fixture.io.scans > scans, "held ordinary output retains activation completion while reclamation continues");
		fixture.drain(); worker.run_one({10000, 100}); fixture.drain(); auto result = worker.take_snapshot_result();
		check(result && result->reason == SnapshotReason::Installed && result->receiver_term == 1 && !result->rejection && !worker.busy() && worker.applied_index() == 1 && !worker.fenced() && worker.role() == Role::Follower, "matching activation alone releases cutover and reports durable correlated success with a fresh election deadline");
		unsigned ticks = 0; for (unsigned turn = 0; turn < 20; ++turn) { ticks += worker.try_submit(Tick{10000, 100}) == SubmitResult::Accepted; worker.run_one({10000, 100}); fixture.drain(); }
		check(ticks && worker.term() == 1 && worker.role() == Role::Follower, "delayed activation refreshes the deadline before a same-time admitted Tick");
		Receive append{2, AppendRequest{1, 19, 1, 1, 2, 0, {{2, 1, EntryKind::Command, "next"}}}};
		for (unsigned turn = 0; turn < 100 && worker.try_submit(append) != SubmitResult::Accepted; ++turn) { worker.run_one({10000, 100}); fixture.drain(); }
		for (unsigned turn = 0; turn < 100 && worker.applied_index() < 2; ++turn) { worker.run_one({10000, 100}); fixture.drain(); }
		check(worker.applied_index() == 2 && worker.committed() == 2 && fixture.application_state == "image/next" && !worker.fenced(), "live installation resumes durable replication and application delivery");
		auto local = worker.reserve_checkpoint(8); check(bool(local), "drained installation result permits another local checkpoint"); if (local) { worker.cancel_checkpoint(*local); }
	}
	SnapshotFixture fixture("install-cancel"); auto& worker = fixture.worker;
	auto id = worker.reserve_snapshot(fixture.context("image")); if (!id) { return; } validate_incoming(fixture, *id, "image");
	auto outstanding = worker.accounting()->outstanding;
	check(worker.request_install(*id) == SubmitResult::Accepted && worker.accounting()->outstanding.logical_bytes > outstanding.logical_bytes && worker.cancel_snapshot(*id) == CancelResult::Canceled && !worker.busy(), "pre-Core cancellation refunds the dedicated control permit without mutating consensus");
	worker.take_snapshot_result(); unsigned accepted = 0; for (unsigned turn = 0; turn < 10; ++turn) { accepted += worker.try_submit(Read{300 + turn}) == SubmitResult::Accepted; fixture.pump(); }
	check(accepted >= 3 && worker.accounting()->outstanding.logical_bytes == 0, "canceling accepted install clears debt and all reservation ownership");
}

void snapshot_rejection_branches() {
	for (unsigned branch = 0; branch < 3; ++branch) {
		SnapshotFixture fixture("install-reject-" + std::to_string(branch)); auto& worker = fixture.worker;
		if (branch) {
			Receive prefix{2, AppendRequest{1, 30, 0, 0, 1, 0, {{1, 1, EntryKind::NoOp, ""}}}};
			for (unsigned turn = 0; turn < 100 && worker.try_submit(prefix) != SubmitResult::Accepted; ++turn) { fixture.pump(); }
			for (unsigned turn = 0; turn < 100 && worker.applied_index() != 1; ++turn) { fixture.pump(); }
		}
		auto context = fixture.context("image"); if (branch == 2) { context.leader_term = 2; }
		auto id = worker.reserve_snapshot(context); if (!id) { check(false, "rejection fixture reserves candidate"); continue; } validate_incoming(fixture, *id, "image");
		if (!branch) {
			Receive newer{2, AppendRequest{2, 31, 0, 0, 0, 0, {}}};
			for (unsigned turn = 0; turn < 100 && worker.try_submit(newer) != SubmitResult::Accepted; ++turn) { fixture.pump(); }
			for (unsigned turn = 0; turn < 100 && (worker.busy() || worker.term() != 2); ++turn) { fixture.pump(); } fixture.drain();
		}
		auto before = worker.storage_frontier(); check(worker.request_install(*id) == SubmitResult::Accepted, "validated candidate admits each Core rejection branch");
		bool saw_pending = false; std::optional<SnapshotResult> result;
		for (unsigned turn = 0; turn < 100 && !result; ++turn) {
			worker.run_one({0, 100});
			if (branch == 2 && worker.busy() && worker.term() == 2 && worker.storage_frontier().sequence == before.sequence) {
				saw_pending = true; check(worker.cancel_snapshot(*id) == CancelResult::TooLate && !worker.take_snapshot_result(), "higher-term caught-up rejection waits for owned ordinary persistence");
			}
			fixture.drain(); result = worker.take_snapshot_result();
		}
		check(result && result->reason == SnapshotReason::Rejected && result->rejection == (branch ? InstallRejectReason::CaughtUp : InstallRejectReason::StaleTerm) && result->receiver_term == (branch == 1 ? 1 : 2) && !worker.busy() && !worker.fenced() && !worker.snapshot_activation(), "Core rejection is correlated only after durable receiver state and never activates the image");
		check(worker.storage_frontier().generation == before.generation && !worker.storage_frontier().checkpoint && (branch != 2 || (saw_pending && worker.storage_frontier().sequence == before.sequence + 1)), "rejection publishes no replacement and higher-term caught-up persists one local hard-state batch");
	}
}

void snapshot_old_application() {
	for (unsigned completion = 0; completion < 3; ++completion) {
		SnapshotFixture fixture("install-old-application-" + std::to_string(completion)); auto& worker = fixture.worker;
		Receive prefix{2, AppendRequest{1, 33, 0, 0, 1, 0, {{1, 1, EntryKind::Command, "old"}, {2, 1, EntryKind::Command, "captured"}}}};
		for (unsigned turn = 0; turn < 100 && worker.try_submit(prefix) != SubmitResult::Accepted; ++turn) { fixture.pump(); }
		Index old = 0;
		for (unsigned turn = 0; turn < 100 && !old; ++turn) {
			worker.run_one({0, 100}); for (const auto& action : worker.take_actions()) { if (auto range = std::get_if<Committed>(&action)) { old = range->entries.back().index; } }
		}
		check(old == 1 && worker.applied_index() == 0, "installation holds a real previously delivered application range");
		auto context = fixture.context("image"); context.descriptor.through = 2; auto id = worker.reserve_snapshot(context); if (!id) { continue; }
		validate_incoming(fixture, *id, "image"); check(worker.request_install(*id) == SubmitResult::Accepted, "installation accepts while old application work remains delivered");
		bool applied_while_persisting = false;
		for (unsigned turn = 0; turn < 200 && !worker.storage_frontier().checkpoint; ++turn) {
			fixture.pump(); if (completion == 0 && !applied_while_persisting && worker.busy() && !worker.storage_frontier().checkpoint) { worker.applied({old}); applied_while_persisting = true; }
		}
		check(completion != 0 || applied_while_persisting, "pre-publication application completion occurs while installation owns Core persistence");
		for (unsigned turn = 0; turn < 10; ++turn) { fixture.pump(); }
		if (completion) { check(worker.busy() && !worker.snapshot_activation() && !worker.take_snapshot_result(), "published incoming boundary cannot activate before old application work drains"); }
		if (completion == 2) { worker.application_failed(old, "old application failed"); check(worker.fenced(), "old application failure fences after incoming publication"); continue; }
		if (completion == 1) { worker.applied({old}); }
		auto activation = worker.snapshot_activation(); check(bool(activation), "old application completion releases the dedicated activation request");
		if (activation) { fixture.application_state = "image"; worker.snapshot_activated(*id, activation->token); fixture.pump(); }
		auto result = worker.take_snapshot_result(); check(result && result->reason == SnapshotReason::Installed && worker.applied_index() == 2 && fixture.application_state == "image" && !worker.fenced(), "application completion before or after publication installs the same immutable image");
	}
}

void snapshot_install_reopening() {
	for (bool matching : {false, true}) { for (unsigned close_phase = 0; close_phase < 3; ++close_phase) {
		SnapshotFixture::Directory directory("install-reopen-" + std::to_string(matching) + "-" + std::to_string(close_phase));
		std::uint64_t sequence = 0; std::string live_application = "old"; Term incoming_term = matching && close_phase == 0 ? 1 : 2;
		{
			FaultIO io(directory.path); Worker worker(io, SnapshotFixture::fixed(), SnapshotFixture::quota(), limits(), {}, {4, 1}, SnapshotLimits{7, 100000});
			kronuz::journal::Identity identity{}; identity[0] = 'S'; worker.create(identity); while (!worker.ready()) { worker.run_one({0, 100}); }
			auto drain = [&] { for (const auto& action : worker.take_actions()) { if (auto range = std::get_if<Committed>(&action)) { worker.applied({range->entries.back().index}); } } };
			auto pump = [&] { worker.run_one({0, 100}); drain(); }; worker.try_submit(Start{}); drain();
			Receive vote{2, VoteRequest{1, 0, 0}};
			for (unsigned turn = 0; turn < 100 && worker.try_submit(vote) != SubmitResult::Accepted; ++turn) { pump(); }
			for (unsigned turn = 0; turn < 100 && worker.busy(); ++turn) { pump(); }
			Receive prefix{2, AppendRequest{1, 35, 0, 0, 1, 0, {{1, 1, EntryKind::NoOp, ""}, {2, 1, EntryKind::Command, "captured"}, {3, 1, EntryKind::Command, "retained"}}}};
			for (unsigned turn = 0; turn < 100 && worker.try_submit(prefix) != SubmitResult::Accepted; ++turn) { pump(); }
			for (unsigned turn = 0; turn < 100 && worker.applied_index() != 1; ++turn) { pump(); }
			auto fixed = SnapshotFixture::fixed(); SnapshotContext context{2, incoming_term, 13, 17, {fixed.cluster, fixed.configuration, 7, 2, matching ? Term{1} : Term{2}, 5, kronuz::journal::crc32c("image")}};
			auto id = worker.reserve_snapshot(context); if (!id) { check(false, "reopen fixture reserves incoming image"); continue; }
			worker.offer_snapshot_chunk(*id, 0, "image", true); Token eof = 0; std::string candidate;
			for (unsigned turn = 0; turn < 200 && !eof; ++turn) { pump(); if (auto view = worker.validation_chunk()) { if (view->verified_eof) { eof = view->chunk; } else { candidate.append(view->bytes.data(), view->bytes.size()); } worker.consume_validation(*id, view->chunk); } }
			check(candidate == "image", "reopen fixture builds its unpublished application candidate from verified data");
			worker.validation_succeeded(*id, eof); sequence = worker.storage_frontier().sequence; worker.request_install(*id);
			for (unsigned turn = 0; turn < 200 && !worker.storage_frontier().checkpoint; ++turn) { pump(); }
			check(worker.storage_frontier().checkpoint && worker.storage_frontier().sequence == sequence && worker.base_index() == 0 && !worker.fenced(), "reopen fixture reaches Store publication before Core durable completion");
			if (close_phase) {
				for (unsigned turn = 0; turn < 20 && !worker.snapshot_activation(); ++turn) { pump(); }
				check(worker.snapshot_activation() && worker.base_index() == 2 && worker.applied_index() == 1 && !worker.take_snapshot_result(), "reopen fixture can close after Core publication and before application activation");
				if (close_phase == 2) { live_application = candidate; }
			}
			// Normal close deliberately loses transient activation/correlation state.
		}
		{
			FaultIO io(directory.path); kronuz::journal::Journal journal(io); bool decoded = false;
			auto frontier = journal.recover([](auto, auto) {}, [&](const auto& selected, auto& reader, auto dependencies) {
				std::string bytes(static_cast<std::size_t>(reader.descriptor().length), '\0'); reader.read_at(0, std::span<char>(bytes.data(), bytes.size()));
				auto bundle = decode_checkpoint(bytes, SnapshotFixture::fixed(), selected.base_sequence, selected.dependencies.at(0), limits()); decoded = true;
				check(dependencies.size() == 1 && bundle.storage_sequence == sequence && bundle.application_format == 7 && bundle.state.configuration == SnapshotFixture::fixed() && bundle.state.hard.term == incoming_term && bundle.state.hard.voted_for == (incoming_term == 1 ? std::optional<NodeId>{2} : std::nullopt), "persisted receiver bundle preserves fixed identity and same-term ballot or clears a higher-term ballot");
				check(bundle.state.entries.size() == (matching ? 1 : 0) && (!matching || bundle.state.entries[0].payload == "retained"), "persisted receiver bundle retains only the correctly matching local suffix");
			});
			check(decoded && frontier.identity[0] == 'S' && frontier.sequence == sequence, "receiver storage identity and exact covered sequence survive publication");
		}
		{
			FaultIO io(directory.path); Worker worker(io, SnapshotFixture::fixed(), SnapshotFixture::quota(), limits(), {}, {4, 1}, SnapshotLimits{7, 100000});
			std::string application;
			worker.recover([&](auto& reader) { application.resize(static_cast<std::size_t>(reader.descriptor().length)); reader.read_at(0, std::span<char>(application.data(), application.size())); });
			while (!worker.ready()) { worker.run_one({0, 100}); }
			check(application == "image" && live_application == (close_phase == 2 ? "image" : "old") && worker.term() == incoming_term && worker.base_index() == 2 && worker.committed() == 2 && worker.applied_index() == 2 && worker.storage_frontier().sequence == sequence && !worker.snapshot_activation() && !worker.take_snapshot_result(), "recovery selects the installed image and receiver-local boundary without transient transfer state");
			auto drain = [&] { for (const auto& action : worker.take_actions()) { if (auto range = std::get_if<Committed>(&action)) { for (const auto& entry : range->entries) { if (entry.kind == EntryKind::Command) { application += "/" + entry.payload; } } worker.applied({range->entries.back().index}); } } };
			auto pump = [&] { worker.run_one({0, 100}); drain(); }; worker.try_submit(Start{}); drain();
			for (unsigned turn = 0; turn < 10; ++turn) { pump(); }
			check(application == "image", "recovery does not replay the uncommitted retained suffix");
			Receive next{2, matching ? AppendRequest{2, 36, 3, 1, 3, 0, {}} : AppendRequest{2, 36, 2, 2, 3, 0, {{3, 2, EntryKind::Command, "retained"}}}};
			for (unsigned turn = 0; turn < 100 && worker.try_submit(next) != SubmitResult::Accepted; ++turn) { pump(); }
			for (unsigned turn = 0; turn < 100 && worker.applied_index() != 3; ++turn) { pump(); }
			check(application == "image/retained" && worker.applied_index() == 3 && worker.committed() == 3 && !worker.fenced(), "recovered matching/conflicting suffixes resume replication and reconstruct the same application contents");
		}
	} }
}

void snapshot_install_admission() {
	for (bool all_control_slots : {false, true}) {
		SnapshotFixture fixture("install-admission-" + std::to_string(all_control_slots)); auto& worker = fixture.worker;
		auto context = fixture.context("image"); context.descriptor.through = 2;
		auto id = worker.reserve_snapshot(context); if (!id) { continue; } validate_incoming(fixture, *id, "image");
		Event event = all_control_slots ? Event{Tick{100, 100}} : Event{Receive{2, AppendRequest{1, 39, 0, 0, 1, 0, {{1, 1, EntryKind::NoOp, ""}}}}};
		for (unsigned turn = 0; turn < 100 && worker.try_submit(event) != SubmitResult::Accepted; ++turn) { fixture.pump(); }
		check(worker.busy(), "installation admission fixture holds a real ordinary persistence continuation");
		auto reservations = worker.accounting()->outstanding; auto before = worker.storage_frontier(); auto result = worker.request_install(*id);
		if (all_control_slots) {
			check(result == SubmitResult::Pressure && worker.snapshot_validated(*id) && worker.accounting()->outstanding == reservations && worker.storage_frontier().sequence == before.sequence, "control pressure preserves ordinary pack, validated candidate and storage frontier");
			for (unsigned turn = 0; turn < 100 && worker.busy(); ++turn) { worker.run_one({100, 100}); fixture.drain(); }
			check(!worker.busy() && !worker.fenced() && worker.request_install(*id) == SubmitResult::Accepted && worker.cancel_snapshot(*id) == CancelResult::Canceled, "pressure creates no cutover debt and ordinary completion releases control capacity"); worker.take_snapshot_result();
		} else {
			check(result == SubmitResult::Accepted && worker.accounting()->outstanding.logical_bytes > reservations.logical_bytes, "incoming cutover permit coexists with an active ordinary pack");
			for (unsigned turn = 0; turn < 200 && !worker.snapshot_activation(); ++turn) { fixture.pump(); }
			auto activation = worker.snapshot_activation(); check(activation && worker.storage_frontier().sequence > before.sequence && !worker.fenced(), "ordinary pack finishes intact and incoming publication covers its later exact receiver sequence");
			if (activation) { fixture.application_state = "image"; worker.snapshot_activated(*id, activation->token); fixture.pump(); }
			check(worker.take_snapshot_result().has_value() && worker.applied_index() == 2, "admitted installation follows an existing ordinary persistence continuation");
		}
	}
}
void snapshot_publication_faults() {
	for (unsigned fault = 0; fault < 3; ++fault) {
		SnapshotFixture::Directory directory("install-publication-fault-" + std::to_string(fault));
		{
			FaultIO io(directory.path); Worker worker(io, SnapshotFixture::fixed(), SnapshotFixture::quota(), limits(), {}, {4, 1}, SnapshotLimits{7, 100000});
			kronuz::journal::Identity identity{}; identity[0] = 'S'; worker.create(identity); while (!worker.ready()) { worker.run_one({0, 100}); } worker.try_submit(Start{}); worker.take_actions();
			auto fixed = SnapshotFixture::fixed(); SnapshotContext context{2, 1, 13, 17, {fixed.cluster, fixed.configuration, 7, 1, 1, 5, kronuz::journal::crc32c("image")}};
			auto id = worker.reserve_snapshot(context); if (!id) { continue; } worker.offer_snapshot_chunk(*id, 0, "image", true); Token eof = 0;
			for (unsigned turn = 0; turn < 200 && !eof; ++turn) { worker.run_one({0, 100}); worker.take_actions(); if (auto view = worker.validation_chunk()) { if (view->verified_eof) { eof = view->chunk; } worker.consume_validation(*id, view->chunk); } }
			worker.validation_succeeded(*id, eof); io.syncs_until_failure = fault + 1; worker.request_install(*id);
			for (unsigned turn = 0; turn < 200 && !worker.fenced(); ++turn) { worker.run_one({0, 100}); worker.take_actions(); }
			check(worker.fenced() && !worker.snapshot_activation() && !worker.take_snapshot_result() && worker.accounting()->tainted, "bundle sealing and both publication barriers fence without activation or success on uncertainty");
		}
		{
			FaultIO io(directory.path); Worker worker(io, SnapshotFixture::fixed(), SnapshotFixture::quota(), limits()); std::string image;
			worker.recover([&](auto& reader) { image.resize(static_cast<std::size_t>(reader.descriptor().length)); reader.read_at(0, std::span<char>(image.data(), image.size())); });
			while (!worker.ready()) { worker.run_one({0, 100}); }
			check(!worker.fenced() && (fault == 2 ? image == "image" && worker.base_index() == 1 && worker.committed() == 1 : image.empty() && worker.base_index() == 0 && worker.committed() == 0), "reopen selects the exact old or renamed new frontier after each publication failure boundary");
		}
	}
}

void owned_snapshot_sources() {
	std::optional<SourceInfo> foreign_source;
	for (auto [length, fault] : std::array<std::pair<std::size_t, unsigned>, 9>{{{0, 0}, {9, 0}, {65553, 0}, {9, 1}, {9, 2}, {9, 3}, {65553, 4}, {65553, 5}, {65553, 6}}}) {
		SnapshotFixture fixture("source-" + std::to_string(length) + "-" + std::to_string(fault), SnapshotLimits{7, 100000}, SnapshotFixture::quota(), true, fault >= 5); auto& worker = fixture.worker; std::string image(length, 's');
		SnapshotChannel source_channel(worker);
		Identity local{}, remote{}; local[0] = 'l'; remote[0] = 'r';
		TrustedSession source_session{2, local, remote};
		if (fault >= 5) { source_channel.register_session(source_session); }
		auto id = worker.reserve_snapshot(fixture.context(image)); if (!id) { check(false, "source fixture reserves incoming publication"); continue; }
		validate_incoming(fixture, *id, image); worker.request_install(*id);
		for (unsigned turn = 0; turn < 200 && !worker.snapshot_activation(); ++turn) { fixture.pump(); }
		auto activation = worker.snapshot_activation(); if (!activation) { check(false, "source fixture publishes candidate"); continue; }
		worker.snapshot_activated(*id, activation->token);
		for (unsigned turn = 0; turn < 100 && worker.busy(); ++turn) { fixture.pump(); } worker.take_snapshot_result();
		fixture.io.fail_read = fault == 1 || fault == 2; fixture.io.read_failure_after = fault == 2; fixture.io.corrupt_payload_read = fault == 3;
		std::map<NodeId, Receive> responses; unsigned contacts = 0;
		auto drain = [&] {
			for (const auto& action : worker.take_actions()) {
				if (auto committed = std::get_if<Committed>(&action)) { worker.applied({committed->entries.back().index}); }
				if (auto send = std::get_if<Send>(&action)) {
					if (auto vote = std::get_if<VoteRequest>(&send->message); vote && send->peer == 3) { responses.insert_or_assign(3, Receive{3, VoteResponse{vote->term, true}}); }
					if (auto append = std::get_if<AppendRequest>(&send->message)) {
						if (send->peer == 3) { auto matched = append->previous + append->entries.size(); responses.insert_or_assign(3, Receive{3, AppendResponse{append->term, append->rpc, true, matched, matched + 1, append->read_probe}}); }
						else if (!append->previous && append->entries.empty()) { ++contacts; }
						else { responses.insert_or_assign(2, Receive{2, AppendResponse{append->term, append->rpc, false, 0, 1, append->read_probe}}); }
					}
				}
			}
		};
		std::uint64_t source_clock = 0;
		auto pump = [&](std::uint64_t now) {
			source_clock = std::max(source_clock, now);
			if (!responses.empty()) { auto next = responses.begin(); if (worker.try_submit(next->second) == SubmitResult::Accepted) { responses.erase(next); } }
			worker.run_one({source_clock, 100});
			if (auto job = worker.take_io_operation()) {
				if (fault >= 5 && std::dynamic_pointer_cast<kronuz::journal::ArtifactRead>(job)) {
					job->submitted(); auto outcome = kronuz::journal::detail::execute_primitive(job->io(), job->request());
					if (fault == 6) { worker.close(); }
					source_channel.session_closed(source_session);
					check(!source_channel.session_open(source_session) && !worker.source_chunk(), "channel disconnect cancels a source with an accepted verification original");
					auto debt = worker.accounting()->outstanding;
					for (unsigned wait = 0; wait < 20; ++wait) { worker.run_one({source_clock, 100}); check(!job->done() && worker.accounting()->outstanding == debt, "source disconnect retains authentic completion and debt"); }
					check(job->complete(std::move(outcome)), "disconnected source original settles exactly once");
					kronuz::journal::drive_synchronously(*job);
					worker.run_one({source_clock, 100});
					check(!worker.fenced() && !worker.source_chunk(), "disconnected verifier never resumes a destroyed consumer");
					if (fault == 5) { worker.close(); }
				} else { kronuz::journal::drive_synchronously(*job); }
			}
			drain();
		};
		worker.try_submit(Tick{100, 100}); drain();
		if (fault >= 5) {
			for (unsigned turn = 0; turn < 10000 && !worker.closing(); ++turn) { pump(100 + turn / 10); }
			check(worker.closing(), "source regression reaches accepted verification disconnect");
			for (unsigned turn = 0; turn < 10000 && !worker.drained(); ++turn) { worker.run_one({source_clock, 100}); }
			check(worker.drained() && !worker.fenced(), "disconnected source completes bounded terminal retirement");
			continue;
		}
		if (fault == 4) {
			auto reads = fixture.io.payload_reads; bool canceled = false;
			for (unsigned turn = 0; turn < 1000 && !canceled; ++turn) { pump(100 + turn / 10); if (fixture.io.payload_reads > reads) { canceled = worker.source_peer_failed(2) == ValidationAck::Accepted; } }
			auto after = fixture.io.payload_reads; auto now = source_clock; for (unsigned turn = 0; turn < 30; ++turn) { pump(now); }
			check(canceled && !worker.snapshot_source() && !worker.fenced() && fixture.io.payload_reads == after, "peer cancellation during source verification releases IO ownership and retains its failure across busy callbacks"); continue;
		}
		for (unsigned turn = 0; turn < 1000 && !worker.snapshot_source(); ++turn) { pump(100 + turn / 10); }
		auto source = worker.snapshot_source();
		if (fault) { check(worker.fenced() && !source, "source metadata/read failures and checksum corruption fence before transmission"); continue; }
		check(source && source->peer == 2 && source->descriptor.application_bytes == length && source->descriptor.application_format == 7 && source->key.boundary == LogBoundary{1, 1} && worker.committed() == 2, "owned source binds completed incoming format, publication, flight and healthy peer commitment");
		if (!source) { continue; }
		if (foreign_source) { check(worker.source_failed(foreign_source->source) == ValidationAck::Stale && worker.consume_source(foreign_source->source, 1) == ValidationAck::Stale && !worker.fenced(), "foreign source callbacks cannot consume or fence another owner"); }
		foreign_source = source;
		std::string actual; bool final = false, held = false; Token previous = 0;
		for (unsigned turn = 0; turn < 1000 && !final; ++turn) {
			pump(250 + turn / 10); auto chunk = worker.source_chunk(); if (!chunk) { continue; }
			check(chunk->offset == actual.size() && chunk->chunk > previous && chunk->bytes.size() <= 65536, "owned source has exact bounded offsets and distinct monotonic chunk tokens");
			if (!held) {
				auto incoming_context = fixture.context("incoming"); incoming_context.leader_term = worker.term(); incoming_context.authenticated_peer = 3;
				auto incoming = worker.reserve_snapshot(incoming_context); check(incoming.has_value(), "outbound source and incoming replacement use independent owned slots");
				if (incoming) { worker.offer_snapshot_chunk(*incoming, 0, "incoming", true); for (unsigned prepare = 0; prepare < 100 && !worker.validation_chunk(); ++prepare) { pump(300 + prepare); } }
				auto reads = fixture.io.payload_reads, scans = fixture.io.scans; auto token = chunk->chunk; auto data = std::string(chunk->bytes.data(), chunk->bytes.size()); auto before_contacts = contacts; bool proposed = false;
				for (unsigned wait = 0; wait < 60; ++wait) { if (!proposed) { proposed = worker.try_submit(Propose{799, "more"}) == SubmitResult::Accepted; } worker.try_submit(Read{800 + wait}); pump(400 + wait * 20); }
				check(incoming && worker.validation_chunk() && worker.snapshot_buffer_capacity() == 131072 && proposed && worker.committed() == 3, "held inbound and outbound views permit healthy voter commitment and bounded independent mailboxes");
				if (incoming) { worker.cancel_snapshot(*incoming); worker.take_snapshot_result(); }
				for (unsigned pending = 0; pending < 30 && worker.try_submit(Read{1700}) != SubmitResult::Accepted; ++pending) { pump(1600); }
				auto held_reads = fixture.io.payload_reads, held_scans = fixture.io.scans;
				for (unsigned pending = 0; pending < 30; ++pending) { worker.run_one({1600 + pending, 100}); }
				check(fixture.io.scans > held_scans && fixture.io.payload_reads == held_reads && worker.source_chunk() && worker.try_submit(Read{1701}) == SubmitResult::Busy, "held ordinary output and source view continue reclamation without rereading"); drain();
				auto same = worker.source_chunk(); check(same && same->chunk == token && std::string(same->bytes.data(), same->bytes.size()) == data && fixture.io.payload_reads == reads && fixture.io.scans > scans && contacts > before_contacts, "held source mailbox preserves bytes while timers and reclamation continue"); held = true;
			}
			actual.append(chunk->bytes.data(), chunk->bytes.size()); final = chunk->final; previous = chunk->chunk;
			check(worker.consume_source(source->source, chunk->chunk + 1000000) == ValidationAck::Stale && worker.consume_source(source->source, chunk->chunk) == ValidationAck::Accepted && worker.consume_source(source->source, chunk->chunk) == ValidationAck::Stale, "only exact unconsumed source token advances output");
		}
		check(final && actual == image && worker.snapshot_source() && !worker.source_chunk() && !worker.fenced(), "verified source streams complete image and distinct final EOF while retaining flight");
		check(worker.source_failed(source->source) == ValidationAck::Accepted, "owned source failure is accepted independently of output");
		for (unsigned turn = 0; turn < 30; ++turn) { pump(1500); }
		check(worker.consume_source(source->source, previous) == ValidationAck::Stale && !worker.fenced(), "released source rejects obsolete callbacks without fencing");
	}
}

void binary_transfer_integration(std::size_t length, unsigned mode = 0, bool measure = false, bool async = false) {
	using Clock = std::chrono::steady_clock;
	struct Node {
		SnapshotFixture::Directory directory; std::shared_ptr<FaultIO> owned_io; FaultIO& io; Worker worker; SnapshotChannel transfer;
		std::deque<Receive> messages; std::string application, candidate; Token eof = 0;
		Node(NodeId id, std::size_t size, unsigned mode, bool async) : directory("binary-" + std::to_string(size) + "-" + std::to_string(mode) + "-" + std::to_string(id)), owned_io(std::make_shared<FaultIO>(directory.path)), io(*owned_io),
			worker(owned_io, [&] { auto fixed = SnapshotFixture::fixed(); fixed.local = id; return fixed; }(), {{1024ull * 1024 * 1024, 4096}, {16 * 1024, 3}, {256ull * 1024 * 1024, 4}, 8, 3}, limits(), {}, {4, 1, false, async}, SnapshotLimits{7, 128ull * 1024 * 1024}), transfer(worker) {
			kronuz::journal::Identity identity{}; identity[0] = static_cast<char>(id); worker.create(identity);
			for (unsigned turn = 0; turn < 100 && !worker.ready(); ++turn) { worker.run_one({0, 100}); if (auto job = worker.take_io_operation()) { kronuz::journal::drive_synchronously(*job); } }
			worker.try_submit(Start{}); worker.take_actions();
		}
	};
	Node leader(1, length, mode, async), receiver(2, length, mode, async), healthy(3, length, mode, async); std::array<Node*, 3> nodes{&leader, &receiver, &healthy};
	std::string image(length, 'i'); leader.application = image; healthy.application = image;
	Identity first{}, second{}; first[0] = 'a'; second[0] = 'b'; TrustedSession outgoing{2, first, second}, incoming{1, second, first};
	leader.transfer.register_session(outgoing); receiver.transfer.register_session(incoming);
	bool opened_peer = false, installed_result = false, suffix = false, proposed = false, disturbed = false;
	std::uint64_t leader_offset = 0; std::string duplicate_begin; bool duplicated = false; std::optional<CheckpointId> capture;
	std::size_t offered = 0, wire_bytes = 0, maximum_buffers = 0, allocated_buffers = 0, queue_peak = 0; std::uint64_t clock = 100;
	auto start = Clock::now(); auto cpu_start = std::clock(); Token observed_leader = 0, observed_receiver = 0;
	auto drain = [&](Node& node) {
		for (const auto& action : node.worker.take_actions()) {
			if (auto fenced = std::get_if<Fenced>(&action)) { std::cerr << "binary node " << node.worker.configuration().local << ": " << fenced->reason << "\n"; }
			if (auto range = std::get_if<Committed>(&action)) { for (const auto& entry : range->entries) { if (entry.kind == EntryKind::Command) { node.application += "/" + entry.payload; } } node.worker.applied({range->entries.back().index}); }
			if (auto send = std::get_if<Send>(&action)) {
				if (!opened_peer && (send->peer == 2 || node.worker.configuration().local == 2)) { continue; }
				auto destination = nodes.at(static_cast<std::size_t>(send->peer - 1)); destination->messages.push_back({node.worker.configuration().local, send->message}); queue_peak = std::max(queue_peak, destination->messages.size());
				check(destination->messages.size() <= 64, "integration host keeps ordinary RPC queue bounded");
			}
		}
	};
	auto wire = [&](Node& from, Node& to, TrustedSession session, Token& observed) {
		auto output = from.transfer.outbound(); if (!output) { return; }
		if (observed != output->token) {
			observed = output->token; auto frame = decode_transfer(std::string_view(output->bytes.data(), output->bytes.size()));
			if (&from == &leader && frame.kind == TransferKind::Begin && mode == 0 && !measure) { duplicate_begin.assign(output->bytes.data(), output->bytes.size()); }
			if (frame.kind == TransferKind::Result && static_cast<unsigned char>(frame.body[0]) == static_cast<unsigned char>(TransferOutcome::Installed)) {
				installed_result = true;
				if (mode == 2 && !disturbed) { from.transfer.consume_output(output->token, output->bytes.size()); disturbed = true; leader_offset = 60040; return; }
			}
		}
		auto count = std::min(output->bytes.size(), length > 65553 ? std::size_t{32768} : std::size_t{127});
		auto input = to.transfer.receive(session, output->bytes.first(count)); check(input.result != TransferReceive::Closed, "trusted partial binary delivery stays open");
		if (input.consumed) { from.transfer.consume_output(output->token, input.consumed); wire_bytes += input.consumed; }
	};
	for (unsigned turn = 0; turn < 100000; ++turn) {
		clock = 100 + turn / 20;
		for (auto node : nodes) {
			if (!node->messages.empty()) { auto result = node->worker.try_submit(node->messages.front()); if (result == SubmitResult::Accepted) { node->messages.pop_front(); } }
			node->worker.run_one({node == &receiver && !opened_peer ? 0 : clock + (node == &leader ? leader_offset : 0), node == &leader ? 100u : 200u}); if (auto job = node->worker.take_io_operation()) { kronuz::journal::drive_synchronously(*job); } drain(*node);
		}
		if (!capture && !opened_peer && leader.worker.applied_index() == 1) { capture = leader.worker.reserve_checkpoint(length); if (capture) { leader.worker.attach_capture(*capture, {1000, 1, leader.worker.term()}); } }
		if (capture && !opened_peer && offered <= length) {
			auto count = std::min<std::size_t>(65536, length - offered); auto result = leader.worker.offer_application_chunk(*capture, std::string_view(image).substr(offered, count), offered + count == length);
			if (result == SubmitResult::Accepted) { offered += count; if (offered == length) { ++offered; } }
		}
		if (leader.worker.base_index() == 1) { opened_peer = true; }
		if (leader.worker.snapshot_source() && !proposed) { if (leader.worker.try_submit(Propose{2000, "tail"}) == SubmitResult::Accepted) { proposed = true; } drain(leader); }
		if (mode == 3) { if (auto result = receiver.worker.pending_snapshot_result(); result && result->reason == SnapshotReason::Installed) { installed_result = true; } }
		leader.transfer.run_one({clock + leader_offset, 100}); receiver.transfer.run_one({clock, 200});
		if (mode == 3 && !disturbed && receiver.worker.storage_frontier().checkpoint) {
			leader.transfer.session_closed(outgoing); receiver.transfer.session_closed(incoming);
			first[0] = 'x'; second[0] = 'y'; outgoing = {2, first, second}; incoming = {1, second, first};
			leader.transfer.register_session(outgoing); receiver.transfer.register_session(incoming); disturbed = true;
		}
		if (mode == 3) { if (auto result = receiver.worker.pending_snapshot_result(); result && result->reason == SnapshotReason::Installed) { installed_result = true; } }
		wire(leader, receiver, incoming, observed_leader); wire(receiver, leader, outgoing, observed_receiver);
		if (!duplicated && !duplicate_begin.empty() && receiver.transfer.incoming_snapshot() && !receiver.transfer.outbound()) { auto repeated = receiver.transfer.receive(incoming, std::span<const char>(duplicate_begin.data(), duplicate_begin.size())); wire_bytes += repeated.consumed; check(repeated.consumed == duplicate_begin.size() && repeated.result == TransferReceive::Accepted, "duplicate Begin traverses active real Workers before transfer completion"); duplicated = true; }
		if (auto view = receiver.worker.validation_chunk()) {
			check(view->offset == receiver.candidate.size(), "binary receiver validates exact candidate offsets"); receiver.candidate.append(view->bytes.data(), view->bytes.size());
			receiver.worker.consume_validation(view->snapshot, view->chunk); if (view->verified_eof) { receiver.eof = view->chunk; receiver.worker.validation_succeeded(view->snapshot, view->chunk); }
			if (mode == 1 && !disturbed && !view->verified_eof) {
				auto previous = incoming; leader.transfer.session_closed(outgoing); receiver.transfer.session_closed(incoming);
				first[0] = 'x'; second[0] = 'y'; outgoing = {2, first, second}; incoming = {1, second, first};
				leader.transfer.register_session(outgoing); receiver.transfer.register_session(incoming);
				check(receiver.transfer.receive(previous, {}).result == TransferReceive::Closed && !receiver.worker.fenced(), "old session callback cannot consume new connection state");
				receiver.candidate.clear(); receiver.eof = 0; disturbed = true;
			}
		}
		if (auto activation = receiver.worker.snapshot_activation()) { check(receiver.eof && receiver.candidate == image, "actual binary transfer reaches validated exact image before activation"); if (mode == 5) {
			check(receiver.worker.cancel_snapshot(activation->snapshot) == CancelResult::TooLate, "published activation failure fixture cannot cancel its installation");
			check(receiver.worker.snapshot_activation_failed(activation->snapshot, activation->token) == ValidationAck::Accepted && receiver.worker.fenced(), "matching activation failure fences actual binary receiver");
			receiver.transfer.run_one({clock, 200});
			check(receiver.transfer.drained() && !receiver.transfer.incoming_snapshot() && !receiver.transfer.outbound() && !receiver.transfer.session_open(incoming) && receiver.transfer.retained_bytes() == 0, "fenced TooLate installation releases channel ownership without emitting a result");
			leader.transfer.session_closed(outgoing); return;
		} receiver.application = std::move(receiver.candidate); receiver.worker.snapshot_activated(activation->snapshot, activation->token); }
		maximum_buffers = std::max(maximum_buffers, leader.transfer.retained_bytes() + receiver.transfer.retained_bytes());
		allocated_buffers = std::max(allocated_buffers, leader.transfer.buffer_capacity() + receiver.transfer.buffer_capacity() + leader.worker.snapshot_buffer_capacity() + receiver.worker.snapshot_buffer_capacity());
		if (installed_result && receiver.worker.applied_index() == 2 && leader.worker.applied_index() == 2 && healthy.worker.applied_index() == 2) { suffix = true; break; }
		if (leader.worker.fenced() || receiver.worker.fenced() || healthy.worker.fenced()) { break; }
	}
	if (!suffix) { std::cerr << "binary progress: bytes=" << wire_bytes << " capture=" << bool(capture) << " base=" << leader.worker.base_index() << " term=" << leader.worker.term() << " role=" << int(leader.worker.role()) << " commit=" << leader.worker.committed() << " receiver=" << receiver.worker.applied_index() << " healthy=" << healthy.worker.applied_index() << " source=" << bool(leader.worker.snapshot_source()) << "\n"; }
	check(installed_result && suffix && receiver.application == image + "/tail" && leader.application == receiver.application && healthy.application == receiver.application && !leader.worker.snapshot_source(), "receiver-generated durable Installed result advances real sender flight and ordinary suffix produces exact application state");
	check(queue_peak <= 64 && maximum_buffers <= maximum_transfer_bytes * 2 + 400, "binary stop-and-wait retains bounded frame storage independently of image size");
	auto elapsed = std::chrono::duration<double>(Clock::now() - start).count(); auto cpu = double(std::clock() - cpu_start) / CLOCKS_PER_SEC; rusage usage{}; getrusage(RUSAGE_SELF, &usage);
	if (!measure) { return; }
	std::ofstream measurements(std::filesystem::current_path() / ".scratch" / "snapshot-transfer-benchmark.csv", std::ios::app);
	measurements << length << ',' << elapsed << ',' << cpu << ',' << wire_bytes << ',' << maximum_buffers << ',' << allocated_buffers << ',' << usage.ru_maxrss << ',' << leader.io.payload_reads << ',' << receiver.io.payload_reads << '\n';
}

void checkpoint_format_recovery() {
	for (bool known : {false, true}) {
		SnapshotFixture::Directory directory("format-recovery-" + std::to_string(known));
		{
			FaultIO io(directory.path); Worker worker(io, configuration(), SnapshotFixture::quota(), limits(), {}, {4, 1}, known ? std::optional<SnapshotLimits>{{7, 100000}} : std::nullopt);
			kronuz::journal::Identity identity{}; identity[0] = 'f'; worker.create(identity);
			for (unsigned turn = 0; turn < 100 && !worker.ready(); ++turn) { worker.run_one({0, 100}); } worker.try_submit(Start{}); worker.take_actions();
			auto pump = [&] { worker.run_one({100, 100}); for (const auto& action : worker.take_actions()) { if (auto committed = std::get_if<Committed>(&action)) { worker.applied({committed->entries.back().index}); } } };
			for (unsigned turn = 0; turn < 100 && worker.applied_index() != 1; ++turn) { pump(); }
			auto id = worker.reserve_checkpoint(5); if (!id) { check(false, "format fixture reserves local capture"); continue; }
			worker.attach_capture(*id, {1, 1, worker.term()}); worker.offer_application_chunk(*id, "image", true);
			for (unsigned turn = 0; turn < 100 && worker.base_index() != 1; ++turn) { pump(); }
			check(worker.base_index() == 1 && !worker.fenced(), "format fixture publishes exact local image");
		}
		{
			FaultIO io(directory.path); Worker worker(io, configuration(), SnapshotFixture::quota(), limits(), {}, {4, 1}, SnapshotLimits{8, 100000}); bool restored = false;
			auto restore = [&](auto& reader) { std::array<char, 5> bytes{}; reader.read_at(0, bytes); restored = std::string_view(bytes.data(), bytes.size()) == "image"; };
			if (known) { check(throws([&] { worker.recover(restore); }) && worker.fenced() && !restored, "changed format policy rejects known persisted format before application activation"); }
			else { worker.recover(restore); for (unsigned turn = 0; turn < 100 && !worker.ready(); ++turn) { worker.run_one({100, 100}); } check(restored && worker.ready() && worker.base_index() == 1 && !worker.snapshot_source(), "legacy unknown-format image remains recoverable without inferred export format"); }
		}
	}
}

void legacy_source_export_rejection() {
	SnapshotFixture::Directory directory("legacy-source-export"); auto fixed = SnapshotFixture::fixed();
	{
		FaultIO io(directory.path); kronuz::journal::Store store(io, SnapshotFixture::quota(), 1024); kronuz::journal::Identity identity{}; identity[0] = 'v'; store.create(identity); while (!store.inventory_step(1).complete) {}
		auto initialization = encode_initialization(fixed); auto append = store.reserve_append(kronuz::journal::AdmissionClass::Control, initialization.size()); store.append(*append, initialization);
		auto replacement = store.reserve_replacement(5, checkpoint_detail::maximum_size(limits())); store.begin_artifact(*replacement, kronuz::journal::ArtifactPart::Application); store.write_chunk(*replacement, "image"); auto application = store.finish_artifact(*replacement);
		RecoveredState state{fixed, {1, 1, 1}, 1, 1, {}, 1}; auto bundle = encode_checkpoint(state, 1, application, limits());
		store.begin_artifact(*replacement, kronuz::journal::ArtifactPart::Bundle); store.write_chunk(*replacement, bundle); store.finish_artifact(*replacement); store.publish(*replacement, 1);
	}
	FaultIO io(directory.path); Worker worker(io, fixed, SnapshotFixture::quota(), limits(), {}, {4, 1}, SnapshotLimits{7, 100000}); std::string restored;
	worker.recover([&](auto& reader) { std::array<char, 5> bytes{}; reader.read_at(0, bytes); restored.assign(bytes.data(), bytes.size()); }); for (unsigned turn = 0; turn < 100 && !worker.ready(); ++turn) { worker.run_one({0, 100}); }
	std::map<NodeId, Receive> responses; unsigned needed = 0; bool exported = false;
	auto drain = [&] { for (const auto& action : worker.take_actions()) {
		if (std::holds_alternative<SnapshotNeeded>(action)) { ++needed; }
		if (auto committed = std::get_if<Committed>(&action)) { worker.applied({committed->entries.back().index}); }
		if (auto send = std::get_if<Send>(&action)) {
			if (auto vote = std::get_if<VoteRequest>(&send->message); vote && send->peer == 3) { responses.insert_or_assign(3, Receive{3, VoteResponse{vote->term, true}}); }
			if (auto append = std::get_if<AppendRequest>(&send->message)) { auto matched = append->previous + append->entries.size(); responses.insert_or_assign(send->peer, Receive{send->peer, AppendResponse{append->term, append->rpc, send->peer == 3, send->peer == 3 ? matched : 0, send->peer == 3 ? matched + 1 : 1, append->read_probe}}); }
		}
	} };
	worker.try_submit(Start{}); drain();
	for (unsigned turn = 0; turn < 1000; ++turn) {
		if (!responses.empty()) { auto next = responses.begin(); if (worker.try_submit(next->second) == SubmitResult::Accepted) { responses.erase(next); } }
		worker.run_one({100 + turn / 5, 100}); drain(); exported |= worker.snapshot_source().has_value();
	}
	check(restored == "image" && worker.role() == Role::Leader && worker.committed() == 2 && needed >= 2 && !exported && !worker.fenced(), "actual legacy v1 recovery cannot label or export unknown format under current snapshot policy");
}

void binary_channel_validation() {
	for (unsigned branch = 0; branch < 10; ++branch) {
		SnapshotFixture fixture("channel-validation-" + std::to_string(branch)); SnapshotChannel channel(fixture.worker);
		Identity local{}, remote{}; local[0] = 'l'; remote[0] = 'r'; TrustedSession session{2, local, remote}; channel.register_session(session);
		auto context = fixture.context("data"); auto fixed = fixture.worker.configuration(); TransferEnvelope envelope{fixed.cluster, fixed.configuration, remote, local, 2, 1, {1, 17, {1, 1}}};
		auto bytes = encode_transfer(TransferKind::Begin, envelope, encode_snapshot(context.descriptor, *fixture.worker.snapshot_policy()));
		auto frontier = fixture.worker.storage_frontier(); auto used = fixture.worker.accounting()->used;
		if (branch < 8) {
			std::array<std::size_t, 8> offsets{0, 8, 12, 16, 32, 48, 64, 80}; bytes[offsets[branch]] ^= static_cast<char>(255);
			auto result = channel.receive(session, std::span<const char>(bytes.data(), bytes.size()));
			check(result.result == TransferReceive::Closed && !fixture.worker.fenced() && fixture.worker.accounting()->used == used && fixture.worker.storage_frontier().generation == frontier.generation && !channel.incoming_snapshot(), "malformed or unbound Begin closes only its session before storage reservation"); continue;
		}
		for (char byte : bytes) { auto result = channel.receive(session, std::span<const char>(&byte, 1)); check(result.consumed == 1 && result.result == TransferReceive::Accepted, "single-byte fragmented Begin stays bounded and correlated"); }
		auto id = channel.incoming_snapshot(); check(id.has_value(), "complete Begin owns receiver replacement"); auto reserved = fixture.worker.accounting()->outstanding;
		auto result = channel.receive(session, std::span<const char>(bytes.data(), bytes.size()));
		check(result.result == TransferReceive::Busy && result.consumed == bytes.size() && fixture.worker.accounting()->outstanding == reserved, "duplicate Begin under control pressure retains one frame and one replacement reservation");
		auto output = channel.outbound(); if (!output) { check(false, "Accepted output is retained"); continue; }
		auto token = output->token; channel.consume_output(token, 1); auto remainder = channel.outbound();
		Identity third{}; third[0] = 't'; TrustedSession other{3, local, third}; channel.register_session(other); channel.session_closed(other);
		check(channel.outbound()->token == token && channel.outbound()->bytes.size() == remainder->bytes.size(), "closing another channel cannot preempt a partially written control frame");
		channel.consume_output(token, remainder->bytes.size()); channel.run_one({0, 100}); output = channel.outbound();
		check(output && decode_transfer(std::string_view(output->bytes.data(), output->bytes.size())).kind == TransferKind::Accepted && channel.incoming_snapshot() == id && fixture.worker.accounting()->outstanding == reserved, "duplicate Begin reuses exact owned operation after control output drains");
		channel.consume_output(output->token, output->bytes.size());
		if (branch == 8) {
			auto invalid = encode_transfer(TransferKind::Chunk, envelope, transfer_chunk_body(99, 1, "data", true));
			check(channel.receive(session, std::span<const char>(invalid.data(), invalid.size())).result == TransferReceive::Closed && !fixture.worker.fenced(), "out-of-order data closes session without fencing healthy storage");
		} else {
			auto chunk = encode_transfer(TransferKind::Chunk, envelope, transfer_chunk_body(99, 0, "data", false));
			check(channel.receive(session, std::span<const char>(chunk.data(), chunk.size())).result == TransferReceive::Accepted, "first bounded chunk enters owned receiver mailbox");
			output = channel.outbound(); check(output && decode_transfer(std::string_view(output->bytes.data(), output->bytes.size())).kind == TransferKind::Credit, "credit follows mailbox acceptance without claiming durability");
			channel.consume_output(output->token, output->bytes.size()); auto final = encode_transfer(TransferKind::Chunk, envelope, transfer_chunk_body(100, 4, "", true));
			check(channel.receive(session, std::span<const char>(final.data(), final.size())).result == TransferReceive::Busy && !channel.outbound(), "busy receiver consumes no final input and creates no credit");
			for (unsigned turn = 0; turn < 30 && !channel.outbound(); ++turn) { fixture.pump(); channel.run_one({0, 100}); }
			check(channel.outbound().has_value(), "retained final frame retries after mailbox storage drains"); channel.session_closed(session);
		}
		channel.run_one({0, 100}); check(!channel.incoming_snapshot() && !fixture.worker.pending_snapshot_result() && !fixture.worker.fenced(), "session loss cancels legal staging and drains its correlated terminal result");
	}
}

void channel_dispatch_and_unstarted_result() {
	for (bool unstarted : {false, true}) {
		SnapshotFixture fixture("dispatch-unstarted-" + std::to_string(unstarted), SnapshotLimits{7, 100000}, SnapshotFixture::quota(), !unstarted); SnapshotChannel channel(fixture.worker);
		Identity local{}, remote{}; local[0] = 'l'; remote[0] = 'r'; TrustedSession session{2, local, remote}; channel.register_session(session);
		auto fixed = fixture.worker.configuration(); TransferEnvelope envelope{fixed.cluster, fixed.configuration, remote, local, 2, 1, {1, 17, {1, 1}}};
		if (!unstarted) {
			auto control = encode_transfer(TransferKind::Accepted, envelope, {}); std::string joined; for (unsigned count = 0; count < 100; ++count) { joined += control; }
			auto first = channel.receive(session, std::span<const char>(joined.data(), joined.size()));
			check(first.result == TransferReceive::Accepted && first.consumed == control.size(), "one receive call dispatches one frame from arbitrarily concatenated controls");
			std::size_t consumed = first.consumed; while (consumed < joined.size()) { auto next = channel.receive(session, std::span<const char>(joined.data() + consumed, joined.size() - consumed)); check(next.result == TransferReceive::Accepted && next.consumed == control.size(), "caller retries exact remaining prefix under bounded dispatch"); consumed += next.consumed; }
			check(!fixture.worker.fenced() && !channel.incoming_snapshot() && channel.retained_bytes() == 0, "bounded concatenated dispatch retains no extra queue or storage state"); continue;
		}
		auto context = fixture.context("data"); auto begin = encode_transfer(TransferKind::Begin, envelope, encode_snapshot(context.descriptor, *fixture.worker.snapshot_policy()));
		channel.receive(session, std::span<const char>(begin.data(), begin.size())); auto id = channel.incoming_snapshot(); if (!id) { check(false, "unstarted receiver owns candidate"); continue; }
		auto accepted = channel.outbound(); channel.consume_output(accepted->token, accepted->bytes.size()); auto data = encode_transfer(TransferKind::Chunk, envelope, transfer_chunk_body(99, 0, "data", true)); channel.receive(session, std::span<const char>(data.data(), data.size()));
		for (unsigned turn = 0; turn < 200; ++turn) {
			fixture.pump(); if (auto view = fixture.worker.validation_chunk()) { auto token = view->chunk; auto final = view->verified_eof; fixture.worker.consume_validation(view->snapshot, token); if (final) { fixture.worker.validation_succeeded(view->snapshot, token); } }
			channel.run_one({0, 100}); auto output = channel.outbound(); if (!output) { continue; }
			auto frame = decode_transfer(std::string_view(output->bytes.data(), output->bytes.size()));
			if (frame.kind == TransferKind::Result) {
				auto body = frame.body.substr(8); check(frame.body[0] == static_cast<char>(TransferOutcome::Failed) && kronuz::journal::get64(body) == 0 && fixture.worker.term() == 0 && !fixture.worker.fenced(), "Busy before Start generates a healthy termless Failed result");
				SnapshotFixture::Directory directory("unstarted-result-sender"); FaultIO io(directory.path); auto configuration = fixed; configuration.local = 2;
				Worker sender(io, configuration, SnapshotFixture::quota(), limits(), {}, {4, 1}, SnapshotLimits{7, 100000}); kronuz::journal::Identity identity{}; identity[0] = 'z'; sender.create(identity); for (unsigned ready = 0; ready < 100 && !sender.ready(); ++ready) { sender.run_one({0, 100}); }
				SnapshotChannel sender_channel(sender); TrustedSession reverse{1, remote, local}; sender_channel.register_session(reverse); auto delivered = sender_channel.receive(reverse, output->bytes);
				check(delivered.result == TransferReceive::Accepted && delivered.consumed == output->bytes.size() && !sender.fenced() && sender.term() == 0, "term-zero generated result crosses real adapter without fabricating or observing a term");
				channel.consume_output(output->token, output->bytes.size()); check(channel.drained(), "unstarted rejected operation drains before channel destruction"); break;
			}
			channel.consume_output(output->token, output->bytes.size());
		}
		check(channel.drained(), "unstarted failure completes bounded owned result delivery");
	}
}

void binary_transfer_codec() {
	Identity cluster{}, configuration{}, local{}, remote{}; cluster[0] = 'c'; configuration[0] = 'f'; local[0] = 'l'; remote[0] = 'r';
	TransferEnvelope envelope{cluster, configuration, local, remote, 1, 2, {3, 5, {7, 2}}};
	SnapshotDescriptor descriptor{cluster, configuration, 0, 7, 2, 3, kronuz::journal::crc32c("abc")}; SnapshotPolicy policy{cluster, configuration, 0, 65536};
	auto bytes = encode_transfer(TransferKind::Begin, envelope, encode_snapshot(descriptor, policy)); auto decoded = decode_transfer(bytes);
	check(decoded.envelope.key == envelope.key && decode_snapshot(decoded.body, policy) == descriptor, "binary envelope preserves full snapshot correlation and descriptor");
	for (std::size_t length = 0; length < bytes.size(); ++length) { check(throws([&] { decode_transfer(std::string_view(bytes).substr(0, length)); }), "every truncated binary Begin fails closed"); }
	for (std::size_t offset : {std::size_t{0}, std::size_t{8}, std::size_t{12}, std::size_t{13}}) { auto corrupted = bytes; corrupted[offset] ^= static_cast<char>(255); check(throws([&] { decode_transfer(corrupted); }), "invalid magic, length, kind and reserved fields reject"); }
	auto chunk = encode_transfer(TransferKind::Chunk, envelope, transfer_chunk_body(8, 0, "abc", true)); check(decode_transfer(chunk).body.size() == 27, "bounded chunk framing round trips");
	chunk[transfer_header_bytes + 20] = 2; check(throws([&] { decode_transfer(chunk); }), "unknown chunk flags reject");
	check(throws([&] { encode_transfer(TransferKind::Chunk, envelope, std::string(65561, 'x')); }), "oversized frame rejects before encoding");
}

int main(int argc, char** argv) {
	try {
		if (argc == 2 && std::string_view(argv[1]) == "--terminal-regressions") { terminal_original_settlement(); terminal_checkpoint_originals(); terminal_output_and_dispatch_failure(); terminal_snapshot_readback(); terminal_published_activation(); terminal_reclamation_original(); std::cout << checks << " terminal checks, " << failures << " failures\n"; return failures ? 1 : 0; }
		terminal_original_settlement(); terminal_checkpoint_originals(); terminal_output_and_dispatch_failure(); terminal_snapshot_readback(); terminal_published_activation(); terminal_reclamation_original();
		if (argc == 2 && std::string_view(argv[1]) == "--retained-admission-benchmark") { admission_benchmark = true; real_worker(); return failures ? 1 : 0; }
		if (argc == 2 && std::string_view(argv[1]) == "--source-regressions") { owned_snapshot_sources(); std::cout << checks << " source checks, " << failures << " failures\n"; return failures ? 1 : 0; }
		if (argc == 2 && std::string_view(argv[1]) == "--completion-regressions") { completion_appends(); completion_storage(); completion_channel_cancellation(); completion_snapshot_readback(); retired_completion_worker(); for (unsigned mode : {0u, 1u, 3u}) { binary_transfer_integration(65553, mode, false, true); } std::cout << checks << " completion checks, " << failures << " failures\n"; return failures ? 1 : 0; }
		if (argc == 2 && std::string_view(argv[1]) == "--review-regressions") { channel_dispatch_and_unstarted_result(); binary_transfer_integration(65553); binary_transfer_integration(65553, 5); owned_snapshot_sources(); std::cout << checks << " review checks, " << failures << " failures\n"; return failures ? 1 : 0; }
		if (argc == 2 && std::string_view(argv[1]) == "--transfer-benchmark") { std::ofstream(std::filesystem::current_path() / ".scratch" / "snapshot-transfer-benchmark.csv") << "image_bytes,wall_s,cpu_s,wire_bytes,retained_frame_bytes,payload_buffer_capacity,process_maxrss_native,sender_payload_reads,receiver_payload_reads\n"; binary_transfer_integration(1048576, 0, true); binary_transfer_integration(67108864, 0, true); std::cout << checks << " benchmark checks, " << failures << " failures\n"; return failures ? 1 : 0; }
		completion_appends(); completion_storage(); completion_channel_cancellation(); completion_snapshot_readback(); retired_completion_worker(); for (unsigned mode : {0u, 1u, 3u}) { binary_transfer_integration(65553, mode, false, true); } checkpoint_format_recovery(); legacy_source_export_rejection(); binary_channel_validation(); channel_dispatch_and_unstarted_result(); binary_transfer_codec(); for (std::size_t length : {std::size_t{0}, std::size_t{9}, std::size_t{65553}, std::size_t{1048576}}) { binary_transfer_integration(length); }
		for (unsigned mode : {1u, 2u, 3u, 5u}) { binary_transfer_integration(65553, mode); }
		owned_snapshot_sources(); snapshot_reception_and_validation(); snapshot_policy_and_rejection(); snapshot_cancellation_phases(); snapshot_read_faults(); snapshot_restart_cleanup(); snapshot_protocol_progress(); snapshot_installation(); snapshot_rejection_branches(); snapshot_old_application(); snapshot_install_reopening(); snapshot_install_admission(); snapshot_publication_faults(); maintenance_preserves_protocol_timers(); real_worker(); partial_control_pack(); failures_and_multi_action_output(); interrupted_initialization(); checkpoint_crash_and_progress(); preparation_preserves_protocol_timers(); checkpoint_cancellation_and_completion(); checkpoint_faults(); } catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " worker checks, " << failures << " failures\n"; return failures ? 1 : 0;
}
