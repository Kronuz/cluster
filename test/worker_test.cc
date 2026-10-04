#include "consensus/worker.h"
#include "journal/posix.h"
#include <iostream>
#include <map>

using namespace cluster::consensus;
namespace {
int checks = 0, failures = 0;
void check(bool value, std::string_view message) { ++checks; if (!value) { ++failures; std::cerr << "FAIL: " << message << '\n'; } }
template <class F> bool throws(F&& function) { try { function(); return false; } catch (const std::exception&) { return true; } }
FixedConfiguration configuration() { FixedConfiguration result; result.local = 1; result.voters = {1}; result.cluster[0] = 'C'; result.configuration[0] = 'V'; return result; }
Limits limits() { Limits result; result.command_bytes = 1024; result.rpc_bytes = 2048; result.log_bytes = 4096; result.log_entries = 32; result.control_entries = 4; result.rpc_entries = 4; return result; }
kronuz::journal::AdmissionLimits admission() { return {{4000, 100}, {2000, 3}, {1000, 4}, 5, 3}; }
class FaultIO final : public kronuz::journal::IO {
public:
	explicit FaultIO(const std::filesystem::path& directory) : backend(directory) {}
	bool fail_sync = false;
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
	void sync_directory() override { if (fail_sync) { fail_sync = false; throw std::runtime_error("injected worker directory barrier failure"); } backend.sync_directory(); ++directory_syncs; }
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
		check(worker.try_submit(Propose{3, "state"}) == SubmitResult::Accepted, "small admitted command retains its commit continuation");
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
	Directory directory; FaultIO io; Worker worker; bool unexpected_install = false;
	explicit SnapshotFixture(std::string name, std::optional<SnapshotLimits> snapshots = SnapshotLimits{7, 100000}, kronuz::journal::AdmissionLimits capacity = quota())
		: directory(std::move(name)), io(directory.path), worker(io, fixed(), capacity, limits(), {}, {4, 1}, snapshots) {
		kronuz::journal::Identity identity{}; identity[0] = 'S'; worker.create(identity);
		for (unsigned turn = 0; turn < 100 && !worker.ready(); ++turn) { worker.run_one({0, 100}); }
		worker.try_submit(Start{}); drain();
	}
	void drain() {
		for (const auto& action : worker.take_actions()) {
			unexpected_install |= std::holds_alternative<PersistInstall>(action) || std::holds_alternative<ActivateInstall>(action) || std::holds_alternative<InstallCompleted>(action) || std::holds_alternative<CheckpointPublished>(action);
			if (auto range = std::get_if<Committed>(&action)) { worker.applied({range->entries.back().index}); }
		}
	}
	std::uint64_t staged_bytes() const {
		std::uint64_t total = 0;
		for (const auto& entry : std::filesystem::directory_iterator(directory.path)) { if (entry.path().filename().string().starts_with("artifact-")) { total += entry.file_size(); } }
		return total;
	}
	void pump() { worker.run_one({0, 100}); drain(); }
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
	check(throws([&] { worker.offer_snapshot_chunk(*id, 1, "a"); }) && throws([&] { worker.offer_snapshot_chunk(*id, 0, ""); }) && throws([&] { worker.offer_snapshot_chunk(*id, 0, "a", true); }) && throws([&] { worker.offer_snapshot_chunk(*id, 0, "abcd", true); }) && throws([&] { worker.offer_snapshot_chunk(*id, 0, std::string(65537, 'x')); }) && !worker.fenced(), "invalid offsets, zero chunks, early final and excess sizes leave reception unchanged");
	auto first = worker.offer_snapshot_chunk(*id, 0, "a"); auto busy = worker.offer_snapshot_chunk(*id, 1, "b");
	check(first.result == SubmitResult::Accepted && first.next_offset == 1 && busy.result == SubmitResult::Busy && busy.next_offset == 1 && throws([&] { worker.offer_snapshot_chunk(*id, 0, "a"); }), "busy input accepts no bytes and duplicate offsets reject");
	for (unsigned turn = 0; turn < 100 && worker.offer_snapshot_chunk(*id, 1, "bc").result == SubmitResult::Busy; ++turn) { fixture.pump(); }
	SnapshotOffer final{SubmitResult::Busy, 3};
	for (unsigned turn = 0; turn < 100 && final.result == SubmitResult::Busy; ++turn) { fixture.pump(); final = worker.offer_snapshot_chunk(*id, 3, "", true); }
	check(final.result == SubmitResult::Accepted, "empty final marker closes a fully offered nonempty image");
	SnapshotFixture other("foreign"); auto other_id = other.worker.reserve_snapshot(other.context("abc"));
	check(other_id && worker.consume_validation(*other_id, 1) == ValidationAck::Stale && worker.validation_succeeded(*other_id, 1) == ValidationAck::Stale && worker.cancel_snapshot(*other_id) == CancelResult::Stale && throws([&] { worker.offer_snapshot_chunk(*other_id, 0, "a"); }), "foreign session callbacks cannot change an incoming candidate");
	check(worker.reject_snapshot_validation(*id) == CancelResult::Canceled, "semantic rejection cancels healthy staged storage");
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

int main() {
	try { snapshot_reception_and_validation(); snapshot_policy_and_rejection(); snapshot_cancellation_phases(); snapshot_read_faults(); snapshot_restart_cleanup(); snapshot_protocol_progress(); maintenance_preserves_protocol_timers(); real_worker(); partial_control_pack(); failures_and_multi_action_output(); interrupted_initialization(); checkpoint_crash_and_progress(); preparation_preserves_protocol_timers(); checkpoint_cancellation_and_completion(); checkpoint_faults(); } catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " worker checks, " << failures << " failures\n"; return failures ? 1 : 0;
}
