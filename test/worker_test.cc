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
	unsigned scans = 0, artifacts = 0, directory_syncs = 0;
	auto acquire_owner(bool create) -> std::unique_ptr<kronuz::journal::OwnerLock> override { return backend.acquire_owner(create); }
	auto open_existing(std::string_view name) -> std::unique_ptr<kronuz::journal::File> override { return backend.open_existing(name); }
	auto create_exclusive(std::string_view name) -> std::unique_ptr<kronuz::journal::File> override { auto file = backend.create_exclusive(name); if (name.starts_with("artifact-")) { ++artifacts; } return file; }
	void replace(std::string_view source, std::string_view destination) override { backend.replace(source, destination); }
	void remove(std::string_view name) override { backend.remove(name); }
	auto scan_directory() -> std::unique_ptr<kronuz::journal::DirectoryCursor> override { ++scans; return backend.scan_directory(); }
	auto entry_footprint(std::string_view name) -> std::optional<kronuz::journal::EntryFootprint> override { return backend.entry_footprint(name); }
	auto open_reclaim_candidate(std::string_view name) -> std::unique_ptr<kronuz::journal::File> override { return backend.open_reclaim_candidate(name); }
	void sync_directory() override { if (fail_sync) { fail_sync = false; throw std::runtime_error("injected worker directory barrier failure"); } backend.sync_directory(); ++directory_syncs; }
private:
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
}
int main() {
	try { maintenance_preserves_protocol_timers(); real_worker(); partial_control_pack(); failures_and_multi_action_output(); interrupted_initialization(); checkpoint_crash_and_progress(); preparation_preserves_protocol_timers(); checkpoint_cancellation_and_completion(); checkpoint_faults(); } catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " worker checks, " << failures << " failures\n"; return failures ? 1 : 0;
}
