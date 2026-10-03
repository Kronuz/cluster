#include "consensus/core.h"
#include "consensus/storage.h"
#include "journal/journal.h"
#include "journal/posix.h"
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <random>

using namespace cluster::consensus;
namespace {
int checks = 0, failures = 0;
void check(bool value, std::string_view message) {
	++checks;
	if (!value) { ++failures; std::cerr << "FAIL: " << message << '\n'; }
}
template <class Function> bool throws(Function&& function) {
	try { function(); return false; } catch (const std::exception&) { return true; }
}
FixedConfiguration config(NodeId local, std::size_t voters = 3) {
	FixedConfiguration result; result.cluster[0] = 'C'; result.configuration[0] = 'V'; result.local = local;
	for (NodeId node = 1; node <= voters; ++node) { result.voters.push_back(node); }
	return result;
}
RecoveredState empty(NodeId local, std::size_t voters = 3) {
	RecoveredState state; state.configuration = config(local, voters); return state;
}
template <class T> const T* find(const Actions& actions) {
	for (const auto& action : actions) { if (auto value = std::get_if<T>(&action)) { return value; } }
	return nullptr;
}

struct Host {
	NodeId id;
	Limits limits;
	std::unique_ptr<Core> core;
	std::vector<std::string> journal;
	std::vector<std::string> applied_commands;
	std::vector<RequestId> ready_reads;
	std::optional<Persist> pending;
	std::optional<Committed> applying;
	bool auto_persist = true, auto_apply = true;
	explicit Host(NodeId local, std::size_t voters = 3, Limits bounds = {}) : id(local), limits(bounds), core(std::make_unique<Core>(config(local, voters), empty(local, voters), bounds)) {
		journal.push_back(encode_initialization(config(local, voters)));
	}
	RecoveredState recovered() const {
		Recovery recovery(core->configuration(), limits);
		for (std::size_t i = 0; i < journal.size(); ++i) { recovery.replay(i + 1, journal[i]); }
		return recovery.finish(journal.size());
	}
	void restart() {
		auto state = recovered(); auto configuration = state.configuration;
		core = std::make_unique<Core>(std::move(configuration), std::move(state), limits);
		pending.reset(); applying.reset(); applied_commands.clear(); ready_reads.clear();
	}
};
struct Envelope { NodeId from, to; Message message; };
struct Network {
	std::map<NodeId, std::unique_ptr<Host>> hosts;
	std::deque<Envelope> network;
	std::set<NodeId> isolated;
	std::uint64_t now = 0;
	explicit Network(std::size_t count = 3, Limits limits = {}) {
		for (NodeId i = 1; i <= count; ++i) { hosts.emplace(i, std::make_unique<Host>(i, count, limits)); }
	}
	Host& host(NodeId id) { return *hosts.at(id); }
	void consume(NodeId id, Actions actions) {
		auto& host = this->host(id);
		for (auto& action : actions) {
			if (auto persist = std::get_if<Persist>(&action)) {
				check(!host.pending, "only one disk batch outstanding"); host.pending = std::move(*persist);
				if (host.auto_persist) { complete_disk(id); }
			} else if (auto send = std::get_if<Send>(&action)) {
				network.push_back(Envelope{id, send->peer, std::move(send->message)});
			} else if (auto committed = std::get_if<Committed>(&action)) {
				check(!host.applying, "only one application batch outstanding"); host.applying = std::move(*committed);
				if (host.auto_apply) { complete_apply(id); }
			} else if (auto ready = std::get_if<ReadReady>(&action)) { host.ready_reads.push_back(ready->request); }
			else if (std::holds_alternative<Fenced>(action)) { check(false, "normal deterministic host unexpectedly fenced"); }
		}
	}
	void complete_disk(NodeId id) {
		auto& host = this->host(id);
		if (!host.pending) { throw std::logic_error("missing pending disk batch"); }
		auto pending = std::move(*host.pending); host.pending.reset();
		host.journal.push_back(encode_storage_batch(pending.batch));
		// Semantic storage replay checks each completed batch, not just CRCs.
		host.recovered();
		consume(id, host.core->step(Persisted{pending.token}));
		check(host.core->durable_hard_state() == host.recovered().hard, "durable hard state agrees with storage replay");
	}
	void complete_apply(NodeId id) {
		auto& host = this->host(id);
		if (!host.applying) { throw std::logic_error("missing application batch"); }
		auto committed = std::move(*host.applying); host.applying.reset();
		for (const auto& entry : committed.entries) {
			if (entry.kind == EntryKind::Command) { host.applied_commands.push_back(entry.payload); }
		}
		consume(id, host.core->step(Applied{committed.entries.back().index}));
	}
	void start() {
		for (auto& [id, host] : hosts) {
			consume(id, host->core->step(Tick{now, 100 + 20 * (id - 1)}));
			consume(id, host->core->step(Start{}));
		}
	}
	void tick(std::uint64_t time) {
		now = time;
		for (auto& [id, host] : hosts) { consume(id, host->core->step(Tick{time, 100 + 20 * (id - 1)})); }
	}
	void elect(NodeId id = 1) {
		now = 100;
		consume(id, host(id).core->step(Tick{100, 100})); pump();
		check(host(id).core->role() == Role::Leader, "fixed voter majority elects leader");
	}
	void pump(std::size_t bound = 10000) {
		while (!network.empty() && bound--) {
			auto envelope = std::move(network.front()); network.pop_front();
			if (isolated.contains(envelope.from) || isolated.contains(envelope.to)) { continue; }
			consume(envelope.to, host(envelope.to).core->step(Tick{now, 100 + 20 * (envelope.to - 1)}));
			consume(envelope.to, host(envelope.to).core->step(Receive{envelope.from, std::move(envelope.message)}));
		}
		check(network.empty(), "message delivery drains within deterministic bound");
	}
};

void persistence_barriers() {
	Core core(config(1), empty(1)); core.step(Start{});
	auto election = core.step(Tick{100, 100}); auto persist = find<Persist>(election);
	check(persist && election.size() == 1 && !find<Send>(election), "term and self-vote persistence precedes election requests");
	if (!persist) { return; }
	auto token = persist->token;
	check(core.step(Persisted{token + 1}).empty() && core.busy(), "wrong persistence token cannot release election");
	check(find<Reject>(core.step(Propose{1, "busy"})), "proposal while persisting is rejected");
	check(core.step(Receive{2, VoteResponse{1, true}}).empty(), "retryable peer message may be dropped while persisting");
	auto requests = core.step(Persisted{token});
	check(find<Send>(requests) && core.durable_hard_state().voted_for == 1, "matching durable completion releases election requests");
	check(core.step(Persisted{token}).empty(), "duplicate completion has no effect");
	auto leader = core.step(Receive{2, VoteResponse{1, true}}); auto noop = find<Persist>(leader);
	check(noop && noop->batch.log && noop->batch.log->entries.front().kind == EntryKind::NoOp && !find<Send>(leader), "current-term no-op persists before replication");
	if (!noop) { return; }
	auto failure = core.step(Failed{FailureSource::Storage, noop->token, "disk full"});
	check(core.role() == Role::Fenced && find<Fenced>(failure), "failed no-op persistence fences consensus");
	check(core.step(Tick{500, 100}).empty(), "fenced node cannot campaign or acknowledge traffic");

	Core follower(config(2), empty(2)); follower.step(Start{});
	auto grant = follower.step(Receive{1, VoteRequest{3, 0, 0}}); auto ballot = find<Persist>(grant);
	check(ballot && !find<Send>(grant), "higher term and granted ballot persist before response");
	if (ballot) {
		auto response = follower.step(Persisted{ballot->token});
		check(find<Send>(response) && follower.durable_hard_state().term == 3, "durable vote response is released");
	}
	auto second_vote = follower.step(Receive{3, VoteRequest{3, 0, 0}}); auto denial = find<Send>(second_vote);
	check(denial && !std::get<VoteResponse>(denial->message).granted, "second same-term vote is denied");
	check(follower.step(Receive{99, VoteRequest{10, 0, 0}}).empty() && follower.term() == 3, "nonvoter cannot inflate term");
}

void replication_and_restart() {
	Network cluster; cluster.start(); cluster.elect();
	check(cluster.host(1).core->committed() == 1 && cluster.host(1).core->applied() == 1, "leader no-op commits and applies through durable majority");
	cluster.consume(1, cluster.host(1).core->step(Propose{7, "command"})); cluster.pump(); cluster.tick(120); cluster.pump();
	for (NodeId id : {1u, 2u, 3u}) { check(cluster.host(id).applied_commands == std::vector<std::string>{"command"}, "each voter applies command once"); }
	for (NodeId id : {1u, 2u, 3u}) {
		auto& host = cluster.host(id); host.restart(); cluster.consume(id, host.core->step(Start{}));
		check(host.applied_commands == std::vector<std::string>{"command"}, "restart rebuilds only durable committed application state");
	}
}

void affirmative_majority_and_inheritance() {
	Core candidate(config(1, 5), empty(1, 5)); candidate.step(Start{});
	auto initial = candidate.step(Tick{100, 100}); candidate.step(Persisted{find<Persist>(initial)->token});
	candidate.step(Receive{2, VoteResponse{1, true}}); candidate.step(Receive{3, VoteResponse{1, false}});
	check(candidate.role() == Role::Candidate, "two affirmative voters and denial cannot elect in five-voter configuration");
	candidate.step(Receive{2, VoteResponse{1, true}});
	check(candidate.role() == Role::Candidate, "duplicate affirmative vote cannot make a majority");
	auto elected = candidate.step(Receive{4, VoteResponse{1, true}});
	check(candidate.role() == Role::Leader && find<Persist>(elected), "third distinct affirmative voter elects and stages no-op");

	Network inherited;
	for (NodeId id : {1u, 2u}) {
		auto& host = inherited.host(id);
		host.journal.push_back(encode_storage_batch(StorageBatch{HardState{1, 3, 0}, LogMutation{1, {{1, 1, EntryKind::Command, "inherited"}}}}));
		host.restart();
	}
	inherited.start(); inherited.elect(); inherited.tick(120); inherited.pump();
	check(inherited.host(1).applied_commands == std::vector<std::string>{"inherited"} && inherited.host(1).core->committed() == 2,
		"current-term no-op commits inherited prefix without new user command");
}

void simultaneous_completions() {
	Network cluster(1); auto& host = cluster.host(1); host.auto_apply = false;
	cluster.start(); cluster.elect();
	check(host.applying && host.core->committed() == 1, "application completion may lag durable no-op commitment");
	host.auto_persist = false;
	cluster.consume(1, host.core->step(Propose{1, "next"}));
	check(host.pending && host.core->busy(), "new log persistence overlaps preceding application work");
	cluster.complete_apply(1);
	check(host.core->applied() == 1 && host.core->busy(), "Applied completion is retained during persistence");
	cluster.complete_disk(1);
	check(host.pending && !host.applying, "commit-index persistence precedes later application delivery");
	cluster.complete_disk(1);
	check(host.applying && host.core->committed() == 2, "later durable commitment delivers exactly one bounded range");
	cluster.complete_apply(1);
	check(host.applied_commands == std::vector<std::string>{"next"}, "overlapping disk/app work cannot lose completion");
}

void reads_and_partitions() {
	Network cluster; cluster.start(); cluster.elect();
	cluster.consume(1, cluster.host(1).core->step(Read{10}));
	check(cluster.host(1).ready_reads.empty(), "read cannot reuse election or preceding heartbeat responses");
	cluster.pump();
	check(cluster.host(1).ready_reads == std::vector<RequestId>{10}, "fresh quorum probe permits applied leader read");
	cluster.isolated.insert(1);
	cluster.consume(1, cluster.host(1).core->step(Read{11})); cluster.pump();
	check(cluster.host(1).ready_reads.size() == 1, "isolated old leader cannot complete a fresh read");
	cluster.now = 220;
	cluster.consume(2, cluster.host(2).core->step(Tick{220, 100})); cluster.pump();
	check(cluster.host(2).core->role() == Role::Leader, "surviving fixed majority elects successor under partition");
	cluster.isolated.clear(); cluster.tick(260); cluster.pump();
	check(cluster.host(1).core->role() == Role::Follower && cluster.host(1).ready_reads.size() == 1,
		"higher-term successor invalidates isolated leader's pending read");
}

void overlapping_reads_preserve_data() {
	Network cluster; cluster.start(); cluster.elect();
	cluster.consume(1, cluster.host(1).core->step(Propose{30, "delayed data"}));
	check(!cluster.network.empty(), "data replication starts before read admission");
	auto original = std::move(cluster.network.front()); cluster.network.pop_front();
	auto original_request = std::get<AppendRequest>(original.message);
	auto first = cluster.host(1).core->step(Read{31});
	auto second = cluster.host(1).core->step(Read{32});
	check(!find<Send>(first) && !find<Send>(second), "overlapping reads do not preempt outstanding data RPCs");
	cluster.consume(original.to, cluster.host(original.to).core->step(Receive{1, std::move(original.message)}));
	auto acknowledgement = std::find_if(cluster.network.begin(), cluster.network.end(), [&](const auto& envelope) {
		auto response = std::get_if<AppendResponse>(&envelope.message);
		return envelope.from == original.to && response && response->rpc == original_request.rpc;
	});
	check(acknowledgement != cluster.network.end(), "real follower produces delayed correlated data acknowledgement");
	if (acknowledgement == cluster.network.end()) { return; }
	auto reply = std::move(*acknowledgement); cluster.network.erase(acknowledgement);
	cluster.consume(1, cluster.host(1).core->step(Receive{reply.from, std::move(reply.message)}));
	check(cluster.host(1).core->committed() == 2, "delayed data acknowledgement remains valid after two reads");
	check(cluster.host(1).ready_reads.empty(), "data RPC issued before reads cannot authorize them");
	cluster.pump();
	check(cluster.host(1).ready_reads == std::vector<RequestId>({31, 32}), "queued overlapping reads complete through fresh probes");
}

void application_lag_does_not_spin_reads() {
	Network cluster; auto& leader = cluster.host(1); leader.auto_apply = false;
	cluster.start(); cluster.elect();
	cluster.consume(1, leader.core->step(Read{35})); cluster.pump(100);
	check(cluster.network.empty() && leader.ready_reads.empty(), "quorum-ready read awaiting Applied stops probing without advancing time");
	cluster.complete_apply(1);
	check(leader.ready_reads == std::vector<RequestId>{35}, "application completion releases quorum-ready read");

	Network partial(5); partial.start(); partial.elect();
	partial.isolated = {3, 4, 5};
	partial.consume(1, partial.host(1).core->step(Read{36})); partial.pump(100);
	check(partial.network.empty() && partial.host(1).ready_reads.empty(), "responsive minority stops probing while remaining read quorum is partitioned");

	Network capacity; capacity.start(); capacity.elect(); capacity.isolated.insert(3);
	auto& smaller = capacity.host(2); auto state = smaller.recovered(); auto configuration = state.configuration;
	smaller.limits.log_entries = 1; smaller.limits.control_entries = 1;
	smaller.core = std::make_unique<Core>(std::move(configuration), std::move(state), smaller.limits);
	capacity.consume(2, smaller.core->step(Tick{100, 120})); capacity.consume(2, smaller.core->step(Start{}));
	capacity.consume(1, capacity.host(1).core->step(Propose{37, "peer at capacity"})); capacity.pump(100);
	check(capacity.network.empty() && capacity.host(1).core->committed() == 1, "capacity rejection backs off without a fixed-time replication loop");
}

void stale_rpc_and_failure_transitions() {
	Network cluster; cluster.start(); cluster.elect();
	cluster.consume(1, cluster.host(1).core->step(Propose{40, "delayed"}));
	auto old = std::get<AppendRequest>(cluster.network.front().message);
	cluster.host(1).core->step(Tick{140, 100}); // Retry replaces expired RPC correlation.
	auto stale_success = cluster.host(1).core->step(Receive{2, AppendResponse{1, old.rpc, true, 2, 3, old.read_probe}});
	auto stale_failure = cluster.host(1).core->step(Receive{2, AppendResponse{1, old.rpc, false, 0, 1, old.read_probe}});
	check(stale_success.empty() && stale_failure.empty() && cluster.host(1).core->committed() == 1,
		"old successes and failures cannot advance or rewind a replaced RPC");
	auto higher = cluster.host(1).core->step(Receive{2, AppendResponse{2, old.rpc, false, 0, 1, old.read_probe}});
	check(find<Persist>(higher) && cluster.host(1).core->role() == Role::Follower && cluster.host(1).core->term() == 2,
		"valid higher term persists before obsolete RPC correlation is filtered");

	for (unsigned transition = 1; transition <= 5; ++transition) {
		Network failed(1); auto& host = failed.host(1); host.auto_persist = false; failed.start();
		failed.consume(1, host.core->step(Tick{100, 100}));
		for (unsigned phase = 1; phase < transition; ++phase) {
			failed.complete_disk(1);
			if (!host.pending) { failed.consume(1, host.core->step(Propose{phase, "command"})); }
		}
		check(host.pending.has_value(), "term/no-op/commit/command transition has pending storage");
		if (!host.pending) { continue; }
		auto failure = host.core->step(Failed{FailureSource::Storage, host.pending->token, "injected failure"});
		check(find<Fenced>(failure) && !find<Send>(failure) && !find<Committed>(failure), "each persistence transition fences before dependent effects");
	}
}

void bounds_and_semantic_recovery() {
	Limits limits; limits.log_entries = 4; limits.control_entries = 2;
	Network bounded(1, limits); bounded.start(); bounded.elect();
	bounded.consume(1, bounded.host(1).core->step(Propose{1, "first"}));
	auto full = bounded.host(1).core->step(Propose{2, "second"}); auto rejection = find<Reject>(full);
	check(rejection && rejection->reason == RejectReason::LogFull, "proposal admission retains control-entry reserve");
	check(bounded.host(1).core->last_index() == 2, "rejected admission neither truncates nor appends log");
	Recovery recovery(config(1)); recovery.replay(1, encode_initialization(config(1)));
	recovery.replay(2, encode_storage_batch(StorageBatch{HardState{1, 1, 0}, LogMutation{1, {{1, 1, EntryKind::Command, "old"}}}}));
	recovery.replay(3, encode_storage_batch(StorageBatch{HardState{2, 2, 0}, LogMutation{1, {{1, 2, EntryKind::Command, "replacement"}}}}));
	recovery.replay(4, encode_storage_batch(StorageBatch{HardState{2, 2, 1}, std::nullopt}));
	auto state = recovery.finish(4);
	check(state.entries.size() == 1 && state.entries.front().payload == "replacement" && state.hard.commit_index == 1,
		"logical suffix replacement replays from immutable storage operations");
	Recovery committed(config(1)); committed.replay(1, encode_initialization(config(1)));
	committed.replay(2, encode_storage_batch(StorageBatch{HardState{1, 1, 1}, LogMutation{1, {{1, 1, EntryKind::Command, "committed"}}}}));
	check(throws([&] { committed.replay(3, encode_storage_batch(StorageBatch{HardState{2, 2, 1}, LogMutation{1, {{1, 2, EntryKind::Command, "bad"}}}})); }),
		"replay cannot replace acknowledged committed prefix");
	check(throws([&] { committed.finish(2); }), "failed semantic replay cannot publish partial state");
	Recovery mismatch(config(2));
	check(throws([&] { mismatch.replay(1, encode_initialization(config(1))); }), "recovered state binds exact local voter configuration");
	Recovery ballot(config(1)); ballot.replay(1, encode_initialization(config(1)));
	ballot.replay(2, encode_storage_batch(StorageBatch{HardState{1, 1, 0}, std::nullopt}));
	check(throws([&] { ballot.replay(3, encode_storage_batch(StorageBatch{HardState{1, 2, 0}, std::nullopt})); }), "semantic replay rejects a second ballot in one term");
	Recovery content(config(1)); content.replay(1, encode_initialization(config(1)));
	content.replay(2, encode_storage_batch(StorageBatch{HardState{1, 1, 0}, LogMutation{1, {{1, 1, EntryKind::Command, "first"}}}}));
	check(throws([&] { content.replay(3, encode_storage_batch(StorageBatch{std::nullopt, LogMutation{1, {{1, 1, EntryKind::Command, "different"}}}})); }),
		"semantic replay rejects different content at the same index and term");

	RecoveredState retained = empty(2); retained.hard = HardState{1, 1, 0}; retained.entries = {{1, 1, EntryKind::Command, "first"}};
	Core conflict(config(2), std::move(retained)); conflict.step(Start{});
	auto rejected_content = conflict.step(Receive{1, AppendRequest{1, 1, 0, 0, 0, 0, {{1, 1, EntryKind::Command, "different"}}}});
	check(find<Fenced>(rejected_content) && !find<Send>(rejected_content), "live consensus fails closed on same-index same-term content conflict");

	RecoveredState exhausted = empty(1); exhausted.hard = HardState{1, 1, 4};
	for (Index index = 1; index <= 4; ++index) { exhausted.entries.push_back(Entry{index, 1, EntryKind::NoOp, {}}); }
	Core full_core(config(1), std::move(exhausted), limits); full_core.step(Start{});
	auto election = full_core.step(Tick{100, 100}); full_core.step(Persisted{find<Persist>(election)->token});
	auto unable_to_lead = full_core.step(Receive{2, VoteResponse{2, true}});
	check(full_core.role() == Role::Follower && full_core.last_index() == 4 && !find<Persist>(unable_to_lead),
		"exhausted control reserve cannot publish leadership or append beyond log bound");

	RecoveredState last_term = empty(1); last_term.hard = HardState{std::numeric_limits<Term>::max() - 1, std::nullopt, 0};
	Core term_core(config(1), std::move(last_term)); term_core.step(Start{});
	auto last_election = term_core.step(Tick{100, 100}); term_core.step(Persisted{find<Persist>(last_election)->token});
	auto last_noop = term_core.step(Receive{2, VoteResponse{std::numeric_limits<Term>::max(), true}});
	check(term_core.role() == Role::Leader && find<Persist>(last_noop), "final representable term may elect and append its no-op");
	term_core.step(Persisted{find<Persist>(last_noop)->token});
	auto step_down = term_core.step(Receive{2, AppendRequest{std::numeric_limits<Term>::max(), 900, 0, 0, 0, 0, {}}});
	check(term_core.role() == Role::Follower && !find<Fenced>(step_down), "maximum-term leader can step down without erasing its ballot");
	term_core.step(Tick{1000, 100});
	check(term_core.term() == std::numeric_limits<Term>::max() && term_core.role() == Role::Follower,
		"strict election terms cannot wrap after exhaustion");
}

void real_journal_integration() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("consensus-journal-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	std::vector<std::string> commands;
	{
		kronuz::journal::PosixIO io(directory); kronuz::journal::Journal journal(io, 1024 * 1024);
		kronuz::journal::Identity identity{}; identity[0] = 'J'; journal.create(identity);
		journal.append_batch(encode_initialization(config(1, 1)));
		Core core(config(1, 1), empty(1, 1));
		std::deque<Action> actions;
		auto enqueue = [&](Actions next) { for (auto& action : next) { actions.push_back(std::move(action)); } };
		auto pump = [&] {
			while (!actions.empty()) {
				auto action = std::move(actions.front()); actions.pop_front();
				if (auto persist = std::get_if<Persist>(&action)) {
					journal.append_batch(encode_storage_batch(persist->batch));
					enqueue(core.step(Persisted{persist->token}));
				} else if (auto committed = std::get_if<Committed>(&action)) {
					for (const auto& entry : committed->entries) { if (entry.kind == EntryKind::Command) { commands.push_back(entry.payload); } }
					enqueue(core.step(Applied{committed->entries.back().index}));
				} else if (std::holds_alternative<Fenced>(action)) { check(false, "real journal host unexpectedly fenced"); }
			}
		};
		enqueue(core.step(Start{})); enqueue(core.step(Tick{100, 100})); pump();
		enqueue(core.step(Propose{1, "durable command"})); pump();
		check(core.committed() == 2 && core.applied() == 2 && commands == std::vector<std::string>{"durable command"},
			"real POSIX journal establishes every consensus persistence completion");
	}
	{
		kronuz::journal::PosixIO io(directory); kronuz::journal::Journal journal(io, 1024 * 1024);
		Recovery recovery(config(1, 1));
		auto frontier = journal.recover([&](auto sequence, std::string_view batch) { recovery.replay(sequence, batch); });
		auto state = recovery.finish(frontier.sequence);
		check(state.hard.commit_index == 2 && state.entries.size() == 2, "actual journal reopening reconstructs strict committed state");
		Core restored(config(1, 1), std::move(state)); auto committed = restored.step(Start{});
		auto delivery = find<Committed>(committed);
		check(delivery && delivery->entries.back().payload == "durable command", "restored application delivery includes durable command");
	}
}

void reordered_crash_schedules(std::size_t voters, std::uint64_t seed) {
	Network cluster(voters); cluster.start(); std::mt19937_64 random(seed);
	auto consistent_prefixes = [&] {
		for (const auto& [left_id, left] : cluster.hosts) {
			for (const auto& [right_id, right] : cluster.hosts) {
				auto common = std::min(left->applied_commands.size(), right->applied_commands.size());
				check(std::equal(left->applied_commands.begin(), left->applied_commands.begin() + static_cast<std::ptrdiff_t>(common), right->applied_commands.begin()),
					"reordered/crash schedule preserves one applied command prefix");
			}
		}
	};
	for (unsigned iteration = 0; iteration < 1000; ++iteration) {
		cluster.tick(cluster.now + 1 + random() % 8);
		if (iteration % 200 == 0) { cluster.isolated.clear(); }
		if (random() % 31 == 0) {
			auto id = 1 + random() % voters;
			if (!cluster.isolated.erase(id)) { cluster.isolated.insert(id); }
		}
		if (random() % 83 == 0) {
			auto id = 1 + random() % voters; auto& host = cluster.host(id); host.restart();
			cluster.consume(id, host.core->step(Tick{cluster.now, 100 + 20 * (id - 1)}));
			cluster.consume(id, host.core->step(Start{}));
		}
		if (random() % 7 == 0) {
			auto id = 1 + random() % voters;
			cluster.consume(id, cluster.host(id).core->step(Propose{iteration + 1, std::to_string(seed) + ":" + std::to_string(iteration)}));
		}
		unsigned deliveries = static_cast<unsigned>(random() % 4);
		while (deliveries-- && !cluster.network.empty()) {
			auto position = cluster.network.begin() + static_cast<std::ptrdiff_t>(random() % cluster.network.size());
			auto envelope = std::move(*position); cluster.network.erase(position);
			if (cluster.isolated.contains(envelope.from) || cluster.isolated.contains(envelope.to) || random() % 5 == 0) { continue; }
			if (random() % 10 == 0) { cluster.network.push_back(envelope); }
			cluster.consume(envelope.to, cluster.host(envelope.to).core->step(Receive{envelope.from, std::move(envelope.message)}));
		}
		while (cluster.network.size() > 2048) { cluster.network.pop_front(); } // Bounded lossy transport model.
		if (iteration % 10 == 0) { consistent_prefixes(); }
	}
	cluster.isolated.clear();
	for (unsigned round = 0; round < 40; ++round) { cluster.tick(cluster.now + 10); cluster.pump(); }
	consistent_prefixes();
	NodeId leader = 0;
	for (const auto& [id, host] : cluster.hosts) { if (host->core->role() == Role::Leader) { leader = id; } }
	check(leader != 0, "healed reordered/crash schedule elects a leader");
	if (leader) {
		cluster.consume(leader, cluster.host(leader).core->step(Propose{2000, "after healing"})); cluster.pump();
		for (unsigned round = 0; round < 4; ++round) { cluster.tick(cluster.now + 10); cluster.pump(); }
		for (const auto& [id, host] : cluster.hosts) {
			check(!host->applied_commands.empty() && host->applied_commands.back() == "after healing", "healed voters converge and apply a new command");
		}
	}
	std::cout << "reordered schedule: voters=" << voters << ", seed=" << seed << '\n';
}
} // namespace

int main() {
	try { persistence_barriers(); replication_and_restart(); affirmative_majority_and_inheritance(); simultaneous_completions(); reads_and_partitions(); overlapping_reads_preserve_data(); application_lag_does_not_spin_reads(); stale_rpc_and_failure_transitions(); bounds_and_semantic_recovery(); real_journal_integration(); reordered_crash_schedules(3, 0x52414654); reordered_crash_schedules(5, 0x434c5553); }
	catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " consensus checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
