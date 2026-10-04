#include "consensus/core.h"
#include "consensus/admission.h"
#include "consensus/storage.h"
#include "consensus/checkpoint.h"
#include "consensus/snapshot.h"
#include "journal/journal.h"
#include "journal/posix.h"
#include "journal/store.h"
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

void delayed_completions_do_not_campaign() {
	Core candidate(config(1), empty(1)); candidate.step(Start{});
	auto election = candidate.step(Tick{100, 100}); auto ballot = find<Persist>(election);
	if (!ballot) { check(false, "delayed candidate has an election ballot"); return; }
	candidate.step(Tick{1000, 100}); auto completed = candidate.step(Persisted{ballot->token});
	check(candidate.term() == 1 && !candidate.busy() && !find<Persist>(completed) && find<Send>(completed), "delayed candidate completion releases votes without another campaign");
	auto next = candidate.step(Tick{1000, 100});
	check(find<Persist>(next) && candidate.term() == 2, "separately admitted tick at the same time may start the next campaign");
	Core follower(config(2), empty(2)); follower.step(Start{});
	auto voted = follower.step(Receive{1, VoteRequest{3, 0, 0}}); auto vote = find<Persist>(voted);
	if (!vote) { check(false, "delayed follower has a durable vote transition"); return; }
	follower.step(Tick{1000, 100}); auto response = follower.step(Persisted{vote->token});
	check(follower.term() == 3 && follower.role() == Role::Follower && !find<Persist>(response) && find<Send>(response), "delayed follower completion cannot opportunistically campaign");
	check(find<Persist>(follower.step(Tick{1000, 100})) && follower.term() == 4, "next admitted tick campaigns after delayed follower persistence");
	Core single(config(1, 1), empty(1, 1)); single.step(Start{});
	auto actions = single.step(Tick{100, 100}); unsigned completions = 0;
	while (auto persist = find<Persist>(actions)) {
		if (++completions > 4) { check(false, "single-voter continuation chain is bounded"); return; }
		auto token = persist->token; single.step(Tick{1000 + completions * 1000, 100}); actions = single.step(Persisted{token});
	}
	check(completions == 3 && single.role() == Role::Leader && single.committed() == 1 && find<Committed>(actions), "slow single-voter ballot no-op and commit finish without an extra tick");
	single.step(Applied{1});
	check(find<ReadReady>(single.step(Read{99})), "application delivery and fresh single-voter reads still complete immediately");
	Core leader(config(1), empty(1)); leader.step(Start{});
	auto initial = leader.step(Tick{100, 100}); leader.step(Persisted{find<Persist>(initial)->token});
	auto elected = leader.step(Receive{2, VoteResponse{1, true}}); auto noop = find<Persist>(elected);
	leader.step(Tick{1000, 100}); auto replication = leader.step(Persisted{noop->token});
	check(find<Send>(replication) && leader.role() == Role::Leader, "leader replication still drives immediately after slow no-op persistence");
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


void local_checkpoint_core() {
	auto state = empty(1); state.hard = HardState{1, 2, 3};
	for (Index index = 1; index <= 4; ++index) { state.entries.push_back(Entry{index, 1, EntryKind::Command, "value"}); }
	Limits limits; limits.rpc_entries = 1;
	Core core(config(1), state, limits);
	core.step(Start{}); auto second = core.step(Applied{1});
	check(find<Committed>(second) && find<Committed>(second)->first == 2, "checkpoint fixture has an outstanding application range");
	auto invalid = core.step(LocalCheckpoint{10, 7, 2, 1, config(1).cluster, config(1).configuration});
	check(find<Reject>(invalid) && !core.busy(), "checkpoint cannot include unapplied state");
	auto action = core.step(LocalCheckpoint{11, 7, 1, 1, config(1).cluster, config(1).configuration});
	auto checkpoint = find<PersistCheckpoint>(action);
	check(checkpoint && checkpoint->state.base_index == 1 && checkpoint->state.applied_index == 1 && checkpoint->state.entries.size() == 3 && checkpoint->state.entries.back().index == 4,
		"checkpoint captures the full suffix including uncommitted entries");
	if (!checkpoint) { return; }
	auto token = checkpoint->token;
	check(core.step(Persisted{token + 1}).empty() && core.base_index() == 0, "wrong checkpoint completion cannot compact");
	auto rejected = core.step(Propose{12, "busy"}); check(find<Reject>(rejected), "checkpoint cutover reserves persistence admission");
	core.step(Applied{2}); check(core.applied() == 2 && core.base_index() == 0, "reliable application completion survives pending checkpoint");
	auto completion = core.step(Persisted{token});
	check(core.base_index() == 1 && core.applied() == 2 && core.last_index() == 4 && find<CheckpointPublished>(completion), "checkpoint completion preserves newer live application state");
	check(find<Committed>(completion) && find<Committed>(completion)->first == 3, "checkpoint completion delivers only remaining unapplied entries");
	core.step(Applied{3});
	check(core.step(Persisted{token}).empty() && core.base_index() == 1, "duplicate checkpoint completion cannot compact twice");
	auto prefix = core.step(Receive{2, AppendRequest{1, 90, 0, 0, 3, 0, {}}});
	auto reply = find<Send>(prefix);
	check(reply && std::get<AppendResponse>(reply->message).compacted && std::get<AppendResponse>(reply->message).compacted->index == 1, "compacted follower reports its explicit boundary");
	Core restored(config(1), checkpoint->state, limits); auto replay = restored.step(Start{});
	check(restored.applied() == 1 && find<Committed>(replay) && find<Committed>(replay)->first == 2, "checkpoint restart replays committed suffix above application boundary");
	auto failure = restored.step(StorageFault{"artifact preparation failed"});
	check(restored.role() == Role::Fenced && find<Fenced>(failure), "out-of-band storage failure fences without pending persistence");
}


void compacted_index_exhaustion() {
	for (Index base : {std::numeric_limits<Index>::max() - 1, std::numeric_limits<Index>::max() - 2}) {
		auto state = empty(1, 1); state.base_index = state.applied_index = state.hard.commit_index = base;
		state.base_term = state.hard.term = 1;
		Core core(config(1, 1), state); core.step(Start{}); auto election = core.step(Tick{100, 100});
		auto vote = find<Persist>(election); check(vote, "near-exhausted voter durably records its ballot"); if (!vote) { continue; }
		auto elected = core.step(Persisted{vote->token});
		if (base == std::numeric_limits<Index>::max() - 1) {
			check(core.role() == Role::Follower && !find<Persist>(elected) && core.last_index() == base, "index exhaustion prevents an unrepresentable election no-op");
		} else {
			auto noop = find<Persist>(elected); check(noop && noop->batch.log->entries.back().index == base + 1, "final representable election no-op remains admissible");
			if (!noop) { continue; } auto commit = core.step(Persisted{noop->token}); auto persist_commit = find<Persist>(commit);
			if (persist_commit) { core.step(Persisted{persist_commit->token}); }
			auto proposal = core.step(Propose{1, "cannot wrap"});
			check(find<Reject>(proposal) && !find<Persist>(proposal) && core.last_index() == base + 1, "index exhaustion rejects proposal without wraparound");
		}
	}
}



const AppendRequest* append_to(const Actions& actions, NodeId peer) {
	for (const auto& action : actions) {
		if (auto send = std::get_if<Send>(&action); send && send->peer == peer) {
			if (auto request = std::get_if<AppendRequest>(&send->message)) { return request; }
		}
	}
	return nullptr;
}
Actions elect_checkpoint_fixture(Core& core) {
	core.step(Start{}); if (core.committed() > core.applied()) { core.step(Applied{core.committed()}); }
	auto election = core.step(Tick{100, 100}); auto vote = find<Persist>(election);
	if (!vote) { throw std::runtime_error("fixture election ballot"); }
	core.step(Persisted{vote->token}); auto majority = core.step(Receive{2, VoteResponse{core.term(), true}});
	auto noop = find<Persist>(majority); if (!noop) { throw std::runtime_error("fixture no-op"); }
	return core.step(Persisted{noop->token});
}
void compacted_replication() {
	auto state = empty(1); state.hard = HardState{1, 1, 2};
	for (Index index = 1; index <= 3; ++index) { state.entries.push_back(Entry{index, 1, EntryKind::Command, "value"}); }
	Core core(config(1), state); auto traffic = elect_checkpoint_fixture(core); auto first = append_to(traffic, 2);
	if (!first) { throw std::runtime_error("fixture first flight"); }
	auto rewind = core.step(Receive{2, AppendResponse{2, first->rpc, false, 0, 1, 0}}); auto old = append_to(rewind, 2);
	if (!old) { throw std::runtime_error("fixture prefix flight"); }
	auto hint = core.step(Receive{2, AppendResponse{2, old->rpc, false, 0, 3, 0, LogBoundary{2, 1}}});
	check(core.committed() == 2 && !find<Persist>(hint), "compacted boundary hint grants no commitment evidence");
	auto retry = core.step(Tick{140, 100}); auto verified = append_to(retry, 2);
	check(verified && verified->previous == 2, "compacted boundary selects a fresh Append prefix to verify");
	if (!verified) { return; }
	auto advance = core.step(Receive{2, AppendResponse{2, verified->rpc, true, 4, 5, verified->read_probe}});
	auto commit = find<Persist>(advance); check(commit && core.committed() == 2, "verified fresh Append stages commitment behind durability");
	if (!commit) { return; } core.step(Persisted{commit->token}); core.step(Applied{4});
	auto read = core.step(Read{80}); auto probe = append_to(read, 2); if (!probe) { throw std::runtime_error("fixture read probe"); }
	auto no_quorum = core.step(Receive{2, AppendResponse{2, probe->rpc, false, 0, 5, probe->read_probe, LogBoundary{4, 2}}});
	check(!find<ReadReady>(no_quorum), "compacted hint contributes no fresh read quorum");
	auto retries = core.step(Tick{180, 100}); auto fresh = append_to(retries, 2);
	if (!fresh) { throw std::runtime_error("fixture fresh read probe"); }
	auto ready = core.step(Receive{2, AppendResponse{2, fresh->rpc, true, 4, 5, fresh->read_probe}});
	check(find<ReadReady>(ready), "fresh verified Append establishes the pending read quorum");

	state.base_index = state.applied_index = 2; state.base_term = 1; state.entries.erase(state.entries.begin(), state.entries.begin() + 2);
	Core compacted(config(1), state); auto election = elect_checkpoint_fixture(compacted); auto append = append_to(election, 2);
	if (!append) { throw std::runtime_error("compacted fixture flight"); }
	auto needed = compacted.step(Receive{2, AppendResponse{2, append->rpc, false, 0, 1, 0}});
	check(find<SnapshotNeeded>(needed), "leader requests snapshot when a peer needs compacted history");
	for (std::uint64_t tick = 140; tick <= 340; tick += 40) {
		auto actions = compacted.step(Tick{tick, 100});
		check(!find<SnapshotNeeded>(actions) && compacted.role() == Role::Leader, "lagging peer cannot spin snapshot requests or fence leader");
	}


	auto empty_suffix = empty(1); empty_suffix.base_index = empty_suffix.applied_index = empty_suffix.hard.commit_index = 2;
	empty_suffix.base_term = empty_suffix.hard.term = 1;
	Core boundary_only(config(1), empty_suffix); auto boundary_traffic = elect_checkpoint_fixture(boundary_only); auto boundary_append = append_to(boundary_traffic, 2);
	if (!boundary_append) { throw std::runtime_error("empty suffix flight"); }
	check(boundary_append->previous == 2 && boundary_append->previous_term == 1, "empty retained suffix election uses checkpoint boundary term");
	auto boundary_ack = boundary_only.step(Receive{2, AppendResponse{2, boundary_append->rpc, true, 3, 4, 0}}); auto boundary_commit = find<Persist>(boundary_ack);
	if (!boundary_commit) { throw std::runtime_error("empty suffix commit"); } boundary_only.step(Persisted{boundary_commit->token}); boundary_only.step(Applied{3});
	auto boundary_read = boundary_only.step(Read{81}); auto boundary_probe = append_to(boundary_read, 2);
	if (!boundary_probe) { throw std::runtime_error("empty suffix read"); }
	auto boundary_ready = boundary_only.step(Receive{2, AppendResponse{2, boundary_probe->rpc, true, 3, 4, boundary_probe->read_probe}});
	check(find<ReadReady>(boundary_ready), "empty recovered suffix supports election replication and fresh quorum reads");

	state = empty(1); state.hard = HardState{1, 1, 2};
	for (Index index = 1; index <= 3; ++index) { state.entries.push_back(Entry{index, 1, EntryKind::Command, "value"}); }
	Core crossing(config(1), state); traffic = elect_checkpoint_fixture(crossing); first = append_to(traffic, 2);
	rewind = crossing.step(Receive{2, AppendResponse{2, first->rpc, false, 0, 1, 0}}); old = append_to(rewind, 2);
	if (!old) { throw std::runtime_error("crossing fixture flight"); } auto rpc = old->rpc;
	auto publication = crossing.step(LocalCheckpoint{90, 1, 2, 1, config(1).cluster, config(1).configuration}); auto checkpoint = find<PersistCheckpoint>(publication);
	if (!checkpoint) { throw std::runtime_error("crossing fixture checkpoint"); } crossing.step(Persisted{checkpoint->token});
	auto acknowledged = crossing.step(Receive{2, AppendResponse{2, rpc, true, 4, 5, 0}});
	check(crossing.base_index() == 2 && crossing.role() == Role::Leader && find<Persist>(acknowledged), "pre-cutover flight verifies its original suffix after local compaction");
}

void checkpoint_semantic_recovery() {
	auto state = empty(1); state.base_index = state.applied_index = 5; state.base_term = state.hard.term = 2; state.hard.commit_index = 260;
	for (Index index = 6; index <= 265; ++index) { state.entries.push_back(Entry{index, 2, EntryKind::Command, "retained"}); }
	kronuz::journal::ArtifactDescriptor application{}; application.identity[0] = 'A'; application.length = 17; application.checksum = 42;
	auto bytes = encode_checkpoint(CheckpointBundle{40, application, state});
	auto decoded = decode_checkpoint(bytes, config(1), 40, application);
	check(decoded.state.entries.size() == 260, "checkpoint decoder uses retained-log bound rather than Append RPC entry bound");
	check(!decoded.application_format, "legacy checkpoint application format remains unknown");
	for (std::uint32_t format : {std::uint32_t{0}, std::uint32_t{7}, std::numeric_limits<std::uint32_t>::max()}) {
		auto versioned = encode_checkpoint(CheckpointBundle{40, application, state, format});
		auto recovered = decode_checkpoint(versioned, config(1), 40, application);
		check(recovered.application_format == format && recovered.state.base_index == state.base_index && recovered.state.entries == state.entries && versioned.size() == bytes.size() + 4, "versioned checkpoint preserves explicit format including zero");
		for (std::size_t length = 0; length < versioned.size(); ++length) { check(throws([&] { decode_checkpoint(std::string_view(versioned).substr(0, length), config(1), 40, application); }), "every truncated versioned checkpoint fails closed"); }
	}
	Recovery recovery(config(1)); recovery.restore(std::move(decoded.state), 40);
	recovery.replay(41, encode_storage_batch(StorageBatch{HardState{3, 2, 260}, LogMutation{261, {{261, 3, EntryKind::Command, "replacement"}}}}));
	auto restored = recovery.finish(41);
	check(restored.base_index == 5 && restored.applied_index == 5 && restored.entries.back().index == 261 && restored.hard.commit_index == 260,
		"checkpoint semantic replay replaces only the uncommitted suffix with base-aware offsets");
	for (std::size_t length = 0; length < bytes.size(); ++length) {
		check(throws([&] { decode_checkpoint(std::string_view(bytes).substr(0, length), config(1), 40, application); }), "every truncated checkpoint bundle fails closed");
	}
	check(throws([&] { decode_checkpoint(bytes, config(2), 40, application); }), "checkpoint cannot import another local voter identity");
	check(throws([&] { decode_checkpoint(bytes, config(1), 41, application); }), "checkpoint cannot substitute covered storage sequence");
	auto different = application; ++different.length;
	check(throws([&] { decode_checkpoint(bytes, config(1), 40, different); }), "checkpoint application reference must match manifest dependency");
	state.hard.commit_index = 4;
	check(throws([&] { encode_checkpoint(CheckpointBundle{40, application, state}); }), "checkpoint cannot exceed durable commitment");
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
		std::optional<kronuz::journal::PreparedArtifact> application;
		std::deque<Action> actions;
		auto enqueue = [&](Actions next) { for (auto& action : next) { actions.push_back(std::move(action)); } };
		auto pump = [&] {
			while (!actions.empty()) {
				auto action = std::move(actions.front()); actions.pop_front();
				if (auto persist = std::get_if<Persist>(&action)) {
					journal.append_batch(encode_storage_batch(persist->batch));
					enqueue(core.step(Persisted{persist->token}));
				} else if (auto checkpoint = std::get_if<PersistCheckpoint>(&action)) {
					if (!application) { throw std::logic_error("missing immutable capture"); }
					auto sequence = journal.frontier().sequence;
					auto bytes = encode_checkpoint(checkpoint->state, sequence, application->descriptor());
					auto builder = journal.prepare_artifact();
					while (!bytes.empty()) {
						auto count = std::min(bytes.size(), std::size_t(64 * 1024)); builder.append_chunk(std::string_view(bytes).substr(0, count)); bytes.erase(0, count);
					}
					auto bundle = builder.finish(); journal.publish_checkpoint(bundle, std::array{*application}, sequence);
					// Simulate process death after durable publication but before
					// delivering the completion to the old protocol executor.
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
		// Pin nonempty application state at A=2, then advance the live state
		// before publishing the older image and its retained command suffix.
		auto builder = journal.prepare_artifact(); builder.append_chunk("durable command"); application = builder.finish();
		enqueue(core.step(Propose{2, "after snapshot"})); pump();
		enqueue(core.step(LocalCheckpoint{3, 1, 2, 1, config(1, 1).cluster, config(1, 1).configuration})); pump();
		check(core.base_index() == 0 && core.applied() == 3 && core.busy(), "durable checkpoint cannot compact the old core without its completion");
	}
	{
		kronuz::journal::PosixIO io(directory); kronuz::journal::Journal journal(io, 1024 * 1024);
		Recovery recovery(config(1, 1)); std::vector<std::string> restored_commands;
		auto frontier = journal.recover([&](auto sequence, std::string_view batch) { recovery.replay(sequence, batch); },
			[&](const auto& selected, auto& bundle, auto dependencies) {
				if (dependencies.size() != 1 || selected.dependencies.size() != 1) { throw std::runtime_error("checkpoint dependency count"); }
				std::string bytes(static_cast<std::size_t>(bundle.descriptor().length), '\0'); std::size_t offset = 0;
				while (offset < bytes.size()) { auto count = std::min(bytes.size() - offset, std::size_t(64 * 1024)); offset += bundle.read_at(offset, std::span<char>(bytes.data() + offset, count)); }
				auto decoded = decode_checkpoint(bytes, config(1, 1), selected.base_sequence, selected.dependencies[0]);
				recovery.restore(std::move(decoded.state), decoded.storage_sequence);
				std::string image(static_cast<std::size_t>(dependencies[0].descriptor().length), '\0');
				std::size_t read = 0;
				while (read < image.size()) { read += dependencies[0].read_at(read, std::span<char>(image.data() + read, image.size() - read)); }
				restored_commands.push_back(std::move(image));
			});
		auto state = recovery.finish(frontier.sequence);
		check(state.base_index == 2 && state.hard.commit_index == 3 && state.entries.size() == 1, "actual journal reopening reconstructs strict committed state");
		Core restored(config(1, 1), std::move(state)); auto committed = restored.step(Start{});
		auto delivery = find<Committed>(committed);
		check(delivery && delivery->entries.back().payload == "after snapshot", "restored application delivery includes only committed suffix command");
		if (delivery) {
			for (const auto& entry : delivery->entries) { if (entry.kind == EntryKind::Command) { restored_commands.push_back(entry.payload); } }
			restored.step(Applied{delivery->entries.back().index});
		}
		check(restored_commands == std::vector<std::string>{"durable command", "after snapshot"} && restored.applied() == 3,
			"crash between publication and completion restores nonempty application state and replays suffix exactly once");
	}
}

void real_store_integration() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("consensus-store-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	std::vector<std::string> commands;
	{
		kronuz::journal::PosixIO io(directory); kronuz::journal::Store journal(io, {{16 * 1024 * 1024, 256}, {16 * 1024, 4}, {2 * 1024 * 1024 + 1024, 4}, 32}, 1024 * 1024);
		kronuz::journal::Identity identity{}; identity[0] = 'J'; journal.create(identity);
		while (!journal.inventory_step(1).complete) {}
		auto append = [&](kronuz::journal::AdmissionClass kind, std::string_view bytes) {
			auto reservation = journal.reserve_append(kind, bytes.size());
			if (!reservation) { throw std::runtime_error("unexpected Store pressure in integration fixture"); }
			journal.append(*reservation, bytes);
		};
		append(kronuz::journal::AdmissionClass::Control, encode_initialization(config(1, 1)));
		Core core(config(1, 1), empty(1, 1));
		std::optional<kronuz::journal::ArtifactDescriptor> application;
		std::optional<kronuz::journal::ReplacementId> replacement;
		std::deque<Action> actions;
		auto enqueue = [&](Actions next) { for (auto& action : next) { actions.push_back(std::move(action)); } };
		auto pump = [&] {
			while (!actions.empty()) {
				auto action = std::move(actions.front()); actions.pop_front();
				if (auto persist = std::get_if<Persist>(&action)) {
					auto kind = kronuz::journal::AdmissionClass::Control;
					if (persist->batch.log && std::any_of(persist->batch.log->entries.begin(), persist->batch.log->entries.end(), [](const Entry& entry) { return entry.kind == EntryKind::Command; })) { kind = kronuz::journal::AdmissionClass::Normal; }
					append(kind, encode_storage_batch(persist->batch));
					enqueue(core.step(Persisted{persist->token}));
				} else if (auto checkpoint = std::get_if<PersistCheckpoint>(&action)) {
					if (!application) { throw std::logic_error("missing immutable capture"); }
					auto sequence = journal.frontier().sequence;
					auto bytes = encode_checkpoint(checkpoint->state, sequence, *application);
					journal.begin_artifact(*replacement, kronuz::journal::ArtifactPart::Bundle);
					while (!bytes.empty()) {
						auto count = std::min(bytes.size(), std::size_t(64 * 1024)); journal.write_chunk(*replacement, std::string_view(bytes).substr(0, count)); bytes.erase(0, count);
					}
					journal.finish_artifact(*replacement); journal.publish(*replacement, sequence);
					// Simulate process death after durable publication but before
					// delivering the completion to the old protocol executor.
				} else if (auto committed = std::get_if<Committed>(&action)) {
					for (const auto& entry : committed->entries) { if (entry.kind == EntryKind::Command) { commands.push_back(entry.payload); } }
					enqueue(core.step(Applied{committed->entries.back().index}));
				} else if (std::holds_alternative<Fenced>(action)) { check(false, "real Store host unexpectedly fenced"); }
			}
		};
		enqueue(core.step(Start{})); enqueue(core.step(Tick{100, 100})); pump();
		enqueue(core.step(Propose{1, "durable command"})); pump();
		check(core.committed() == 2 && core.applied() == 2 && commands == std::vector<std::string>{"durable command"},
			"real POSIX Store establishes every consensus persistence completion");
		// Pin nonempty application state at A=2, then advance the live state
		// before publishing the older image and its retained command suffix.
		replacement = journal.reserve_replacement(1024, 1024 * 1024);
		if (!replacement) { throw std::runtime_error("checkpoint capacity unavailable"); }
		journal.begin_artifact(*replacement, kronuz::journal::ArtifactPart::Application); journal.write_chunk(*replacement, "durable command"); application = journal.finish_artifact(*replacement);
		enqueue(core.step(Propose{2, "after snapshot"})); pump();
		enqueue(core.step(LocalCheckpoint{3, 1, 2, 1, config(1, 1).cluster, config(1, 1).configuration})); pump();
		check(core.base_index() == 0 && core.applied() == 3 && core.busy(), "durable checkpoint cannot compact the old core without its completion");
	}
	{
		kronuz::journal::PosixIO io(directory); kronuz::journal::Store journal(io, {{16 * 1024 * 1024, 256}, {16 * 1024, 4}, {2 * 1024 * 1024 + 1024, 4}, 32}, 1024 * 1024);
		Recovery recovery(config(1, 1)); std::vector<std::string> restored_commands;
		auto frontier = journal.recover([&](auto sequence, std::string_view batch) { recovery.replay(sequence, batch); },
			[&](const auto& selected, auto& bundle, auto dependencies) {
				if (dependencies.size() != 1 || selected.dependencies.size() != 1) { throw std::runtime_error("checkpoint dependency count"); }
				std::string bytes(static_cast<std::size_t>(bundle.descriptor().length), '\0'); std::size_t offset = 0;
				while (offset < bytes.size()) { auto count = std::min(bytes.size() - offset, std::size_t(64 * 1024)); offset += bundle.read_at(offset, std::span<char>(bytes.data() + offset, count)); }
				auto decoded = decode_checkpoint(bytes, config(1, 1), selected.base_sequence, selected.dependencies[0]);
				recovery.restore(std::move(decoded.state), decoded.storage_sequence);
				std::string image(static_cast<std::size_t>(dependencies[0].descriptor().length), '\0');
				std::size_t read = 0;
				while (read < image.size()) { read += dependencies[0].read_at(read, std::span<char>(image.data() + read, image.size() - read)); }
				restored_commands.push_back(std::move(image));
			});
		while (!journal.inventory_step(1).complete) {}
		auto state = recovery.finish(frontier.sequence);
		check(state.base_index == 2 && state.hard.commit_index == 3 && state.entries.size() == 1, "actual journal reopening reconstructs strict committed state");
		Core restored(config(1, 1), std::move(state)); auto committed = restored.step(Start{});
		auto delivery = find<Committed>(committed);
		check(delivery && delivery->entries.back().payload == "after snapshot", "restored application delivery includes only committed suffix command");
		if (delivery) {
			for (const auto& entry : delivery->entries) { if (entry.kind == EntryKind::Command) { restored_commands.push_back(entry.payload); } }
			restored.step(Applied{delivery->entries.back().index});
		}
		check(restored_commands == std::vector<std::string>{"durable command", "after snapshot"} && restored.applied() == 3,
			"crash between publication and completion restores nonempty application state and replays suffix exactly once");
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

void event_admission_plans() {
	using kronuz::journal::AdmissionClass;
	auto tick = plan_event(Tick{100, 100}, false);
	check(tick.count == 3 && tick.appends[0].encoded_bytes == 34 && tick.appends[1].encoded_bytes == 43 && tick.appends[2].encoded_bytes == 34, "campaign admission owns its complete three-write continuation");
	check(plan_event(Tick{100, 100}, true).count == 0, "time updates during persistence require no new pack");
	auto proposal = plan_event(Propose{1, ""}, false);
	check(proposal.count == 2 && proposal.appends[0].kind == AdmissionClass::Normal && proposal.appends[1].kind == AdmissionClass::Control, "empty commands preserve normal/control classification");
	AppendRequest request{1, 1, 0, 0, 0, 0, {}};
	for (unsigned i = 0; i < 256; ++i) { request.entries.push_back({i + 1, 1, EntryKind::NoOp, ""}); }
	auto noop = plan_event(Receive{2, request}, false);
	check(noop.count == 1 && noop.appends[0].kind == AdmissionClass::Control && noop.appends[0].encoded_bytes == 5422, "maximum no-op RPC uses checked protected framing bound");
	request.entries[0].kind = EntryKind::Command;
	check(plan_event(Receive{2, request}, false).appends[0].kind == AdmissionClass::Normal, "any command moves the entire atomic RPC to normal admission");
	request.entries.push_back({257, 1, EntryKind::NoOp, ""});
	check(throws([&] { plan_event(Receive{2, request}, true); }), "busy admission still rejects oversized typed RPCs");
	check(throws([] { plan_event(Persisted{1}, false); }) && throws([] { plan_event(LocalCheckpoint{1, 1, 1, 1, {}, {}}, false); }), "external persistence completions and raw checkpoint requests reject explicitly");
	check(plan_event(Start{}, false).count == 0 && plan_event(Read{1}, false).count == 0, "nonpersistent events do not charge a storage pack");
	for (Message message : {Message{VoteResponse{9, false}}, Message{AppendRequest{9, 1, 0, 0, 0, 0, {}}}}) {
		Core core(config(1), empty(1)); core.step(Start{});
		auto event = Receive{2, message}; auto plan = plan_event(event, false);
		auto actions = core.step(event); auto persist = find<Persist>(actions);
		check(persist && plan.count > 0 && storage_batch_size(persist->batch) <= plan.appends[0].encoded_bytes, "higher-term hard-state writes fit response and empty-append plans");
	}
	for (auto kind : {EntryKind::NoOp, static_cast<EntryKind>(99)}) {
		AppendRequest malformed{1, 1, 0, 0, 0, 0, {{1, 1, kind, "payload"}}};
		check(throws([&] { plan_event(Receive{2, malformed}, true); }), "busy malformed no-op and unknown-kind RPCs reject before admission");
	}
	Limits small; small.command_bytes = 4; small.rpc_bytes = 5;
	AppendRequest aggregate{1, 1, 0, 0, 0, 0, {{1, 1, EntryKind::Command, "four"}, {2, 1, EntryKind::Command, "four"}}};
	check(throws([&] { plan_event(Receive{2, aggregate}, false, small); }), "aggregate RPC payload bounds reject before planning");
}

void storage_footprint_plans() {
	using namespace cluster::consensus;
	for (bool hard : {false, true}) {
		for (bool log : {false, true}) {
			for (unsigned count : {0u, 1u, 256u}) {
				if (!log && count) { continue; }
				StorageBatch batch;
				if (hard) { batch.hard = HardState{3, 1, 0}; }
				if (log) {
					batch.log = LogMutation{1, {}};
					for (unsigned i = 0; i < count; ++i) { batch.log->entries.push_back({i + 1, 3, EntryKind::Command, std::string(i % 7, 'x')}); }
				}
				check(storage_batch_size(batch) == encode_storage_batch(batch).size(), "planned storage footprint equals actual mixed-payload encoding");
			}
		}
	}
	check(storage_batch_size(true, false) == 34 && storage_batch_size(false, true, 1) == 43, "control continuation framing bounds match codec");
	check(throws([] { storage_batch_size(false, false, 1); }) && throws([] { storage_batch_size(false, true, 0, 1); }), "invalid planning shapes reject without IO");
	check(throws([] { storage_batch_size(true, true, 1, std::numeric_limits<std::size_t>::max()); }), "payload aggregate overflow rejects without allocation");
	if constexpr (sizeof(std::size_t) > sizeof(std::uint32_t)) {
		check(throws([] { storage_batch_size(false, true, static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) + 1); }), "entry count beyond wire representation rejects");
	}
}

void portable_snapshot_descriptors() {
	auto fixed = config(1); SnapshotPolicy policy{fixed.cluster, fixed.configuration, 7, 4096};
	SnapshotDescriptor descriptor{fixed.cluster, fixed.configuration, 7, 42, 3, 9, kronuz::journal::crc32c("123456789")};
	check(descriptor.application_crc32c == 0xe3069283u && kronuz::journal::crc32c("") == 0, "portable payload checksum uses standard CRC32C vectors");
	auto bytes = encode_snapshot(descriptor, policy);
	check(bytes.size() == 72 && bytes.substr(0, 8) == "RFTSNP01" && decode_snapshot(bytes, policy) == descriptor, "portable snapshot has exact versioned framing and round trips");
	check(static_cast<unsigned char>(bytes[40]) == 7 && static_cast<unsigned char>(bytes[44]) == 42 && static_cast<unsigned char>(bytes[52]) == 3 && static_cast<unsigned char>(bytes[60]) == 9, "portable scalar fields use fixed little-endian offsets");
	auto wide = descriptor; auto wide_policy = policy;
	wide_policy.application_format = wide.application_format = 0x01020304u;
	wide.through = 0x0102030405060708ull; wide.term = 0x1112131415161718ull;
	wide_policy.maximum_application_bytes = wide.application_bytes = 0x2122232425262728ull;
	wide.application_crc32c = 0x31323334u;
	auto wide_bytes = encode_snapshot(wide, wide_policy);
	check(wide_bytes.substr(40) == std::string_view("\x04\x03\x02\x01\x08\x07\x06\x05\x04\x03\x02\x01\x18\x17\x16\x15\x14\x13\x12\x11\x28\x27\x26\x25\x24\x23\x22\x21\x34\x33\x32\x31", 32), "multi-byte scalar wire fixture is independently little endian");
	check(decode_snapshot(wide_bytes, wide_policy) == wide, "wide portable scalar fields round trip");
	for (std::size_t length = 0; length < bytes.size(); ++length) {
		check(throws([&] { decode_snapshot(std::string_view(bytes).substr(0, length), policy); }), "every truncated descriptor rejects before payload allocation");
	}
	check(throws([&] { decode_snapshot(bytes + "x", policy); }), "trailing snapshot framing rejects");
	auto unknown = bytes; unknown[7] = '2';
	check(throws([&] { decode_snapshot(unknown, policy); }), "unknown portable snapshot version rejects");
	for (std::size_t offset : {std::size_t{8}, std::size_t{24}, std::size_t{40}}) {
		auto foreign = bytes; foreign[offset] ^= 1;
		check(throws([&] { decode_snapshot(foreign, policy); }), "foreign identity or unsupported application format rejects");
	}
	auto invalid = descriptor;
	invalid.through = 0; check(throws([&] { encode_snapshot(invalid, policy); }), "zero included index rejects");
	invalid.through = std::numeric_limits<Index>::max(); check(throws([&] { encode_snapshot(invalid, policy); }), "maximum included index rejects instead of overflowing a next index");
	invalid = descriptor; invalid.term = 0; check(throws([&] { encode_snapshot(invalid, policy); }), "zero included term rejects");
	invalid = descriptor; invalid.application_bytes = 4097; check(throws([&] { encode_snapshot(invalid, policy); }), "payload length above receiver policy rejects");
	invalid.application_bytes = std::numeric_limits<std::uint64_t>::max(); check(throws([&] { encode_snapshot(invalid, policy); }), "maximum declared payload rejects without allocation");
	invalid = descriptor; invalid.application_bytes = 0;
	check(throws([&] { encode_snapshot(invalid, policy); }), "empty payload requires the empty CRC32C");
	invalid.application_crc32c = 0; check(decode_snapshot(encode_snapshot(invalid, policy), policy) == invalid, "empty application image is valid");
	invalid = descriptor; invalid.through = std::numeric_limits<Index>::max() - 1; invalid.term = std::numeric_limits<Term>::max(); invalid.application_bytes = policy.maximum_application_bytes;
	check(decode_snapshot(encode_snapshot(invalid, policy), policy) == invalid, "maximum legal index, term, and receiver length round trip");
	policy.application_format = invalid.application_format = 0;
	check(decode_snapshot(encode_snapshot(invalid, policy), policy) == invalid, "application format zero is accepted only by explicit matching policy");
	policy.maximum_application_bytes = invalid.application_bytes = std::numeric_limits<std::uint64_t>::max();
	check(decode_snapshot(encode_snapshot(invalid, policy), policy) == invalid, "explicit wide payload policy does not allocate declared application bytes");
}

void portable_snapshot_installation() {
	auto fixture = [] {
		auto state = empty(1); state.hard = {3, 2, 2};
		state.entries = {{1, 1, EntryKind::Command, "first"}, {2, 1, EntryKind::Command, "second"}, {3, 2, EntryKind::Command, "boundary"}, {4, 3, EntryKind::Command, "suffix"}};
		return state;
	};
	auto request = [] {
		auto fixed = config(1); return InstallPrepared{13, 77, 2, 3, fixed.cluster, fixed.configuration, {3, 2}};
	};
	for (bool before : {false, true}) { for (bool retain : {false, true}) { for (bool higher : {false, true}) {
		Core core(config(1), fixture()); auto initial = core.step(Start{});
		check(find<Committed>(initial) && find<Committed>(initial)->entries.back().index == 2, "install fixture has an outstanding committed application range");
		auto prepared = request(); prepared.leader_term += higher; if (!retain) { prepared.boundary.term = 3; }
		auto actions = core.step(prepared); auto persist = find<PersistInstall>(actions);
		check(persist && !find<ActivateInstall>(actions) && !find<InstallCompleted>(actions) && core.busy(), "prepared installation exposes persistence only");
		if (!persist) { continue; } auto token = persist->token;
		check(persist->prepared == 77 && persist->state.configuration == config(1) && persist->state.hard.term == prepared.leader_term &&
			persist->state.hard.voted_for == (higher ? std::optional<NodeId>{} : std::optional<NodeId>{2}) && persist->state.hard.commit_index == 3 &&
			persist->state.base_index == 3 && persist->state.base_term == prepared.boundary.term && persist->state.applied_index == 3 &&
			persist->state.entries.size() == (retain ? 1 : 0), "installation derives receiver identity, ballot and boundary-matched suffix");
		Recovery validator(config(1)); validator.validate_checkpoint_state(persist->state);
		check(core.step(Persisted{token + 100}).empty() && core.step(InstallActivated{token}).empty() && core.committed() == 2 && core.base_index() == 0, "wrong persistence and premature activation release no installation effects");
		check(core.step(Tick{1000, 100}).empty() && core.step(Receive{3, VoteRequest{9, 4, 3}}).empty(), "busy installation advances time without campaigning or peer processing");
		if (before) { check(core.step(Applied{2}).empty() && core.applied() == 2, "outstanding application completion remains reliable before installation persistence"); }
		auto published = core.step(Persisted{token}); auto activate = find<ActivateInstall>(published);
		check(activate && activate->token == token && activate->prepared == 77 && activate->boundary.index == 3 &&
			!find<InstallCompleted>(published) && !find<Committed>(published) && !find<Send>(published) && core.busy() && core.base_index() == 3 &&
			core.committed() == 3 && core.last_index() == (retain ? 4 : 3) && core.applied() == (before ? 2 : 0), "durable publication requests activation while isolating the transitional application cursor");
		check(core.step(Persisted{token}).empty() && core.step(Tick{1100, 100}).empty() && core.step(InstallActivated{token + 100}).empty(), "published installation cannot complete or campaign from duplicate, stale or timer events");
		auto blocked = core.step(Propose{14, "blocked"}); auto blocked_read = core.step(Read{15});
		check(find<Reject>(blocked) && find<Reject>(blocked)->reason == RejectReason::Busy && find<Reject>(blocked_read) && find<Reject>(blocked_read)->reason == RejectReason::Busy, "ordinary proposals and reads stay gated through activation");
		if (!before) { check(core.step(Applied{2}).empty() && core.applied() == 2, "outstanding application completion remains reliable after installation persistence"); }
		auto completed = core.step(InstallActivated{token}); auto success = find<InstallCompleted>(completed);
		check(success && success->request == 13 && success->prepared == 77 && success->boundary.index == 3 && !core.busy() && core.applied() == 3 && core.role() == Role::Follower, "matching activation alone publishes installation success and releases admission");
		check(core.step(InstallActivated{token}).empty() && core.step(Tick{1100, 100}).empty(), "activation resets the follower deadline and duplicates are harmless");
		Entry next{4, 3, EntryKind::Command, retain ? "suffix" : "new suffix"};
		auto resumed = core.step(Receive{2, AppendRequest{prepared.leader_term, 11, 3, prepared.boundary.term, 4, 0, {next}}});
		auto append = find<Persist>(resumed);
		check(append && append->batch.hard && append->batch.hard->commit_index == 4 && !find<Committed>(resumed), "resumed replication persists the next commitment before delivery");
		if (append) {
			auto delivered = core.step(Persisted{append->token}); auto committed = find<Committed>(delivered);
			check(committed && committed->first == 4 && committed->entries.size() == 1 && committed->entries.front() == next, "resumed application receives the retained or newly appended suffix payload");
			core.step(Applied{4}); check(core.committed() == 4 && core.applied() == 4 && core.base_index() == 3, "resumed follower durably commits and applies beyond the installed image");
		}
		auto election = core.step(Tick{1200, 100}); check(find<Persist>(election), "a separately admitted expired Tick may campaign after installation activation");
	} } }
	for (bool higher : {false, true}) {
		Core core(config(1), fixture()); core.step(Start{}); auto prepared = request(); prepared.boundary = {2, 1}; prepared.leader_term += higher;
		auto stale = core.step(prepared);
		if (higher) {
			auto hard = find<Persist>(stale); check(hard && hard->batch.hard && !find<InstallRejected>(stale) && !find<PersistInstall>(stale), "caught-up higher-term snapshot persists its receiver term before rejection");
			if (!hard) { continue; } stale = core.step(Persisted{hard->token});
		}
		auto rejected = find<InstallRejected>(stale);
		check(rejected && rejected->reason == InstallRejectReason::CaughtUp && core.base_index() == 0 && core.committed() == 2 && core.applied() == 0 &&
			core.durable_hard_state().voted_for == (higher ? std::optional<NodeId>{} : std::optional<NodeId>{2}), "caught-up images preserve application state and the proper receiver ballot");
	}
	for (unsigned invalid = 0; invalid < 12; ++invalid) {
		Core core(config(1), fixture()); core.step(Start{}); auto prepared = request();
		switch (invalid) {
		case 0: prepared.request = 0; break; case 1: prepared.prepared = 0; break;
		case 2: prepared.authenticated_peer = 1; break; case 3: prepared.authenticated_peer = 4; break;
		case 4: prepared.cluster[0] ^= 1; break; case 5: prepared.configuration[0] ^= 1; break;
		case 6: prepared.leader_term = 0; break; case 7: prepared.leader_term = 2; break;
		case 8: prepared.boundary.index = 0; break; case 9: prepared.boundary.index = std::numeric_limits<Index>::max(); break;
		case 10: prepared.boundary.term = 0; break; case 11: prepared.boundary.term = 4; break;
		}
		auto actions = core.step(prepared); auto rejected = find<InstallRejected>(actions);
		check(rejected && rejected->reason == (invalid == 7 ? InstallRejectReason::StaleTerm : InstallRejectReason::Invalid) && !core.busy() && core.term() == 3 && core.committed() == 2, "invalid or stale sender context changes no receiver state");
	}
	{
		Core unstarted(config(1), fixture()); auto rejected = unstarted.step(request());
		check(find<InstallRejected>(rejected) && find<InstallRejected>(rejected)->reason == InstallRejectReason::Busy && !unstarted.busy(), "unstarted Core does not admit installation");
		Core core(config(1), fixture()); core.step(Start{}); core.step(Tick{100, 100}); auto term = core.term();
		auto prepared = request(); prepared.leader_term = 9; rejected = core.step(prepared);
		check(find<InstallRejected>(rejected) && find<InstallRejected>(rejected)->reason == InstallRejectReason::Busy && core.busy() && core.term() == term, "ordinary pending persistence rejects installation without observing another term");
	}
	{
		Core core(config(1), fixture()); core.step(Start{}); core.step(Applied{2}); auto prepared = request();
		prepared.boundary.index = std::numeric_limits<Index>::max() - 1;
		prepared.boundary.term = prepared.leader_term = std::numeric_limits<Term>::max();
		auto actions = core.step(prepared); auto persist = find<PersistInstall>(actions);
		check(persist && persist->state.entries.empty() && !persist->state.hard.voted_for, "maximum legal installed boundary and term preserve no incompatible suffix or ballot");
		if (persist) {
			auto token = persist->token; core.step(Persisted{token}); core.step(InstallActivated{token});
			check(core.last_index() == prepared.boundary.index && core.applied() == prepared.boundary.index && core.step(Tick{1000, 100}).empty() && core.role() == Role::Follower, "installed index and term exhaustion cannot wrap into a campaign");
		}
	}
	for (unsigned failure = 0; failure < 5; ++failure) {
		Core core(config(1), fixture()); core.step(Start{}); auto actions = core.step(request()); auto token = find<PersistInstall>(actions)->token;
		check(core.step(Failed{FailureSource::Storage, token + 100, "stale"}).empty(), "stale install storage failure cannot fence another operation");
		if (failure != 0 && failure != 4) { core.step(Persisted{token}); }
		Actions failed;
		if (failure == 0) { failed = core.step(Failed{FailureSource::Storage, token, "disk"}); }
		else if (failure == 1) { failed = core.step(InstallActivated{token}); }
		else if (failure == 2) {
			check(core.step(InstallActivationFailed{token + 100, "stale"}).empty(), "stale activation failure is ignored");
			failed = core.step(InstallActivationFailed{token, "application"});
		} else { failed = core.step(Failed{FailureSource::Application, 2, "old application"}); }
		check(find<Fenced>(failed) && core.role() == Role::Fenced && !core.busy(), "storage, early activation, activation failure and old application failure fence installation");
	}
	for (const Event& internal : std::vector<Event>{request(), InstallActivated{1}, InstallActivationFailed{1, "failure"}}) {
		check(throws([&] { plan_event(internal, false); }) && throws([&] { plan_event(internal, true); }), "trusted installation events reject through ordinary worker admission");
	}
}

void snapshot_timeout_retries() {
	auto state = empty(1); state.hard = HardState{1, 1, 2}; state.base_index = state.applied_index = 2; state.base_term = 1; state.entries = {{3, 1, EntryKind::Command, "tail"}};
	Core core(config(1), state); auto election = elect_checkpoint_fixture(core); auto first = append_to(election, 2); if (!first) { throw std::runtime_error("snapshot retry fixture flight"); }
	auto needed = core.step(Receive{2, AppendResponse{2, first->rpc, false, 0, 1, 0}}); check(find<SnapshotNeeded>(needed), "snapshot timeout fixture requests missing history");
	core.step(Tick{60100, 100}); auto retry = core.step(Tick{60140, 100}); auto probe = append_to(retry, 2);
	check(probe && probe->previous == 2, "expired snapshot request retries through a fresh ordinary prefix probe");
	if (probe) { auto replacement = core.step(Receive{2, AppendResponse{2, probe->rpc, false, 0, 1, 0}}); check(find<SnapshotNeeded>(replacement), "an empty peer after timeout receives another snapshot request"); }
}

struct SnapshotSenderFixture {
	static RecoveredState state() {
		auto result = empty(1); result.hard = {1, 1, 2}; result.base_index = result.applied_index = 2; result.base_term = 1; result.entries = {{3, 1, EntryKind::Command, "tail"}}; return result;
	}
	Core core; SnapshotKey key{}; Actions requested, election;
	explicit SnapshotSenderFixture(Timing timing = {}) : core(config(1), state(), {}, timing) {
		election = elect_checkpoint_fixture(core); auto first = append_to(election, 2); if (!first) { throw std::runtime_error("sender fixture initial data flight"); }
		requested = core.step(Receive{2, AppendResponse{2, first->rpc, false, 0, 1, 0}}); auto needed = find<SnapshotNeeded>(requested); if (!needed) { throw std::runtime_error("sender fixture snapshot request"); } key = needed->key;
	}
	Actions ready() { return core.step(SnapshotSourceReady{2, key}); }
	Actions reply(SnapshotReply result) { return core.step(Receive{2, SnapshotResponse{core.term(), key, result}}); }
	Actions commit_healthy_peer() {
		auto request = append_to(election, 3); if (!request) { throw std::runtime_error("sender fixture healthy flight"); }
		auto response = core.step(Receive{3, AppendResponse{core.term(), request->rpc, true, request->previous + request->entries.size(), request->previous + request->entries.size() + 1, request->read_probe}});
		auto persisted = find<Persist>(response); if (!persisted) { throw std::runtime_error("sender fixture healthy commit"); }
		auto completed = core.step(Persisted{persisted->token}); core.step(Applied{4}); return completed;
	}
};
void snapshot_sender_correlation() {
	SnapshotSenderFixture fixture; auto& core = fixture.core;
	check(!find<SnapshotReleased>(fixture.reply(SnapshotReply::Installed)), "snapshot result before source readiness cannot complete a flight");
	for (unsigned mismatch = 0; mismatch < 4; ++mismatch) {
		auto key = fixture.key; NodeId peer = 2;
		switch (mismatch) { case 0: peer = 3; break; case 1: ++key.transfer; break; case 2: key.leader_term = 1; break; case 3: ++key.boundary.index; break; }
		check(!find<SnapshotTransmit>(core.step(SnapshotSourceReady{peer, key})), "source readiness requires exact peer, epoch, token and boundary");
	}
	check(find<SnapshotTransmit>(fixture.ready()) && !find<SnapshotTransmit>(fixture.ready()), "exact source readiness transitions once to Sending");
	for (unsigned mismatch = 0; mismatch < 5; ++mismatch) {
		auto key = fixture.key; NodeId peer = 2; Term term = 2;
		switch (mismatch) { case 0: peer = 3; break; case 1: ++key.transfer; break; case 2: key.leader_term = 1; break; case 3: ++key.boundary.index; break; case 4: term = 1; break; }
		check(!find<SnapshotReleased>(core.step(Receive{peer, SnapshotResponse{term, key, SnapshotReply::Installed}})) && core.committed() == 2, "incorrect snapshot reply correlation supplies no progress");
	}
	auto installed = fixture.reply(SnapshotReply::Installed); auto next = append_to(installed, 2);
	check(find<SnapshotReleased>(installed) && next && next->previous == 2 && core.committed() == 2 && !find<Persist>(installed), "Installed advances only its included boundary and does not commit the newer tail");
	check(!find<SnapshotReleased>(fixture.reply(SnapshotReply::Installed)), "duplicate installed response cannot complete another flight");

	SnapshotSenderFixture reads; auto healthy = reads.commit_healthy_peer(); reads.ready(); auto& reader = reads.core; reader.step(Read{80});
	if (auto request = append_to(healthy, 3)) { reader.step(Receive{3, AppendResponse{2, request->rpc, true, 4, 5, request->read_probe}}); }
	auto success = reads.reply(SnapshotReply::Installed); auto probe = append_to(success, 2);
	check(!find<ReadReady>(success) && probe && probe->read_probe, "Installed contributes no read quorum and resumes a separately correlated read probe");
	if (probe) { auto through = probe->previous + probe->entries.size(); auto result = reader.step(Receive{2, AppendResponse{2, probe->rpc, true, through, through + 1, probe->read_probe}}); check(find<ReadReady>(result), "fresh ordinary prefix verification can complete the pending quorum read"); }
}
void snapshot_sender_keepalive_pacing() {
	Timing timing; timing.rpc_timeout = 10; SnapshotSenderFixture fixture(timing); fixture.commit_healthy_peer(); auto& core = fixture.core; auto request = append_to(fixture.requested, 2);
	check(request && request->previous == 0 && request->previous_term == 0 && request->commit == 0 && request->read_probe == 0 && request->entries.empty(), "snapshot contact uses an empty independent genesis keepalive");
	if (!request) { return; } auto rpc = request->rpc;
	core.step(Receive{2, AppendResponse{2, rpc, true, 4, 5, 0}});
	for (unsigned repeat = 0; repeat < 10; ++repeat) {
		auto tick = core.step(Tick{100, 100}); auto read = core.step(Read{100 + repeat});
		check(!append_to(tick, 2) && !append_to(read, 2) && !find<Reject>(read), "fixed-time keepalive acknowledgment then Tick/accepted Read cannot create an immediate contact loop");
	}
	check(!append_to(core.step(Tick{110, 100}), 2), "shorter ordinary RPC timeout cannot bypass snapshot keepalive pacing");
	auto later = core.step(Tick{120, 100}); auto refreshed = append_to(later, 2);
	check(refreshed && refreshed->rpc != rpc && refreshed->previous == 0 && core.committed() == 4 && !find<ReadReady>(later), "periodic keepalive replacement is fresh and supplies neither replication nor read evidence");
	fixture.ready();
	for (std::uint64_t now = 140; now <= 400; now += 20) { auto actions = core.step(Tick{now, 100}); auto keepalive = append_to(actions, 2); check(keepalive && keepalive->previous == 0 && !keepalive->read_probe && !find<SnapshotNeeded>(actions), "source and transfer delays keep periodic contact even when replies are dropped"); }
}
void snapshot_sender_retries_and_retention() {
	Timing timing; timing.snapshot_timeout = 300;
	for (SnapshotReply result : {SnapshotReply::CaughtUp, SnapshotReply::Rejected}) {
		SnapshotSenderFixture fixture(timing); fixture.ready(); auto released = fixture.reply(result);
		check(find<SnapshotReleased>(released) && !find<Persist>(released) && fixture.core.committed() == 2, "caught-up or rejected snapshots release without fabricated progress");
		for (unsigned repeat = 0; repeat < 10; ++repeat) { check(!find<SnapshotNeeded>(fixture.core.step(Tick{100, 100})), "fixed-time rejection cannot immediately request another image"); }
		auto retry = fixture.core.step(Tick{140, 100}); auto prefix = append_to(retry, 2);
		if (result == SnapshotReply::CaughtUp) {
			check(prefix && prefix->previous == 2 && !find<SnapshotNeeded>(retry), "CaughtUp selects fresh ordinary prefix verification without snapshot acknowledgment");
			if (prefix) { auto through = prefix->previous + prefix->entries.size(); auto commit = fixture.core.step(Receive{2, AppendResponse{2, prefix->rpc, true, through, through + 1, 0}}); check(find<Persist>(commit), "verified CaughtUp prefix can subsequently commit the replicated tail"); }
		} else { auto needed = find<SnapshotNeeded>(retry); check(needed && needed->key.transfer != fixture.key.transfer, "rejected image retries with a new transfer token after backoff"); }
	}
	SnapshotSenderFixture failure(timing); auto unavailable = failure.core.step(SnapshotTransferFailed{2, failure.key});
	check(find<SnapshotReleased>(unavailable), "source unavailability retires its exact AwaitSource flight");
	auto replacement = failure.core.step(Tick{140, 100}); auto needed = find<SnapshotNeeded>(replacement);
	check(needed && needed->key.transfer != failure.key.transfer && !find<SnapshotTransmit>(failure.ready()), "stale source completion cannot replace a fresh retry flight");

	for (bool sending : {false, true}) {
		SnapshotSenderFixture fixture(timing); fixture.commit_healthy_peer(); if (sending) { fixture.ready(); }
		auto cfg = config(1); LocalCheckpoint checkpoint{90, 70, 4, 2, cfg.cluster, cfg.configuration}; auto action = fixture.core.step(checkpoint);
		if (sending) {
			auto rejected = find<Reject>(action); check(rejected && rejected->reason == RejectReason::Busy && !find<PersistCheckpoint>(action), "active Sending preserves the suffix by rejecting compaction beyond its boundary");
			auto expired = fixture.core.step(Tick{400, 100}); check(find<SnapshotReleased>(expired), "bounded transfer timeout releases suffix retention");
			action = fixture.core.step(checkpoint); check(find<PersistCheckpoint>(action), "compaction becomes available after sending timeout");
		} else {
			auto capture = find<PersistCheckpoint>(action); check(capture, "AwaitSource permits local checkpoint advancement");
			if (capture) { auto published = fixture.core.step(Persisted{capture->token}); check(find<SnapshotReleased>(published) && !find<SnapshotTransmit>(fixture.ready()), "changed base invalidates old source readiness and retires its request"); }
		}
	}
}

void snapshot_sender_lost_ack_and_terms() {
	SnapshotSenderFixture lost; lost.ready(); auto failure = lost.core.step(SnapshotTransferFailed{2, lost.key});
	check(find<SnapshotReleased>(failure) && lost.core.committed() == 2 && !find<Persist>(failure) && !append_to(lost.core.step(Tick{100, 100}), 2), "lost install acknowledgment releases Sending without progress or same-time retry");
	auto retry = lost.core.step(Tick{140, 100}); auto probe = append_to(retry, 2);
	check(probe && probe->previous == 2 && !find<SnapshotNeeded>(retry), "post-send failure tries a fresh prefix before another complete image");
	if (probe) { auto through = probe->previous + probe->entries.size(); auto verified = lost.core.step(Receive{2, AppendResponse{2, probe->rpc, true, through, through + 1, 0}}); auto durable = find<Persist>(verified); check(durable && lost.core.committed() == 2, "only fresh prefix proof stages post-snapshot tail commitment"); if (durable) { lost.core.step(Persisted{durable->token}); check(lost.core.committed() == 4, "lost acknowledgment recovery commits only after owned durability completion"); } }
	for (bool obsolete : {false, true}) { for (bool fail : {false, true}) {
		SnapshotSenderFixture fixture; fixture.ready(); auto key = fixture.key; if (obsolete) { ++key.transfer; }
		auto observed = fixture.core.step(Receive{2, SnapshotResponse{3, key, SnapshotReply::Rejected}}); auto persist = find<Persist>(observed);
		check(persist && !find<RoleChanged>(observed) && !find<SnapshotReleased>(observed) && fixture.core.durable_hard_state().term == 2 && fixture.core.role() == Role::Follower, "valid higher durable receiver term suppresses leadership but holds dependent flight releases behind persistence");
		if (!persist) { continue; }
		if (fail) { auto fenced = fixture.core.step(Failed{FailureSource::Storage, persist->token, "injected term failure"}); check(find<Fenced>(fenced) && fixture.core.role() == Role::Fenced, "higher-term storage uncertainty fences with current or obsolete transfer tokens"); }
		else { auto completed = fixture.core.step(Persisted{persist->token}); auto released = find<SnapshotReleased>(completed); check(released && released->key == fixture.key && fixture.core.durable_hard_state().term == 3 && !fixture.core.durable_hard_state().voted_for && find<RoleChanged>(completed), "higher receiver term durably clears the ballot before exposing role and exact old-resource release"); }
	} }
	SnapshotSenderFixture malformed; malformed.ready();
	for (unsigned invalid = 0; invalid < 5; ++invalid) {
		auto key = malformed.key; SnapshotReply result = SnapshotReply::Installed; NodeId peer = 2;
		switch (invalid) { case 0: key.transfer = 0; break; case 1: key.boundary.term = 3; break; case 2: result = static_cast<SnapshotReply>(99); break; case 3: peer = 99; break; case 4: peer = 1; break; }
		auto output = malformed.core.step(Receive{peer, SnapshotResponse{99, key, result}}); check(output.empty() && malformed.core.term() == 2 && malformed.core.role() == Role::Leader, "invalid response or unauthenticated member assertion cannot mutate term or flight state");
	}
	auto fenced = malformed.core.step(StorageFault{"uncertain storage"}); auto released = find<SnapshotReleased>(fenced);
	check(released && released->key == malformed.key && released->reason == SnapshotReleaseReason::Fenced && find<Fenced>(fenced), "direct fencing retires active source resources without a progress acknowledgment");
}
void snapshot_sender_receiver_contact() {
	SnapshotSenderFixture fixture; auto& leader = fixture.core; Core follower(config(2), empty(2)); follower.step(Tick{100, 100}); follower.step(Start{});
	auto initial = append_to(fixture.requested, 2); if (!initial) { throw std::runtime_error("contact fixture genesis keepalive"); }
	auto contact = follower.step(Receive{1, *initial}); if (auto persist = find<Persist>(contact)) { follower.step(Tick{100, 100}); follower.step(Persisted{persist->token}); }
	fixture.ready(); unsigned delivered = 1;
	for (std::uint64_t now = 120; now <= 400; now += 20) {
		auto actions = leader.step(Tick{now, 100}); follower.step(Tick{now, 100});
		if (auto request = append_to(actions, 2)) { ++delivered; auto output = follower.step(Receive{1, *request}); check(!find<Persist>(output) && follower.role() == Role::Follower && follower.term() == 2, "long transfer keepalive refreshes the actual receiver without campaigning or data progress"); }
		if (auto request = append_to(actions, 3)) {
			auto through = request->previous + request->entries.size(); auto output = leader.step(Receive{3, AppendResponse{2, request->rpc, true, through, through + 1, request->read_probe}});
			if (auto persist = find<Persist>(output)) { leader.step(Tick{now, 100}); auto completed = leader.step(Persisted{persist->token}); if (auto range = find<Committed>(completed)) { leader.step(Applied{range->entries.back().index}); } }
		}
		// Deliberately drop every receiver keepalive response.
	}
	check(delivered >= 14 && follower.committed() == 0 && follower.last_index() == 0 && leader.committed() == 4 && leader.role() == Role::Leader, "dropped contact replies neither starve healthy replication nor manufacture receiver progress");
}
void snapshot_sender_restart_clock_and_admission() {
	SnapshotSenderFixture old; old.ready(); auto recovered = SnapshotSenderFixture::state(); recovered.hard = {2, 1, 2}; recovered.entries.push_back({4, 2, EntryKind::NoOp, ""});
	Core restarted(config(1), recovered); auto election = elect_checkpoint_fixture(restarted); auto first = append_to(election, 2); if (!first) { throw std::runtime_error("restarted sender initial flight"); }
	auto requested = restarted.step(Receive{2, AppendResponse{3, first->rpc, false, 0, 1, 0}}); auto needed = find<SnapshotNeeded>(requested); if (!needed) { throw std::runtime_error("restarted sender snapshot request"); }
	auto key = needed->key; check(key.transfer == old.key.transfer && key.leader_term == 3 && old.key.leader_term == 2, "restart can reuse numeric transfers only under a newly persisted leader epoch"); restarted.step(SnapshotSourceReady{2, key});
	check(!find<SnapshotReleased>(restarted.step(Receive{2, SnapshotResponse{3, old.key, SnapshotReply::Installed}})), "old incarnation cannot complete a same-numbered restarted flight");
	check(find<SnapshotReleased>(restarted.step(Receive{2, SnapshotResponse{3, key, SnapshotReply::Installed}})), "restarted leader accepts its exact new epoch and transfer");

	SnapshotSenderFixture saturated; saturated.ready(); auto expired = saturated.core.step(Tick{std::numeric_limits<std::uint64_t>::max(), 100}); check(find<SnapshotReleased>(expired), "clock saturation retires an earlier transfer exactly once");
	for (unsigned repeat = 0; repeat < 10; ++repeat) { auto actions = saturated.core.step(Tick{std::numeric_limits<std::uint64_t>::max(), 100}); check(!find<SnapshotNeeded>(actions) && !append_to(actions, 2), "saturated-clock backoff cannot spin snapshot or prefix retries"); }
	for (bool busy : {false, true}) {
		auto plan = plan_event(Receive{2, SnapshotResponse{2, old.key, SnapshotReply::Installed}}, busy); check(plan.count == (busy ? 0 : 1) && (busy || (plan.appends[0].kind == kronuz::journal::AdmissionClass::Control && plan.appends[0].encoded_bytes == storage_batch_size(true, false))), "snapshot response owns one conservative control hard-state budget");
		check(throws([&] { plan_event(SnapshotSourceReady{2, old.key}, busy); }) && throws([&] { plan_event(SnapshotTransferFailed{2, old.key}, busy); }), "trusted source transitions reject ordinary Worker submission even while busy");
		auto invalid = old.key; invalid.transfer = 0; check(throws([&] { plan_event(Receive{2, SnapshotResponse{99, invalid, SnapshotReply::Installed}}, busy); }), "malformed response validates before Worker backpressure or term effects");
	}
}

void snapshot_sender_maximum_fanout() {
	auto cfg = config(1); cfg.voters.clear(); for (NodeId peer = 1; peer <= 31; ++peer) { cfg.voters.push_back(peer); }
	auto state = SnapshotSenderFixture::state(); state.configuration = cfg; Limits capacity; capacity.voters = 31;
	Core core(cfg, state, capacity); core.step(Start{}); auto campaign = core.step(Tick{100, 100}); auto ballot = find<Persist>(campaign); if (!ballot) { throw std::runtime_error("fanout ballot"); } core.step(Persisted{ballot->token});
	Actions traffic;
	for (NodeId voter = 2; voter <= 16; ++voter) { auto actions = core.step(Receive{voter, VoteResponse{2, true}}); if (auto noop = find<Persist>(actions)) { traffic = core.step(Persisted{noop->token}); } }
	std::map<NodeId, SnapshotKey> keys;
	for (NodeId peer = 2; peer <= 31; ++peer) {
		auto append = append_to(traffic, peer); if (!append) { throw std::runtime_error("fanout data flight"); }
		auto request = core.step(Receive{peer, AppendResponse{2, append->rpc, false, 0, 1, 0}}); auto needed = find<SnapshotNeeded>(request); if (!needed) { throw std::runtime_error("fanout snapshot request"); } keys.emplace(peer, needed->key);
		check(find<SnapshotTransmit>(core.step(SnapshotSourceReady{peer, needed->key})), "maximum-voter source readiness owns one bounded flight per peer");
	}
	auto heartbeat = core.step(Tick{120, 100}); unsigned contacts = 0;
	for (const auto& action : heartbeat) { if (auto send = std::get_if<Send>(&action)) { if (auto append = std::get_if<AppendRequest>(&send->message)) { contacts += append->previous == 0 && append->commit == 0 && !append->read_probe && append->entries.empty(); } } }
	auto bound = 2 * capacity.reads + 4 * capacity.voters + 16;
	check(keys.size() == 30 && contacts == 30 && heartbeat.size() <= bound, "maximum-voter simultaneous keepalives fit the existing Worker action bound without payload");
	auto transition = core.step(Receive{2, SnapshotResponse{3, keys.at(2), SnapshotReply::Rejected}}); auto persisted = find<Persist>(transition); if (!persisted) { throw std::runtime_error("fanout higher-term persistence"); }
	auto released = core.step(Persisted{persisted->token}); unsigned count = 0;
	for (const auto& action : released) { if (auto flight = std::get_if<SnapshotReleased>(&action)) { ++count; check(flight->key == keys.at(flight->peer), "leadership loss releases each exact maximum-fanout capability"); } }
	check(count == 30 && released.size() <= bound && core.durable_hard_state().term == 3, "simultaneous source releases and role effects fit the old action bound after durable term change");
}

int main() {
	try { snapshot_sender_maximum_fanout(); snapshot_sender_lost_ack_and_terms(); snapshot_sender_receiver_contact(); snapshot_sender_restart_clock_and_admission(); snapshot_sender_correlation(); snapshot_sender_keepalive_pacing(); snapshot_sender_retries_and_retention(); snapshot_timeout_retries(); portable_snapshot_installation(); portable_snapshot_descriptors(); event_admission_plans(); storage_footprint_plans(); persistence_barriers(); delayed_completions_do_not_campaign(); replication_and_restart(); affirmative_majority_and_inheritance(); simultaneous_completions(); reads_and_partitions(); overlapping_reads_preserve_data(); application_lag_does_not_spin_reads(); stale_rpc_and_failure_transitions(); bounds_and_semantic_recovery(); local_checkpoint_core(); compacted_index_exhaustion(); compacted_replication(); checkpoint_semantic_recovery(); real_journal_integration(); real_store_integration(); reordered_crash_schedules(3, 0x52414654); reordered_crash_schedules(5, 0x434c5553); }
	catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " consensus checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
