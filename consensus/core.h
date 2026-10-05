#pragma once

#include "types.h"
#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <type_traits>

namespace cluster::consensus {

// One owning executor. No I/O, clock reads, entropy, or application callbacks.
// Persist changes are staged internally; all dependent actions are deferred.
class Core {
public:
	Core(FixedConfiguration configuration, RecoveredState recovered, Limits limits = {}, Timing timing = {})
		: configuration_(std::move(configuration)), hard_(recovered.hard), durable_(hard_),
		entries_(std::move(recovered.entries)), base_index_(recovered.base_index), base_term_(recovered.base_term), limits_(limits), timing_(timing), election_delay_(timing.election_min), applied_(recovered.applied_index) {
		if (configuration_ != recovered.configuration || configuration_.local == 0 || configuration_.voters.empty() ||
			configuration_.voters.size() > limits.voters || limits.voters > 31 || !member(configuration_.local)) {
			throw std::invalid_argument("invalid or mismatched fixed configuration");
		}
		std::set<NodeId> voters(configuration_.voters.begin(), configuration_.voters.end());
		if (voters.size() != configuration_.voters.size() || voters.contains(0)) { throw std::invalid_argument("invalid voters"); }
		if (limits.command_bytes == 0 || limits.rpc_entries == 0 || limits.log_entries == 0 || limits.reads == 0 || limits.uncommitted_entries == 0 ||
			limits.control_entries == 0 || limits.control_entries > limits.log_entries ||
			limits.command_bytes > limits.rpc_bytes || limits.rpc_bytes > limits.log_bytes ||
			timing.heartbeat == 0 || timing.rpc_timeout == 0 || timing.snapshot_timeout == 0 || timing.election_min <= timing.heartbeat ||
			timing.election_max < timing.election_min) { throw std::invalid_argument("invalid consensus limits or timing"); }
		if (base_index_ >= maximum || ((base_index_ == 0) != (base_term_ == 0)) || base_term_ > hard_.term ||
			entries_.size() >= maximum - base_index_ || recovered.applied_index != base_index_ || hard_.commit_index < base_index_) {
			throw std::invalid_argument("invalid recovered checkpoint boundary");
		}
		if (hard_.voted_for && !member(*hard_.voted_for)) { throw std::invalid_argument("recovered ballot names a nonvoter"); }
		if (hard_.term == 0 && hard_.voted_for) { throw std::invalid_argument("term zero cannot contain a ballot"); }
		if (entries_.size() > limits_.log_entries || hard_.commit_index > last_index()) { throw std::invalid_argument("invalid recovered log bounds"); }
		Term previous_term = base_term_;
		for (std::size_t i = 0; i < entries_.size(); ++i) {
			const auto& entry = entries_[i];
			if (entry.index != base_index_ + i + 1 || !valid_entry(entry) || entry.term > hard_.term || entry.term < previous_term ||
				entry.payload.size() > limits_.log_bytes - log_bytes_) { throw std::invalid_argument("invalid recovered entry"); }
			log_bytes_ += entry.payload.size(); previous_term = entry.term;
		}
	}
	Core(const Core&) = delete;
	Core& operator=(const Core&) = delete;
	Role role() const noexcept { return role_; }
	Term term() const noexcept { return hard_.term; }
	Index committed() const noexcept { return durable_.commit_index; }
	Index applied() const noexcept { return applied_; }
	std::size_t retained_log_bytes() const noexcept { return log_bytes_; }
	Index last_index() const noexcept { return base_index_ + static_cast<Index>(entries_.size()); }
	Index base_index() const noexcept { return base_index_; }
	LogBoundary base_boundary() const noexcept { return {base_index_, base_term_}; }
	std::optional<SnapshotKey> awaiting_snapshot(NodeId peer) const {
		auto found = peers_.find(peer);
		if (role_ != Role::Leader || found == peers_.end() || !found->second.snapshot || found->second.snapshot->phase != SnapshotPhase::AwaitSource) { return std::nullopt; }
		return found->second.snapshot->key;
	}
	bool snapshot_active(NodeId peer, const SnapshotKey& key) const {
		auto found = peers_.find(peer);
		return role_ == Role::Leader && hard_.term == durable_.term && found != peers_.end() && found->second.snapshot && found->second.snapshot->key == key;
	}
	bool snapshot_sending(NodeId peer, const SnapshotKey& key) const {
		auto found = peers_.find(peer);
		return snapshot_active(peer, key) && found->second.snapshot->phase == SnapshotPhase::Sending;
	}
	bool busy() const noexcept { return pending_.has_value() || install_.has_value(); }
	const HardState& durable_hard_state() const noexcept { return durable_; }
	const FixedConfiguration& configuration() const noexcept { return configuration_; }

	Actions step(Event event) {
		Actions output;
		if (role_ == Role::Fenced) { return output; }
		try { std::visit([&](auto&& value) { handle(std::move(value), output); }, std::move(event)); }
		catch (const std::exception& error) { fence(error.what(), output); }
		return output;
	}

private:
	struct Pending { Token token; HardState hard; Actions deferred; bool elected = false; std::optional<std::pair<Index, Term>> checkpoint; };
	struct PendingInstall {
		Token token, prepared; RequestId request; LogBoundary boundary; HardState hard;
		Actions deferred; bool retain_suffix, published = false;
	};
	struct Flight { Token rpc; Index previous, through; Token read_probe; std::uint64_t sent; bool keepalive = false; };
	enum class SnapshotPhase { AwaitSource, Sending };
	struct SnapshotFlight { SnapshotKey key; SnapshotPhase phase; std::uint64_t started; };
	struct Peer { Index next = 1, matched = 0; std::optional<Flight> flight; std::uint64_t retry_after = 0; std::optional<SnapshotFlight> snapshot{}; std::optional<std::uint64_t> snapshot_retry_clock{}; };
	struct PendingRead { RequestId request; Token probe; Term term; Index index; std::set<NodeId> acknowledgements; };
	static constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
	bool member(NodeId node) const { return std::find(configuration_.voters.begin(), configuration_.voters.end(), node) != configuration_.voters.end(); }
	std::size_t quorum() const { return configuration_.voters.size() / 2 + 1; }
	Token needed_probe(NodeId peer) const {
		for (const auto& read : reads_) {
			if (read.acknowledgements.size() < quorum() && !read.acknowledgements.contains(peer)) { return read.probe; }
		}
		return 0;
	}
	std::size_t log_offset(Index index) const {
		if (index <= base_index_ || index > last_index()) { throw std::out_of_range("log index outside retained suffix"); }
		return static_cast<std::size_t>(index - base_index_ - 1);
	}
	const Entry& log_entry(Index index) const { return entries_.at(log_offset(index)); }
	Term log_term(Index index) const { return index == base_index_ ? base_term_ : log_entry(index).term; }
	bool valid_entry(const Entry& entry) const {
		return entry.index > 0 && entry.index < maximum && entry.term > 0 && entry.payload.size() <= limits_.command_bytes &&
			((entry.kind == EntryKind::NoOp && entry.payload.empty()) || entry.kind == EntryKind::Command);
	}
	std::uint64_t deadline(std::uint64_t delay) const { return delay > maximum - now_ ? maximum : now_ + delay; }
	void reset_election() { election_deadline_ = deadline(election_delay_); }
	Token token() {
		if (next_token_ == maximum) { throw std::overflow_error("consensus token space exhausted; restart required"); }
		return ++next_token_;
	}
	void fence(std::string reason, Actions& output) {
		output.clear(); release_snapshots(SnapshotReleaseReason::Fenced, output);
		role_ = Role::Fenced; pending_.reset(); install_.reset(); peers_.clear(); reads_.clear();
		output.emplace_back(Fenced{std::move(reason)});
	}
	void follower(NodeId leader, Actions& deferred) {
		if (role_ != Role::Follower || leader_ != leader) { deferred.emplace_back(RoleChanged{Role::Follower, hard_.term, leader}); }
		release_snapshots(SnapshotReleaseReason::LeadershipLost, deferred);
		role_ = Role::Follower; leader_ = leader; peers_.clear(); grants_.clear();
		for (const auto& read : reads_) { deferred.emplace_back(Reject{read.request, RejectReason::NotLeader}); }
		reads_.clear(); reset_election();
	}
	void persist(StorageBatch batch, Actions deferred, Actions& output, bool elected = false) {
		if (busy()) { throw std::logic_error("overlapping persistence"); }
		auto id = token();
		pending_.emplace(Pending{id, hard_, std::move(deferred), elected, std::nullopt});
		output.emplace_back(Persist{id, std::move(batch)});
	}
	void handle(Start, Actions& output) {
		if (started_) { return; }
		started_ = true; reset_election(); deliver(output);
	}
	void handle(Tick tick, Actions& output) {
		if (tick.now < now_ || tick.election_delay < timing_.election_min || tick.election_delay > timing_.election_max) { return; }
		now_ = tick.now; election_delay_ = tick.election_delay;
		if (started_ && !busy()) { drive(output, true); }
	}
	void handle(Persisted completion, Actions& output) {
		if (install_) {
			if (install_->published || completion.token != install_->token) { return; }
			auto& operation = *install_;
			if (operation.retain_suffix) {
				auto count = static_cast<std::size_t>(operation.boundary.index - base_index_);
				for (std::size_t i = 0; i < count; ++i) { log_bytes_ -= entries_[i].payload.size(); }
				entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(count));
			} else { entries_.clear(); log_bytes_ = 0; }
			base_index_ = operation.boundary.index; base_term_ = operation.boundary.term;
			hard_ = durable_ = operation.hard; operation.published = true;
			output = std::move(operation.deferred);
			output.emplace_back(ActivateInstall{operation.token, operation.prepared, operation.boundary});
			return; // Application activation still owns the exclusive busy gate.
		}
		if (!pending_ || completion.token != pending_->token) { return; }
		auto completed = std::move(*pending_); pending_.reset(); durable_ = completed.hard;
		if (completed.checkpoint) {
			auto [through, term] = *completed.checkpoint;
			auto count = static_cast<std::size_t>(through - base_index_);
			for (std::size_t i = 0; i < count; ++i) { log_bytes_ -= entries_[i].payload.size(); }
			entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(count));
			base_index_ = through; base_term_ = term;
		}
		output = std::move(completed.deferred);
		if (completed.elected) { become_leader(output); }
		if (!pending_) {
			advance_commit(output);
			if (!pending_) { deliver(output); ready_reads(output); drive(output, false); }
		}
	}
	void handle(StorageFault failure, Actions& output) { fence(std::move(failure.error), output); }
	void handle(InstallPrepared request, Actions& output) {
		auto reject = [&](InstallRejectReason reason) { output.emplace_back(InstallRejected{request.request, request.prepared, reason}); };
		if (!started_ || busy()) { reject(InstallRejectReason::Busy); return; }
		if (request.request == 0 || request.prepared == 0 || request.authenticated_peer == configuration_.local || !member(request.authenticated_peer) ||
			request.cluster != configuration_.cluster || request.configuration != configuration_.configuration ||
			request.leader_term == 0 || request.boundary.index == 0 || request.boundary.index == maximum ||
			request.boundary.term == 0 || request.boundary.term > request.leader_term) { reject(InstallRejectReason::Invalid); return; }
		if (request.leader_term < hard_.term) { reject(InstallRejectReason::StaleTerm); return; }
		auto next_hard = durable_;
		if (request.leader_term > next_hard.term) { next_hard.term = request.leader_term; next_hard.voted_for.reset(); }
		if (request.boundary.index <= durable_.commit_index) {
			Actions deferred; hard_ = next_hard; follower(request.authenticated_peer, deferred);
			deferred.emplace_back(InstallRejected{request.request, request.prepared, InstallRejectReason::CaughtUp});
			if (hard_ != durable_) { persist(StorageBatch{hard_, std::nullopt}, std::move(deferred), output); }
			else { output = std::move(deferred); }
			return;
		}
		bool retain = request.boundary.index <= last_index() && log_term(request.boundary.index) == request.boundary.term;
		next_hard.commit_index = request.boundary.index;
		RecoveredState state{configuration_, next_hard, request.boundary.index, request.boundary.term, {}, request.boundary.index};
		if (retain) {
			auto first = static_cast<std::size_t>(request.boundary.index - base_index_);
			state.entries.assign(entries_.begin() + static_cast<std::ptrdiff_t>(first), entries_.end());
		}
		Actions deferred; hard_ = next_hard; follower(request.authenticated_peer, deferred);
		auto id = token(); install_.emplace(PendingInstall{id, request.prepared, request.request, request.boundary, next_hard, std::move(deferred), retain});
		output.emplace_back(PersistInstall{id, request.prepared, std::move(state)});
	}
	void handle(InstallActivated completion, Actions& output) {
		if (!install_ || !install_->published || completion.token != install_->token) { return; }
		if (delivered_through_) { fence("snapshot activation precedes outstanding application completion", output); return; }
		auto operation = std::move(*install_); install_.reset(); applied_ = operation.boundary.index; reset_election();
		output.emplace_back(InstallCompleted{operation.request, operation.prepared, operation.boundary});
	}
	void handle(InstallActivationFailed failure, Actions& output) {
		if (install_ && install_->published && failure.token == install_->token) { fence(std::move(failure.error), output); }
	}
	void release_snapshots(SnapshotReleaseReason reason, Actions& output) {
		for (const auto& [id, peer] : peers_) { if (peer.snapshot) { output.emplace_back(SnapshotReleased{id, peer.snapshot->key, reason}); } }
	}
	void release_snapshot(NodeId id, Peer& peer, SnapshotReleaseReason reason, Actions& output, bool probe = false) {
		auto key = peer.snapshot->key; output.emplace_back(SnapshotReleased{id, key, reason}); peer.snapshot.reset();
		if (peer.flight && peer.flight->keepalive) { peer.flight.reset(); }
		peer.retry_after = deadline(timing_.rpc_timeout); peer.snapshot_retry_clock = now_;
		if (probe) { peer.next = std::max(peer.matched + 1, key.boundary.index + 1); }
	}
	void handle(SnapshotSourceReady ready, Actions& output) {
		if (busy() || role_ != Role::Leader || !valid_snapshot_key(ready.key)) { return; }
		auto found = peers_.find(ready.peer); if (found == peers_.end()) { return; } auto& peer = found->second;
		if (!peer.snapshot || peer.snapshot->key != ready.key || peer.snapshot->phase != SnapshotPhase::AwaitSource || ready.key.leader_term != durable_.term) { return; }
		if (ready.key.boundary != LogBoundary{base_index_, base_term_}) { release_snapshot(ready.peer, peer, SnapshotReleaseReason::Stale, output); return; }
		peer.snapshot->phase = SnapshotPhase::Sending; peer.snapshot->started = now_; output.emplace_back(SnapshotTransmit{ready.peer, ready.key});
	}
	void handle(SnapshotTransferFailed failure, Actions& output) {
		if (busy() || role_ != Role::Leader || !valid_snapshot_key(failure.key)) { return; }
		auto found = peers_.find(failure.peer); if (found == peers_.end() || !found->second.snapshot || found->second.snapshot->key != failure.key) { return; }
		bool probe = found->second.snapshot->phase == SnapshotPhase::Sending;
		release_snapshot(failure.peer, found->second, SnapshotReleaseReason::Unavailable, output, probe);
	}

	void handle(LocalCheckpoint request, Actions& output) {
		if (!started_ || busy()) { output.emplace_back(Reject{request.request, RejectReason::Busy}); return; }
		if (request.capture == 0 || request.cluster != configuration_.cluster || request.configuration != configuration_.configuration ||
			request.through <= base_index_ || request.through > applied_ || applied_ > durable_.commit_index ||
			log_term(request.through) != request.term) {
			output.emplace_back(Reject{request.request, RejectReason::InvalidCheckpoint}); return;
		}
		for (const auto& [id, peer] : peers_) {
			if (peer.snapshot && peer.snapshot->phase == SnapshotPhase::Sending && request.through > peer.snapshot->key.boundary.index) { output.emplace_back(Reject{request.request, RejectReason::Busy}); return; }
		}
		RecoveredState state{configuration_, durable_, request.through, request.term, {}, request.through};
		auto first = static_cast<std::size_t>(request.through - base_index_);
		state.entries.assign(entries_.begin() + static_cast<std::ptrdiff_t>(first), entries_.end());
		auto id = token();
		Actions deferred; deferred.emplace_back(CheckpointPublished{request.request, request.through});
		pending_.emplace(Pending{id, durable_, std::move(deferred), false, std::pair{request.through, request.term}});
		output.emplace_back(PersistCheckpoint{id, request.capture, std::move(state)});
	}
	void handle(Failed failure, Actions& output) {
		if (failure.source == FailureSource::Storage) {
			if (install_) { if (install_->published || failure.token != install_->token) { return; } }
			else if (!pending_ || failure.token != pending_->token) { return; }
		} else if (failure.token != delivered_through_ || delivered_through_ == 0) { return; }
		fence(std::move(failure.error), output);
	}
	void handle(Applied completion, Actions& output) {
		if (completion.through <= applied_) { return; }
		if (completion.through != delivered_through_ || completion.through > durable_.commit_index) {
			fence("invalid application completion", output); return;
		}
		applied_ = completion.through; delivered_through_ = 0;
		if (!busy()) { deliver(output); ready_reads(output); }
	}
	void handle(Propose proposal, Actions& output) {
		if (busy()) { output.emplace_back(Reject{proposal.request, RejectReason::Busy}); return; }
		if (!started_ || role_ != Role::Leader) { output.emplace_back(Reject{proposal.request, RejectReason::NotLeader}); return; }
		if (proposal.command.size() > limits_.command_bytes) { output.emplace_back(Reject{proposal.request, RejectReason::TooLarge}); return; }
		if (last_index() - durable_.commit_index >= limits_.uncommitted_entries) {
			output.emplace_back(Reject{proposal.request, RejectReason::Busy}); return;
		}
		if (last_index() >= maximum - 1 || entries_.size() >= limits_.log_entries - limits_.control_entries || proposal.command.size() > limits_.log_bytes - log_bytes_) {
			output.emplace_back(Reject{proposal.request, RejectReason::LogFull}); return;
		}
		Entry entry{last_index() + 1, hard_.term, EntryKind::Command, std::move(proposal.command)};
		log_bytes_ += entry.payload.size(); entries_.push_back(entry);
		Actions deferred; deferred.emplace_back(ProposalPlaced{proposal.request, hard_.term, entry.index});
		persist(StorageBatch{std::nullopt, LogMutation{entry.index, {std::move(entry)}}}, std::move(deferred), output);
	}
	void handle(Read read, Actions& output) {
		if (busy()) { output.emplace_back(Reject{read.request, RejectReason::Busy}); return; }
		if (!started_ || role_ != Role::Leader) { output.emplace_back(Reject{read.request, RejectReason::NotLeader}); return; }
		if (durable_.commit_index == 0 || log_term(durable_.commit_index) != hard_.term) {
			output.emplace_back(Reject{read.request, RejectReason::NotReady}); return;
		}
		if (reads_.size() >= limits_.reads) { output.emplace_back(Reject{read.request, RejectReason::Busy}); return; }
		for (const auto& pending : reads_) {
			if (pending.request == read.request) { output.emplace_back(Reject{read.request, RejectReason::DuplicateRequest}); return; }
		}
		auto probe = token();
		reads_.push_back(PendingRead{read.request, probe, hard_.term, durable_.commit_index, {configuration_.local}});
		// Preserve data/probe correlation. Later reads queue behind the oldest
		// pending probe; each quorum observation must follow its admission.
		for (auto& [id, peer] : peers_) {
			auto needed = needed_probe(id);
			if (!peer.flight && needed) { replicate(id, peer, output, needed); }
		}
		ready_reads(output);
	}
	void handle(Receive receive, Actions& output) {
		if (!started_ || busy() || receive.authenticated_peer == configuration_.local || !member(receive.authenticated_peer)) { return; }
		if (!std::visit([&](const auto& message) { return valid(message); }, receive.message)) { return; }
		auto incoming_term = std::visit([](const auto& message) { return message.term; }, receive.message);
		bool changed = incoming_term > hard_.term;
		Actions deferred;
		if (changed) {
			hard_.term = incoming_term; hard_.voted_for.reset(); follower(0, deferred);
		}
		StorageBatch batch;
		std::visit([&](auto&& message) { receive_message(receive.authenticated_peer, std::move(message), batch, deferred); }, std::move(receive.message));
		if (changed || hard_ != durable_) { batch.hard = hard_; }
		if (batch.hard || batch.log) { persist(std::move(batch), std::move(deferred), output); }
		else {
			output = std::move(deferred); advance_commit(output);
			if (!pending_) { deliver(output); ready_reads(output); }
		}
	}
	bool valid(const VoteRequest& request) const { return request.term > 0 && request.last_index < maximum && request.last_term <= request.term && ((request.last_index == 0) == (request.last_term == 0)); }
	bool valid(const VoteResponse&) const { return true; }
	bool valid(const SnapshotResponse& response) const { return valid_snapshot_response(response); }
	bool valid(const AppendResponse& response) const {
		return response.rpc != 0 && response.matched < maximum && response.next_hint > 0 &&
			(!response.compacted || (!response.success && response.compacted->index > 0 && response.compacted->index < maximum &&
				response.compacted->term > 0 && response.compacted->term <= response.term && response.next_hint == response.compacted->index + 1));
	}
	bool valid(const AppendRequest& request) const {
		if (request.term == 0 || request.rpc == 0 || request.previous >= maximum || request.previous_term > request.term ||
			((request.previous == 0) != (request.previous_term == 0)) || request.entries.size() > limits_.rpc_entries ||
			request.entries.size() >= maximum - request.previous) { return false; }
		Index index = request.previous; Term term = request.previous_term; std::size_t bytes = 0;
		for (const auto& entry : request.entries) {
			if (!valid_entry(entry) || entry.index != ++index || entry.term < term || entry.term > request.term ||
				entry.payload.size() > limits_.rpc_bytes - bytes) { return false; }
			bytes += entry.payload.size(); term = entry.term;
		}
		return true;
	}
	void receive_message(NodeId source, VoteRequest request, StorageBatch&, Actions& output) {
		bool grant = request.term == hard_.term && (!hard_.voted_for || *hard_.voted_for == source) &&
			(request.last_term > log_term(last_index()) || (request.last_term == log_term(last_index()) && request.last_index >= last_index()));
		if (grant) { hard_.voted_for = source; reset_election(); }
		output.emplace_back(Send{source, VoteResponse{hard_.term, grant}});
	}
	void receive_message(NodeId source, VoteResponse response, StorageBatch&, Actions& output) {
		if (response.term != hard_.term || role_ != Role::Candidate || !response.granted) { return; }
		grants_.insert(source);
		if (grants_.size() >= quorum()) { become_leader(output); }
	}
	void receive_message(NodeId source, AppendRequest request, StorageBatch& batch, Actions& output) {
		if (request.term < hard_.term) {
			output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, last_index() + 1, request.read_probe}}); return;
		}
		follower(source, output);
		if (base_index_ && (request.previous < base_index_ || (request.previous == base_index_ && request.previous_term != base_term_))) {
			output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, base_index_ + 1, request.read_probe, LogBoundary{base_index_, base_term_}}}); return;
		}
		if (request.previous > last_index() || log_term(request.previous) != request.previous_term) {
			auto hint = request.previous < base_index_ ? base_index_ + 1 : (request.previous > last_index() ? last_index() + 1 : request.previous);
			output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, hint, request.read_probe}}); return;
		}
		for (const auto& entry : request.entries) {
			if (entry.index <= last_index() && log_entry(entry.index).term == entry.term && log_entry(entry.index) != entry) {
				throw std::runtime_error("same-index same-term content conflict");
			}
		}
		std::size_t first = 0;
		while (first < request.entries.size() && request.entries[first].index <= last_index() &&
			log_entry(request.entries[first].index) == request.entries[first]) { ++first; }
		if (first < request.entries.size()) {
			auto replace_from = request.entries[first].index;
			if (replace_from <= hard_.commit_index) {
				// Honest peers can never conflict with committed state. Fail closed.
				throw std::runtime_error("conflict with committed log prefix");
			}
			auto retained_bytes = log_bytes_;
			for (Index i = replace_from; i <= last_index(); ++i) { retained_bytes -= log_entry(i).payload.size(); }
			auto additional = request.entries.size() - first;
			if (replace_from - base_index_ - 1 > limits_.log_entries || additional > limits_.log_entries - (replace_from - base_index_ - 1)) {
				output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, replace_from, request.read_probe}}); return;
			}
			for (std::size_t i = first; i < request.entries.size(); ++i) {
				if (request.entries[i].payload.size() > limits_.log_bytes - retained_bytes) {
					output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, replace_from, request.read_probe}}); return;
				}
				retained_bytes += request.entries[i].payload.size();
			}
			std::vector<Entry> suffix(request.entries.begin() + static_cast<std::ptrdiff_t>(first), request.entries.end());
			entries_.resize(static_cast<std::size_t>(replace_from - base_index_ - 1));
			entries_.insert(entries_.end(), suffix.begin(), suffix.end()); log_bytes_ = retained_bytes;
			batch.log = LogMutation{replace_from, std::move(suffix)};
		}
		auto verified = request.previous + request.entries.size();
		hard_.commit_index = std::max(hard_.commit_index, std::min(request.commit, verified));
		output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, true, verified, verified + 1, request.read_probe}});
	}
	void receive_message(NodeId source, AppendResponse response, StorageBatch&, Actions& output) {
		if (role_ != Role::Leader || response.term != hard_.term) { return; }
		auto& peer = peers_.at(source);
		if (!peer.flight || peer.flight->rpc != response.rpc) { return; }
		auto flight = *peer.flight;
		if (response.read_probe != flight.read_probe) { return; }
		// Preserve its send time across replies so fixed-time callbacks cannot
		// turn independent snapshot contact into an immediate request loop.
		if (flight.keepalive) { return; }
		if (response.success) {
			if (response.matched != flight.through) { return; }
			peer.matched = std::max(peer.matched, response.matched); peer.next = peer.matched + 1; peer.retry_after = 0;
			if (flight.read_probe) {
				for (auto& read : reads_) { if (read.probe == flight.read_probe && read.term == hard_.term) { read.acknowledgements.insert(source); } }
			}
		} else {
			// A stale failure cannot reach this path. Durable peers cannot lose a
			// matched prefix on restart; never rewind below verified progress.
			auto previous_next = peer.next;
			if (response.compacted && response.compacted->index >= base_index_) {
				auto boundary = *response.compacted;
				if (boundary.index > last_index() || log_term(boundary.index) != boundary.term) {
					throw std::runtime_error("compacted peer boundary conflicts with leader log");
				}
				// This selects a prefix to verify in a new RPC; it is not an ACK.
				peer.next = std::max(peer.matched + 1, boundary.index + 1);
			} else {
				peer.next = std::max(peer.matched + 1, std::min(response.next_hint, peer.next > 1 ? peer.next - 1 : 1));
			}
			// Capacity rejection or an unusable hint must not form an immediate
			// request/response loop. Retry on a later monotonic tick.
			peer.retry_after = peer.next < previous_next ? 0 : deadline(timing_.rpc_timeout);
		}
		peer.flight.reset();
		auto needed = needed_probe(source);
		if (peer.next <= last_index() || needed) {
			replicate(source, peer, output, needed);
		}
	}
	void receive_message(NodeId source, SnapshotResponse response, StorageBatch&, Actions& output) {
		if (role_ != Role::Leader || response.term != hard_.term || response.key.leader_term != durable_.term) { return; }
		auto& peer = peers_.at(source);
		if (!peer.snapshot || peer.snapshot->phase != SnapshotPhase::Sending || peer.snapshot->key != response.key) { return; }
		if (response.result == SnapshotReply::Installed) {
			auto boundary = peer.snapshot->key.boundary;
			output.emplace_back(SnapshotReleased{source, response.key, SnapshotReleaseReason::Installed}); peer.snapshot.reset();
			if (peer.flight && peer.flight->keepalive) { peer.flight.reset(); }
			peer.matched = std::max(peer.matched, boundary.index); peer.next = peer.matched + 1; peer.retry_after = 0; peer.snapshot_retry_clock.reset();
			replicate(source, peer, output, needed_probe(source));
		} else { release_snapshot(source, peer, response.result == SnapshotReply::CaughtUp ? SnapshotReleaseReason::CaughtUp : SnapshotReleaseReason::Rejected, output, response.result == SnapshotReply::CaughtUp); }
	}

	void drive(Actions& output, bool allow_campaign) {
		if (!started_ || busy()) { return; }
		if (role_ != Role::Leader) {
			// Campaigns require a separately admitted Tick. Slow persistence
			// must not start another election from its completion callback.
			if (!allow_campaign || now_ < election_deadline_ || hard_.term == maximum) { return; }
			++hard_.term; hard_.voted_for = configuration_.local;
			role_ = Role::Candidate; leader_ = 0; grants_ = {configuration_.local}; reset_election();
			Actions deferred; deferred.emplace_back(RoleChanged{role_, hard_.term, 0});
			for (NodeId voter : configuration_.voters) {
				if (voter != configuration_.local) { deferred.emplace_back(Send{voter, VoteRequest{hard_.term, last_index(), log_term(last_index())}}); }
			}
			persist(StorageBatch{hard_, std::nullopt}, std::move(deferred), output, quorum() == 1); return;
		}
		bool heartbeat = now_ >= heartbeat_deadline_;
		for (auto& [id, peer] : peers_) {
			if (peer.snapshot && peer.snapshot->phase == SnapshotPhase::AwaitSource && peer.snapshot->key.boundary != LogBoundary{base_index_, base_term_}) { release_snapshot(id, peer, SnapshotReleaseReason::Stale, output); }
			if (peer.snapshot && now_ - peer.snapshot->started >= timing_.snapshot_timeout) { release_snapshot(id, peer, SnapshotReleaseReason::TimedOut, output, true); }
			if (peer.flight && !peer.flight->keepalive && now_ - peer.flight->sent >= timing_.rpc_timeout) { peer.flight.reset(); }
			if (peer.snapshot || (!peer.flight && (heartbeat || peer.next <= last_index()))) {
				auto probe = needed_probe(id);
				replicate(id, peer, output, probe);
			}
		}
		if (heartbeat) { heartbeat_deadline_ = deadline(timing_.heartbeat); }
	}
	void become_leader(Actions& output) {
		if (busy()) { throw std::logic_error("leader election before vote persistence"); }
		if (last_index() >= maximum - 1 || entries_.size() >= limits_.log_entries) {
			follower(0, output); return; // Control reserve exhausted: fail closed.
		}
		role_ = Role::Leader; leader_ = configuration_.local; peers_.clear(); grants_.clear();
		for (NodeId voter : configuration_.voters) { if (voter != configuration_.local) { peers_.emplace(voter, Peer{last_index() + 1, 0, {}}); } }
		Entry noop{last_index() + 1, hard_.term, EntryKind::NoOp, {}}; entries_.push_back(noop);
		Actions deferred; deferred.emplace_back(RoleChanged{role_, hard_.term, leader_});
		persist(StorageBatch{std::nullopt, LogMutation{noop.index, {noop}}}, std::move(deferred), output);
		heartbeat_deadline_ = now_;
	}
	void snapshot_keepalive(NodeId id, Peer& peer, Actions& output) {
		if (peer.flight && (!peer.flight->keepalive || now_ - peer.flight->sent < timing_.heartbeat)) { return; }
		auto rpc = token(); peer.flight = Flight{rpc, 0, 0, 0, now_, true};
		output.emplace_back(Send{id, AppendRequest{hard_.term, rpc, 0, 0, 0, 0, {}}});
	}

	void replicate(NodeId id, Peer& peer, Actions& output, Token probe) {
		if (peer.snapshot) { snapshot_keepalive(id, peer, output); return; }
		if (peer.flight || now_ < peer.retry_after || (peer.snapshot_retry_clock && now_ <= *peer.snapshot_retry_clock)) { return; }
		if (peer.next <= base_index_) {
			SnapshotKey key{durable_.term, token(), {base_index_, base_term_}};
			peer.snapshot.emplace(SnapshotFlight{key, SnapshotPhase::AwaitSource, now_}); peer.snapshot_retry_clock.reset();
			output.emplace_back(SnapshotNeeded{id, key}); snapshot_keepalive(id, peer, output); return;
		}
		Index previous = peer.next - 1;
		std::vector<Entry> batch; std::size_t bytes = 0;
		for (Index index = peer.next; index <= last_index() && batch.size() < limits_.rpc_entries; ++index) {
			const auto& entry = log_entry(index);
			if (entry.payload.size() > limits_.rpc_bytes - bytes) { break; }
			bytes += entry.payload.size(); batch.push_back(entry);
		}
		auto rpc = token();
		peer.flight = Flight{rpc, previous, previous + batch.size(), probe, now_};
		output.emplace_back(Send{id, AppendRequest{hard_.term, rpc, previous, log_term(previous), durable_.commit_index, probe, std::move(batch)}});
	}
	void advance_commit(Actions& output) {
		if (busy() || role_ != Role::Leader) { return; }
		std::vector<Index> matches{last_index()};
		for (const auto& [id, peer] : peers_) { matches.push_back(peer.matched); }
		std::sort(matches.begin(), matches.end(), std::greater<Index>());
		auto candidate = matches[quorum() - 1];
		if (candidate <= durable_.commit_index || log_term(candidate) != hard_.term) { return; }
		hard_.commit_index = candidate;
		persist(StorageBatch{hard_, std::nullopt}, {}, output);
	}
	void deliver(Actions& output) {
		if (busy() || delivered_through_ || applied_ >= durable_.commit_index) { return; }
		std::vector<Entry> batch; std::size_t bytes = 0;
		for (Index index = applied_ + 1; index <= durable_.commit_index && batch.size() < limits_.rpc_entries; ++index) {
			const auto& entry = log_entry(index);
			if (entry.payload.size() > limits_.rpc_bytes - bytes) { break; }
			bytes += entry.payload.size(); batch.push_back(entry);
		}
		delivered_through_ = batch.back().index;
		output.emplace_back(Committed{applied_ + 1, std::move(batch)});
	}
	void ready_reads(Actions& output) {
		if (busy() || role_ != Role::Leader) { return; }
		for (auto read = reads_.begin(); read != reads_.end();) {
			if (read->term == hard_.term && read->acknowledgements.size() >= quorum() && applied_ >= read->index) {
				output.emplace_back(ReadReady{read->request, read->index}); read = reads_.erase(read);
			} else { ++read; }
		}
	}

	FixedConfiguration configuration_;
	HardState hard_, durable_;
	std::vector<Entry> entries_;
	Index base_index_ = 0;
	Term base_term_ = 0;
	Limits limits_;
	Timing timing_;
	Role role_ = Role::Follower;
	NodeId leader_ = 0;
	std::size_t log_bytes_ = 0;
	std::uint64_t now_ = 0, election_delay_, election_deadline_ = 0, heartbeat_deadline_ = 0;
	Token next_token_ = 0;
	Index applied_ = 0, delivered_through_ = 0;
	bool started_ = false;
	std::optional<Pending> pending_;
	std::optional<PendingInstall> install_;
	std::map<NodeId, Peer> peers_;
	std::set<NodeId> grants_;
	std::vector<PendingRead> reads_;
};

} // namespace cluster::consensus
