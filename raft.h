/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * Copyright (c) 2015-2019 Dubalu LLC  (the Raft algorithm this faithfully ports)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// cluster::Raft -- a leader-election + replicated-log consensus module, a faithful
// (line-for-line) port of the proven Raft in Xapiand's discovery.cc, made generic: the
// algorithm and the byte-identical wire format are the library's; the transport, the node
// type, the membership, and the cluster-lifecycle side effects are injected through a
// RaftDelegate the app implements. It runs on one reactor loop (bus.io()) via
// reactor::PeriodicTimer election/heartbeat timers -- single-threaded, no locks on its
// state. add_command() is the one thread-safe entry point (posts onto the loop).
//
// Faithfulness: every state transition, vote rule, log-matching/commit rule, and the
// Xapiand primary-preference hybrid (eligible + prefers/is_superset) are preserved
// verbatim. Only the trace logging (L_RAFT*) is dropped -- side-band observability, not
// behavior; add an app trace hook if you want it back. The timing constants that were
// fixed in Xapiand are now RaftConfig (defaults reproduce Xapiand's exactly).
//
// Standalone Asio only (ASIO_STANDALONE), header-only, C++20.

#pragma once

#include "length.h"
#include "reactor_events.h"

#include <asio.hpp>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace cluster {

enum class RaftRole { FOLLOWER, CANDIDATE, LEADER };

// The logical Raft messages. The app maps these onto its wire type values (both directions)
// and routes an incoming RAFT_* datagram to Raft::on_message(logical, content).
enum class RaftMessage {
	REQUEST_VOTE,
	REQUEST_VOTE_RESPONSE,
	APPEND_ENTRIES,
	HEARTBEAT,
	APPEND_ENTRIES_RESPONSE,
	HEARTBEAT_RESPONSE,
	ADD_COMMAND,
};

struct RaftLogEntry {
	std::uint64_t term;
	std::string command;
};

// The timing knobs (seconds). Defaults reproduce Xapiand's (HEARTBEAT_TIMEOUT = 1s):
// heartbeat 1, election_init 4, election_min 10, election_max 30. Tests pass tiny values.
struct RaftConfig {
	double heartbeat_timeout = 1.0;
	double election_init = 4.0;
	double election_min = 10.0;
	double election_max = 30.0;
};

// The injected seams. Node is the app's node type (default-constructible = "empty").
template <typename Node>
struct RaftDelegate {
	virtual ~RaftDelegate() = default;

	// transport: broadcast a raft message (the app maps the logical type to its wire type
	// and prepends nothing -- the payload is already the full body).
	virtual void broadcast(RaftMessage msg, const std::string& payload) = 0;

	// node model / serialization (byte-identical to how the app serialises its node)
	virtual Node local_node() = 0;
	virtual std::string serialise(const Node& node) = 0;
	// parse a node off the wire (advance *p), update the membership table, and return it;
	// nullopt => ignore this message (unknown/nonexistent node).
	virtual std::optional<Node> parse_node(const char** p, const char* end) = 0;
	virtual std::string node_id(const Node& node) = 0;   // stable voting id (lower_name)

	// membership / quorum
	virtual std::size_t total_nodes() = 0;
	virtual std::size_t alive_nodes() = 0;
	virtual bool quorum(std::size_t total, std::size_t count) = 0;
	virtual bool prefers(const Node& a, const Node& b) = 0;   // is_superset(a,b): a outranks b
	virtual bool is_alive(const std::string& id) = 0;

	// cluster-lifecycle gates + hooks
	virtual bool active() = 0;    // may participate now (state in JOINING/SETUP/READY)
	virtual bool ready() = 0;     // state == READY (heartbeat leader path gate)
	virtual bool joining() = 0;   // state == JOINING (single-node bootstrap special-case)
	virtual void ensure_setup() = 0;                      // JOINING -> SETUP + setup_node
	virtual void set_leader(const Node& leader) = 0;      // _raft_set_leader_node
	virtual void apply(const std::string& command) = 0;   // _raft_apply_command
};

template <typename Node>
class Raft {
public:
	Raft(asio::any_io_executor ex, RaftConfig cfg, RaftDelegate<Node>* delegate)
		: ex_(ex), cfg_(cfg), d_(delegate),
		  election_timer_(ex), heartbeat_timer_(ex), add_command_signal_(ex), relinquish_signal_(ex),
		  rng_(std::random_device{}()) {
		election_timer_.set_callback([this] { election_timeout_cb(); });
		heartbeat_timer_.set_callback([this] { heartbeat_cb(); });
		add_command_signal_.set_callback([this] { drain_add_commands(); });
		relinquish_signal_.set_callback([this] { request_vote(false); });
	}

	// Arm the initial (fast) election timeout. Call once the transport is up.
	void start() { election_timeout_reset(cfg_.election_init); }

	// Stop the timers (call before tearing down the loop).
	void stop() { election_timer_.stop(); heartbeat_timer_.stop(); }

	// Dispatch an incoming raft message (routed by the app from the bus). On the loop.
	void on_message(RaftMessage msg, std::string_view content) {
		switch (msg) {
			case RaftMessage::REQUEST_VOTE:            on_request_vote(content); break;
			case RaftMessage::REQUEST_VOTE_RESPONSE:   on_request_vote_response(content); break;
			case RaftMessage::APPEND_ENTRIES:
			case RaftMessage::HEARTBEAT:               on_append_entries(msg, content); break;
			case RaftMessage::APPEND_ENTRIES_RESPONSE:
			case RaftMessage::HEARTBEAT_RESPONSE:      on_append_entries_response(msg, content); break;
			case RaftMessage::ADD_COMMAND:             on_add_command(content); break;
		}
	}

	// Add a command to the replicated log. Thread-safe (posts onto the loop).
	void add_command(const std::string& command) {
		{ std::lock_guard<std::mutex> lk(cmd_mtx_); cmd_queue_.push_back(command); }
		add_command_signal_.send();
	}

	// Step down + reset the election timeout (Xapiand's _raft_request_vote(false) /
	// raft_relinquish_leadership). Thread-safe (posts onto the loop); used when the app
	// detects a lost leader / lost quorum (e.g. a CLUSTER_BYE from the leader).
	void relinquish_leadership() { relinquish_signal_.send(); }

	RaftRole role() const { return role_; }
	std::uint64_t term() const { return current_term_; }

	// Whether this node may become leader / vote for itself (Xapiand's raft_eligible; the
	// primary-preference hybrid). Default true.
	void set_eligible(bool e) { eligible_ = e; }

private:
	// ---- helpers ----
	double random_election() {
		std::uniform_real_distribution<double> dist(cfg_.election_min, cfg_.election_max);
		return dist(rng_);
	}

	void leader_heartbeat_reset(double timeout) {   // _raft_leader_heartbeat_reset
		election_timer_.stop();
		heartbeat_timer_.set_repeat(timeout);
		heartbeat_timer_.again();
	}
	void election_timeout_reset(double timeout) {   // _raft_leader_election_timeout_reset
		election_timer_.set_repeat(timeout);
		election_timer_.again();
		heartbeat_timer_.stop();
	}

	void set_leader(const Node& node) { d_->set_leader(node); }   // _raft_set_leader_node
	void apply_command(const std::string& command) { d_->apply(command); }

	void commit_log() {   // _raft_commit_log
		auto last_index = log_.size();
		for (std::size_t index = commit_index_ + 1; index <= last_index; ++index) {
			if (log_[index - 1].term == current_term_) {
				std::size_t matches = 1;
				for (const auto& mi : match_indexes_) {
					if (mi.second >= index) { ++matches; }
				}
				if (d_->quorum(d_->total_nodes(), matches)) {
					commit_index_ = index;
					if (commit_index_ > last_applied_) {
						while (commit_index_ > last_applied_ && last_applied_ < last_index) {
							apply_command(log_[last_applied_].command);
							++last_applied_;
						}
					}
				}
			}
		}
	}

	// ---- election ----
	void request_vote(bool immediate) {   // _raft_request_vote
		set_leader(Node{});

		if (immediate) {
			++current_term_;
			if (eligible_) {
				role_ = RaftRole::CANDIDATE;
				voted_for_.reset();
				next_indexes_.clear();
				match_indexes_.clear();
				votes_granted_ = 0;
				votes_denied_ = 0;
				voters_.clear();
			} else {
				role_ = RaftRole::FOLLOWER;
				voted_for_.reset();
				next_indexes_.clear();
				match_indexes_.clear();
			}

			election_timeout_reset(random_election());

			auto last_log_index = log_.size();
			auto last_log_term = last_log_index > 0 ? log_[last_log_index - 1].term : 0;
			Node local = d_->local_node();
			d_->broadcast(RaftMessage::REQUEST_VOTE,
				d_->serialise(local) +
				serialise_bool(eligible_) +
				serialise_length(current_term_) +
				serialise_length(last_log_term) +
				serialise_length(last_log_index));
		} else {
			role_ = RaftRole::FOLLOWER;
			voted_for_.reset();
			next_indexes_.clear();
			match_indexes_.clear();
			if (current_term_ == 0) {
				election_timeout_reset(cfg_.election_init);
			} else {
				election_timeout_reset(random_election());
			}
		}
	}

	void election_timeout_cb() {   // raft_leader_election_timeout_cb
		if (d_->joining()) {
			d_->ensure_setup();
			if (current_term_ == 0) {
				++current_term_;
				Node local = d_->local_node();
				role_ = RaftRole::LEADER;
				voted_for_.reset();
				next_indexes_.clear();
				match_indexes_.clear();
				leader_heartbeat_reset(cfg_.heartbeat_timeout);
				set_leader(local);
				return;
			}
			// fallthrough into the participating path
		} else if (!d_->active()) {
			return;
		}
		if (role_ == RaftRole::LEADER) { return; }
		request_vote(true);
	}

	void on_request_vote(std::string_view message) {   // raft_request_vote
		if (!d_->active()) { return; }
		const char* p = message.data();
		const char* p_end = p + message.size();

		auto node_opt = d_->parse_node(&p, p_end);
		if (!node_opt) { return; }
		const Node& node = *node_opt;

		bool eligible = false;
		if (!unserialise_bool(&p, p_end, eligible)) { return; }
		std::uint64_t term = 0;
		if (!unserialise_length(&p, p_end, term)) { return; }
		if (term > current_term_) {
			current_term_ = term;
			role_ = RaftRole::FOLLOWER;
			voted_for_.reset();
			next_indexes_.clear();
			match_indexes_.clear();
			election_timeout_reset(random_election());
		}

		Node local = d_->local_node();

		if (term == current_term_) {
			if (!voted_for_) {
				if (!eligible || d_->prefers(local, node)) {
					if (!eligible_) { return; }   // I refuse to vote (me)
					voted_for_ = local;
					if (voters_.insert(d_->node_id(local)).second) { ++votes_granted_; }
				} else if (role_ == RaftRole::FOLLOWER) {
					std::uint64_t remote_last_log_term = 0;
					std::uint64_t remote_last_log_index_u = 0;
					if (!unserialise_length(&p, p_end, remote_last_log_term)) { return; }
					if (!unserialise_length(&p, p_end, remote_last_log_index_u)) { return; }
					std::size_t remote_last_log_index = static_cast<std::size_t>(remote_last_log_index_u);
					auto last_log_index = log_.size();
					auto last_log_term = last_log_index > 0 ? log_[last_log_index - 1].term : 0;
					if (last_log_term < remote_last_log_term) {
						voted_for_ = node;
						if (voters_.insert(d_->node_id(local)).second) { ++votes_denied_; }
					} else if (last_log_term == remote_last_log_term) {
						if (log_.size() <= remote_last_log_index) {
							voted_for_ = node;
							if (voters_.insert(d_->node_id(local)).second) { ++votes_denied_; }
						}
					}
				}
			}
			if (d_->joining()) { d_->ensure_setup(); }
		}

		auto total_nodes = d_->total_nodes();
		d_->broadcast(RaftMessage::REQUEST_VOTE_RESPONSE,
			d_->serialise(local) +
			serialise_length(term) +
			serialise_length(total_nodes) +
			d_->serialise(voted_for_ ? *voted_for_ : Node{}));
	}

	void on_request_vote_response(std::string_view message) {   // raft_request_vote_response
		if (!d_->active()) { return; }
		if (role_ != RaftRole::CANDIDATE) { return; }

		const char* p = message.data();
		const char* p_end = p + message.size();

		auto node_opt = d_->parse_node(&p, p_end);
		if (!node_opt) { return; }
		const Node& node = *node_opt;
		Node local = d_->local_node();

		std::uint64_t term = 0;
		if (!unserialise_length(&p, p_end, term)) { return; }
		if (term > current_term_) {
			current_term_ = term;
			role_ = RaftRole::FOLLOWER;
			voted_for_.reset();
			next_indexes_.clear();
			match_indexes_.clear();
			election_timeout_reset(random_election());
		}

		if (term == current_term_) {
			std::uint64_t total_nodes_u = 0;
			if (!unserialise_length(&p, p_end, total_nodes_u)) { return; }
			std::size_t total_nodes = std::max(static_cast<std::size_t>(total_nodes_u), d_->total_nodes());

			if (voters_.insert(d_->node_id(node)).second) {
				auto voted_for_node = d_->parse_node(&p, p_end);
				if (voted_for_node) {
					if (d_->prefers(local, *voted_for_node)) { ++votes_granted_; }
					else { ++votes_denied_; }
				} else {
					++votes_denied_;
				}
			}
			if (d_->quorum(total_nodes, votes_granted_ + votes_denied_)) {
				if (votes_granted_ > votes_denied_) {
					role_ = RaftRole::LEADER;
					voted_for_.reset();
					next_indexes_.clear();
					match_indexes_.clear();

					leader_heartbeat_reset(cfg_.heartbeat_timeout);
					set_leader(local);

					auto entry_index = log_.size();
					auto prev_log_index = entry_index - 1;
					auto prev_log_term = entry_index > 1 ? log_[prev_log_index - 1].term : 0;
					d_->broadcast(RaftMessage::APPEND_ENTRIES,
						d_->serialise(local) +
						serialise_length(current_term_) +
						serialise_length(prev_log_index) +
						serialise_length(prev_log_term) +
						serialise_length(commit_index_));

					if (d_->joining()) { d_->ensure_setup(); }
				}
			}
		}
	}

	// ---- append entries (leader recognition + log replication) ----
	void on_append_entries(RaftMessage type, std::string_view message) {   // raft_append_entries
		if (!d_->active()) { return; }

		const char* p = message.data();
		const char* p_end = p + message.size();

		auto node_opt = d_->parse_node(&p, p_end);
		if (!node_opt) { return; }
		const Node& node = *node_opt;
		Node local = d_->local_node();

		std::uint64_t term = 0;
		if (!unserialise_length(&p, p_end, term)) { return; }

		if (role_ == RaftRole::LEADER) {
			if (!d_->prefers(local, node)) {
				request_vote(true);
			}
			return;
		}

		if (term < current_term_) {
			request_vote(true);
			return;
		}

		if (term > current_term_) {
			current_term_ = term;
			role_ = RaftRole::FOLLOWER;
			voted_for_.reset();
			next_indexes_.clear();
			match_indexes_.clear();
		}

		std::size_t next_index = 0;
		std::size_t match_index = 0;
		bool success = false;

		if (term == current_term_) {
			std::uint64_t prev_log_index_u = 0;
			std::uint64_t prev_log_term = 0;
			if (!unserialise_length(&p, p_end, prev_log_index_u)) { return; }
			if (!unserialise_length(&p, p_end, prev_log_term)) { return; }
			std::size_t prev_log_index = static_cast<std::size_t>(prev_log_index_u);

			if (role_ == RaftRole::CANDIDATE) {
				role_ = RaftRole::FOLLOWER;
				voted_for_.reset();
				next_indexes_.clear();
				match_indexes_.clear();
			}

			election_timeout_reset(random_election());
			set_leader(node);

			auto last_index = log_.size();
			auto entry_index = prev_log_index + 1;
			if (entry_index <= 1 || (prev_log_index <= last_index && log_[prev_log_index - 1].term == prev_log_term)) {
				std::uint64_t leader_commit_u = 0;
				if (!unserialise_length(&p, p_end, leader_commit_u)) { return; }
				std::size_t leader_commit = static_cast<std::size_t>(leader_commit_u);

				if (p != p_end) {
					std::uint64_t last_log_index_u = 0;
					std::uint64_t entry_term = 0;
					std::string_view entry_command;
					if (!unserialise_length(&p, p_end, last_log_index_u)) { return; }
					if (!unserialise_length(&p, p_end, entry_term)) { return; }
					if (!unserialise_string(&p, p_end, entry_command)) { return; }
					std::size_t last_log_index = static_cast<std::size_t>(last_log_index_u);
					(void)last_log_index;

					if (entry_index <= last_index) {
						if (entry_index >= 1 && log_[entry_index - 1].term != entry_term) {
							log_.resize(entry_index - 1);
							log_.push_back({entry_term, std::string(entry_command)});
							last_index = log_.size();
						} else if (entry_index == last_log_index_u) {
							return;
						}
					} else {
						log_.push_back({entry_term, std::string(entry_command)});
						last_index = log_.size();
					}
				}

				if (leader_commit > commit_index_) {
					commit_index_ = std::min(leader_commit, entry_index);
					if (commit_index_ > last_applied_) {
						while (commit_index_ > last_applied_ && last_applied_ < last_index) {
							apply_command(log_[last_applied_].command);
							++last_applied_;
						}
					}
				}

				if (leader_commit == commit_index_) {
					if (d_->joining()) { d_->ensure_setup(); }
				}

				next_index = last_index + 1;
				match_index = entry_index;
				success = true;
			}
		}

		RaftMessage response_type = (type == RaftMessage::HEARTBEAT)
			? RaftMessage::HEARTBEAT_RESPONSE
			: RaftMessage::APPEND_ENTRIES_RESPONSE;
		d_->broadcast(response_type,
			d_->serialise(local) +
			serialise_length(term) +
			serialise_length(success ? 1 : 0) +
			(success
				? serialise_length(next_index) + serialise_length(match_index)
				: std::string()));
	}

	void on_append_entries_response(RaftMessage /*type*/, std::string_view message) {   // raft_append_entries_response
		if (!d_->active()) { return; }

		const char* p = message.data();
		const char* p_end = p + message.size();

		auto node_opt = d_->parse_node(&p, p_end);
		if (!node_opt) { return; }
		const Node& node = *node_opt;

		if (role_ != RaftRole::LEADER) { return; }

		std::uint64_t term = 0;
		if (!unserialise_length(&p, p_end, term)) { return; }
		if (term > current_term_) {
			current_term_ = term;
			role_ = RaftRole::FOLLOWER;
			voted_for_.reset();
			next_indexes_.clear();
			match_indexes_.clear();
			election_timeout_reset(random_election());
		}

		if (term == current_term_) {
			bool success = false;
			if (!unserialise_bool_len(&p, p_end, success)) { return; }
			std::string id = d_->node_id(node);
			if (success) {
				std::uint64_t next_index = 0, match_index = 0;
				if (!unserialise_length(&p, p_end, next_index)) { return; }
				if (!unserialise_length(&p, p_end, match_index)) { return; }
				next_indexes_[id] = static_cast<std::size_t>(next_index);
				match_indexes_[id] = static_cast<std::size_t>(match_index);
			} else {
				auto it = next_indexes_.find(id);
				auto& next_index = it == next_indexes_.end()
					? next_indexes_[id] = log_.size() + 2
					: it->second;
				if (next_index > 1) { --next_index; }
			}
			commit_log();
		}
	}

	void on_add_command(std::string_view message) {   // raft_add_command (message handler)
		if (!d_->active()) { return; }
		const char* p = message.data();
		const char* p_end = p + message.size();
		auto node_opt = d_->parse_node(&p, p_end);
		if (!node_opt) { return; }
		if (role_ != RaftRole::LEADER) { return; }
		std::string_view command;
		if (!unserialise_string(&p, p_end, command)) { return; }
		do_add_command(std::string(command));
	}

	// ---- heartbeat (leader) ----
	void heartbeat_cb() {   // raft_leader_heartbeat_cb
		if (!d_->ready()) { return; }
		if (role_ != RaftRole::LEADER) { return; }

		auto total_nodes = d_->total_nodes();
		auto alive_nodes = d_->alive_nodes();
		if (!d_->quorum(total_nodes, alive_nodes)) {
			request_vote(false);
			return;
		}

		Node local = d_->local_node();
		auto last_log_index = log_.size();
		if (last_log_index > 0) {
			auto entry_index = last_log_index + 1;
			for (const auto& ni : next_indexes_) {
				if (d_->is_alive(ni.first)) {
					if (entry_index > ni.second) { entry_index = ni.second; }
				}
			}
			if (entry_index > 0 && entry_index <= last_log_index) {
				auto prev_log_index = entry_index - 1;
				auto prev_log_term = entry_index > 1 ? log_[prev_log_index - 1].term : 0;
				auto entry_term = log_[entry_index - 1].term;
				auto entry_command = log_[entry_index - 1].command;
				d_->broadcast(RaftMessage::APPEND_ENTRIES,
					d_->serialise(local) +
					serialise_length(current_term_) +
					serialise_length(prev_log_index) +
					serialise_length(prev_log_term) +
					serialise_length(commit_index_) +
					serialise_length(last_log_index) +
					serialise_length(entry_term) +
					serialise_string(entry_command));
				return;
			}
		}

		auto last_log_term = last_log_index > 0 ? log_[last_log_index - 1].term : 0;
		d_->broadcast(RaftMessage::HEARTBEAT,
			d_->serialise(local) +
			serialise_length(current_term_) +
			serialise_length(last_log_index) +
			serialise_length(last_log_term) +
			serialise_length(commit_index_));
	}

	// ---- add command (leader append / follower forward) ----
	void do_add_command(const std::string& command) {   // _raft_add_command
		if (role_ == RaftRole::LEADER) {
			if (commit_index_ < log_.size() && log_[commit_index_].term != current_term_) {
				log_.resize(commit_index_);
			}
			if (std::find_if(log_.begin(), log_.end(), [&](const RaftLogEntry& e) {
					return e.command == command;
				}) != log_.end()) {
				return;
			}
			log_.push_back({current_term_, command});
			commit_log();
		} else {
			Node local = d_->local_node();
			d_->broadcast(RaftMessage::ADD_COMMAND, d_->serialise(local) + serialise_string(command));
		}
	}

	void drain_add_commands() {   // raft_add_command_async_cb
		for (;;) {
			std::string command;
			{
				std::lock_guard<std::mutex> lk(cmd_mtx_);
				if (cmd_queue_.empty()) { break; }
				command = std::move(cmd_queue_.front());
				cmd_queue_.pop_front();
			}
			do_add_command(command);
		}
	}

	// success is serialised as a length (0/1), not a bool, in the *_RESPONSE body.
	static bool unserialise_bool_len(const char** p, const char* end, bool& out) {
		std::uint64_t v = 0;
		if (!unserialise_length(p, end, v)) { return false; }
		out = (v != 0);
		return true;
	}

	asio::any_io_executor ex_;
	RaftConfig cfg_;
	RaftDelegate<Node>* d_;

	reactor::PeriodicTimer election_timer_;
	reactor::PeriodicTimer heartbeat_timer_;
	reactor::Signal add_command_signal_;
	reactor::Signal relinquish_signal_;

	RaftRole role_ = RaftRole::FOLLOWER;
	std::uint64_t current_term_ = 0;
	std::optional<Node> voted_for_;
	std::set<std::string> voters_;
	std::size_t votes_granted_ = 0;
	std::size_t votes_denied_ = 0;
	std::map<std::string, std::size_t> next_indexes_;
	std::map<std::string, std::size_t> match_indexes_;
	std::vector<RaftLogEntry> log_;
	std::size_t commit_index_ = 0;
	std::size_t last_applied_ = 0;
	bool eligible_ = true;

	std::mutex cmd_mtx_;
	std::deque<std::string> cmd_queue_;
	std::mt19937_64 rng_;
};

}  // namespace cluster
