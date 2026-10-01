/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
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

// A ready-to-use RaftDelegate::is_alive() implementation: track when each
// peer was last heard from, and report "alive" only within a timeout of
// that -- see raft.h's own is_alive() comment for why this matters. Found
// via a real production bug in a RaftDelegate consumer (Detent,
// github.com/Kronuz/JustKnobs): a delegate whose is_alive() just checked
// static configured membership (always true for any configured peer,
// alive or not) let a single permanently-dead peer silently and
// permanently block every future commit, even with an otherwise-healthy
// majority -- heartbeat_cb() picks the next log entry to broadcast as the
// MINIMUM next_index among is_alive()-reported-alive peers, so a dead
// peer wrongly reported alive freezes that pick at its last (stale) value
// forever.
//
// This is a reference implementation, not a requirement -- a delegate with
// its own authoritative liveness source (a membership/gossip layer, a
// test harness that explicitly tracks which simulated nodes are "up")
// should use that instead. This exists for the common case: a delegate
// whose only liveness signal is "do Raft messages keep arriving from this
// peer."

#pragma once

#include <chrono>
#include <map>
#include <mutex>
#include <string>

namespace cluster {

class LivenessTracker {
public:
	// `timeout`: how long since a peer's last message before it's reported
	// dead. raft.h's own delegates typically use 3-4x their heartbeat
	// interval, long enough to absorb a missed heartbeat or two without
	// flapping, short enough to detect a real death promptly.
	explicit LivenessTracker(std::chrono::duration<double> timeout)
		: timeout_(timeout), created_at_(std::chrono::steady_clock::now()) {}

	// Call this whenever a message is successfully parsed from `id` --
	// typically from within parse_node() or wherever a delegate first
	// identifies the sender of an incoming Raft/bus message.
	void touch(const std::string& id) {
		std::lock_guard<std::mutex> lock(mutex_);
		last_seen_[id] = std::chrono::steady_clock::now();
	}

	// A peer never yet touch()'d is judged against the tracker's OWN
	// construction time, not treated as permanently alive -- the "benefit
	// of the doubt" is a grace window for a brand-new peer that hasn't had
	// a chance to send anything yet, not an exemption for a peer that
	// never, ever establishes contact. Without this, a peer that's dead
	// from the very start (never once touch()'d) would be misreported
	// alive forever, the same class of bug this class exists to fix.
	[[nodiscard]] bool is_alive(const std::string& id) const {
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = last_seen_.find(id);
		auto since = (it == last_seen_.end()) ? created_at_ : it->second;
		return (std::chrono::steady_clock::now() - since) < timeout_;
	}

	// How many of `ids` are currently alive -- a convenience for a
	// delegate's own alive_nodes() over its known peer id list.
	template <typename Container>
	[[nodiscard]] std::size_t count_alive(const Container& ids) const {
		std::size_t count = 0;
		for (const auto& id : ids) {
			if (is_alive(id)) ++count;
		}
		return count;
	}

private:
	mutable std::mutex mutex_;
	std::chrono::duration<double> timeout_;
	std::chrono::steady_clock::time_point created_at_;
	std::map<std::string, std::chrono::steady_clock::time_point> last_seen_;
};

} // namespace cluster
