// Fixed-membership, controlled-message regression schedules for legacy Raft.
// Only election initiation uses a timer; all protocol delivery is explicit.
#include "raft.h"

#include <chrono>
#include <cstdio>
#include <memory>
#include <stdexcept>

using cluster::RaftMessage;
using cluster::serialise_length;
using cluster::serialise_string;

static int failures = 0;
static void check(bool value, const char* description) {
	std::printf("%s: %s\n", value ? "PASS" : "FAIL", description);
	if (!value) { ++failures; }
}

struct Node { std::string id; };
struct Delegate : cluster::RaftDelegate<Node> {
	struct Message { RaftMessage type; std::string body; };
	std::size_t members;
	std::string me;
	std::vector<Message> sent;
	std::vector<std::string> applied;
	explicit Delegate(std::size_t count, std::string id = "A") : members(count), me(std::move(id)) {}
	void broadcast(RaftMessage type, const std::string& body) override { sent.push_back({type, body}); }
	Node local_node() override { return {me}; }
	std::string serialise(const Node& node) override { return serialise_string(node.id); }
	std::optional<Node> parse_node(const char** p, const char* end) override {
		std::string_view id;
		if (!cluster::unserialise_string(p, end, id) || id.size() != 1 ||
			id[0] < 'A' || id[0] >= 'A' + members) { return std::nullopt; }
		return Node{std::string(id)};
	}
	std::string node_id(const Node& node) override { return node.id; }
	std::size_t total_nodes() override { return members; }
	std::size_t alive_nodes() override { return members; }
	bool quorum(std::size_t total, std::size_t votes) override { return votes > total / 2; }
	bool prefers(const Node& a, const Node& b) override { return a.id == b.id; }
	bool is_alive(const std::string&) override { return true; }
	bool active() override { return true; }
	bool ready() override { return true; }
	bool joining() override { return false; }
	void ensure_setup() override {}
	void set_leader(const Node&) override {}
	void apply(const std::string& command) override { applied.push_back(command); }
};

struct Fixture {
	asio::io_context io;
	Delegate delegate;
	cluster::Raft<Node> raft;
	bool keep_heartbeats;
	static cluster::RaftConfig config(bool heartbeats) {
		cluster::RaftConfig cfg;
		cfg.election_init = 0.001;
		cfg.election_min = cfg.election_max = cfg.heartbeat_timeout = 3600;
		if (heartbeats) { cfg.heartbeat_timeout = 0.001; }
		return cfg;
	}
	explicit Fixture(std::size_t members = 3, std::string id = "A", bool heartbeats = false)
		: delegate(members, std::move(id)), raft(io.get_executor(), config(heartbeats), &delegate), keep_heartbeats(heartbeats) {}
	~Fixture() { raft.stop(); io.restart(); io.poll(); }
	void candidate(bool loopback = true) {
		raft.start();
		io.restart();
		io.run_for(std::chrono::milliseconds(20));
		raft.stop();
		io.restart();
		io.poll();
		if (raft.role() != cluster::RaftRole::CANDIDATE) { throw std::runtime_error("election did not start"); }
		if (!loopback) { return; }
		// The legacy bus loops requests back to the sender to cast its own vote.
		for (const auto& message : delegate.sent) {
			if (message.type == RaftMessage::REQUEST_VOTE) {
				auto body = message.body;
				raft.on_message(RaftMessage::REQUEST_VOTE, body);
				break;
			}
		}
	}
	void vote(const char* peer, const char* recipient = "A") {
		raft.on_message(RaftMessage::REQUEST_VOTE_RESPONSE,
			serialise_string(peer) + serialise_length(raft.term()) +
			serialise_length(delegate.members) + serialise_string(recipient));
	}
	void leader() {
		candidate();
		vote("B");
		if (raft.role() != cluster::RaftRole::LEADER) { throw std::runtime_error("leader not elected"); }
		if (!keep_heartbeats) { raft.stop(); }
		io.restart(); io.poll();
	}
	void command(const char* value) {
		raft.on_message(RaftMessage::ADD_COMMAND, serialise_string("A") + serialise_string(value));
	}
};

static std::string append(const char* leader, std::uint64_t term, std::uint64_t prev,
	std::uint64_t prev_term, std::uint64_t committed) {
	return serialise_string(leader) + serialise_length(term) + serialise_length(prev) +
		serialise_length(prev_term) + serialise_length(committed);
}

static void heartbeat_safety() {
	Fixture follower;
	follower.raft.on_message(RaftMessage::HEARTBEAT, append("B", 1, 0, 0, 1));
	const auto& response = follower.delegate.sent.back().body;
	const char* p = response.data(); const char* end = p + response.size();
	std::string_view remote_id;
	std::uint64_t term = 0, success = 0, next = 0, matched = 0;
	bool valid = cluster::unserialise_string(&p, end, remote_id) &&
		cluster::unserialise_length(&p, end, term) && cluster::unserialise_length(&p, end, success) &&
		cluster::unserialise_length(&p, end, next) && cluster::unserialise_length(&p, end, matched) && p == end;
	check(valid && success == 1 && next == 1 && matched == 0, "empty heartbeat acknowledges only the verified log prefix");
	follower.raft.on_message(RaftMessage::APPEND_ENTRIES,
		append("B", 1, 0, 0, 1) + serialise_length(1) + serialise_length(1) + serialise_string("first"));
	check(follower.delegate.applied == std::vector<std::string>{"first"}, "heartbeat cannot advance commit beyond existing entries");
	auto acknowledgements = follower.delegate.sent.size();
	auto expected_ack = serialise_string("A") + serialise_length(1) + serialise_length(1) +
		serialise_length(2) + serialise_length(1);
	follower.raft.on_message(RaftMessage::APPEND_ENTRIES,
		append("B", 1, 0, 0, 1) + serialise_length(1) + serialise_length(1) + serialise_string("first"));
	check(follower.delegate.sent.size() == acknowledgements + 1 &&
		follower.delegate.sent.back().type == RaftMessage::APPEND_ENTRIES_RESPONSE &&
		follower.delegate.sent.back().body == expected_ack,
		"a matching retransmission sends a fresh acknowledgement");

	Fixture leader;
	leader.leader();
	Fixture peer(3, "B");
	peer.raft.on_message(RaftMessage::HEARTBEAT, append("A", leader.raft.term(), 0, 0, 0));
	leader.raft.on_message(RaftMessage::HEARTBEAT_RESPONSE, peer.delegate.sent.back().body);
	leader.command("isolated");
	check(leader.delegate.applied.empty(), "heartbeat progress cannot commit a later unreplicated command");
	Fixture mixed;
	mixed.leader();
	mixed.raft.on_message(RaftMessage::HEARTBEAT_RESPONSE, serialise_string("B") +
		serialise_length(mixed.raft.term()) + serialise_length(1) + serialise_length(1) + serialise_length(1));
	mixed.command("old-heartbeat");
	check(mixed.delegate.applied.empty(), "old follower heartbeat progress cannot commit a new leader's command");
	mixed.raft.on_message(RaftMessage::HEARTBEAT_RESPONSE, serialise_string("B") +
		serialise_length(mixed.raft.term()) + serialise_length(1) + serialise_length(2) + serialise_length(1));
	check(mixed.delegate.applied.empty(), "delayed old heartbeat cannot commit an existing unreplicated entry");
	mixed.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, serialise_string("A") +
		serialise_length(mixed.raft.term()) + serialise_length(1) + serialise_length(2) + serialise_length(1));
	check(mixed.delegate.applied.empty(), "self response cannot count a voter twice");
	mixed.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, serialise_string("B") +
		serialise_length(mixed.raft.term()) + serialise_length(1) + serialise_length(3) + serialise_length(2));
	check(mixed.delegate.applied.empty(), "response beyond the local tail cannot manufacture progress");
	mixed.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, serialise_string("B") +
		serialise_length(mixed.raft.term()) + serialise_length(1) + serialise_length(2) + serialise_length(1));
	check(mixed.delegate.applied == std::vector<std::string>{"old-heartbeat"},
		"a data acknowledgement permits majority commitment");
	Fixture longer;
	longer.leader();
	longer.command("one"); longer.command("two"); longer.command("three");
	longer.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, serialise_string("B") +
		serialise_length(longer.raft.term()) + serialise_length(1) + serialise_length(4) + serialise_length(1));
	check(longer.delegate.applied == std::vector<std::string>{"one"},
		"old follower's longer local tail does not invalidate a verified data acknowledgement");
	longer.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, serialise_string("B") +
		serialise_length(longer.raft.term()) + serialise_length(1) + serialise_length(4) + serialise_length(3));
	check(longer.delegate.applied == std::vector<std::string>({"one", "two", "three"}),
		"data acknowledgements can complete replay after an old follower's larger next hint");

	Fixture restarted_leader(3, "A", true);
	restarted_leader.leader();
	restarted_leader.command("before-restart");
	restarted_leader.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, serialise_string("B") +
		serialise_length(restarted_leader.raft.term()) + serialise_length(1) + serialise_length(2) + serialise_length(1));
	restarted_leader.command("after-restart");
	Fixture restarted_peer(3, "B");
	restarted_peer.raft.on_message(RaftMessage::APPEND_ENTRIES,
		append("A", restarted_leader.raft.term(), 1, restarted_leader.raft.term(), 1) +
		serialise_length(2) + serialise_length(restarted_leader.raft.term()) + serialise_string("after-restart"));
	restarted_leader.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, restarted_peer.delegate.sent.back().body);
	restarted_leader.delegate.sent.clear();
	restarted_leader.io.restart(); restarted_leader.io.run_for(std::chrono::milliseconds(10));
	bool replayed_prefix = false;
	for (const auto& message : restarted_leader.delegate.sent) {
		if (message.type != RaftMessage::APPEND_ENTRIES) { continue; }
		const char* position = message.body.data(); const char* finish = position + message.body.size();
		std::string_view id; std::uint64_t message_term = 0, previous = 0;
		if (cluster::unserialise_string(&position, finish, id) &&
			cluster::unserialise_length(&position, finish, message_term) &&
			cluster::unserialise_length(&position, finish, previous) && previous == 0) {
			replayed_prefix = true;
		}
	}
	check(replayed_prefix, "a restarted volatile follower can replay below its previously acknowledged prefix");
	for (const auto& message : restarted_leader.delegate.sent) {
		if (message.type == RaftMessage::APPEND_ENTRIES) {
			restarted_peer.raft.on_message(message.type, message.body);
		}
	}
	restarted_leader.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, restarted_peer.delegate.sent.back().body);
	restarted_leader.delegate.sent.clear();
	restarted_leader.io.restart(); restarted_leader.io.run_for(std::chrono::milliseconds(10));
	for (const auto& message : restarted_leader.delegate.sent) {
		if (message.type == RaftMessage::APPEND_ENTRIES) { restarted_peer.raft.on_message(message.type, message.body); }
	}
	restarted_leader.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, restarted_peer.delegate.sent.back().body);
	restarted_peer.raft.on_message(RaftMessage::HEARTBEAT,
		append("A", restarted_leader.raft.term(), 2, restarted_leader.raft.term(), 2));
	check(restarted_peer.delegate.applied == std::vector<std::string>({"before-restart", "after-restart"}),
		"a restarted follower replays both entries and catches up after rejection");
}

static void affirmative_majority() {
	Fixture election(5);
	election.candidate();
	election.vote("B");
	election.vote("C", "C");
	check(election.raft.role() == cluster::RaftRole::CANDIDATE,
		"five voters cannot elect with two grants and one denial");
	election.vote("B");
	check(election.raft.role() == cluster::RaftRole::CANDIDATE,
		"a duplicated affirmative response cannot create a quorum");
	election.vote("D");
	check(election.raft.role() == cluster::RaftRole::LEADER,
		"three distinct affirmative votes elect a leader in five voters");
}

static std::string request(const char* peer, std::uint64_t term) {
	return serialise_string(peer) + cluster::serialise_bool(true) + serialise_length(term) +
		serialise_length(0) + serialise_length(0);
}

static std::string voted_for(const Fixture& node) {
	const auto& message = node.delegate.sent.back();
	const char* p = message.body.data(); const char* end = p + message.body.size();
	std::string_view peer, vote;
	std::uint64_t term = 0, members = 0;
	if (message.type != RaftMessage::REQUEST_VOTE_RESPONSE ||
		!cluster::unserialise_string(&p, end, peer) || !cluster::unserialise_length(&p, end, term) ||
		!cluster::unserialise_length(&p, end, members) || !cluster::unserialise_string(&p, end, vote) || p != end) {
		throw std::runtime_error("invalid vote response");
	}
	return std::string(vote);
}

static void retained_vote() {
	Fixture candidate;
	candidate.candidate();
	candidate.raft.on_message(RaftMessage::HEARTBEAT, append("B", candidate.raft.term(), 0, 0, 0));
	candidate.raft.on_message(RaftMessage::REQUEST_VOTE, request("C", candidate.raft.term()));
	check(voted_for(candidate) == "A", "same-term leader traffic cannot erase a candidate's vote");

	Fixture step_down;
	step_down.candidate();
	step_down.raft.request_vote();
	step_down.io.restart(); step_down.io.poll();
	step_down.raft.on_message(RaftMessage::REQUEST_VOTE, request("B", step_down.raft.term()));
	check(voted_for(step_down) == "A", "explicit same-term step-down cannot grant a second vote");
	Fixture elected_leader;
	elected_leader.leader();
	elected_leader.raft.request_vote();
	elected_leader.io.restart(); elected_leader.io.poll();
	elected_leader.raft.on_message(RaftMessage::REQUEST_VOTE, request("B", elected_leader.raft.term()));
	check(voted_for(elected_leader) == "A", "an elected leader retains its ballot after same-term step-down");

	Fixture delayed_self;
	delayed_self.candidate(false);
	delayed_self.raft.on_message(RaftMessage::HEARTBEAT, append("B", delayed_self.raft.term(), 0, 0, 0));
	delayed_self.raft.on_message(RaftMessage::REQUEST_VOTE, request("C", delayed_self.raft.term()));
	check(voted_for(delayed_self) == "A", "a candidate records its self-vote before multicast loopback");
	delayed_self.raft.on_message(RaftMessage::REQUEST_VOTE, request("C", delayed_self.raft.term() + 1));
	check(voted_for(delayed_self) == "C", "a new term permits a new vote");
}

static void inherited_log() {
	Fixture successor;
	// B replicated x to A and acknowledged its majority, but died before
	// advertising the commit index. A holds x without knowing it committed.
	successor.raft.on_message(RaftMessage::APPEND_ENTRIES,
		append("B", 1, 0, 0, 0) + serialise_length(1) + serialise_length(1) + serialise_string("inherited"));
	check(successor.delegate.applied.empty(), "the successor has not learned the old leader's commit index");
	successor.candidate(); successor.vote("C");
	check(successor.raft.role() == cluster::RaftRole::LEADER && successor.raft.term() == 2,
		"the successor wins the next term with the surviving fixed majority");
	successor.command("new-term");
	successor.raft.on_message(RaftMessage::APPEND_ENTRIES_RESPONSE, serialise_string("C") +
		serialise_length(2) + serialise_length(1) + serialise_length(3) + serialise_length(2));
	check(successor.delegate.applied == std::vector<std::string>({"inherited", "new-term"}),
		"a new leader preserves and commits its inherited prefix before its new command");
}

int main() {
	try { heartbeat_safety(); affirmative_majority(); retained_vote(); inherited_log(); }
	catch (const std::exception& e) { check(false, e.what()); }
	return failures == 0 ? 0 : 1;
}
