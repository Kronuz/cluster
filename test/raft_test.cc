// raft_test -- the standalone proof that the extracted Raft is correct and reusable: N
// in-memory nodes over a fake broadcast bus (no sockets, no Xapiand) run the real
// algorithm and must elect exactly one stable leader, then replicate + apply a command on
// every node. This is what makes the extraction trustworthy before any app wiring.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "length.h"
#include "raft.h"

using namespace std::chrono_literals;

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

// The app's node type: just an id ("" == empty).
struct TestNode { std::string id; };

struct Harness;

// The injected seams, implemented over an in-memory membership. One per node.
struct TestDelegate : cluster::RaftDelegate<TestNode> {
	Harness* h;
	std::string me;
	std::vector<std::string> applied;
	std::string leader;

	TestDelegate(Harness* h_, std::string me_) : h(h_), me(std::move(me_)) {}

	void broadcast(cluster::RaftMessage msg, const std::string& payload) override;
	std::size_t total_nodes() override;
	std::size_t alive_nodes() override;
	bool is_alive(const std::string& id) override;
	std::optional<TestNode> parse_node(const char** p, const char* end) override;

	TestNode local_node() override { return TestNode{me}; }
	std::string serialise(const TestNode& n) override { return cluster::serialise_string(n.id); }
	std::string node_id(const TestNode& n) override { return n.id; }
	bool quorum(std::size_t total, std::size_t count) override { return count * 2 > total; }
	bool prefers(const TestNode& a, const TestNode& b) override { return a.id == b.id; }  // self only -> standard election
	bool active() override { return true; }
	bool ready() override { return true; }
	bool joining() override { return false; }
	void ensure_setup() override {}
	void set_leader(const TestNode& l) override { leader = l.id; }
	void apply(const std::string& command) override { applied.push_back(command); }
};

struct Harness {
	asio::io_context io;
	std::set<std::string> ids;
	std::set<std::string> dead;   // nodes removed (failover): they neither send nor receive
	struct NodeCtx {
		std::string id;
		std::unique_ptr<TestDelegate> del;
		std::unique_ptr<cluster::Raft<TestNode>> raft;
	};
	std::vector<NodeCtx> nodes;

	void deliver(cluster::RaftMessage msg, const std::string& payload) {
		// every live node hears every datagram (multicast + loopback), sender included.
		for (auto& n : nodes) {
			if (dead.count(n.id) != 0) { continue; }
			cluster::Raft<TestNode>* r = n.raft.get();
			asio::post(io, [r, msg, payload] { r->on_message(msg, payload); });
		}
	}
};

void TestDelegate::broadcast(cluster::RaftMessage msg, const std::string& payload) { h->deliver(msg, payload); }
std::size_t TestDelegate::total_nodes() { return h->ids.size(); }
std::size_t TestDelegate::alive_nodes() { return h->ids.size(); }
bool TestDelegate::is_alive(const std::string& id) { return h->ids.count(id) != 0; }
std::optional<TestNode> TestDelegate::parse_node(const char** p, const char* end) {
	std::string_view sv;
	if (!cluster::unserialise_string(p, end, sv)) { return std::nullopt; }
	std::string id(sv);
	if (id.empty() || h->ids.count(id) == 0) { return std::nullopt; }   // empty / nonexistent
	return TestNode{id};
}

// Run f() ON the loop and return its result (race-free snapshot of raft state).
template <typename F>
static auto on_loop(asio::io_context& io, F f) -> decltype(f()) {
	std::promise<decltype(f())> pr;
	auto fut = pr.get_future();
	asio::post(io, [&] { pr.set_value(f()); });
	return fut.get();
}

// Build an N-node cluster, elect a leader, replicate a command; optionally then kill the
// leader and require a new one to be elected among the survivors.
static void run_scenario(const std::vector<std::string>& ids, bool failover) {
	std::printf("== raft: %zu nodes%s ==\n", ids.size(), failover ? " + leader failover" : "");

	Harness h;
	for (const auto& id : ids) { h.ids.insert(id); }

	cluster::RaftConfig cfg;   // fast timings for the test (Xapiand defaults are seconds)
	cfg.heartbeat_timeout = 0.02;
	cfg.election_init = 0.03;
	cfg.election_min = 0.05;
	cfg.election_max = 0.15;

	for (const auto& id : ids) {
		Harness::NodeCtx ctx;
		ctx.id = id;
		ctx.del = std::make_unique<TestDelegate>(&h, id);
		ctx.raft = std::make_unique<cluster::Raft<TestNode>>(h.io.get_executor(), cfg, ctx.del.get());
		h.nodes.push_back(std::move(ctx));
	}

	auto guard = asio::make_work_guard(h.io);
	std::thread loop([&h] { h.io.run(); });
	for (auto& n : h.nodes) { n.raft->start(); }

	std::this_thread::sleep_for(1500ms);   // converge (init + a couple randomized retries)

	auto count_leaders = [&](const std::set<std::string>& skip) {
		return on_loop(h.io, [&] {
			int c = 0; std::string who;
			for (auto& n : h.nodes) {
				if (skip.count(n.id) != 0) { continue; }
				if (n.raft->role() == cluster::RaftRole::LEADER) { ++c; who = n.id; }
			}
			return std::pair<int, std::string>{c, who};
		});
	};

	auto [leaders, leader_id] = count_leaders({});
	check(leaders == 1, "exactly one leader is elected");
	std::string agreed = h.nodes[0].del->leader;
	bool all_agree = !agreed.empty() && h.ids.count(agreed) != 0;
	for (auto& n : h.nodes) { if (n.del->leader != agreed) { all_agree = false; } }
	check(all_agree, "all nodes agree on the same (real) leader");

	// replicate a command from an arbitrary node (a follower forwards to the leader).
	h.nodes[0].raft->add_command("hello-cluster");
	std::this_thread::sleep_for(800ms);
	int applied_everywhere = 0;
	for (auto& n : h.nodes) {
		for (const auto& c : n.del->applied) { if (c == "hello-cluster") { ++applied_everywhere; break; } }
	}
	std::printf("     command applied on %d/%zu nodes\n", applied_everywhere, h.nodes.size());
	check(applied_everywhere == static_cast<int>(h.nodes.size()), "the command is committed + applied on every node");

	if (failover) {
		// kill the leader: it stops sending/receiving and leaves the membership, so quorum
		// is now over the survivors. A new leader must be elected among them.
		std::string dead_leader = leader_id;
		on_loop(h.io, [&] {
			for (auto& n : h.nodes) { if (n.id == dead_leader) { n.raft->stop(); } }
			h.dead.insert(dead_leader);
			h.ids.erase(dead_leader);
			return 0;
		});
		std::this_thread::sleep_for(2000ms);   // survivors detect the loss + re-elect
		auto [l2, who2] = count_leaders(h.dead);
		std::printf("     new leader among survivors: %s\n", who2.empty() ? "<none>" : who2.c_str());
		check(l2 == 1, "a new leader is elected after the old one fails");
		check(!who2.empty() && who2 != dead_leader, "the new leader is a different, surviving node");
	}

	for (auto& n : h.nodes) { asio::post(h.io, [r = n.raft.get()] { r->stop(); }); }
	std::this_thread::sleep_for(50ms);
	guard.reset();
	h.io.stop();
	loop.join();
}

int main() {
	run_scenario({"n1", "n2", "n3"}, /*failover=*/true);
	run_scenario({"n1", "n2", "n3", "n4", "n5"}, /*failover=*/false);
	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
