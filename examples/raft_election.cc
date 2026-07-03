// raft_election -- a demo of cluster::Raft: five in-memory nodes run the real consensus
// algorithm, elect a leader, and replicate a command to every node. Prints what happens.
// (In-memory so it needs no network; a real deployment gives each node a cluster::Bus.)
//
//   cmake --build build && ./build/cluster_raft_election

#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

#include "mem_cluster.h"

using namespace std::chrono_literals;

int main() {
	std::vector<std::string> ids = {"n1", "n2", "n3", "n4", "n5"};

	cluster::RaftConfig cfg;   // brisk timings for a quick demo
	cfg.heartbeat_timeout = 0.05;
	cfg.election_init = 0.05;
	cfg.election_min = 0.1;
	cfg.election_max = 0.3;

	mem::Cluster c;
	c.build(ids, cfg);

	auto guard = asio::make_work_guard(c.io);
	std::thread loop([&c] { c.io.run(); });
	for (auto& n : c.nodes) { n.raft->start(); }

	std::printf("five nodes start with no leader and run for a term...\n");
	std::this_thread::sleep_for(1500ms);

	auto [n, who] = c.leaders();
	std::printf("  elected leader: %s (%d leader%s, term %llu)\n",
		who.c_str(), n, n == 1 ? "" : "s",
		static_cast<unsigned long long>(c.on_loop([&] { return c.nodes[0].raft->term(); })));

	std::printf("replicate a command from a follower...\n");
	c.nodes[0].raft->add_command("set x = 42");
	std::this_thread::sleep_for(600ms);

	for (auto& node : c.nodes) {
		std::printf("  [%s] applied: ", node.id.c_str());
		for (const auto& cmd : node.del->applied) { std::printf("\"%s\" ", cmd.c_str()); }
		std::printf("\n");
	}

	for (auto& node : c.nodes) { asio::post(c.io, [r = node.raft.get()] { r->stop(); }); }
	std::this_thread::sleep_for(50ms);
	guard.reset();
	c.io.stop();
	loop.join();
	return 0;
}
