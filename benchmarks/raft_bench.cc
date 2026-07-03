// raft_bench -- how fast consensus forms: over many trials, build a fresh 3-node cluster
// and measure the time from start to a leader being elected. Election latency is bounded
// by the randomized election timeout (a Raft tuning knob, not CPU), so it is reported
// alongside the config that produced it -- this is the number you tune, and the harness
// proves the algorithm reaches consensus every trial.
//
//   cmake --build build && ./build/cluster_raft_bench [trials]

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <thread>
#include <vector>

#include "mem_cluster.h"

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

int main(int argc, char** argv) {
	int trials = argc > 1 ? std::atoi(argv[1]) : 20;

	cluster::RaftConfig cfg;
	cfg.heartbeat_timeout = 0.01;
	cfg.election_init = 0.01;
	cfg.election_min = 0.02;
	cfg.election_max = 0.06;

	std::printf("== raft election latency (3 nodes, %d trials) ==\n", trials);
	std::printf("  config: heartbeat %.0fms, election %.0f-%.0fms\n",
		cfg.heartbeat_timeout * 1000, cfg.election_min * 1000, cfg.election_max * 1000);

	std::vector<double> ms;
	int converged = 0;
	for (int t = 0; t < trials; ++t) {
		mem::Cluster c;
		c.build({"n1", "n2", "n3"}, cfg);
		auto guard = asio::make_work_guard(c.io);
		std::thread loop([&c] { c.io.run(); });

		auto t0 = Clock::now();
		for (auto& n : c.nodes) { n.raft->start(); }

		double elapsed = 0;
		for (int i = 0; i < 2000; ++i) {   // poll up to ~2s
			auto [n, who] = c.leaders();
			if (n == 1) { elapsed = std::chrono::duration<double, std::milli>(Clock::now() - t0).count(); ++converged; break; }
			std::this_thread::sleep_for(1ms);
		}
		if (elapsed > 0) { ms.push_back(elapsed); }

		for (auto& node : c.nodes) { asio::post(c.io, [r = node.raft.get()] { r->stop(); }); }
		std::this_thread::sleep_for(5ms);
		guard.reset();
		c.io.stop();
		loop.join();
	}

	std::sort(ms.begin(), ms.end());
	double avg = ms.empty() ? 0 : std::accumulate(ms.begin(), ms.end(), 0.0) / ms.size();
	std::printf("  converged: %d/%d trials\n", converged, trials);
	if (!ms.empty()) {
		std::printf("  election latency: min %.1f ms, median %.1f ms, avg %.1f ms, max %.1f ms\n",
			ms.front(), ms[ms.size() / 2], avg, ms.back());
	}
	return converged == trials ? 0 : 1;
}
