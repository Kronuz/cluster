// A minimal in-memory N-node cluster for demonstrating and benchmarking cluster::Raft
// without sockets: all nodes share one io_context, a broadcast is delivered to every live
// node (multicast + loopback), and membership is a plain set. This is support code for the
// example and the benchmark -- NOT part of the library.

#pragma once

#include <asio.hpp>

#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "length.h"
#include "raft.h"

namespace mem {

struct Node { std::string id; };   // "" == empty

struct Cluster;

struct Delegate : cluster::RaftDelegate<Node> {
	Cluster* c;
	std::string me;
	std::vector<std::string> applied;
	std::string leader;
	Delegate(Cluster* c_, std::string me_) : c(c_), me(std::move(me_)) {}

	void broadcast(cluster::RaftMessage msg, const std::string& payload) override;
	std::size_t total_nodes() override;
	std::size_t alive_nodes() override;
	bool is_alive(const std::string& id) override;
	std::optional<Node> parse_node(const char** p, const char* end) override;

	Node local_node() override { return Node{me}; }
	std::string serialise(const Node& n) override { return cluster::serialise_string(n.id); }
	std::string node_id(const Node& n) override { return n.id; }
	bool quorum(std::size_t total, std::size_t count) override { return count * 2 > total; }
	bool prefers(const Node& a, const Node& b) override { return a.id == b.id; }
	bool active() override { return true; }
	bool ready() override { return true; }
	bool joining() override { return false; }
	void ensure_setup() override {}
	void set_leader(const Node& l) override { leader = l.id; }
	void apply(const std::string& command) override { applied.push_back(command); }
};

struct Cluster {
	asio::io_context io;
	std::set<std::string> ids;
	std::set<std::string> dead;
	struct NodeCtx {
		std::string id;
		std::unique_ptr<Delegate> del;
		std::unique_ptr<cluster::Raft<Node>> raft;
	};
	std::vector<NodeCtx> nodes;

	void build(const std::vector<std::string>& node_ids, const cluster::RaftConfig& cfg) {
		for (const auto& id : node_ids) { ids.insert(id); }
		for (const auto& id : node_ids) {
			NodeCtx ctx;
			ctx.id = id;
			ctx.del = std::make_unique<Delegate>(this, id);
			ctx.raft = std::make_unique<cluster::Raft<Node>>(io.get_executor(), cfg, ctx.del.get());
			nodes.push_back(std::move(ctx));
		}
	}

	void deliver(cluster::RaftMessage msg, const std::string& payload) {
		for (auto& n : nodes) {
			if (dead.count(n.id) != 0) { continue; }
			cluster::Raft<Node>* r = n.raft.get();
			asio::post(io, [r, msg, payload] { r->on_message(msg, payload); });
		}
	}

	// Run f() on the loop and return its result (race-free snapshot).
	template <typename F>
	auto on_loop(F f) -> decltype(f()) {
		std::promise<decltype(f())> pr;
		auto fut = pr.get_future();
		asio::post(io, [&] { pr.set_value(f()); });
		return fut.get();
	}

	std::pair<int, std::string> leaders() {
		return on_loop([&] {
			int c = 0; std::string who;
			for (auto& n : nodes) {
				if (dead.count(n.id) != 0) { continue; }
				if (n.raft->role() == cluster::RaftRole::LEADER) { ++c; who = n.id; }
			}
			return std::pair<int, std::string>{c, who};
		});
	}
};

inline void Delegate::broadcast(cluster::RaftMessage msg, const std::string& payload) { c->deliver(msg, payload); }
inline std::size_t Delegate::total_nodes() { return c->ids.size(); }
inline std::size_t Delegate::alive_nodes() { return c->ids.size(); }
inline bool Delegate::is_alive(const std::string& id) { return c->ids.count(id) != 0; }
inline std::optional<Node> Delegate::parse_node(const char** p, const char* end) {
	std::string_view sv;
	if (!cluster::unserialise_string(p, end, sv)) { return std::nullopt; }
	std::string id(sv);
	if (id.empty() || c->ids.count(id) == 0) { return std::nullopt; }
	return Node{id};
}

}  // namespace mem
