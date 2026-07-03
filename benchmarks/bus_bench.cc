// bus_bench -- measures the CPU cost the Bus itself adds: framing a message on send and
// parsing+validating a datagram on receive. UDP throughput is OS-bound and not the point;
// this is the per-message overhead the library contributes (the varint token codec + the
// version/type/token checks), so a hot gossip path can be reasoned about.
//
//   cmake --build build && ./build/cluster_bus_bench

#include <chrono>
#include <cstdio>
#include <string>

#include "bus.h"
#include "length.h"

using Clock = std::chrono::steady_clock;

template <typename F>
static double ns_per_op(std::size_t iters, F&& f) {
	auto t0 = Clock::now();
	for (std::size_t i = 0; i < iters; ++i) { f(i); }
	auto t1 = Clock::now();
	return std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(iters);
}

int main() {
	std::printf("== cluster bus overhead ==\n");

	const std::string token = "benchmark-cluster";
	const std::string content(200, 'x');   // a typical small gossip payload

	// The framing a send() does, in isolation (no socket): version + type + token + content.
	volatile std::size_t sink = 0;
	double frame_ns = ns_per_op(2'000'000, [&](std::size_t i) {
		std::string message;
		message.reserve(3 + token.size() + 8 + content.size());
		message.push_back('\x01');
		message.push_back('\x00');
		message.push_back(static_cast<char>(static_cast<unsigned char>(i & 0x1f)));
		message.append(cluster::serialise_string(token));
		message.append(content);
		sink += message.size();
	});

	// A pre-framed datagram, parsed + validated the way Bus::on_datagram does.
	std::string frame;
	frame.push_back('\x01'); frame.push_back('\x00'); frame.push_back('\x05');
	frame.append(cluster::serialise_string(token));
	frame.append(content);
	std::size_t dispatched = 0;
	double parse_ns = ns_per_op(2'000'000, [&](std::size_t) {
		const char* p = frame.data();
		const char* end = p + frame.size();
		if (frame.size() < 4) { return; }
		auto major = static_cast<unsigned char>(*p++);
		auto minor = static_cast<unsigned char>(*p++);
		if (major > 1 || (major == 1 && minor > 0)) { return; }
		int type = static_cast<unsigned char>(*p++);
		if (type < 0 || type >= 32) { return; }
		std::string_view tok;
		if (!cluster::unserialise_string(&p, end, tok)) { return; }
		if (tok.empty() || tok != token) { return; }
		++dispatched;
	});

	std::printf("  frame a %zu-byte message : %6.1f ns/op\n", content.size(), frame_ns);
	std::printf("  parse + validate a frame : %6.1f ns/op  (%zu dispatched)\n", parse_ns, dispatched);
	std::printf("  (sink=%zu -- keeps the framing from being optimized away)\n", static_cast<std::size_t>(sink));
	return 0;
}
