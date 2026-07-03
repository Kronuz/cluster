// bus_test -- validates the length codec (round-trip, wire-compat edge cases) and the
// cluster::Bus: two buses on the same group+token exchange a typed message; a mismatched
// token is filtered; a too-new protocol version is dropped. The multicast section is
// loopback-pinned + env-probed (skips if the host can't multicast, e.g. under a VPN).

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>

#include "bus.h"
#include "length.h"

using namespace std::chrono_literals;

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

static unsigned short free_udp_port() {
	int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}

static bool raw_multicast_loopback_works(unsigned short port) {
	int s = ::socket(AF_INET, SOCK_DGRAM, 0);
	int on = 1;
	::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
	::setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
	int loop = 1; ::setsockopt(s, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
	in_addr lo{}; lo.s_addr = ::inet_addr("127.0.0.1");
	::setsockopt(s, IPPROTO_IP, IP_MULTICAST_IF, &lo, sizeof(lo));
	ip_mreq mreq{};
	mreq.imr_multiaddr.s_addr = ::inet_addr("239.255.42.97");
	mreq.imr_interface.s_addr = ::inet_addr("127.0.0.1");
	::setsockopt(s, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port); a.sin_addr.s_addr = htonl(INADDR_ANY);
	::bind(s, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	sockaddr_in grp{}; grp.sin_family = AF_INET; grp.sin_port = htons(port); grp.sin_addr.s_addr = ::inet_addr("239.255.42.97");
	::sendto(s, "probe", 5, 0, reinterpret_cast<sockaddr*>(&grp), sizeof(grp));
	std::this_thread::sleep_for(150ms);
	::fcntl(s, F_SETFL, O_NONBLOCK);
	char buf[64];
	ssize_t got = ::recv(s, buf, sizeof(buf), 0);
	::close(s);
	return got > 0;
}

static reactor::UdpOptions loopback_group() {
	reactor::UdpOptions o;
	o.reuse_port = true;
	o.multicast_group = "239.255.42.96";
	o.multicast_interface = "127.0.0.1";
	o.multicast_loop = true;
	o.multicast_ttl = 1;
	return o;
}

int main() {
	std::printf("== cluster bus test ==\n");

	// [A] length codec round-trips across the boundaries (verbatim, 0xff continuation).
	{
		bool ok = true;
		for (unsigned long long v : {0ull, 1ull, 254ull, 255ull, 256ull, 300ull, 70000ull, 1ull << 40}) {
			std::string s = cluster::serialise_length(v);
			const char* p = s.data();
			unsigned long long out = 0;
			ok = ok && cluster::unserialise_length(&p, p + s.size(), out) && out == v && p == s.data() + s.size();
		}
		check(ok, "serialise_length/unserialise_length round-trip across 255 boundary");

		std::string s = cluster::serialise_string("hello-token");
		const char* p = s.data();
		std::string_view sv;
		bool sok = cluster::unserialise_string(&p, p + s.size(), sv);
		check(sok && sv == "hello-token", "serialise_string/unserialise_string round-trip");

		// serialise_bool: one byte '1'/'0', round-trips both ways.
		bool bv = false;
		std::string bt = cluster::serialise_bool(true), bf = cluster::serialise_bool(false);
		const char* bp = bt.data();
		bool bok = cluster::unserialise_bool(&bp, bp + bt.size(), bv) && bv;
		const char* bp2 = bf.data();
		bok = bok && cluster::unserialise_bool(&bp2, bp2 + bf.size(), bv) && !bv;
		check(bok, "serialise_bool/unserialise_bool round-trip (both values)");

		// truncated inputs are rejected, not crashed.
		const char* q = s.data();
		std::string_view sv2;
		check(!cluster::unserialise_string(&q, q + 2, sv2), "a truncated length-prefixed string is rejected");
	}

	// [B] two buses, same token: a typed message is framed, delivered, and dispatched with
	// the right type + content. A different-token bus does NOT receive it (scoping).
	{
		unsigned short probe = free_udp_port();
		if (!raw_multicast_loopback_works(probe)) {
			std::printf("  skip: no multicast loopback in this environment (VPN?) -- bus exchange not exercised\n");
		} else {
			unsigned short port = free_udp_port();
			std::atomic<int> a_type{-1}, b_type{-1}, other_hits{0};
			std::mutex m; std::string a_content, b_content;

			cluster::Bus a(1, 0, "clusterX", 32, [&](int t, std::string_view c, const asio::ip::udp::endpoint&) {
				a_type.store(t); { std::lock_guard<std::mutex> lk(m); a_content.assign(c); }
			});
			cluster::Bus b(1, 0, "clusterX", 32, [&](int t, std::string_view c, const asio::ip::udp::endpoint&) {
				b_type.store(t); { std::lock_guard<std::mutex> lk(m); b_content.assign(c); }
			});
			cluster::Bus other(1, 0, "OTHER-cluster", 32, [&](int, std::string_view, const asio::ip::udp::endpoint&) {
				other_hits.fetch_add(1);
			});
			a.set_options(loopback_group()); b.set_options(loopback_group()); other.set_options(loopback_group());
			a.start(port); b.start(port); other.start(port);
			std::this_thread::sleep_for(120ms);

			asio::post(a.io(), [&] { a.send(5, "payload-5"); });
			for (int i = 0; i < 400 && (a_type.load() < 0 || b_type.load() < 0); ++i) { std::this_thread::sleep_for(5ms); }

			check(a_type.load() == 5, "sender's own bus dispatches the message (type 5)");
			check(b_type.load() == 5, "the peer bus dispatches the message (type 5)");
			{ std::lock_guard<std::mutex> lk(m); check(a_content == "payload-5" && b_content == "payload-5", "content intact on both"); }
			check(other_hits.load() == 0, "a different-token bus filters the message out (cluster scoping)");
		}
	}

	// [C] a datagram with a newer protocol version is dropped (a raw frame we craft).
	{
		unsigned short probe = free_udp_port();
		if (!raw_multicast_loopback_works(probe)) {
			std::printf("  skip: no multicast loopback (version-drop not exercised)\n");
		} else {
			unsigned short port = free_udp_port();
			std::atomic<int> hits{0};
			cluster::Bus rx(1, 0, "verclus", 32, [&](int, std::string_view, const asio::ip::udp::endpoint&) { hits.fetch_add(1); });
			rx.set_options(loopback_group());
			rx.start(port);
			// A sender advertising major=2 (> our 1): rx must drop it.
			cluster::Bus newer(2, 0, "verclus", 32, [](int, std::string_view, const asio::ip::udp::endpoint&) {});
			newer.set_options(loopback_group());
			newer.start(port);
			std::this_thread::sleep_for(120ms);
			asio::post(newer.io(), [&] { newer.send(3, "from-the-future"); });
			std::this_thread::sleep_for(200ms);
			check(hits.load() == 0, "a too-new protocol version is dropped");
		}
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
