// gossip -- a tiny demo of cluster::Bus: three nodes on one multicast group each announce
// themselves and print what they hear from the others. Shows the whole API: a typed,
// token-scoped message bus where every node hears every datagram.
//
//   cmake --build build && ./build/cluster_gossip
//
// (Pinned to the loopback interface so it is self-contained and works under a VPN.)

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "bus.h"

using namespace std::chrono_literals;

// The demo's one message type.
enum : int { MSG_ANNOUNCE = 1 };

static reactor::UdpOptions loopback_group() {
	reactor::UdpOptions o;
	o.reuse_port = true;
	o.multicast_group = "239.192.168.55";
	o.multicast_interface = "127.0.0.1";   // self-contained; drop this to use the default route
	o.multicast_loop = true;
	o.multicast_ttl = 1;
	return o;
}

int main() {
	const char* names[] = {"alice", "bob", "carol"};
	const unsigned short port = 48555;

	std::vector<std::unique_ptr<cluster::Bus>> nodes;
	for (const char* name : names) {
		std::string me = name;
		auto bus = std::make_unique<cluster::Bus>(
			/*major*/1, /*minor*/0, /*token*/"demo-cluster", /*max_type*/8,
			[me](int type, std::string_view content, const asio::ip::udp::endpoint&) {
				if (type == MSG_ANNOUNCE && content != me) {
					std::printf("  [%s] heard: %.*s\n", me.c_str(),
						static_cast<int>(content.size()), content.data());
				}
			});
		bus->set_options(loopback_group());
		bus->start(port);
		nodes.push_back(std::move(bus));
	}

	std::this_thread::sleep_for(150ms);   // let the group joins settle

	std::printf("three nodes announce themselves on the bus:\n");
	for (std::size_t i = 0; i < nodes.size(); ++i) {
		auto* bus = nodes[i].get();
		std::string name = names[i];
		asio::post(bus->io(), [bus, name] { bus->send(MSG_ANNOUNCE, name); });
	}

	std::this_thread::sleep_for(300ms);   // let the announcements circulate
	std::printf("done.\n");

	for (auto& n : nodes) { n->stop(); }
	return 0;
}
