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
