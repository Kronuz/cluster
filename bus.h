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

// cluster::Bus -- a versioned, cluster-token-scoped, typed multicast message bus over
// reactor::UdpServer. It is the shared substrate every cluster protocol (the Raft module,
// the membership gossip, and the app's own messages) rides: one UDP multicast socket, one
// receive loop, and a frame that carries a protocol version, a message type, and the
// cluster token so a node ignores traffic from other clusters (or newer protocols).
//
// The wire frame is byte-compatible with Xapiand's classic discovery transport
// (udp.cc send_message/get_message):
//
//     [major:1][minor:1][type:1][serialise_string(token)][content...]
//
// A received datagram is dropped unless it is >= 4 bytes, its (major,minor) is <= ours,
// its type is in [0, max_type), and its token equals ours; otherwise it is dispatched to
// the on_message handler ON the bus's reactor thread -- the same loop the caller runs its
// timers on (bus.io()), so a single-threaded protocol never races the receive.
//
// Standalone Asio only (ASIO_STANDALONE), header-only, C++20.

#pragma once

#include "length.h"
#include "reactor_udp.h"

#include <asio.hpp>

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

namespace cluster {

class Bus {
public:
	// type, content, sender. Runs on the bus reactor thread.
	using OnMessage = std::function<void(int type, std::string_view content, const asio::ip::udp::endpoint& from)>;

	// `token` scopes the bus (a node ignores datagrams whose token != this one). `max_type`
	// is one past the highest valid message type (a datagram with type >= max_type is
	// dropped, matching the classic get_message guard).
	Bus(std::uint8_t major, std::uint8_t minor, std::string token, int max_type, OnMessage on_message)
		: major_(major), minor_(minor), token_(std::move(token)), max_type_(max_type),
		  on_message_(std::move(on_message)),
		  udp_([this](std::string_view data, const asio::ip::udp::endpoint& from) { on_datagram(data, from); }) {}

	void set_options(const reactor::UdpOptions& options) { udp_.set_options(options); }

	// Bind + join the group + run the receive loop (throws on bind failure, like UdpServer).
	void start(unsigned short port) { udp_.start(port); }
	void stop() { udp_.stop(); }

	// Frame a message and broadcast it to the group. Empty content is a no-op (matching the
	// classic transport, which never sent a bodyless typed message).
	std::size_t send(int type, std::string_view content) {
		if (content.empty()) { return 0; }
		std::string message;
		message.reserve(3 + token_.size() + 8 + content.size());
		message.push_back(static_cast<char>(major_));
		message.push_back(static_cast<char>(minor_));
		message.push_back(static_cast<char>(static_cast<unsigned char>(type)));
		message.append(serialise_string(token_));
		message.append(content.data(), content.size());
		return udp_.send(message);
	}

	// The reactor loop the receive runs on -- run the protocol's timers/posts here.
	asio::io_context& io() { return udp_.io(); }
	reactor::Reactor& reactor() { return udp_.reactor(); }
	reactor::UdpServer& transport() { return udp_; }

private:
	// Parse + validate a datagram (the classic get_message guards), then dispatch. A bad
	// frame is silently dropped -- gossip is best-effort.
	void on_datagram(std::string_view data, const asio::ip::udp::endpoint& from) {
		if (data.size() < 4) { return; }
		const char* p = data.data();
		const char* p_end = p + data.size();

		auto rx_major = static_cast<std::uint8_t>(*p++);
		auto rx_minor = static_cast<std::uint8_t>(*p++);
		if (rx_major > major_ || (rx_major == major_ && rx_minor > minor_)) { return; }

		int type = static_cast<unsigned char>(*p++);
		if (type < 0 || type >= max_type_) { return; }

		std::string_view remote_token;
		if (!unserialise_string(&p, p_end, remote_token)) { return; }
		if (remote_token.empty() || remote_token != token_) { return; }

		if (on_message_) {
			on_message_(type, std::string_view(p, static_cast<std::size_t>(p_end - p)), from);
		}
	}

	std::uint8_t major_;
	std::uint8_t minor_;
	std::string token_;
	int max_type_;
	OnMessage on_message_;
	reactor::UdpServer udp_;
};

}  // namespace cluster
