# cluster

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/cluster/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/cluster/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![Header-only](https://img.shields.io/badge/header--only-yes-green.svg)

Header-only UDP-multicast cluster layer for C++20: a typed message bus and Raft leader
election over Asio. Nodes on one network hear each other through the bus, which runs on
[reactor](https://github.com/Kronuz/reactor)'s event loop; Raft's messages ride the same bus to
elect a leader, and your application decides what a committed command does. Reach for it for
discovery and leader election on one LAN, for state you can rebuild.

## ✨ Highlights

- **`cluster::Bus`.** One multicast socket carrying versioned, typed frames scoped by a cluster
  token; traffic from other clusters or newer protocol versions is dropped.
- **`cluster::Raft`.** Terms, votes, log replication and commit, with the node type and
  `apply(command)` injected through a delegate.
- **Tunable timing.** `RaftConfig` sets the heartbeat and the election window; the defaults
  reproduce Xapiand's (a 1 s heartbeat and a 10 to 30 s election window).
- **`cluster::LivenessTracker`.** Tracks when each peer was last heard, for the delegate's
  `is_alive()`.
- **One loop.** Receives and timers run on the bus reactor, so single-threaded protocol code
  never races.
- **Wire-compatible** with Xapiand's discovery frames.

## 🚀 Quick start

```cpp
#include "bus.h"

#include <chrono>
#include <cstdio>
#include <thread>

int main() {
	cluster::Bus bus(/*major*/ 1, /*minor*/ 0, /*token*/ "my-cluster", /*max_type*/ 32,
		[](int type, std::string_view content, const asio::ip::udp::endpoint&) {
			// runs on the bus reactor thread
			std::printf("type %d: %.*s\n", type, int(content.size()), content.data());
		});
	reactor::UdpOptions o;
	o.multicast_group = "239.192.168.1";
	o.multicast_interface = "127.0.0.1";   // this host only; drop it to use the default route
	o.multicast_loop = true;               // one-host clusters see their own sends
	o.reuse_port = true;                   // several nodes share the port
	bus.set_options(o);
	bus.start(58880);

	asio::post(bus.io(), [&] { bus.send(5, "hello"); });   // framed and sent to the group
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	bus.stop();
	// type 5: hello
}
```

`cluster_gossip` runs three nodes that announce themselves, and `cluster_raft_election` five
in-memory nodes that elect a leader and replicate a command.

## 📦 Installation

Requires a C++20 compiler and CMake 3.16 or newer. CMake fetches
[reactor](https://github.com/Kronuz/reactor), which brings standalone Asio, and
[varint](https://github.com/Kronuz/varint) at pinned commits. CI builds it on Linux (x86_64,
arm64), macOS (arm64, x86_64), and FreeBSD, plus ASan, UBSan, and TSan builds.

```cmake
include(FetchContent)
FetchContent_Declare(cluster
  GIT_REPOSITORY https://github.com/Kronuz/cluster.git
  GIT_TAG        4d894db08879f474199d421738a0184dec939439)  # main tracks the tip
FetchContent_MakeAvailable(cluster)

target_link_libraries(my_app PRIVATE cluster::cluster)
```

Then `#include "bus.h"`, `"raft.h"` or `"liveness.h"`.

## 🎯 When to use it

Use it for discovery and leader election among nodes on one LAN, where the replicated state
can be rebuilt after a restart.

Not for:

- Durable consensus: terms, votes and logs live in memory, and a restarted voter loses them.
  Use [consensus](https://github.com/Kronuz/consensus) with
  [journal](https://github.com/Kronuz/journal).
- Membership gossip out of the box: the HELLO/WAVE naming protocol is the application's (see
  `examples/gossip.cc`).
- An `apply()` that can throw and is not idempotent: a throwing `apply()` keeps the entry for an
  ordered retry, so the delegate must leave state unchanged or replay it consistently
  ([SAFETY.md](SAFETY.md#application-failure-and-ordered-retry)).

## 📚 Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md): the layers, the frame and the threading model.
- [SAFETY.md](SAFETY.md): the Raft safety corrections and what the tests cover.
- [AGENTS.md](AGENTS.md): file map and invariants.
- [Catalog entry](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#cluster): include
  lines, target, dependencies and key API.

## 🛠️ Building and testing

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```

The build includes the demos and two benchmarks: `cluster_bus_bench` times the framing, and
`cluster_raft_bench` how long a 3-node cluster takes to elect a leader for a given election
window.

## 🤝 Contributing

Read [AGENTS.md](AGENTS.md) and the catalog's
[rules of engagement](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#rules-of-engagement)
before opening a pull request; CI must pass on every platform.

## 📄 License

MIT, Copyright (c) 2026 Germán Méndez Bravo. See [LICENSE](LICENSE).
