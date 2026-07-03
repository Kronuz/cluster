# cluster

A UDP-multicast **cluster substrate** — a message bus, Raft consensus, and membership
gossip — on the [Kronuz/reactor](https://github.com/Kronuz/reactor) Asio runtime. Header-only.

This is the reusable half of a gossip/discovery service: the parts that are the same for
any distributed system (framed multicast messaging, leader election, node membership),
with the app-specific parts (the node type, the cluster-lifecycle state machine, what a
committed command *does*) left to the caller. A search engine, another distributed store,
or a coordination service ride the same substrate instead of each re-implementing a
multicast Raft.

## The layers

```
  Kronuz/reactor   UdpServer / PeriodicTimer / Signal   -- the Asio transport + loop
       |
  cluster::Bus     versioned, token-scoped, typed multicast messaging   [done]
       |
  cluster::Raft    term / log / role / votes + election + append + commit   [done]
  cluster::gossip  HELLO/WAVE/SNEER/ENTER/BYE naming + the node table       [next]
       |
  the app          its node type, state hooks, apply(command), its own message types
```

Raft and the membership gossip share one multicast socket (the `Bus`), one node table, and
one cluster state, so they live together in this one lib (Raft is a separable module, not a
separate repo). The app is injected — it provides the node type, the apply callback, and
the cluster-lifecycle hooks; the substrate never learns what a "database" or a "shard" is.

## `cluster::Bus` — the shared message bus

Every protocol rides the `Bus`: one UDP multicast socket, one receive loop, and a frame
that carries a protocol version, a message type, and the cluster token (so a node ignores
traffic from other clusters or newer protocols). The frame is byte-compatible with the
classic Xapiand discovery transport:

```
[major:1][minor:1][type:1][serialise_string(token)][content...]
```

```cpp
#include "bus.h"

cluster::Bus bus(/*major*/1, /*minor*/0, /*token*/"my-cluster", /*max_type*/32,
    [](int type, std::string_view content, const asio::ip::udp::endpoint& from) {
        dispatch(type, content, from);          // on the bus reactor thread
    });
reactor::UdpOptions o;
o.multicast_group = "239.192.168.1";
o.multicast_loop  = true;                       // one-host clusters see their own sends
o.reuse_port      = true;                       // N nodes share the port
bus.set_options(o);
bus.start(58880);
bus.send(5, payload);                           // frame + broadcast to the group
```

A received datagram is dropped unless it is well-formed, its version is `<=` ours, its type
is in range, and its token matches; otherwise it is dispatched on the bus reactor thread —
the same loop the caller runs its timers on (`bus.io()`), so a single-threaded protocol
never races the receive.

## Build

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
ctest --test-dir build --output-on-failure
```

Header-only: add the directory to your include path (or `FetchContent` this repo) and link
`cluster::cluster` (which pulls in `reactor::reactor` + standalone Asio). Point
`-DFETCHCONTENT_SOURCE_DIR_REACTOR=<path>` at a local reactor checkout for dev.

`examples/gossip.cc` (`cluster_gossip`) is a runnable demo — three nodes announce themselves
over the bus and print what they hear. `benchmarks/bus_bench.cc` (`cluster_bus_bench`) measures
the per-message overhead the bus adds (framing + parse/validate — ~30 ns / ~1.5 ns). `test/bus_test.cc`
covers the length codec (wire-compat round-trips) and the bus (a two-node exchange over loopback
multicast, cluster-token scoping, and version rejection).

## Status

`cluster::Bus` (Phase 1) and `cluster::Raft` (Phase 2) are done. Raft is a faithful, generic
port of a proven multicast Raft, validated standalone — `test/raft_test.cc` runs N in-memory
nodes over a fake bus and elects a stable leader, replicates + applies a command on every
node, and re-elects after the leader is killed (3 and 5 nodes); `examples/raft_election.cc`
demos it and `benchmarks/raft_bench.cc` measures election latency (~40 ms with a 20–60 ms
election window). The membership gossip is next; then a search engine (Xapiand) rides the
substrate, retiring its libev Discovery.
