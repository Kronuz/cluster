# AGENTS

Orientation for anyone (human or agent) working on this library.

## What this is

A UDP-multicast **cluster substrate** on the Kronuz/reactor Asio runtime: a message bus,
Raft consensus, and membership gossip — the reusable half of a gossip/discovery service.
The app is injected (its node type, cluster-lifecycle hooks, and apply callback); the
substrate never learns the app's domain. Header-only. Read `README.md` for the layering;
this is the working notes.

## Origin + the fidelity rule

This is being **extracted faithfully** from Xapiand's `src/server/discovery.cc` (a working
libev UDP + Raft + gossip monolith). The wire format, the Raft algorithm, and the gossip
handshake must stay **byte- and behavior-identical** so a migrated Xapiand still forms a
cluster (validated by Xapiand's `harness/cluster_check.sh`). When porting a handler, keep
its logic verbatim and route only the couplings (transport, node/membership, state hooks,
apply) through the injected interfaces. Do not "improve" the algorithm during extraction.

## File map

```
length.h    A varint length + length-prefixed string codec, BYTE-COMPATIBLE with Xapiand's
            serialise_length/serialise_string. Header-only, no exceptions (returns false on
            bad input). Vendored so the wire format interops without a dependency.
bus.h       cluster::Bus -- the versioned, token-scoped, typed multicast message bus over
            reactor::UdpServer. send(type, content) frames [major][minor][type][token][content]
            + broadcasts; a received datagram is validated (version <= ours, token match,
            type in range) then dispatched to on_message on the bus reactor thread. io()
            exposes the loop for the Raft/gossip/app timers.
raft.h      cluster::Raft<Node> -- a line-for-line port of Xapiand's Raft (election/vote/
            append/commit), generic via a RaftDelegate<Node> (~16 injected seams: transport,
            node serialise/parse, membership/quorum, prefers/eligible policy, active/ready/
            joining + ensure_setup lifecycle gates, apply). Timers on the bus loop. Timing is
            RaftConfig (Xapiand defaults). Only trace logging dropped.
examples/gossip.cc     A runnable demo: three nodes announce themselves over the bus and
                       print what they hear (loopback-pinned, self-contained).
examples/raft_election.cc  A Raft demo: five in-memory nodes elect a leader + replicate a
                       command (uses examples/mem_cluster.h, the in-memory harness).
examples/mem_cluster.h  Support: a minimal in-memory N-node cluster (fake broadcast bus +
                       delegate) for the raft example/benchmark. NOT part of the library.
benchmarks/bus_bench.cc  The per-message overhead the bus adds -- framing on send + parse/
                       validate on receive (~30 ns / ~1.5 ns per message; UDP throughput is
                       OS-bound and not the point).
benchmarks/raft_bench.cc  Raft election latency over many trials (consensus formation time;
                       bounded by the election-timeout config, reported alongside it).
test/bus_test.cc   ctest "cluster_bus": length-codec round-trips + a two-node bus exchange
                   over loopback multicast (token scoping, version rejection). Multicast is
                   loopback-pinned + env-probed (skips if the host can't multicast).
test/raft_test.cc  ctest "cluster_raft": N in-memory nodes over a fake bus -- one stable
                   leader elected, a command committed+applied on every node, and re-election
                   after the leader is killed (3 and 5 nodes). No sockets, no Xapiand.
CMakeLists.txt     Header-only INTERFACE target cluster::cluster; FetchContents reactor
                   (-DFETCHCONTENT_SOURCE_DIR_REACTOR for local dev); example/bench/tests
                   only top-level.
```

## Invariants — do not regress these

- **The wire frame is fixed** (`[major][minor][type][serialise_string(token)][content]`) and
  byte-compatible with Xapiand's classic transport. `length.h` must stay identical to
  `serialise_length`. Changing either breaks interop with an un-migrated node.
- **Everything runs on the one bus reactor thread.** The Raft/gossip/app protocols mutate
  shared state (terms, votes, the node table) without locks, so they MUST schedule their
  timers/posts onto `bus.io()` (`reactor::PeriodicTimer` / `reactor::Signal` / `asio::post`),
  never touch that state from another thread. Sends are the exception (UDP send/recv are
  independent), but an ordering-sensitive protocol posts its sends too.
- **A bad datagram is dropped, never fatal.** Gossip is best-effort; `unserialise_*` returns
  false rather than throwing, and `on_datagram` silently drops a malformed/foreign frame.
- **The substrate stays app-agnostic.** No `Node`-the-Xapiand-class, no database, no shard
  knowledge leaks in. The node type + membership + state hooks + apply are injected.
- **Standalone Asio only.** `ASIO_STANDALONE`, C++20; everything rides reactor.

## Build & test

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DFETCHCONTENT_SOURCE_DIR_REACTOR=../reactor \
  && cmake --build build -j && ctest --test-dir build --output-on-failure
```

The bus test's multicast section is loopback-pinned + env-probed, so it stays green under a
VPN (which breaks default-route multicast).

## Status / next

- `cluster::Bus` + `length.h` — done.
- `raft.h` (`cluster::Raft<Node>`) — DONE. A faithful, generic port of Xapiand's Raft
  (election/vote/append/commit) with an injected `RaftDelegate`. Validated standalone by
  `test/raft_test.cc` (election + replication + re-election, 3 & 5 nodes); demoed
  (`examples/raft_election.cc`) + benchmarked (`benchmarks/raft_bench.cc`).
- membership gossip (HELLO/WAVE/SNEER/ENTER/BYE + node table) — next.
- Then Xapiand's Discovery becomes a thin adapter over the substrate; retire libev.
