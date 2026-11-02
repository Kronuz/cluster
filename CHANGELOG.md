# Changelog

Notable changes to `cluster`, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no releases, so
entries are dated.

Upgrading a pin from before 2026-10-30: [the consumer upgrade note](https://github.com/Kronuz/cpp-libraries/blob/main/docs/UPGRADING-2026-10.md#cluster) lists what changes for callers, how to find affected inputs, the commits not to pin, and the speedup.

## 2026-11-01

cluster's own code is unchanged; its reactor and varint pins move, and its README changes.

### Changed

- The reactor pin moves from `fe8c5ce` to `c6b2c3e` (`acff32b`, `9b25fa1`). The range holds
  reactor's `67e22a6`, which removes the worker pools, `Reactor::pool()` and the `workers`
  arguments, renames `OffloadGate` to `AdmissionGate`, and gives `TcpServer` and `UdpServer` new
  constructors. `Bus` already constructs `UdpServer(on_datagram)`, so cluster's API and behavior
  are unchanged; a consumer that also uses reactor directly through this pin gets the new
  constructors. The range also keeps a `TcpServer` accepting after a connection that reset while
  queued (`76c32b4`).
- The varint pin moves from `21af82a` to `ac6003e` (`acff32b`, `9b25fa1`), over varint's
  benchmark and documentation commits; `leb128` gives the same bytes.
- The README follows the family README standard (`007f960`): the sections in the standard's
  order, a FetchContent snippet pinned to a commit, and a quick start that is a complete program
  with its output. The election timing states `RaftConfig`'s defaults, a 1 s heartbeat and a 10
  to 30 s election window; the 20 to 60 ms window it quoted is the benchmark's setting. The
  `apply()` retry note is a "Not for" bullet that links `SAFETY.md`, and the layer diagram stays
  in `ARCHITECTURE.md` only.

### Added

- A Performance section in the README (`9d24870`), from `cluster_bus_bench` and
  `cluster_raft_bench` at `acff32b`, the range over three runs: framing a 200-byte message
  takes 30.7 to 32.5 ns on the Apple M4 Pro and 83.9 to 84.7 ns on the Intel Core i9-9980HK,
  parsing and validating one 1.5 to 2.5 ns and 8.2 to 10.5 ns, and a fresh three-node cluster
  elects a leader in a median of 38.8 to 40.6 ms and 37.4 to 39.5 ms with a 20 to 60 ms
  election window. There is no comparator: the frame is Xapiand's discovery wire format.
