# Strict fixed-voter consensus

`cluster::consensus::Core` is an additive deterministic Raft state machine. It has no sockets, clock reads, randomness, filesystem calls, or application callbacks. It leaves the volatile legacy `cluster::Raft` API and wire format unchanged. This module is not yet connected to Detent's authority transport.

```sh
cmake -S consensus -B .scratch/consensus -DCMAKE_BUILD_TYPE=Release
cmake --build .scratch/consensus -j4
ctest --test-dir .scratch/consensus --output-on-failure
```

The standalone `consensus_demo DIRECTORY create|open [COMMAND]` exercises a single voter, real journal barriers, and deterministic application reconstruction. The directory must already exist and be dedicated to this example. Its fixed demo identities and logical ticks are illustrative; production uses provisioned identities and real monotonic time. `create` is explicit and cannot overwrite initialized state; `open` recovers it.

## Host contract

Construct the core with an exact `FixedConfiguration`, a semantically validated `RecoveredState`, resource limits, and timing bounds. Voter IDs are nonzero, unique, fixed, and include the local node. Three or five voters are the intended authority deployment. Discovery and observed liveness never change the voting denominator. Provision unique cluster/configuration identities externally; bind them to every authenticated transport session before passing `Receive` events.

The host owns one protocol executor and supplies absolute monotonic `Tick` events, with election delays chosen externally within the configured range. Deliver current time before dispatching traffic or completions after slow I/O. While storage is pending, time continues to advance. A matching completion immediately releases its dependent effects and drives leader replication, but never initiates another campaign. Campaigns require a separately admitted Tick after storage becomes available; it may use the same absolute time as a Tick received while busy. Outdated ticks are ignored. Timing affects availability, not the voting or replication evidence required for commitment.

```text
Event → Core::step → Persist(token, batch) → ordered storage worker
                                 ↓ durable completion
                         Persisted(token) → Send / Committed
                                             ↓ application worker
                                       Applied(through)
```

Only one storage batch is outstanding. Stage changes internally, but never release dependent protocol replies, proposal placement, or application delivery before the matching completion. Unknown or duplicate completion tokens cannot release another batch. Storage failure fences the core. Local application completions remain reliable even while a later storage batch is pending; only retryable peer messages may be dropped. A transport receipt is not a Raft acknowledgement.

`ProposalPlaced` means local durable placement, not client write success. A client result comes from deterministic application of a committed command. The application supplies idempotency, transaction conflict evaluation, and bounded client-request ownership. Commands are opaque and may encode an admitted group of application transactions; grouping is necessary to amortize the journal's measured durability barriers.

The core persists term/vote changes before election traffic, appends a persisted current-term no-op on election, and advances commitment only from distinct configured-voter progress at a current-term log position. Commit-index advances are persisted before application delivery. Each peer has one correlated Append RPC; obsolete replies cannot manufacture or rewind progress, while a structurally valid higher term is observed before correlation filtering. A failure that cannot usefully decrease the next index backs off instead of forming an immediate request/response loop.

Only one bounded committed range awaits `Applied`. Completions must acknowledge that delivered range exactly. For reads, a leader first needs a committed current-term entry, then a new correlated quorum probe issued after the read's admission. Its applied cursor must reach the captured committed index before `ReadReady`. Probes preserve existing RPC flights and stop requesting contributions already counted or no longer needed for quorum. Leadership loss rejects pending reads. There are no leases or recent-traffic shortcuts.

## Durable adapter

`storage.h` encodes exact configuration initialization and opaque `StorageBatch` operations for the generic journal. Related hard-state and logical suffix changes fit in one atomic batch. Replacing an uncommitted Raft suffix appends a storage operation; it never physically truncates acknowledged journal history.

`storage_batch_size()` provides checked framing bounds without allocation or IO, and its batch overload measures the actual encoded size. Encoding uses the same helper before reserving output capacity. Admission can plan hard-state and log continuation batches without duplicating their format constants. These helpers do not reserve capacity or admit Core events; the worker supplies that enforcement.

Create a journal explicitly, append `encode_initialization(configuration)`, and construct the empty core only after that append succeeds. Reopen by passing each verified journal batch to `Recovery::replay`, then call `finish(frontier.sequence)` only after `Journal::recover` returns successfully. Semantic replay checks configuration identity, contiguous indexes, monotonic terms and commitment, ballot lifetime, log bounds, and committed-prefix protection. Same-index/same-term entries with different content fail closed. Journal checksums alone do not establish these invariants.

The restarted application rebuilds state only through the durable commit index. External effects need their own idempotency. Do not erase a voter's state and rejoin under its old identity: its forgotten ballot invalidates crash-recovery assumptions. Node replacement and restore epochs require explicit provisioning.

## Local immutable checkpoints

The host pins an immutable application capture at committed, applied index `A`, prepares its artifact on the storage executor, and optionally verifies it before requesting cutover. `LocalCheckpoint` carries that host-owned capture token, `A`, its term, and cluster/configuration identities. The core admits a started, idle request only when its included boundary matches the retained committed log. Followers can checkpoint too. Serialization must use the pinned image at `A`, not mutable application state that has advanced since capture.

`PersistCheckpoint` reserves the same exclusive persistence slot as ordinary storage batches. It captures current durable HardState and every entry after `A`, including uncommitted entries, with the persisted application cursor set to `A`. The ordered storage worker records its exact current journal sequence `S`, encodes a `CheckpointBundle` binding `S` and the application descriptor, prepares the bundle, then publishes both through the journal. `A` is a log/application index; `S` is a storage-operation sequence.

Matching `Persisted` alone compacts the live prefix through `A`. Newer live application progress, outstanding delivered ranges, pending reads, and genuine peer matches survive. Reliable `Applied` completions remain accepted during cutover. Any preparation or readback failure that fences the journal requires trusted local `StorageFault`, which fences the core even without a pending batch. Correlated `Failed` remains appropriate for the current persistence operation.

Recover with the journal's restore callback: require the declared application dependency, decode the bundle against that exact descriptor and `frontier.base_sequence`, call `Recovery::restore`, then replay suffix batches. Call `finish` only after the entire journal recovery succeeds. Recover the application image at `A` and apply committed entries above it again. Never import these local hard-state/configuration bundles as network snapshots; portable installation is a separate protocol.

A compacted follower reports its boundary index and term. A correlated matching hint can choose the next prefix probe but never counts as an acknowledgement. A leader needing unavailable history emits one `SnapshotNeeded` for that peer; the host must keep it pending until the separate installation protocol exists. There is no implied successful catch-up. Legacy wire shapes remain unchanged.

The full retained suffix is copied and encoded after freeze. Payload and entry limits bound this work, but production timing must budget its measured pause. Checkpoint publication alone does not bound accumulated disk usage; reclamation remains pending.

## Bounds and current limitations

Limits cap voters, entry counts, individual command payloads, aggregate retained payloads, RPC entry counts and payloads, uncommitted entry count, and pending reads. `log_bytes` and `rpc_bytes` are payload budgets; entry counts separately bound object/framing overhead. The storage decoder caps payload and framing before allocation. A future wire decoder must independently cap the complete encoded and decompressed message before building these typed events, and the transport must bound its own queues.

Admission stops before the retained-log entry limit, reserving control entries for elections. Exhausting that finite reserve prevents publishing another leader without its no-op. Term exhaustion never wraps. Checkpoint recovery requires an exact included index/term and an application cursor at that included index. Log reclamation, snapshot installation, configuration changes, authenticated transport, and production capacity remain later gates. Continuously available writes at retained-history capacity are not yet supported.

## Qualification

Native AppleClang 17 and Clang 23 AddressSanitizer/UndefinedBehaviorSanitizer Release tests pass the recorded schedules. The deterministic host tests actual persistence/application completions and semantic operation replay. A POSIX integration test writes through the journal, reopens it, and reconstructs committed consensus state. Focused cases cover barriers, wrong completion tokens, affirmative majorities, inherited-prefix commitment through a no-op, simultaneous disk/application work, isolated-leader reads, overlapping reads, blocked application, partial quorum, capacity backoff, stale RPCs, failed storage transitions, semantic corruption, control reserve exhaustion, and maximum terms.

Two reproducible 1,000-step schedules use three voters (seed `1380009556`) and five voters (seed `1129076051`). They vary loss, duplication, reordering, partitions, and durable restarts, check agreement of applied prefixes, and require convergence after healing. These are schedule coverage, not exhaustive model checking or production consensus certification. The standalone build fetches neither Asio nor reactor. [Raft's specification](https://raft.github.io/raft.pdf) is the algorithm reference; [the journal contract](../journal/README.md) records filesystem and hardware assumptions.

## Local checkpoint measurement

`consensus_checkpoint_bench` establishes its fixture through bounded durable batches, then measures the freeze from `LocalCheckpoint` through matching `Persisted` and compaction. Application-artifact preparation and verification are excluded. On the Intel Core i9-9980HK/macOS 15.8.1/AppleClang 17 Release setup, single samples on October 2, 2026 were:

| Retained entries | Payload bytes | Encoded bundle bytes | Capture wall s | Capture CPU s | Encode/write/publish wall s | CPU s | Full frozen wall s | Process peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1,024 | 1,197 | 0.000006 | 0.000011 | 0.085114 | 0.002370 | 0.085141 | 946,176 |
| 65,535 | 67,107,840 | 68,484,227 | 0.064412 | 0.064330 | 0.570458 | 0.477923 | 0.635166 | 572,751,872 |

The largest fixture reserves one retained slot for its included no-op; its command suffix approaches the default 64 MiB payload bound. Process peak RSS comes from `getrusage`, includes fixture initialization and earlier cases, and is not incremental daemon memory. At baseline revision `96ea4dd`, suffix capture, owning bundle argument, validation state, and encoded buffer coexisted, so bounded payloads required a larger memory budget. These single samples do not establish tail latency, fleet capacity, energy, or cost. Run `.scratch/consensus/consensus_checkpoint_bench` from the repository root; its dedicated scratch directories are removed after each case. Production integration must budget this pause or change the mechanism.

Const-reference semantic validation and encoding now avoid two unnecessary suffix copies, and encoding reserves its exact checked size. Use `encode_checkpoint(action.state, covered_sequence, application.descriptor())` when the action already owns the immutable suffix. The owning-bundle overload delegates to the same implementation. Repeating the identical fixture produced:

| Retained entries | Capture wall s | Capture CPU s | Encode/write/publish wall s | CPU s | Full frozen wall s | Process peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.000019 | 0.000024 | 0.083942 | 0.001707 | 0.083970 | 983,040 |
| 65,535 | 0.062530 | 0.062465 | 0.326839 | 0.240048 | 0.389764 | 286,265,344 |

For the large fixture, process peak RSS falls from approximately 546 MiB to 273 MiB, and the measured full freeze from 635 ms to 390 ms. The encoded bundle remains exactly 68,484,227 bytes; validation and durable ordering remain intact. The same single-sample and process-lifetime limitations apply, and 390 ms still needs an explicit production timing budget.
