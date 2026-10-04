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

`plan_event()` in `admission.h` returns a fixed pack of at most three append bounds. Tick campaigning reserves ballot, no-op and commit continuations; proposals reserve Normal command storage and a Control commit continuation. Any Command in an append RPC, including an empty payload, classifies the entire atomic batch as Normal. Internal persistence/application completions, faults and raw checkpoint events reject through this external-event planner. The worker owns their reliable delivery and checkpoint capture separately. Planning validates typed RPC payload/count bounds even while busy and uses exhaustive visitors to require a policy for future event variants. It performs no IO or Core mutation and is not itself enforcement.

## Persistence worker

`Worker` in `worker.h` privately owns Core, Store, semantic recovery and scheduling state on one executor. Its IO backend is exclusive and outlives the worker and escaped recovery readers. `create()` establishes generic storage and queues typed initialization; `recover(restore_application)` verifies history and reconstructs unpublished state. `run_one(Tick)` advances bounded startup inventory and initialization before `ready()` allows protocol admission. A verified empty version 1 frontier can resume interrupted typed initialization; missing or damaged bootstrap metadata still fails closed. A restore callback builds unpublished application state, which becomes usable only after recovery succeeds.

`try_submit(Event)` acquires the complete continuation pack before stepping Core. Pressure releases every partial reservation without changing protocol state. There is no input queue. One persistence action, durable completion, application completion and complete external-action batch are retained. `take_actions()` drains the batch; occupied output blocks further protocol steps while pending IO and maintenance can still proceed. Matching `applied()` is reliable during busy persistence. `application_failed(index, reason)` fences the owned Core and Store for the outstanding delivered index; stale reports are ignored. A terminal notice remains deliverable after an older output batch drains. External persistence completions, application completions, faults and raw checkpoints reject through `try_submit()`.

Configure at least three protected Control slots, five total permits, and a Control pool covering both the complete election pack and the largest no-op RPC append. The constructor validates these requirements before IO. Idle leader ticks need no campaign pack. After `foreground_burst` foreground operations, ordinary admission pauses until `run_one()` executes reclamation of at most `scan_entries` directory entries. One owed, fully admitted Tick may precede that slice while leaving maintenance due, so sustained reads cannot starve heartbeats or election timers. Pressure proceeds to maintenance, which can return capacity. This bounds foreground work by the configured burst plus one timer opportunity. Callers must pump the worker for progress; filesystem-call latency remains unbounded by this scheduler.

If occupied output or pending persistence prevents that Tick, GC retains timer debt. Ordinary admission cannot take the next idle opening until the private timer path services it. Timer-first and GC-first orderings both preserve the next ordinary foreground budget; maintenance-owned ticks do not consume that budget. Reliable completions, output draining, application failures and bounded reclamation remain available while this admission gate is active.

The isolated worker tests cover actual POSIX persistence/recovery, interrupted initialization, all-or-nothing partial pack failure, busy application completion, multiple actions in one output batch, failure while output is occupied, leader ticks under Control pressure and continuous foreground reads. Portable snapshot installation, transport authentication and production capacity qualification remain pending. The worker is not yet used by the legacy Raft API or Detent daemons.

### Worker-owned checkpoint lifecycle

Call `reserve_checkpoint(application_cap)` before capturing immutable application state. It reserves the complete application artifact, maximum consensus bundle and publication footprint as one replacement. The returned `CheckpointId` belongs to this worker session; stale or foreign IDs cannot attach state or cancel another replacement. Attach `CaptureMetadata{request, through, term}` exactly once, with the image pinned at committed, applied index `through`.

Stream that image with `offer_application_chunk(id, bytes, final)`. Each chunk is at most 64 KiB and copied into one fixed mailbox. `Busy` leaves the offered bytes unaccepted; retry them after pumping `run_one()`. Aggregate bytes cannot exceed the reserved cap. An empty final chunk is valid, and accepting the final marker immediately closes the stream. The caller retains the immutable capture until staging finishes or cancellation succeeds.

Preparation alternates with bounded reclamation and preserves owed timer service, including while external output is occupied. Application sealing records cutover debt: ordinary admission pauses until the existing persistence chain, output and application completion drain, then Core captures the checkpoint. GC cannot erase that debt. Core stays busy while the worker encodes the bounded suffix, writes the bundle in 64 KiB slices and publishes both artifacts at its exact journal sequence. A fresh Tick precedes delivery of the matching durable completion. Reliable `Applied` remains available throughout.

`cancel_checkpoint()` returns `Canceled` before Core checkpoint persistence, `TooLate` afterward, or `Stale` for another session or completed operation. Known cancellation retains the charges for written artifacts until durable reclamation removes them. Uncertain preparation or publication fences Core and Store. Staging seals artifacts without a separate full readback; reopening verifies every referenced payload and semantic bundle before publishing recovered state. A crash after publication but before Core completion restores the checkpoint and committed suffix. Tests also exercise the first durable proposal and application after publication.

Create a journal explicitly, append `encode_initialization(configuration)`, and construct the empty core only after that append succeeds. Reopen by passing each verified journal batch to `Recovery::replay`, then call `finish(frontier.sequence)` only after `Journal::recover` returns successfully. Semantic replay checks configuration identity, contiguous indexes, monotonic terms and commitment, ballot lifetime, log bounds, and committed-prefix protection. Same-index/same-term entries with different content fail closed. Journal checksums alone do not establish these invariants.

The restarted application rebuilds state only through the durable commit index. External effects need their own idempotency. Do not erase a voter's state and rejoin under its old identity: its forgotten ballot invalidates crash-recovery assumptions. Node replacement and restore epochs require explicit provisioning.

## Local immutable checkpoints

The host pins an immutable application capture at committed, applied index `A`, prepares its artifact on the storage executor, and optionally verifies it before requesting cutover. `LocalCheckpoint` carries that host-owned capture token, `A`, its term, and cluster/configuration identities. The core admits a started, idle request only when its included boundary matches the retained committed log. Followers can checkpoint too. Serialization must use the pinned image at `A`, not mutable application state that has advanced since capture.

`PersistCheckpoint` reserves the same exclusive persistence slot as ordinary storage batches. It captures current durable HardState and every entry after `A`, including uncommitted entries, with the persisted application cursor set to `A`. The ordered storage worker records its exact current journal sequence `S`, encodes a `CheckpointBundle` binding `S` and the application descriptor, prepares the bundle, then publishes both through the journal. `A` is a log/application index; `S` is a storage-operation sequence.

Matching `Persisted` alone compacts the live prefix through `A`. Newer live application progress, outstanding delivered ranges, pending reads, and genuine peer matches survive. Reliable `Applied` completions remain accepted during cutover. Any preparation or readback failure that fences the journal requires trusted local `StorageFault`, which fences the core even without a pending batch. Correlated `Failed` remains appropriate for the current persistence operation.

Recover with the journal's restore callback: require the declared application dependency, decode the bundle against that exact descriptor and `frontier.base_sequence`, call `Recovery::restore`, then replay suffix batches. Call `finish` only after the entire journal recovery succeeds. Recover the application image at `A` and apply committed entries above it again. Never import these local hard-state/configuration bundles as network snapshots; portable installation is a separate protocol.

A compacted follower reports its boundary index and term. A correlated matching hint can choose the next prefix probe but never counts as an acknowledgement. A leader needing unavailable history emits one `SnapshotNeeded` for that peer; the host must keep it pending until the separate installation protocol exists. There is no implied successful catch-up. Legacy wire shapes remain unchanged.

The full retained suffix is copied and encoded after freeze. Payload and entry limits bound this work, but production timing must budget its measured pause. Worker-owned Store admission reserves replacement capacity before capture and charges obsolete generations until bounded, durable reclamation removes them. Backend mutation outside the exclusive Store contract invalidates these accounting guarantees.

## Bounds and current limitations

Limits cap voters, entry counts, individual command payloads, aggregate retained payloads, RPC entry counts and payloads, uncommitted entry count, and pending reads. `log_bytes` and `rpc_bytes` are payload budgets; entry counts separately bound object/framing overhead. The storage decoder caps payload and framing before allocation. A future wire decoder must independently cap the complete encoded and decompressed message before building these typed events, and the transport must bound its own queues.

Admission stops before the retained-log entry limit, reserving control entries for elections. Exhausting that finite reserve prevents publishing another leader without its no-op. Term exhaustion never wraps. Checkpoint recovery requires an exact included index/term and an application cursor at that included index. Portable snapshot installation, configuration changes, authenticated transport and production capacity remain later gates. Checkpoints reclaim retained history; continuously available fleet writes still require those integration and qualification gates.

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

## Worker checkpoint measurement

`consensus_worker_checkpoint_bench` measures the complete owned lifecycle with a 1 MiB immutable application image and a retained command suffix at the default Core limits. Setup seeds real durable journal batches of at most 256 entries and recovers them through Worker. This exercises a maximum legal recovered log, not ordinary proposal throughput. Run the Release executable from the repository root; its dedicated scratch directories are removed after each case.

Single samples on the same Intel/macOS/AppleClang Release setup on October 3, 2026 were:

| Suffix commands | Bundle bytes | Whole checkpoint wall s | CPU s | Core freeze wall s | CPU s | Longest frozen turn wall s | CPU s | Process peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | 67,032 | 0.154070 | 0.008895 | 0.104915 | 0.002567 | 0.062957 | 0.001341 | 2,670,592 |
| 4,096 | 4,280,472 | 0.181037 | 0.031619 | 0.133159 | 0.026329 | 0.064627 | 0.004445 | 24,391,680 |
| 65,535 | 68,484,227 | 0.587517 | 0.371868 | 0.519703 | 0.367490 | 0.082745 | 0.079962 | 291,508,224 |

Whole-checkpoint time includes streamed application preparation. Core freeze starts immediately before the capture turn and ends after matching completion and compaction; ordinary admission pauses earlier when application sealing records cutover debt. The longest frozen turn exposes whole-suffix capture/encoding and durability-call pauses despite chunked bundle writes. RSS is process-lifetime peak, including fixture construction and earlier cases. These samples establish neither tails nor production capacity, and do not support energy or dollar savings claims. They are not a paired comparison with the earlier adapter benchmark.

## Portable snapshot metadata

`snapshot.h` defines a fixed 72-byte `SnapshotDescriptor` with magic `RFTSNP01`, cluster/configuration identities, application-format version, included index/term, exact payload length and CRC32C. All integers use fixed little-endian encoding. `SnapshotPolicy` supplies the expected identities, supported format and maximum image length; decoding allocates no application payload. Empty images require the CRC32C of empty input. Format zero is allowed when explicitly supported. Included indexes exclude zero and the maximum integer; terms exclude zero and allow the maximum integer.

`encode_snapshot()` and `decode_snapshot()` validate exact framing and policy. This content excludes sender-local node identity, ballot, commit cursor, journal sequence/generation, artifact names and retained suffix. A receiver constructs its own local checkpoint bundle. CRC32C detects accidental corruption; it provides neither authentication nor a collision-resistant snapshot identity. The future authenticated transfer envelope must bind all descriptor fields and payload bytes. This codec does not install state, reserve disk space, authorize peers, handle leader terms or acknowledge application activation.

## Deterministic receiver installation

The transport-free Core accepts trusted `InstallPrepared` after the adapter has completely staged and semantically validated an immutable application image. The event carries a prepared-image token, request correlation, authenticated peer assertion, leader term and included boundary. Core revalidates fixed identities, voter membership, tokens and index/term bounds at cutover. These events and their activation completions reject through ordinary Worker admission; owned transfer and activation use the separate Worker APIs described below.

An image must advance beyond the receiver's durable commit cursor. A caught-up image produces `InstallRejected{..., CaughtUp}` without application rollback. If its valid leader term is higher, that receiver-owned term transition and ballot reset become durable before rejection is exposed. Same-term installation preserves the receiver ballot. A matching local index/term boundary retains the receiver's suffix; an incompatible boundary discards it. `PersistInstall` contains the complete receiver-local configuration, derived hard state, new commit/base boundary and recovered application cursor, never sender-local persistence metadata.

Matching `Persisted` adopts the durable boundary and emits `ActivateInstall`, while Core remains busy. This branch performs no ordinary delivery, quorum-read completion or campaigning. Previously delivered `Committed` work may finish through reliable `Applied` in either phase. Matching `InstallActivated` requires that old range to have drained, advances the live application cursor to the installed boundary, resets the follower election deadline and emits `InstallCompleted`. Premature activation or a matching activation failure fences. Stale tokens cannot release another operation; subsequent campaigning requires a separately admitted Tick.

This increment specifies deterministic receiver transitions. Worker reception and installation are described below; transport authentication and correlated sender acknowledgements remain separate gates. Core's authenticated peer field is a trusted adapter assertion, and CRC32C provides no authentication.

## Unpublished incoming image validation

Worker enables reception only when its optional final constructor argument supplies `SnapshotLimits{application_format, application_bytes}`. Existing callers keep reception disabled. `reserve_snapshot()` checks descriptor policy and sender context before reserving the entire image and maximum receiver-local bundle. `SnapshotContext::authenticated_peer` is a trusted adapter assertion; the adapter must authenticate the transport and bind the descriptor and bytes to that peer. Only configured, nonlocal voters are admitted. Malformed contexts throw without reserving storage or producing a terminal result; capacity or an occupied replacement slot returns no operation.

Local captures and incoming images share one replacement reservation and one 64 KiB mailbox. `offer_snapshot_chunk()` requires exact sequential offsets, bounds each chunk and accepts a final marker only at the declared length. A Busy response consumes no bytes. The final marker closes input immediately. Sealing compares the received length and checksum with the descriptor before incremental rereading begins. A mismatch is a healthy `InvalidImage` result; local read failures or reread checksum corruption fence the entire Worker.

`validation_chunk()` exposes one borrowed data view at a time. Its bytes and token remain stable across Worker turns and ordinary traffic until matching `consume_validation()`, cancellation, fencing or Worker destruction. The application must copy or finish parsing before consuming it and must not retain the span afterward. The application owns parser allocation limits and semantic checks. Every view has a distinct token; stale, duplicate, foreign-session and wrong-operation acknowledgements cannot release the mailbox. Verified EOF appears only after the complete reread checksum succeeds. `validation_succeeded()` accepts only the consumed verified-EOF token and leaves the candidate validated but unpublished.

`cancel_snapshot()` and `reject_snapshot_validation()` close validation handles, release reservation ownership and leave actual staged bytes charged until bounded reclamation completes. A separate `take_snapshot_result()` slot preserves correlation and blocks another local or incoming replacement until drained. It does not block ordinary protocol traffic. Reception, held views and semantic waiting leave protocol timers and bounded reclamation available. Worker operations, callbacks, spans and storage objects remain confined to the owning executor.

Staging and validation alone do not change Core's incoming boundary, publish a receiver checkpoint, activate application state or acknowledge installation to a sender. Installation requires the separate explicit request below. Restart recovers the selected old frontier and reclaims abandoned unpublished images rather than activating them.


## Owned incoming installation and activation

`request_install(id)` requires a validated candidate and reserves one conservative Control append before accepting cutover. Pressure leaves the candidate validated and ordinary traffic available. The permit remains separate from existing protocol continuation packs. Accepted requests latch cutover debt, drain existing persistence and ordinary output, then let Core revalidate the stored sender context and derive receiver-local state. Matching request retries reserve nothing further. Pre-Core cancellation refunds the dedicated permit; cancellation becomes TooLate after either installation publication work or higher-term rejection persistence begins.

An immediate Core rejection cancels staged storage and produces a correlated `Rejected` result. Higher-term caught-up rejection first completes its receiver-owned HardState write. An advancing image captures the exact current receiver sequence and reuses the local checkpoint bundle pipeline, including bounded chunk writes and Store publication. Incoming and local operations move their captured state into the same encoder without retaining an additional suffix copy. Successful Store publication preserves the incoming operation until application activation completes.

`snapshot_activation()` repeatedly exposes the same session-bound request after durable publication, once previously delivered application work and retained Applied events have drained. It does not consume ordinary output capacity. The application retains its validated candidate, switches application state atomically, then reports the exact request with `snapshot_activated(id, token)`. Worker retains that completion independently of ordinary output, refreshes time while Core remains busy, and delivers it when output capacity allows. Only matching Core completion produces `Installed`, releases cutover and permits ordinary admission again. A matching `snapshot_activation_failed(id, token)` fences immediately; published generations never roll back. Previously delivered application failure remains reliable throughout installation.

Core rejection and installation results carry an optional durable receiver term. Generic preparation cancellations do not claim a durable term. Terminal results remain separate from ordinary output and block new replacement operations until taken. Maintenance remains bounded and eligible during output or activation waits. Applications must recover their selected persisted image independently of transient transfer context; reopening after publication restores that image even when live activation or its acknowledgment never completed. This API does not authenticate a network transfer, correlate sender replication progress or establish production pause budgets.
