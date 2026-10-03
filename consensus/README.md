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

The host owns one protocol executor and supplies absolute monotonic `Tick` events, with election delays chosen externally within the configured range. Deliver current time before dispatching traffic or completions after slow I/O. While storage is pending, time continues to advance, and its matching completion reevaluates deadlines. Outdated ticks are ignored. Timing affects availability, not the voting or replication evidence required for commitment.

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

Create a journal explicitly, append `encode_initialization(configuration)`, and construct the empty core only after that append succeeds. Reopen by passing each verified journal batch to `Recovery::replay`, then call `finish(frontier.sequence)` only after `Journal::recover` returns successfully. Semantic replay checks configuration identity, contiguous indexes, monotonic terms and commitment, ballot lifetime, log bounds, and committed-prefix protection. Same-index/same-term entries with different content fail closed. Journal checksums alone do not establish these invariants.

The restarted application rebuilds state only through the durable commit index. External effects need their own idempotency. Do not erase a voter's state and rejoin under its old identity: its forgotten ballot invalidates crash-recovery assumptions. Node replacement and restore epochs require explicit provisioning.

## Bounds and current limitations

Limits cap voters, entry counts, individual command payloads, aggregate retained payloads, RPC entry counts and payloads, uncommitted entry count, and pending reads. `log_bytes` and `rpc_bytes` are payload budgets; entry counts separately bound object/framing overhead. The storage decoder caps payload and framing before allocation. A future wire decoder must independently cap the complete encoded and decompressed message before building these typed events, and the transport must bound its own queues.

Admission stops before the retained-log entry limit, reserving control entries for elections. Exhausting that finite reserve prevents publishing another leader without its no-op. Term exhaustion never wraps. Checkpoint recovery is deliberately rejected in this milestone: base index, base term, and recovered application index must be zero. Durable checkpoints, log reclamation, snapshot installation, configuration changes, authenticated transport, and production capacity remain later gates. Continuously available writes at retained-history capacity are not yet supported.

## Qualification

Native AppleClang 17 and Clang 23 AddressSanitizer/UndefinedBehaviorSanitizer Release tests pass the recorded schedules. The deterministic host tests actual persistence/application completions and semantic operation replay. A POSIX integration test writes through the journal, reopens it, and reconstructs committed consensus state. Focused cases cover barriers, wrong completion tokens, affirmative majorities, inherited-prefix commitment through a no-op, simultaneous disk/application work, isolated-leader reads, overlapping reads, blocked application, partial quorum, capacity backoff, stale RPCs, failed storage transitions, semantic corruption, control reserve exhaustion, and maximum terms.

Two reproducible 1,000-step schedules use three voters (seed `1380009556`) and five voters (seed `1129076051`). They vary loss, duplication, reordering, partitions, and durable restarts, check agreement of applied prefixes, and require convergence after healing. These are schedule coverage, not exhaustive model checking or production consensus certification. The standalone build fetches neither Asio nor reactor. [Raft's specification](https://raft.github.io/raft.pdf) is the algorithm reference; [the journal contract](../journal/README.md) records filesystem and hardware assumptions.
