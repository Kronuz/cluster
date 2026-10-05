# Durable opaque batches

`kronuz::journal::Journal` appends bounded opaque storage batches and publishes a checksummed durable frontier. It has no Raft, reactor, socket, or application-state dependency. This module is additive to the legacy cluster API and can later move into `Kronuz/storage` without moving consensus policy with it.

```sh
cmake -S journal -B .scratch/journal -DCMAKE_BUILD_TYPE=Release
cmake --build .scratch/journal -j4
ctest --test-dir .scratch/journal --output-on-failure
```

## Contract

One serialized executor owns a journal. `append_batch()` uses the synchronous driver of a resumable append operation and returns after its final durability barrier. Completion-based applications use the same sequencing through the additive owned-operation interface below; existing Worker callers retain synchronous defaults, with an opt-in completion append profile. The `IO` instance outlives the journal and every artifact builder, prepared handle, and reader. All operations use the owning storage executor. The batch bound defaults to 64 MiB; callers should select a smaller bound that fits their workload. Admission rejects oversized batches before storage changes.

```text
Append complete batch → sync journal → create/write temporary manifest
→ sync manifest → replace manifest → sync directory → return frontier
```

The frontier names the storage identity, generation, byte offset, and contiguous batch sequence. A batch includes checksums for framing metadata and payload; native structs never appear on disk. Sequence exhaustion and platform offsets are bounded. CRC32C detects accidental corruption and does not authenticate data.

Use `create(identity)` only for a new store. The application supplies a unique 128-bit storage identity and establishes a dedicated directory, including its parent-directory durability, before opening it. Initialization exclusively reserves both the stable owner-lock name and the manifest name. Interrupted initialization may need operator repair; it never silently replaces missing or damaged initialized metadata with empty state.

Use `recover(callback)` when reopening. The callback receives one complete batch at a time, and its view expires when the callback returns. Build unpublished application state: a later record or final barrier can fail, and the state becomes usable only after recovery succeeds. The journal validates the complete required prefix before discarding extra bytes beyond the frontier. Corruption or truncation anywhere inside that prefix, including the last acknowledged batch, fails closed. Recovery seals the selected directory namespace before returning, including after a process restart that preserved an interrupted rename.

Every uncertain I/O or recovery-callback failure fences the journal until it closes and a fresh instance recovers. The stable owner lock remains held while fenced. A failed append has an unknown outcome; recovery may include it, so callers need application-level idempotency. Related changes, such as Raft term/vote updates and suffix-replacement operations, belong in one encoded batch. Replacing an uncommitted Raft suffix appends a new storage operation; it never deletes acknowledged storage history.

## Immutable checkpoint generations

Generation 1 remains readable until the first explicit checkpoint publication. `prepare_artifact()` creates an exclusive immutable artifact and synchronizes its ownership prefix before accepting payload; supply chunks of at most 64 KiB, then call `finish()` to seal its length and checksum and synchronize both contents and filename. The default cumulative artifact bound is 512 MiB. One builder and at most nine prepared leases are admitted; copied prepared handles share one lease. Handles retain the stable owner lock after the journal closes. Abandoned preparation leaves an unreferenced file eligible for reclamation when its ownership prefix survives.

New artifacts use a 44-byte immutable ownership prefix (format magic, store identity, artifact identity, CRC) followed by a 24-byte seal and then payload. The seal binds the prefix, payload length, and payload checksum; finishing replaces only the seal. Required-artifact recovery validates the complete header and payload. Readers also accept sealed version 1 artifacts with their 56-byte headers, so a checkpoint can reuse an older dependency while writing a new bundle. Manifest and consensus bundle formats do not change. Older binaries reject new artifacts; migration is not backward-readable.

`publish_checkpoint(bundle, dependencies, covered_sequence)` requires prepared handles from the same owner session, no duplicate identities, at most eight dependencies, and the exact current batch sequence. The bundle and dependencies are opaque to this layer. Consensus must separately bind the application snapshot boundary, retained suffix, configuration, and hard state inside its bundle.

```text
Use sealed capabilities → create/sync exclusive new journal generation
→ sync directory → write/sync replacement manifest → rename → sync directory
```

The version 2 manifest binds the bundle and every required dependency by identity, length, and checksum. Physical generation names are unique across interrupted retries. Publication preserves the batch sequence; the next append uses `covered_sequence + 1`. Admission errors leave the journal usable, while uncertain I/O fences its owner session. Prepared handles prove successful immutable sealing or pinning from verified recovery. Optional `verify_artifact(handle)` reads back the complete payload before freezing a consensus cutover and fences the owner on failure. Publication checks capabilities and metadata without rereading payloads. Recovery always verifies all referenced bytes.

Reopen with `recover(replay, restore)`. The journal verifies the bundle and all dependencies before passing their bounded readers to `restore`, then replays the suffix. Both callbacks must construct unpublished state until recovery returns successfully. A missing or corrupt referenced artifact fails closed; recovery never falls back to an older generation. Older readers reject version 2 metadata instead of reinitializing it. `pin_artifact()` can retain a currently referenced artifact for a later publication without rewriting it.

### Incremental staged-file verification

`begin_artifact_verification(prepared)` returns an optional move-only `ArtifactVerifier`. Opening checks exact file size and the fixed v1/v2 header, with no payload scan. `read_next(destination)` performs one backend payload read into at most 64 KiB, honoring partial progress while advancing its private offset and CRC32C. Empty destinations reject before EOF; EOF returns zero without IO. Oversized buffers and premature finish reject without fencing. The caller may parse these candidate bytes only into unpublished application state with its own format and allocation limits.

Call `std::move(verifier).finish()` after the exact payload length was consumed. It checks CRC32C and transfers the same file descriptor and reclamation pin into an `ArtifactReader`, without reopening or rereading. This proves byte integrity only; application semantic validation and later durable publication/activation remain separate conditions. Existing synchronous recovery and `verify_artifact()` retain their full-payload validation behavior.

At most nine concurrent verification leases are available per owner session, in addition to the existing recovery-reader bound. Promotion and moves retain a lease until the final verifier or promoted reader closes. Capacity exhaustion returns `nullopt` before IO; moved or foreign preparations reject without fencing. Header, payload or IO corruption fences the shared storage owner and escaped capabilities. All handle operations, moves and destruction remain on the owning serialized executor, and the backend outlives every escaped handle. Closing releases the file before its pin and lease.

The Store overload takes a session-bound `ReplacementId` and `ArtifactPart`, and requires a sealed artifact. Known cancellation closes staging capabilities but escaped verification/read pins keep the file and its actual charges alive. Only final handle release followed by durable reclamation returns that space. Opening performs bounded header IO; each payload step bounds requested bytes and backend calls, while filesystem latency remains outside that bound.

Publication does not itself delete old generations or artifacts. Explicit bounded reclamation is described below; consensus checkpoint integration lives in the separate core module, and network snapshot installation remains pending. The preparation count bounds live handles, not accumulated disk usage.

## Bounded reclamation

Call `reclaim_step(scan_budget)` on the owning storage executor after successful recovery/publication. Each call scans at most 1 through 4,096 directory entries, using an independent streaming cursor. A completed pass closes that cursor; a later call starts a new pass. Namespace mutations may cause omissions or repeats, so cleanup is eventual and each candidate is rechecked against current roots immediately before removal.

The reclaimer always protects the current journal generation, manifest, stable lock, checkpoint bundle and declared dependencies. Active builders register their names before creation; prepared leases and readers retain artifact pins, including readers moved out of recovery callbacks. Reader descriptors close before their pins are released. Existing interfaces bound the registry to nine prepared leases, at most nine escaped recovery readers, and the single active builder; temporary verification reads use an already prepared identity.

Exact reserved filename grammar selects candidates, then a bounded header/frame must prove matching store identity and basename identity. Foreign, malformed, unsafe, and unrelated files remain untouched. Only ownership framing is read; obsolete payloads are not checksummed during cleanup. Missing files are idempotent. Deleted names receive a directory barrier before the step succeeds, and uncertain I/O fences the journal; the consensus host must propagate `StorageFault` even without a pending persistence token.

Returned statistics count scanned, protected, removed and unidentifiable candidates, plus logical bytes unlinked. The byte counter saturates explicitly rather than overflowing. These numbers do not report physical disk space freed. No recursive traversal occurs. POSIX enumeration uses a separately opened directory description rather than sharing offsets through `dup`.

For version 2 artifacts, the separately synchronized ownership prefix proves eligibility even when preparation never sealed the payload. Initial creation failures can leave at most 68 unidentifiable bytes before payload admission. Interrupted version 1 preparation leaves zero placeholder headers, which cannot prove store ownership; later ownership-prefix corruption can also make a large artifact unidentifiable. These files are retained and reported. Automatic scheduling and admission quotas remain required before claiming bounded accumulated storage. An explicit cleanup API alone does not establish a production disk bound.

## Quiescent storage inventory

`Inventory` in `inventory.h` establishes a flat-store census after successful journal recovery. The host must hold the journal's stable owner lock and pause every namespace mutation and file write throughout the census, before starting request admission, preparation, or reclamation. Its IO cursor must enumerate each direct entry exactly once while quiescent. This precondition is external to the helper; it cannot detect every external directory change, and the mutation-tolerant cleanup scan cannot establish exact accounting during writes.

Call `step(budget)` with 1 through 4,096 entries per turn, then inspect `stats().ready()`. The helper retains one streaming cursor and aggregate counters, with no filename or inode table. It counts every direct entry regardless of ownership or filename grammar. POSIX footprint inspection uses `fstatat(AT_SYMLINK_NOFOLLOW)` without opening targets, so foreign files, sparse files, symlinks, and special files contribute without following links or opening devices/FIFOs. Hardlinked entries are counted separately, conservatively including their allocated sizes.

Logical bytes and allocated bytes are separate aggregates; unavailable allocation metrics make only that aggregate unknown. Missing entries, counter overflow, and unexpected subdirectories prevent readiness. Subdirectory contents are not traversed or guessed. An IO failure leaves the inventory terminally failed; discard it and restart a census under quiescence after resolving the failure. Partial totals never authorize admission. This helper provides accounting observations, not enforced quotas, free-volume reservations, or automatic scheduling.

## Storage admission accounting

`Admission` in `admission.h` starts from a ready quiescent inventory. Configure hard limits for logical bytes and directory entries, plus independent control and replacement pools. Normal permits cannot consume either protected pool. Startup usage above a hard limit remains observable and reclaimable but admits no positive reservation. Partially funded pools report pressure and stop normal admission; remaining control capacity is usable. Allocation and free-volume observations do not reserve capacity against other users of the filesystem.

The live permit bound is 3 through 4,096. `control_slots` defaults to one and must leave room for Normal and Replacement. Normal admission preserves the configured number of Control slots and one Replacement slot, counting already active protected permits toward that protection. Control admission leaves one slot for replacement when no replacement is active. A worker with a three-write control continuation pack selects at least three Control slots and five total permits, alongside sufficient byte and entry pools. Only one replacement cycle is admitted. Held pool capacity remains reserved during other settlements and cleanup, preventing it from being funded twice.

A protected pool is funded by its available capacity plus capacity held by live permits. Fully reserving a replacement therefore allows ordinary appends to interleave with preparation using the remaining unprotected capacity. After settlement consumes protection, normal admission pauses if the pool cannot refill until durable cleanup returns space.

Reserve the worst simultaneous added resources before any mutation, including temporary manifests, filenames, and the complete replacement cycle. A move-only `StoragePermit` retains its accounting session after the facade closes. Call `mark_started()` before IO, then `settle(added, removed)` after the operation and any removal barriers complete. Settlement records final charged additions and already charged resources durably removed; it does not count cumulative transient creation traffic. Actual additions stay charged, while unused allowance returns to its originating pool. Existing reader pins remain charged until actual durable deletion. `credit_durable_reclaim()` credits completed deletion only, then refills control capacity before replacement capacity.

The host supplies each durable removal exactly once. The ledger checks aggregate bounds, but cannot prove deletion identity or detect a duplicate credit that still fits the aggregate. Serialize all permit operations and destruction on the owning executor; filesystem identities and completion correlations remain host responsibilities.

Pre-IO cancellation refunds its permit. A started permit destroyed without settlement, explicit `abandon()`, invalid settlement, or `taint()` prevents further admission and retains uncertain reserved capacity. Actual IO failure must also fence the journal. Reopen storage and construct a new ledger from a fresh quiescent census; old permits retain their old accounting session and cannot credit the new one. All permit operations and destruction use the owning storage executor.

This module does not intercept journal mutations. End-to-end enforcement still requires permits for every write/create/replace, a whole-cycle checkpoint reservation, guaranteed bounded maintenance scheduling, and admission before the strict core emits persistence actions. Never discard an emitted persistence action or invent its completion when a quota is reached. Finite control reserves do not promise progress through an indefinitely full volume; actual IO failures still fence.

`Journal` supplies checked footprint plans beside its format encoders. `bootstrap_plan()` assumes an empty directory and the IO creation contract of one zero-length `owner.lock`; it covers the reserved manifest, journal and temporary manifest coexisting. `append_plan(payload_bound)` reserves record growth plus the maximum supported temporary manifest footprint and one entry, so its reservation remains sufficient across checkpoint migration. Successful append settlement charges actual record growth and no net entry change.

Static `append_footprint(payload_bound)` computes the format-only bound before storage opens and rejects payload sizes beyond the 32-bit wire representation. The live `append_plan()` delegates to it while retaining configured-size, availability and frontier-exhaustion checks. Store exposes trusted `fence_storage()` so its owning worker can stop storage after an application or protocol failure even without a pending IO operation.

`checkpoint_plan(payload_bounds)` describes an all-new bundle and dependency set. It includes every artifact header/payload, a new generation and the replacement manifest; only the old manifest is credited at publication. Payload bounds establish peak admission, not exact final charges. Recompute with actual payload lengths immediately before publishing, because the removed manifest footprint belongs to the current generation and can change across another checkpoint. Canceled staging files and obsolete generations remain charged until durable cleanup. The resource value types and checked arithmetic live in `resources.h`, independently of admission policy.

## Private mutation Store

`Store` in `store.h` owns a Journal, startup inventory, accounting session and one replacement operation. Use its backend exclusively through Store and its read-only capabilities on the ordered storage executor. It returns opaque append reservations and replacement IDs, with no mutable Journal, builder or prepared handle access. The IO backend still outlives Store and escaped recovery/verification readers. Fresh creation requires an exclusively provisioned empty directory and checks the complete bootstrap peak before changing names. Recovery holds the stable lock while `inventory_step()` establishes accounting; append and preparation are unavailable until that census is ready. Cleanup during startup invalidates and restarts the census.

Reserve an append as Normal or Control before passing its encoded bytes to `append()`. Its temporary-manifest peak remains valid across a replacement publication. Admission class selection is the trusted worker's responsibility. Reserve the complete replacement before application capture, then prepare Application followed by Bundle through `begin_artifact()`, bounded `write_chunk()` and `finish_artifact()`. Ordinary admitted appends may interleave with preparation. Optional `verify_artifact()` performs readback before the frozen cutover. `publish()` requires both sealed artifacts and the exact current storage sequence, recomputes actual footprints, and charges the new files while crediting only the replaced manifest.

Known cancellation between completed calls closes preparation capabilities and charges the actual staged orphan footprints. Their space returns only after `reclaim_step()` finishes the deletion barrier. Uncertain append, preparation, publication, inventory or cleanup errors fence Journal capabilities and accounting, including escaped readers. Invalid sizes, stale IDs and foreign reservations reject before mutation. The owner must propagate a storage fault even when no consensus persistence action is pending.

This establishes the serialized mutation/accounting boundary under exclusive IO use. It does not implement the service worker, admission before Core transitions, bounded reliable completion queues, or guaranteed maintenance scheduling. Those remain separate integration gates. The POSIX Core→Store test intentionally reserves after Core emits each action; it verifies persistence/recovery rather than that future admission contract.

## POSIX backend

`PosixIO` opens an existing directory and resolves validated single-component filenames relative to its descriptor. It rejects final-component symlinks, special files, shared regular-file inodes, files on another device, and ownership or permissions that permit another user to modify the store. Trusted ancestor directories remain a caller precondition. Ownership is exclusive across cooperating processes through a stable `owner.lock`, which is never replaced or removed by manifest publication.

Linux and FreeBSD use file and directory `fsync`. On macOS, files use `F_FULLFSYNC`; namespace barriers use directory `fsync` followed by `F_FULLFSYNC` on the same-device stable lock file. [Apple's XNU manual](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2) documents persistence of prior device `fsync` operations after full synchronization returns. Unsupported operations fail instead of downgrading the contract.

The durability model assumes a local filesystem that honors atomic same-directory replacement and successful synchronization, plus storage hardware that honors its flush contract. Deterministic fault tests exercise the protocol; they do not qualify a filesystem, mount option, drive, or power-failure behavior. Internally consistent rollback of an entire store requires an external witness to detect. Restoration must use an explicit new epoch at the application layer.

## Scope and tests

Both manifest formats support append and incremental recovery; version 2 adds immutable checkpoint publication. Artifact version 2 adds reclaimable staging ownership. Automatic cleanup scheduling, admission quotas, asynchronous I/O, and authority integration remain separate milestones. Interrupted publication may leave temporary manifests; they cannot become authoritative without the manifest replacement. Disk growth is therefore not yet bounded for a continuously running authority.

The injected filesystem tests track visible and durable file contents separately from visible and durable names. They interrupt operations before and after side effects, exercise both namespace outcomes before a barrier, preserve previously acknowledged frontiers, and check process restart followed by power loss. Additional checks cover partial and zero-progress I/O, callbacks, corruption of every durable byte, truncation of every acknowledged prefix, oversized framing, sequence continuity, missing metadata, and concurrent owners. Checkpoint tests additionally interrupt preparation, publication, and recovery, corrupt every byte of required artifacts, and check owner lifetime and bounded admission. The POSIX test verifies real create/append/checkpoint/reopen, ownership locking, filename confinement, and rejection of symlinks, hardlinks, and special files.

## Initial measurement

`journal_bench` measures 64 sequential durable groups per case, each containing opaque 256-byte operations. Initialization is excluded. On an Intel Core i9-9980HK running macOS 15.8.1, the AppleClang 17 Release build at revision `55e3072` produced these local-filesystem results on October 2, 2026:

| Operations/group | Payload bytes/group | Total wall seconds | Process CPU seconds | Operations/second | p50 batch ms | p99 batch ms |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 256 | 4.006861 | 0.070486 | 15.972603 | 62.164488 | 78.147896 |
| 32 | 8,192 | 4.020624 | 0.076460 | 509.373713 | 62.886624 | 65.350456 |
| 256 | 65,536 | 4.021802 | 0.090530 | 4,073.795345 | 62.942453 | 64.144920 |

This measures durability-barrier amortization, with no transaction encoding, replication, networking, or application execution. The maximum of 64 samples is the reported empirical p99; it cannot characterize a production tail. CPU time is measured using `getrusage`, and no hosting-price or power measurement is available to convert it into cost. The standalone demo occupies 44,544 bytes on disk; the benchmark occupies 42,248 bytes. These executable sizes are not incremental daemon footprint measurements. Run `.scratch/journal/journal_bench` from the repository root to reproduce the workload; it creates and removes dedicated directories under `.scratch/`.

## Checkpoint pause measurement

The initial `checkpoint_bench` separately measured preparing two opaque artifacts and publishing them as bundle plus application dependency. The same Intel Mac and AppleClang 17 Release configuration produced these single-sample results on October 2, 2026:

| Application bytes | Bundle bytes | Preparation wall s | Preparation CPU s | Publication wall s | Publication CPU s |
| --- | --- | --- | --- | --- | --- |
| 1,048,576 | 1,048,576 | 0.054763 | 0.010354 | 0.069765 | 0.007462 |
| 67,108,864 | 67,108,864 | 0.470017 | 0.361971 | 0.385382 | 0.321468 |

At revision `b497af6`, publication included rereading and validating both artifacts before the durable generation cutover. The approximately 385 ms sample for 128 MiB of artifacts is a material pause when setting election deadlines; it is not a tail bound. These opaque bytes do not represent a measured Detent key corpus or actual suffix encoder. Run `.scratch/journal/checkpoint_bench` from the repository root; initialization is excluded, and the workload removes only its own dedicated scratch directories. No cost or power conversion is available.

The version 2 Release demo occupies 57,008 bytes, versus 44,544 bytes for the initial version 1 build (+12,464 bytes). The append benchmark occupies 49,032 bytes versus 42,248 (+6,784 bytes); the new checkpoint benchmark occupies 64,448 bytes. These are standalone executable file sizes from the same local toolchain, not measured runtime memory or integrated daemon footprint.

Readback now runs explicitly before cutover. Repeating the workload with preparation plus verification measured separately produced:

| Application bytes | Bundle bytes | Preparation and verification wall s | CPU s | Publication wall s | CPU s |
| --- | --- | --- | --- | --- | --- |
| 1,048,576 | 1,048,576 | 0.077054 | 0.011474 | 0.063021 | 0.001249 |
| 67,108,864 | 67,108,864 | 0.706718 | 0.655203 | 0.063774 | 0.000888 |

For the 128 MiB case, the single local publication sample fell from 385 ms to 64 ms, with preparation and readback outside that interval. This shifts work outside the freeze rather than eliminating checksum work. A real consensus bundle must be encoded and written after freezing its retained suffix; that work remains a separate pause budget. These samples do not establish production tail latency.

## Reclamation measurement

`reclaim_bench` publishes two checkpoint generations with two opaque artifacts each, releases preparation handles, then scans and synchronizes deletion of four obsolete files. On the same Intel Mac/AppleClang 17 Release configuration, the October 3, 2026 single samples were:

| Bytes/artifact | Logical bytes before | Logical bytes after | Logical bytes unlinked | Files removed | Cleanup wall s | CPU s |
| --- | --- | --- | --- | --- | --- | --- |
| 1,048,576 | 4,194,843 | 2,097,460 | 2,097,383 | 4 | 0.021816 | 0.001311 |
| 67,108,864 | 268,435,995 | 134,218,036 | 134,217,959 | 4 | 0.030882 | 0.010203 |

The obsolete payloads are intentionally not read; ownership headers establish eligibility. These results at revision `d93d88e` measure a nine-file directory and its deletion barrier, not large-directory throughput, physical space recovery, or production tails. Run `.scratch/journal/reclaim_bench` from the repository root. Fixture creation is excluded, and only each workload's own scratch directory is removed afterward.

## Durable staging ownership measurement

Artifact version 2 adds one file synchronization per preparation before payload admission. The same Intel Mac/AppleClang 17 Release checkpoint workload produced these October 3, 2026 single samples:

| Bytes per artifact (two artifacts) | Preparation and verification wall s | CPU s | Publication wall s | CPU s |
| ---: | ---: | ---: | ---: | ---: |
| 1,048,576 | 0.101068 | 0.014643 | 0.062845 | 0.001569 |
| 67,108,864 | 0.766365 | 0.671041 | 0.062841 | 0.001165 |

The preceding version 1 artifact samples took 0.077054 and 0.706718 seconds preparing and verifying the same workloads. These individual runs expose the additional barrier cost but do not isolate it statistically. Publication remains approximately 63 ms in these samples. The format adds 12 header bytes per artifact, does not duplicate payloads, and does not change the checkpoint encoder. Reproduce with `checkpoint_bench`; no power or hosting-cost conversion is available.


## Selecting published dependencies

`Journal` and `Store` expose `select_published_dependency(index)` and `begin_published_verification(selection)`. A selection binds the storage session and a fixed-size publication stamp: storage identity, generation, journal identity, checkpoint base sequence/descriptor and dependency index/descriptor. It performs no IO, consumes no preparation or verification slot and pins no artifact. Its weak session provenance does not retain the stable storage lock. Const accessors expose the dependency, checkpoint, base sequence and generation.

Selection returns no value when no checkpoint or that dependency exists. Opening first rejects expired, moved or foreign session provenance without IO or fencing. A stale publication or exhausted shared nine-verifier capacity returns no value. Ordinary appends preserve the selection because their mutable sequence and offset are excluded; checkpoint replacement invalidates it even when it reuses the same dependency.

Opening validates fixed artifact metadata without scanning the payload, using the existing incremental verifier directly without a prepared-artifact slot. Each bounded `read_next()` makes at most one backend call. Complete checksum verification precedes IO-free promotion. The verifier and promoted reader pin the selected immutable artifact across later publication and reclamation. Opening consumes no Replacement reservation: existing bytes stay charged, and obsolete bytes are credited only after the final pin closes and durable reclamation succeeds. IO or corruption fences the shared storage owner; routine unavailability and caller errors leave it healthy.

This is a local storage capability. It neither establishes a Raft application boundary nor authenticates a transfer. The Worker source adapter must separately bind its completed compacted boundary to the selected publication. All operations remain on the owning executor, and IO must outlive escaped verifier/readers.


## Resumable append operations

Construct `Journal` with `std::shared_ptr<IO>` for completion-driven execution. `begin_append(bytes)` validates and copies the bounded payload, header and target manifest before IO, returning a shared `AppendMutation`. Its state machine exposes one typed primitive at a time: bounded write, file sync, exclusive create, replacement or directory sync. Bootstrap and checkpoint publication use the same manifest publication states. Checkpoint publication and artifact preparation have owned states below; bootstrap and reclamation still require completion integration.

The accepted driver retains the operation before submission and calls `submitted()` before executing its primitive. `complete()` accepts only the matching owner-generation, operation and step token. Foreign or repeated completions do not reap or advance the outstanding primitive. Short writes advance only the reported count; zero or oversized progress fences. Each journal payload write requests at most 64 KiB. Backend callbacks only retain completion results and schedule the owner; operation transitions occur on the owning executor.

A successful primitive does not advance the public frontier. Call `finish_append(operation)` only after the final manifest directory/full-flush barrier completes. Settlement advances public metadata and releases the mutation gate. Dropping a submitted or completed but unsettled operation fences the facade, preventing a later append from overwriting bytes using stale metadata. Unsubmitted cancellation performs no IO and leaves the journal healthy. A driver must retain an accepted operation until its original in-flight request is reaped; destroying it earlier is an ownership violation, never an instruction to wait in a destructor.

The operation retains its backend, journal file and owner session. The owned backend also remains alive through escaped preparation and verification leases; this preserves the stable lock after the facade closes. Borrowed-IO construction remains available for synchronous callers with its existing lifetime precondition, but cannot start asynchronous append operations. While an append is pending, new journal mutations and writes/sealing through an existing builder reject before IO. Pure frontier reads expose the previous settled frontier.

The injected append tests run both drivers with identical before/after IO faults, partial writes and v1/v2 frontiers, comparing recovered histories under both namespace outcomes. Suspended completions prove that executed IO alone cannot publish metadata, stale tokens cannot release work, and facade retirement retains backend and lock lifetime. Native journal, consensus and Worker regressions pass; ASan/UBSan passes all three relevant suites. The initial native backend is described below; complete Store/Worker asynchronous accounting integration remains pending.


## Native BSD completion backend

`bsd_completion.h` provides `BsdCompletionQueue` for macOS and FreeBSD. It accepts one owned `IOOperation`, funds one terminal result slot and rejects additional submissions before IO. The accepted operation retains buffers, filenames, file capabilities, backend and stable lock; the queue retains it until the original primitive is reaped. `poll()` returns an owned completion for the serialized owner to apply, never advances Journal or accounting on a backend thread, and reports native, fallback and completed file-write byte counts separately.

On macOS the qualified path uses POSIX AIO writes with `SIGEV_NONE` and bounded completion polling. Darwin 24 rejects AIO kqueue notification even with newer SDK headers. File full-flush, exclusive creation, rename and directory barriers use one bounded fallback worker, preserving `F_FULLFSYNC`. Its result slot uses release/acquire publication and `EVFILT_USER` wakeups, with no owner-side mutex wait. FreeBSD selects AIO writes and `aio_fsync` with kqueue completion notification; that branch has not been compiled or exercised on FreeBSD and remains unqualified. Linux `io_uring`, native reads, Worker artifact/publication dispatch, native readback and reclamation integration are still pending.

The optional `asio_completion.h` adapter retains its driver and operation across coroutine suspension and requires its configured owner executor. Notifications race a 1 ms watchdog, so lost wakeups cannot discard a completed result and older macOS can poll native AIO. Storage execution disables request cancellation until its accepted operation reaches a terminal state; callers cancel interest separately. Shutdown drains accepted work before destroying the queue. Queue destruction with outstanding IO is an ownership error and never silently blocks to manufacture completion.

The real-file fixture writes a 150,000-byte batch using five native writes and five fallback primitives, then recovers an append completed after its facade and original caller disappear. The Asio fixture holds a completion for 100 ms while the owner services timers and UDP loopback traffic, verifies no early frontier publication, and emits cancellation while persistence continues. Both native and ASan/UBSan tests pass on the Intel Mac. All ten root cluster CTests and all 30 Detent CTests pass after integration. Standalone Release fixture executables occupy 125,000 bytes (backend) and 426,592 bytes (Asio); these include their test programs and do not measure incremental daemon footprint or resident memory. The existing service Worker still runs synchronous storage; backend qualification alone does not establish an asynchronous authority service.

Link `cluster::journal_completion` for native backend use. The journal's standalone build still fetches no Asio or reactor dependency; the Asio fixture is built only in the root cluster build. Run `ctest --test-dir .scratch/build -R 'cluster_completion' --output-on-failure` for both backend and coroutine checks.

### Completion-driven Store appends

`Store(std::shared_ptr<IO>, ...)` retains backend ownership through its Journal. `begin_append(std::move(reservation), bytes)` returns an owned `StoreAppend` using the same append and publication states as synchronous `Store::append`. The job copies its payload before submission, owns its reservation, and settles accounting only after the final directory barrier. Canceling an unsubmitted job refunds the reservation. Once submitted, a driver retains the job until its original primitive is reaped; fencing preserves conservative charges for uncertain effects. Matching completions advance the job, and its `result()` exposes either the settled frontier or the original storage failure.

A job can complete after its Store facade and accounting facade are retired because it retains Journal, IO, stable owner lock and permit ownership. Permit operations and completion delivery still belong to one ordered executor. Real POSIX tests recover such a retired-facade append; fault tests hold an executed but undelivered primitive and prove that neither frontier nor accounting advances early. Owned artifact preparation is described below; checkpoint publication has owned states below; readback and reclamation remain synchronous at this stage.

Append ownership qualification passes all ten native cluster CTests and all 30 native Detent CTests. The final retirement regression passes in the focused 35-check Worker suite; journal, Worker and both completion fixtures pass under ASan/UBSan. These gates preserve synchronous behavior and exercise the opt-in append path, rather than certifying the unfinished asynchronous service.

### Completion-driven artifact preparation

`operation.h` defines the backend-neutral request, token and completion contract without artifact or consensus policy. `ArtifactMutation` uses it for exclusive creation and synchronized ownership framing, one bounded payload chunk, or immutable sealing followed by file and directory barriers. The existing synchronous `ArtifactBuilder` drives these same states. `Journal::begin_artifact_preparation()` requires an owned backend and returns the creation job; after success, `take_builder()` yields exactly one builder facade. Its `begin_append_chunk()` and `begin_finish()` return subsequent owned jobs. `prepared_result()` exposes a sealed capability only after the original final barrier completion is applied.

Each accepted job retains preparation state, backend, file and stable lock. The builder identity is protected before creation; sealing allocates its final pin before removing builder protection. Foreign or duplicate completions cannot advance a phase. Unsubmitted cancellation performs no IO; dropping a started incomplete job fences, and destroying an in-flight job is a driver ownership violation. A completed old job cannot clear another operation's mutation gate. The operation owns its chunk bytes, so caller buffers may be retired after admission.

Store provides `begin_artifact_operation()`, `begin_write_chunk()` and `begin_finish_artifact()` for the admitted replacement cycle. A move-only exact job lease prevents overlapping preparation or cancellation. The cycle retains its entire reservation through every job and changes visible phase/length only on terminal success. Store retirement detaches an active cycle; successful preparation then disposes its capabilities and settles known staged orphan footprints once. Orphans remain charged until durable reclamation. Uncertainty fences and preserves conservative reservation charges. This retirement path applies to preparation only; checkpoint publication must complete its own durability protocol.

Injected tests cover faults before and after every creation/write/seal primitive with full and short writes, stale/duplicate completions, held final barriers, pre-submission cancellation, detached creation/payload/sealing, and retirement of an older empty job while a newer job owns the lease. Both drivers leave matching file footprints and preserve acknowledged history. Real POSIX AIO tests prepare and seal through both Journal and Store, then publish and recover exact payloads. Worker does not yet dispatch these artifact jobs; its checkpoint path remains synchronous until that integration is completed.

Artifact preparation qualification passes all ten native cluster suites (58.12 seconds), all 30 Detent suites (37.23 seconds), and journal/Worker/both completion fixtures under ASan/UBSan (45.10 seconds). Builds report no warnings. These gates qualify the shared artifact algorithms and Store ownership, with Worker artifact dispatch and the complete production asynchronous service still pending.

A fenced owner rejects new append submissions, including the next primitive after an earlier step was reaped. Original already-submitted IO still reaches matching terminal completion. A real POSIX reproduction previously accepted submission after fencing and grew the file from 28 to 52 bytes; the corrected path rejects it and leaves 28 bytes. Native and ASan/UBSan journal suites cover first submission, between-step fencing and original completion reaping.

### Completion-driven checkpoint publication

`Journal::begin_checkpoint(checkpoint, dependencies, covered_sequence)` captures bounded prepared capabilities and target metadata before IO. `CheckpointMutation` creates and synchronizes the new generation, synchronizes its directory entry, then runs the same manifest publication states used by appends. Successful primitive execution cannot select the public frontier. After the original final barrier is reaped, `finish_checkpoint()` moves preallocated metadata into the live frontier and adopts the new generation file. Synchronous `publish_checkpoint()` drives this same operation.

`Store::begin_publication()` holds an exact lease on the complete replacement cycle. It settles the actual new files and replaced-manifest footprint only after Journal frontier settlement. An active publication prevents cancellation and overlapping mutations. Store retirement lets an accepted publication complete its full protocol; it cannot be reclassified as canceled preparation. Uncertainty or abandonment after submission fences and preserves conservative charges. A completed capability does not block a fresh replacement cycle, and old leases cannot release a new cycle.

Fault schedules compare synchronous and suspended publication under every before/after primitive fault, full and short writes, and both namespace outcomes. They require identical recovered generation/history, stale-completion rejection and no early frontier visibility. Additional cases cover unsubmitted cancellation, abandonment after a reaped primitive or after the final barrier without settlement, detached Store success/failure, and whole-cycle accounting. Real POSIX AIO fixtures publish through the admitted job and recover a publication completed after its facade disappears. Worker dispatch, readback and reclamation remain integration gates.

Checkpoint publication qualification passes ten native cluster suites (58.13 seconds), all 30 Detent suites (36.85 seconds), and journal/Worker/both completion fixtures under ASan/UBSan (46.24 seconds). The final builds report no warnings. These gates qualify the shared publication protocol and owned Store settlement; production activation remains pending.
