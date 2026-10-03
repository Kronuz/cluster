# Durable opaque batches

`kronuz::journal::Journal` appends bounded opaque storage batches and publishes a checksummed durable frontier. It has no Raft, reactor, socket, or application-state dependency. This module is additive to the legacy cluster API and can later move into `Kronuz/storage` without moving consensus policy with it.

```sh
cmake -S journal -B .scratch/journal -DCMAKE_BUILD_TYPE=Release
cmake --build .scratch/journal -j4
ctest --test-dir .scratch/journal --output-on-failure
```

## Contract

One thread owns a journal. An asynchronous application uses an ordered worker and translates a successful `append_batch` return into its persistence-completion event. The `IO` instance outlives the journal and every artifact builder, prepared handle, and reader. All operations use the owning storage executor. The batch bound defaults to 64 MiB; callers should select a smaller bound that fits their workload. Admission rejects oversized batches before storage changes.

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

The live permit bound is 3 through 4,096. Normal admission leaves two slots for protected work, and control admission leaves one slot for replacement when no replacement is active. Only one replacement cycle is admitted. Held pool capacity remains reserved during other settlements and cleanup, preventing it from being funded twice.

A protected pool is funded by its available capacity plus capacity held by live permits. Fully reserving a replacement therefore allows ordinary appends to interleave with preparation using the remaining unprotected capacity. After settlement consumes protection, normal admission pauses if the pool cannot refill until durable cleanup returns space.

Reserve the worst simultaneous added resources before any mutation, including temporary manifests, filenames, and the complete replacement cycle. A move-only `StoragePermit` retains its accounting session after the facade closes. Call `mark_started()` before IO, then `settle(added, removed)` after the operation and any removal barriers complete. Settlement records final charged additions and already charged resources durably removed; it does not count cumulative transient creation traffic. Actual additions stay charged, while unused allowance returns to its originating pool. Existing reader pins remain charged until actual durable deletion. `credit_durable_reclaim()` credits completed deletion only, then refills control capacity before replacement capacity.

The host supplies each durable removal exactly once. The ledger checks aggregate bounds, but cannot prove deletion identity or detect a duplicate credit that still fits the aggregate. Serialize all permit operations and destruction on the owning executor; filesystem identities and completion correlations remain host responsibilities.

Pre-IO cancellation refunds its permit. A started permit destroyed without settlement, explicit `abandon()`, invalid settlement, or `taint()` prevents further admission and retains uncertain reserved capacity. Actual IO failure must also fence the journal. Reopen storage and construct a new ledger from a fresh quiescent census; old permits retain their old accounting session and cannot credit the new one. All permit operations and destruction use the owning storage executor.

This module does not intercept journal mutations. End-to-end enforcement still requires permits for every write/create/replace, a whole-cycle checkpoint reservation, guaranteed bounded maintenance scheduling, and admission before the strict core emits persistence actions. Never discard an emitted persistence action or invent its completion when a quota is reached. Finite control reserves do not promise progress through an indefinitely full volume; actual IO failures still fence.

`Journal` supplies checked footprint plans beside its format encoders. `bootstrap_plan()` assumes an empty directory and the IO creation contract of one zero-length `owner.lock`; it covers the reserved manifest, journal and temporary manifest coexisting. `append_plan(payload_bound)` reserves record growth plus the maximum supported temporary manifest footprint and one entry, so its reservation remains sufficient across checkpoint migration. Successful append settlement charges actual record growth and no net entry change.

`checkpoint_plan(payload_bounds)` describes an all-new bundle and dependency set. It includes every artifact header/payload, a new generation and the replacement manifest; only the old manifest is credited at publication. Payload bounds establish peak admission, not exact final charges. Recompute with actual payload lengths immediately before publishing, because the removed manifest footprint belongs to the current generation and can change across another checkpoint. Canceled staging files and obsolete generations remain charged until durable cleanup. The resource value types and checked arithmetic live in `resources.h`, independently of admission policy.

## Private mutation Store

`Store` in `store.h` owns a Journal, startup inventory, accounting session and one replacement operation. Use its backend exclusively through Store on the ordered storage executor. It returns opaque append reservations and replacement IDs, with no mutable Journal, builder or prepared handle access. The IO backend still outlives Store and escaped recovery readers. Fresh creation requires an exclusively provisioned empty directory and checks the complete bootstrap peak before changing names. Recovery holds the stable lock while `inventory_step()` establishes accounting; append and preparation are unavailable until that census is ready. Cleanup during startup invalidates and restarts the census.

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
