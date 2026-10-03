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

Generation 1 remains readable until the first explicit checkpoint publication. `prepare_artifact()` creates an exclusive immutable artifact; supply chunks of at most 64 KiB, then call `finish()` to seal its length and checksum and synchronize both contents and filename. The default cumulative artifact bound is 512 MiB. One builder and at most nine prepared leases are admitted; copied prepared handles share one lease. Handles retain the stable owner lock after the journal closes. Abandoned preparation leaves an unreferenced file.

`publish_checkpoint(bundle, dependencies, covered_sequence)` requires prepared handles from the same owner session, no duplicate identities, at most eight dependencies, and the exact current batch sequence. The bundle and dependencies are opaque to this layer. Consensus must separately bind the application snapshot boundary, retained suffix, configuration, and hard state inside its bundle.

```text
Use sealed capabilities → create/sync exclusive new journal generation
→ sync directory → write/sync replacement manifest → rename → sync directory
```

The version 2 manifest binds the bundle and every required dependency by identity, length, and checksum. Physical generation names are unique across interrupted retries. Publication preserves the batch sequence; the next append uses `covered_sequence + 1`. Admission errors leave the journal usable, while uncertain I/O fences its owner session. Prepared handles prove successful immutable sealing or pinning from verified recovery. Optional `verify_artifact(handle)` reads back the complete payload before freezing a consensus cutover and fences the owner on failure. Publication checks capabilities and metadata without rereading payloads. Recovery always verifies all referenced bytes.

Reopen with `recover(replay, restore)`. The journal verifies the bundle and all dependencies before passing their bounded readers to `restore`, then replays the suffix. Both callbacks must construct unpublished state until recovery returns successfully. A missing or corrupt referenced artifact fails closed; recovery never falls back to an older generation. Older readers reject version 2 metadata instead of reinitializing it. `pin_artifact()` can retain a currently referenced artifact for a later publication without rewriting it.

Publication does not delete old generations or artifacts. Reclamation, orphan cleanup, consensus checkpoint integration, and snapshot installation remain separate work. The preparation count bounds live handles, not accumulated disk usage.

## POSIX backend

`PosixIO` opens an existing directory and resolves validated single-component filenames relative to its descriptor. It rejects final-component symlinks, special files, shared regular-file inodes, files on another device, and ownership or permissions that permit another user to modify the store. Trusted ancestor directories remain a caller precondition. Ownership is exclusive across cooperating processes through a stable `owner.lock`, which is never replaced or removed by manifest publication.

Linux and FreeBSD use file and directory `fsync`. On macOS, files use `F_FULLFSYNC`; namespace barriers use directory `fsync` followed by `F_FULLFSYNC` on the same-device stable lock file. [Apple's XNU manual](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2) documents persistence of prior device `fsync` operations after full synchronization returns. Unsupported operations fail instead of downgrading the contract.

The durability model assumes a local filesystem that honors atomic same-directory replacement and successful synchronization, plus storage hardware that honors its flush contract. Deterministic fault tests exercise the protocol; they do not qualify a filesystem, mount option, drive, or power-failure behavior. Internally consistent rollback of an entire store requires an external witness to detect. Restoration must use an explicit new epoch at the application layer.

## Scope and tests

Both formats support append and incremental recovery; version 2 adds immutable checkpoint publication. Reclamation, automatic orphan cleanup, asynchronous I/O, and consensus integration remain separate milestones. Interrupted publication may leave temporary manifests; they cannot become authoritative without the manifest replacement. Disk growth is therefore not yet bounded for a continuously running authority.

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
