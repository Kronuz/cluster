# Owned I/O completions

`kronuz::completion` is a protocol-independent, C++20 header-only I/O module. It provides trusted-directory POSIX file operations, owned primitive requests and authentic results, completion statistics, and native Linux and BSD/macOS completion backends. It does not fetch dependencies or include journal formats, consensus, HTTP, or reactor code.

```sh
cmake -S completion -B .scratch/completion -DCMAKE_BUILD_TYPE=Release
cmake --build .scratch/completion -j2
ctest --test-dir .scratch/completion --output-on-failure
```

New consumers include `completion/api.h` and use `kronuz::io::completion`. Existing `journal/` include paths and canonical `kronuz::journal` types remain available through forwarding headers. These aliases preserve type identity and existing source APIs; consumers must rebuild against the matching headers. No journal bytes or legacy cluster wire encodings change.

An operation owns every filename, buffer and file lease referenced by its request. Once a driver accepts a request, it retains that operation until the original primitive completes. Cancellation cannot fabricate a result, release accepted buffers, or refund outstanding admission. The owner consumes the typed result before deciding whether an obsolete consumer still needs a continuation. A failed driver with outstanding accepted work cannot safely pretend that work completed.

`drive_synchronously()` executes the same primitives for synchronous callers and dedicated workers. Event-loop owners use native completion queues or the optional `kronuz::completion_asio` adapter from `completion/asio_completion.h`; the host supplies standalone Asio. Native platform support and fallback routing remain the same as the existing journal backend. Opening the trusted directory and acquiring its stable owner lock are startup operations, not asynchronous job submissions.

The POSIX backend requires a caller-created, exclusively owned directory with trusted ancestors. It rejects unsafe basenames, final-component symlinks, foreign-owned or multiply linked files, and group/world-writable entries. Creation remains exclusive and defaults to mode `0600`; an additive constructor argument allows `0640` or `0644` for shared artifacts. The selected permissions are applied before successful creation completes, independently of umask; `owner.lock` always remains `0600`. File synchronization and directory synchronization are separate primitives: replacing a filename does not by itself establish a durable publication.

The standalone test exercises create, write, file synchronization, replace, directory synchronization and reopening, and verifies that a rejected unsafe path produces an authentic error completion. Existing journal and consensus tests continue to qualify the shared native drivers and their ownership rules.

## Opaque image jobs

`completion/allocation.h` supplies an optional `AllocationContext` and `OwnedAllocator<T>` without application dependencies. Empty contexts use the process-lifetime `new_delete_resource()`; managed contexts retain their resource across container rebinds and weak shared controls. Allocation sizes are checked before calling the resource, and deallocation is nonthrowing. This foundation does not yet account image jobs, POSIX controls or native queue storage.

The standalone resource fixture qualifies overflow, alignment, allocation and constructor failures, container growth, and last-owner release on another thread. All three standalone suites pass on macOS, Linux and FreeBSD and under macOS ASan/UBSan. The resource fixture also passes a separate fully instrumented ThreadSanitizer run.

`completion/image.h` provides `ImagePreparation` and `ImagePublication` over the same operation interface. Preparation retains an immutable input buffer, backend and caller-supplied owner/admission lease; writes use at most 64 KiB per primitive and handle short writes. Only completed file and directory synchronization can produce a `PreparedImage` capability. The caller supplies a unique generation basename and validates application semantics before preparation.

Publication takes a prepared descriptor whose opaque bytes identify the separately prepared image. The application checks its expected base before constructing the job and retains the referenced image through its supplied lease. Descriptor replacement is followed by directory synchronization. Once replacement is submitted, an error leaves an uncertain outcome; the application must preserve both possible roots and fence further publication until recovery determines the selected descriptor. Successful publication consumes the prepared descriptor's selection capability. These jobs do not interpret cursors, choose storage quotas, reclaim files, or provide a transactional rollback.

The same jobs run through `drive_synchronously()` on a worker or through `NativeQueue`. Callers must retain the owner/admission lease and drive accepted originals through settlement even after consumer cancellation. The standalone test verifies held originals, rejection of stale tokens, multi-chunk writes, native preparation, and the barrier before durable publication. This is primitive/job qualification, not the complete application generation lifecycle acceptance.

Preparation also accepts a separate immutable byte view and opaque lifetime owner. This allows charged arenas and buffer slices without copying through an owning `std::string`; the existing string constructor is preserved. The caller must supply the matching view and keep its bytes immutable. The accepted operation retains the owner through its original write and sealing chain, releasing it after the final successful directory barrier. Failed operations retain it until operation disposal. The standalone test drops the initiating facade after a write is accepted by `NativeQueue` and checks both slice contents and owner retention through sealing.

Prepared image state now owns its File uniquely. Request file handles alias that admitted state, so accepting a successful Create completion does not allocate another shared control block. Such a file handle retains the state’s backend and admission lease after the initiating facade disappears. Public request/completion file types and constructors are unchanged; header consumers rebuild together.

The allocation fixture observes one Create-adoption allocation against the previous implementation and zero after this change. It also checks file and admission retention. Standalone completion and Detent publication suites pass on macOS, Linux and FreeBSD and under macOS ASan/UBSan. This allocation count establishes no latency or whole-process memory improvement; nested state/name allocations remain for the optional allocation-context extension.
