#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <string>
#include <optional>
#include <stdexcept>

namespace kronuz::journal {

enum class EntryKind { Regular, Directory, Symlink, Other };
struct EntryFootprint {
	EntryKind kind = EntryKind::Other;
	std::uint64_t logical_bytes = 0;
	std::optional<std::uint64_t> allocated_bytes;
	bool operator==(const EntryFootprint&) const = default;
};
struct EntryInspection {
	EntryFootprint footprint;
	std::uint64_t device = 0, inode = 0, owner = 0, links = 0;
	std::uint32_t permissions = 0;
	bool operator==(const EntryInspection&) const = default;
};
enum class LeaseMode { Shared, Exclusive };
struct ManagedCapabilities {
	bool caller_scan = false, entry_inspection = false, file_inspection = false;
	bool nonblocking_leases = false, file_close = false;
	bool cursor_close = false, reusable_controls = false;
	// Separate mutation capability; complete() retains its collector-profile meaning.
	bool reusable_create = false;
	bool complete() const noexcept {
		return caller_scan && entry_inspection && file_inspection && nonblocking_leases && file_close && cursor_close && reusable_controls;
	}
};

// Implementations own a trusted, already-created directory. Names are single
// components; ownership and file-type checks belong to the implementation.
class File {
public:
	virtual ~File() = default;
	virtual std::uint64_t size() = 0;
	virtual std::size_t read_at(std::uint64_t offset, std::span<char> bytes) = 0;
	virtual std::size_t write_at(std::uint64_t offset, std::string_view bytes) = 0;
	virtual void truncate(std::uint64_t size) = 0;
	virtual void sync() = 0;
	virtual EntryInspection inspect() { throw std::logic_error("held-file inspection unsupported"); }
	// Fresh handles only: converting a shared lock may release its protection.
	// False means busy, never a blocking wait. Errors remain exceptional.
	virtual bool try_lease(LeaseMode) { throw std::logic_error("nonblocking file leases unsupported"); }
	// Called by a retained worker-side original after preceding work settles.
	// Supporting backends consume the native handle even on error; never retry.
	virtual void close() { throw std::logic_error("explicit file close unsupported"); }
};

class OwnerLock {
public:
	virtual ~OwnerLock() = default;
};

class DirectoryCursor {
public:
	virtual ~DirectoryCursor() = default;
	// One bounded basename, or end of pass. Concurrent namespace mutations
	// may cause repeats/omissions; callers must recheck roots before removal.
	virtual std::optional<std::string> next() = 0;
	// Managed callers supply retained scratch storage. A present result is the
	// number of copied name bytes (no NUL); null means end of pass. The default
	// deliberately does not call the allocating legacy interface.
	virtual std::optional<std::size_t> next_into(std::span<char>) {
		throw std::logic_error("caller-buffer directory scanning unsupported");
	}
	virtual void close() { throw std::logic_error("explicit cursor close unsupported"); }
};

class IO {
public:
	virtual ~IO() = default;
	virtual ManagedCapabilities managed_capabilities() const noexcept { return {}; }
	// Creation is exclusive. Recovery opens the existing stable lock; neither
	// manifest replacement nor fencing releases or replaces this lock.
	// Successful creation adds exactly one zero-length owner.lock and no
	// other entries; format-layer bootstrap accounting relies on this.
	virtual std::unique_ptr<OwnerLock> acquire_owner(bool create) = 0;
	// Admit reusable closed controls during startup. Into hooks use this same
	// backend, reject live/foreign destinations before IO, and allocate no control.
	virtual std::unique_ptr<File> make_closed_file() { throw std::logic_error("reusable file controls unsupported"); }
	// No control allocation. With a valid closed destination, failure consumes
	// temporary descriptors but may leave an exclusively created name behind.
	virtual void create_exclusive_into(std::string_view, File&) { throw std::logic_error("reusable creation unsupported"); }
	virtual std::unique_ptr<DirectoryCursor> make_closed_cursor() { throw std::logic_error("reusable cursor controls unsupported"); }
	virtual void open_existing_into(std::string_view, File&) { throw std::logic_error("reusable file open unsupported"); }
	virtual bool open_reclaim_candidate_into(std::string_view, File&) { throw std::logic_error("reusable candidate open unsupported"); }
	virtual void scan_directory_into(DirectoryCursor&) { throw std::logic_error("reusable cursor scan unsupported"); }
	virtual std::unique_ptr<File> open_existing(std::string_view name) = 0;
	virtual std::unique_ptr<File> create_exclusive(std::string_view name) = 0;
	virtual void replace(std::string_view source, std::string_view destination) = 0;
	// Missing files are an idempotent success.
	virtual void remove(std::string_view name) = 0;
	virtual std::unique_ptr<DirectoryCursor> scan_directory() { throw std::logic_error("directory scanning unsupported"); }
	// Inspect the directory entry itself without following symlinks or opening
	// its target. Missing names return null; genuine errors throw. A quiescent
	// inventory requires exact-once enumeration with no concurrent mutations.
	virtual std::optional<EntryFootprint> entry_footprint(std::string_view) { throw std::logic_error("entry accounting unsupported"); }
	virtual std::optional<EntryInspection> inspect_entry(std::string_view) { throw std::logic_error("entry inspection unsupported"); }
	// Null means absent or unsafe/nonregular: preserve it. Genuine I/O
	// failures throw. A backend supporting reclamation overrides both hooks.
	virtual std::unique_ptr<File> open_reclaim_candidate(std::string_view name) { return open_existing(name); }
	virtual void sync_directory() = 0;
};

} // namespace kronuz::journal
