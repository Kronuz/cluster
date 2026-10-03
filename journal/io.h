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
};

class IO {
public:
	virtual ~IO() = default;
	// Creation is exclusive. Recovery opens the existing stable lock; neither
	// manifest replacement nor fencing releases or replaces this lock.
	virtual std::unique_ptr<OwnerLock> acquire_owner(bool create) = 0;
	virtual std::unique_ptr<File> open_existing(std::string_view name) = 0;
	virtual std::unique_ptr<File> create_exclusive(std::string_view name) = 0;
	virtual void replace(std::string_view source, std::string_view destination) = 0;
	// Missing files are an idempotent success.
	virtual void remove(std::string_view name) = 0;
	virtual std::unique_ptr<DirectoryCursor> scan_directory() { throw std::logic_error("directory scanning unsupported"); }
	// Null means absent or unsafe/nonregular: preserve it. Genuine I/O
	// failures throw. A backend supporting reclamation overrides both hooks.
	virtual std::unique_ptr<File> open_reclaim_candidate(std::string_view name) { return open_existing(name); }
	virtual void sync_directory() = 0;
};

} // namespace kronuz::journal
