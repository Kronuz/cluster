#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

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

class IO {
public:
	virtual ~IO() = default;
	// Creation is exclusive. Recovery opens the existing stable lock; neither
	// manifest replacement nor fencing releases or replaces this lock.
	virtual std::unique_ptr<OwnerLock> acquire_owner(bool create) = 0;
	virtual std::unique_ptr<File> open_existing(std::string_view name) = 0;
	virtual std::unique_ptr<File> create_exclusive(std::string_view name) = 0;
	virtual void replace(std::string_view source, std::string_view destination) = 0;
	virtual void remove(std::string_view name) = 0;
	virtual void sync_directory() = 0;
};

} // namespace kronuz::journal
