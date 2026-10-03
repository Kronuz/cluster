#pragma once

#include "io.h"
#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace kronuz::journal {

namespace detail {
[[noreturn]] inline void system_failure(const char* operation) {
	throw std::system_error(errno, std::generic_category(), operation);
}
inline void validate_name(std::string_view name) {
	if (name.empty() || name.size() > 128 || name == "." || name == ".." ||
		name.find('/') != name.npos || name.find('\0') != name.npos) {
		throw std::invalid_argument("invalid journal filename");
	}
}
inline void validate_file(int fd) {
	struct stat status{};
	if (::fstat(fd, &status)) { system_failure("stat journal file"); }
	if (!S_ISREG(status.st_mode) || status.st_uid != ::geteuid() || status.st_nlink != 1 || (status.st_mode & 0022)) {
		throw std::runtime_error("journal file requires exclusive ownership and a singly linked regular inode");
	}
}
inline void file_sync(int fd) {
	int result;
#ifdef __APPLE__
	do { result = ::fcntl(fd, F_FULLFSYNC); } while (result < 0 && errno == EINTR);
#else
	do { result = ::fsync(fd); } while (result < 0 && errno == EINTR);
#endif
	if (result < 0) { system_failure("durably sync journal file"); }
}
inline off_t checked_offset(std::uint64_t offset) {
	if (offset > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
		throw std::length_error("journal offset exceeds platform range");
	}
	return static_cast<off_t>(offset);
}
class PosixFile final : public File {
public:
	explicit PosixFile(int fd) : fd_(fd) {}
	~PosixFile() override { ::close(fd_); }
	std::uint64_t size() override {
		struct stat status{};
		if (::fstat(fd_, &status)) { system_failure("stat journal size"); }
		if (status.st_size < 0) { throw std::runtime_error("negative journal size"); }
		return static_cast<std::uint64_t>(status.st_size);
	}
	std::size_t read_at(std::uint64_t offset, std::span<char> bytes) override {
		ssize_t count;
		auto length = std::min(bytes.size(), static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
		do { count = ::pread(fd_, bytes.data(), length, checked_offset(offset)); } while (count < 0 && errno == EINTR);
		if (count < 0) { system_failure("read journal file"); }
		return static_cast<std::size_t>(count);
	}
	std::size_t write_at(std::uint64_t offset, std::string_view bytes) override {
		ssize_t count;
		auto length = std::min(bytes.size(), static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
		do { count = ::pwrite(fd_, bytes.data(), length, checked_offset(offset)); } while (count < 0 && errno == EINTR);
		if (count < 0) { system_failure("write journal file"); }
		return static_cast<std::size_t>(count);
	}
	void truncate(std::uint64_t size) override {
		int result;
		do { result = ::ftruncate(fd_, checked_offset(size)); } while (result < 0 && errno == EINTR);
		if (result < 0) { system_failure("truncate journal tail"); }
	}
	void sync() override { file_sync(fd_); }
private:
	int fd_;
};
} // namespace detail

class PosixIO final : public IO {
public:
	// The application establishes and durably publishes this directory first.
	// Trusted ancestors are a caller precondition; final-component symlinks and
	// directories writable by other users are rejected here.
	explicit PosixIO(const std::filesystem::path& directory) {
		directory_ = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
		if (directory_ < 0) { detail::system_failure("open journal directory"); }
		try {
			struct stat status{};
			if (::fstat(directory_, &status)) { detail::system_failure("stat journal directory"); }
			if (!S_ISDIR(status.st_mode) || status.st_uid != ::geteuid() || (status.st_mode & 0022)) {
				throw std::runtime_error("journal directory requires exclusive ownership");
			}
			device_ = status.st_dev;
		} catch (...) { ::close(directory_); directory_ = -1; throw; }
	}
	~PosixIO() override { ::close(directory_); }
	PosixIO(const PosixIO&) = delete;
	PosixIO& operator=(const PosixIO&) = delete;

	std::unique_ptr<OwnerLock> acquire_owner(bool create) override {
		if (owner_fd_ >= 0) { throw std::logic_error("journal directory already owned"); }
		int fd = open_file("owner.lock", create);
		try {
			if (::flock(fd, LOCK_EX | LOCK_NB)) { detail::system_failure("lock journal owner"); }
			if (create) { detail::file_sync(fd); }
			auto owner = std::make_unique<Lock>(*this, fd);
			owner_fd_ = fd;
			return owner;
		} catch (...) { ::close(fd); throw; }
	}
	std::unique_ptr<File> open_existing(std::string_view name) override { return file(name, false); }
	std::unique_ptr<File> create_exclusive(std::string_view name) override { return file(name, true); }
	void replace(std::string_view source, std::string_view destination) override {
		detail::validate_name(source); detail::validate_name(destination);
		std::string from(source), to(destination);
		if (::renameat(directory_, from.c_str(), directory_, to.c_str())) { detail::system_failure("replace journal manifest"); }
	}
	void remove(std::string_view name) override {
		detail::validate_name(name);
		std::string component(name);
		if (::unlinkat(directory_, component.c_str(), 0)) { detail::system_failure("remove journal file"); }
	}
	void sync_directory() override {
		int result;
		do { result = ::fsync(directory_); } while (result < 0 && errno == EINTR);
		if (result < 0) { detail::system_failure("sync journal directory"); }
#ifdef __APPLE__
		// Directory fsync establishes namespace ordering; fullsync then asks the
		// storage device to flush its cache. Unsupported barriers fail closed.
		if (owner_fd_ < 0) { throw std::logic_error("directory barrier requires owner lock"); }
		detail::file_sync(owner_fd_);
#endif
	}

private:
	class Lock final : public OwnerLock {
	public:
		Lock(PosixIO& io, int fd) : io_(io), fd_(fd) {}
		~Lock() override { io_.owner_fd_ = -1; ::close(fd_); }
	private:
		PosixIO& io_;
		int fd_;
	};
	int open_file(std::string_view name, bool create) {
		detail::validate_name(name);
		std::string component(name);
		int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
		if (create) { flags |= O_CREAT | O_EXCL; }
		int fd;
		do { fd = ::openat(directory_, component.c_str(), flags, 0600); } while (fd < 0 && errno == EINTR);
		if (fd < 0) { detail::system_failure(create ? "create journal file" : "open existing journal file"); }
		try {
			detail::validate_file(fd);
			struct stat status{};
			if (::fstat(fd, &status)) { detail::system_failure("stat journal device"); }
			if (status.st_dev != device_) { throw std::runtime_error("journal file must share its directory's device"); }
		}
		catch (...) { ::close(fd); throw; }
		return fd;
	}
	std::unique_ptr<File> file(std::string_view name, bool create) {
		int fd = open_file(name, create);
		try { return std::make_unique<detail::PosixFile>(fd); }
		catch (...) { ::close(fd); throw; }
	}
	int directory_ = -1, owner_fd_ = -1;
	dev_t device_{};
};

} // namespace kronuz::journal
