#pragma once

#include "io.h"
#include "allocation.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <dirent.h>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace kronuz::journal {

namespace detail {
// Concrete POSIX controls keep default unique_ptr deletion. Their allocation
// header retains the PMR owner until after the most-derived destructor runs.
template <class Base> class ManagedPosixControl final : public Base {
	using Context = kronuz::io::completion::AllocationContext;
	struct Header { Context context; };
	static constexpr std::size_t alignment() { return std::max(alignof(Header), alignof(ManagedPosixControl)); }
	static constexpr std::size_t prefix() { return (sizeof(Header) + alignment() - 1) / alignment() * alignment(); }
public:
	using Base::Base;
	static void* operator new(std::size_t bytes, const Context& context) {
		if (bytes != sizeof(ManagedPosixControl)) { throw std::bad_alloc(); }
		auto memory = context.resource()->allocate(prefix() + bytes, alignment());
		std::construct_at(static_cast<Header*>(memory), Header{context});
		return static_cast<char*>(memory) + prefix();
	}
	static void operator delete(void* pointer) noexcept {
		auto header = reinterpret_cast<Header*>(static_cast<char*>(pointer) - prefix());
		auto context = std::move(header->context);
		std::destroy_at(header);
		context.resource()->deallocate(header, prefix() + sizeof(ManagedPosixControl), alignment());
	}
	static void operator delete(void* pointer, const Context&) noexcept { operator delete(pointer); }
};
[[noreturn]] inline void system_failure(const char* operation) {
	throw std::system_error(errno, std::generic_category(), operation);
}
inline void validate_name(std::string_view name, std::size_t maximum = 128) {
	if (name.empty() || name.size() > maximum || name == "." || name == ".." ||
		name.find('/') != name.npos || name.find('\0') != name.npos) {
		throw std::invalid_argument("invalid journal filename");
	}
}
class PosixName {
public:
	explicit PosixName(std::string_view name, std::size_t maximum = 128) {
		validate_name(name, std::min<std::size_t>(maximum, 255));
		std::copy(name.begin(), name.end(), bytes_.begin());
		bytes_[name.size()] = '\0';
	}
	const char* c_str() const noexcept { return bytes_.data(); }
private:
	std::array<char, 256> bytes_;
};
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
class PosixCursor : public DirectoryCursor {
public:
	explicit PosixCursor(DIR* directory) : directory_(directory) {}
	~PosixCursor() override { if (directory_) { ::closedir(directory_); } }
	void install(DIR* directory) noexcept { directory_ = directory; }
	std::optional<std::string> next() override {
		for (;;) {
			errno = 0; auto entry = ::readdir(directory_);
			if (!entry) { if (errno) { system_failure("scan journal directory"); } return std::nullopt; }
			std::string_view name(entry->d_name);
			if (name != "." && name != "..") { return std::string(name); }
		}
	}
private:
	DIR* directory_;
};
class PosixFile : public File {
public:
	explicit PosixFile(int fd) : fd_(fd) {}
	~PosixFile() override { if (fd_ >= 0) { ::close(fd_); } }
	void install(int fd) noexcept { fd_ = fd; }
	// Trusted completion backend only. The owning File lease retains this FD.
	int native_handle() const noexcept { return fd_; }
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
	explicit PosixIO(const std::filesystem::path& directory, unsigned artifact_mode = 0600)
		: PosixIO(kronuz::io::completion::AllocationContext{}, directory, artifact_mode) {}
	PosixIO(kronuz::io::completion::AllocationContext context, const std::filesystem::path& directory,
		unsigned artifact_mode = 0600)
		: context_(std::move(context)), artifact_mode_(artifact_mode) {
		if (artifact_mode != 0600 && artifact_mode != 0640 && artifact_mode != 0644) {
			throw std::invalid_argument("unsupported artifact permissions");
		}
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
	// Trusted completion backends retain the IO owner while using this capability.
	int native_directory_handle() const noexcept { return directory_; }
	~PosixIO() override { ::close(directory_); }
	PosixIO(const PosixIO&) = delete;
	PosixIO& operator=(const PosixIO&) = delete;

	std::unique_ptr<OwnerLock> acquire_owner(bool create) override {
		if (owner_fd_ >= 0) { throw std::logic_error("journal directory already owned"); }
		auto owner = control<Lock>(*this, -1);
		int fd = open_file("owner.lock", create);
		try {
			if (::flock(fd, LOCK_EX | LOCK_NB)) { detail::system_failure("lock journal owner"); }
			if (create) { detail::file_sync(fd); }
			owner->install(fd);
			owner_fd_ = fd;
			return owner;
		} catch (...) { ::close(fd); throw; }
	}
	std::unique_ptr<File> open_existing(std::string_view name) override { return file(name, false); }
	std::unique_ptr<File> create_exclusive(std::string_view name) override { return file(name, true); }
	void replace(std::string_view source, std::string_view destination) override {
		detail::PosixName from(source), to(destination);
		if (::renameat(directory_, from.c_str(), directory_, to.c_str())) { detail::system_failure("replace journal manifest"); }
	}
	void remove(std::string_view name) override {
		detail::PosixName component(name);
		if (::unlinkat(directory_, component.c_str(), 0) && errno != ENOENT) { detail::system_failure("remove journal file"); }
	}
	std::unique_ptr<DirectoryCursor> scan_directory() override {
		auto cursor = control<detail::PosixCursor>(nullptr);
		// dup() would share the original directory's file offset. Use an
		// independent open description for every reclamation pass.
		int fd = ::openat(directory_, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
		if (fd < 0) { detail::system_failure("open journal scan cursor"); }
		auto directory = ::fdopendir(fd);
		if (!directory) { auto error = errno; ::close(fd); errno = error; detail::system_failure("create journal scan cursor"); }
		cursor->install(directory);
		return cursor;
	}
	std::unique_ptr<File> open_reclaim_candidate(std::string_view name) override {
		detail::PosixName component(name); struct stat status{};
		if (::fstatat(directory_, component.c_str(), &status, AT_SYMLINK_NOFOLLOW)) {
			if (errno == ENOENT) { return nullptr; } detail::system_failure("inspect reclamation candidate");
		}
		if (!S_ISREG(status.st_mode) || status.st_uid != ::geteuid() || status.st_nlink != 1 || (status.st_mode & 0022) ||
			(status.st_mode & 0600) != 0600 || status.st_dev != device_) { return nullptr; }
		return file(name, false);
	}
	std::optional<EntryFootprint> entry_footprint(std::string_view name) override {
		// Census foreign names too, beyond the journal's own 128-byte bound.
		detail::PosixName component(name, 255); struct stat status{};
		if (::fstatat(directory_, component.c_str(), &status, AT_SYMLINK_NOFOLLOW)) {
			if (errno == ENOENT) { return std::nullopt; } detail::system_failure("inspect storage footprint");
		}
		if (status.st_size < 0 || status.st_blocks < 0 ||
			static_cast<std::uint64_t>(status.st_blocks) > std::numeric_limits<std::uint64_t>::max() / 512) {
			throw std::overflow_error("invalid storage footprint");
		}
		auto kind = S_ISREG(status.st_mode) ? EntryKind::Regular : (S_ISDIR(status.st_mode) ? EntryKind::Directory :
			(S_ISLNK(status.st_mode) ? EntryKind::Symlink : EntryKind::Other));
		return EntryFootprint{kind, static_cast<std::uint64_t>(status.st_size), static_cast<std::uint64_t>(status.st_blocks) * 512};
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
	class Lock : public OwnerLock {
	public:
		Lock(PosixIO& io, int fd) : io_(io), fd_(fd) {}
		~Lock() override { if (fd_ >= 0) { io_.owner_fd_ = -1; ::close(fd_); } }
		void install(int fd) noexcept { fd_ = fd; }
	private:
		PosixIO& io_;
		int fd_;
	};
	int open_file(std::string_view name, bool create) {
		detail::PosixName component(name);
		int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
		if (create) { flags |= O_CREAT | O_EXCL; }
		int fd;
		do { fd = ::openat(directory_, component.c_str(), flags, 0600); } while (fd < 0 && errno == EINTR);
		if (fd < 0) { detail::system_failure(create ? "create journal file" : "open existing journal file"); }
		try {
			// Artifact readers may run under a different UID in a system service.
			// Apply the selected read-only sharing mode before Create can succeed;
			// the stable owner lock always remains private, regardless of umask.
			if (create) {
				auto mode = name == "owner.lock" ? 0600u : artifact_mode_;
				int result;
				do { result = ::fchmod(fd, mode); } while (result < 0 && errno == EINTR);
				if (result < 0) { detail::system_failure("set artifact permissions"); }
			}
			detail::validate_file(fd);
			struct stat status{};
			if (::fstat(fd, &status)) { detail::system_failure("stat journal device"); }
			if (status.st_dev != device_) { throw std::runtime_error("journal file must share its directory's device"); }
		}
		catch (...) { ::close(fd); throw; }
		return fd;
	}
	std::unique_ptr<File> file(std::string_view name, bool create) {
		auto result = control<detail::PosixFile>(-1);
		result->install(open_file(name, create));
		return result;
	}
	template <class T, class... Args> std::unique_ptr<T> control(Args&&... args) {
		if (context_.managed()) {
			return std::unique_ptr<T>(new (context_) detail::ManagedPosixControl<T>(std::forward<Args>(args)...));
		}
		return std::make_unique<T>(std::forward<Args>(args)...);
	}
	kronuz::io::completion::AllocationContext context_;
	int directory_ = -1, owner_fd_ = -1;
	dev_t device_{};
	unsigned artifact_mode_ = 0600;
};

inline std::shared_ptr<PosixIO> make_posix_io(kronuz::io::completion::AllocationContext context,
	const std::filesystem::path& directory, unsigned artifact_mode = 0600) {
	return std::allocate_shared<PosixIO>(kronuz::io::completion::OwnedAllocator<PosixIO>(context),
		context, directory, artifact_mode);
}

} // namespace kronuz::journal
