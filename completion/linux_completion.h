#pragma once
#if !defined(__linux__)
#error "Linux completion IO requires Linux"
#endif
#include "completion.h"
#include "posix.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <linux/io_uring.h>
#include <semaphore>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <thread>

namespace kronuz::journal {
enum class LinuxCompletionPolicy { Automatic, RequireNative, Fallback };

// Optional kernel-call seam for deterministic ownership fault schedules.
// Production uses the system calls directly. Hooks must obey syscall semantics.
struct LinuxCompletionHooks {
	int (*setup)(unsigned, io_uring_params *) = nullptr;
	int (*enter)(int) = nullptr;
	void (*inspect)(io_uring_cqe &) = nullptr;
};

// One original primitive and one completion slot. Only the owner submits/polls;
// the fallback worker performs the existing validated IO without mutating state.
class LinuxCompletionQueue {
  public:
	explicit LinuxCompletionQueue(LinuxCompletionPolicy policy = LinuxCompletionPolicy::Automatic,
								  LinuxCompletionHooks hooks = {})
		: hooks_(hooks) {
		wake_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
		if (wake_ < 0)
			detail::system_failure("create IO eventfd");
		try {
			if (policy != LinuxCompletionPolicy::Fallback) {
				try {
					setup();
				} catch (const std::system_error &error) {
					setup_error_ = error.code().value();
					retire_ring();
					if (policy == LinuxCompletionPolicy::RequireNative)
						throw;
				}
			}
			worker_ = std::thread([this] { fallback_loop(); });
		} catch (...) {
			retire_ring();
			::close(wake_);
			throw;
		}
	}
	LinuxCompletionQueue(const LinuxCompletionQueue &) = delete;
	LinuxCompletionQueue &operator=(const LinuxCompletionQueue &) = delete;
	~LinuxCompletionQueue() {
		// Accepted resources cannot be retired while execution remains uncertain.
		if (pending_)
			std::terminate();
		stop_ = true;
		work_.release();
		worker_.join();
		retire_ring();
		::close(wake_);
	}
	int notification_descriptor() const noexcept { return wake_; }
	bool needs_native_polling() const noexcept { return false; }
	bool native_available() const noexcept { return ring_ >= 0; }
	int setup_error() const noexcept { return setup_error_; }
	bool busy() const noexcept { return static_cast<bool>(pending_); }
	CompletionStats stats() const noexcept {
		auto result = stats_;
		result.notification_errors += wake_errors_.load(std::memory_order_relaxed);
		return result;
	}
	bool submit(const std::shared_ptr<IOOperation> &operation) {
		if (pending_)
			return false;
		if (!operation || operation->done() || operation->in_flight())
			throw std::invalid_argument("invalid IO operation submission");
		auto request = operation->request();
		const auto kind = static_cast<std::size_t>(request.kind);
		if (kind >= stats_.native_by_primitive.size())
			throw std::invalid_argument("invalid IO primitive");
		io_uring_sqe entry{};
		// All checks and owned basename allocations precede publication.
		bool native = prepare(entry, operation->io(), request);
		if (native && identifier_ == UINT64_MAX)
			throw std::overflow_error("completion identifier exhausted");
		if (native && load(sq_tail_) - load(sq_head_) >= parameters_.sq_entries)
			throw std::logic_error("reserved submission ring occupied");
		operation->submitted();
		pending_ = operation;
		request_ = std::move(request);
		stats_.maximum_outstanding = 1;
		if (!native) {
			++stats_.fallback_submitted;
			++stats_.fallback_by_primitive.at(kind);
			work_.release();
			return true;
		}
		entry.user_data = ++identifier_;
		entry.flags = IOSQE_ASYNC;
		const auto tail = load(sq_tail_);
		const auto index = tail & *sq_mask_;
		sqes_[index] = entry;
		sq_array_[index] = index;
		native_pending_ = true;
		++stats_.native_submitted;
		++stats_.native_by_primitive.at(kind);
		store(sq_tail_, tail + 1); // Kernel may now own the original request.
		kick();
		return true;
	}
	std::optional<OwnedCompletion> poll() {
		std::uint64_t notification;
		// A single bounded drain is enough: eventfd coalesces notifications.
		if (::read(wake_, &notification, sizeof(notification)) < 0 && errno != EAGAIN && errno != EINTR)
			++stats_.notification_errors;
		if (!pending_)
			return std::nullopt;
		if (native_pending_) {
			kick(); // Retry the same published entry, never submit a replacement.
			const auto head = load(cq_head_);
			if (head == load(cq_tail_))
				return std::nullopt;
			if (load(cq_tail_) - head != 1)
				throw std::logic_error("unexpected IO completion count");
			auto completed = cqes_[head & *cq_mask_];
			if (hooks_.inspect)
				hooks_.inspect(completed);
			if (completed.user_data != identifier_ || completed.flags)
				throw std::logic_error("unexpected IO completion ownership");
			store(cq_head_, head + 1);
			native_pending_ = false;
			++stats_.native_completed;
			result_ = {};
			result_.token = request_.token;
			if (completed.res < 0 && !(request_.kind == PrimitiveKind::Remove && completed.res == -ENOENT))
				result_ = error_completion(-completed.res);
			else if (completed.res > 0)
				result_.count = static_cast<std::size_t>(completed.res);
		} else {
			if (!ready_.load(std::memory_order_acquire))
				return std::nullopt;
			++stats_.fallback_completed;
		}
		if (request_.kind == PrimitiveKind::Write)
			stats_.file_write_bytes += result_.count;
		if (request_.kind == PrimitiveKind::Read)
			stats_.file_read_bytes += result_.count;
		OwnedCompletion result{std::move(pending_), std::move(result_)};
		request_ = {};
		source_[0] = '\0';
		destination_[0] = '\0';
		ready_.store(false, std::memory_order_relaxed);
		return result;
	}

  private:
	static unsigned load(unsigned *value) noexcept {
		return std::atomic_ref<unsigned>(*value).load(std::memory_order_acquire);
	}
	static void store(unsigned *value, unsigned next) noexcept {
		std::atomic_ref<unsigned>(*value).store(next, std::memory_order_release);
	}
	static std::size_t extent(std::size_t offset, std::size_t count, std::size_t width) {
		if (count > (SIZE_MAX - offset) / width)
			throw std::overflow_error("IO ring mapping size");
		return offset + count * width;
	}
	template <class T>
	static T *field(void *mapping, std::size_t size, std::size_t offset, std::size_t count = 1) {
		if (extent(offset, count, sizeof(T)) > size || offset % alignof(T))
			throw std::runtime_error("invalid IO ring mapping");
		return reinterpret_cast<T *>(static_cast<char *>(mapping) + offset);
	}
	void *map(std::size_t size, std::uint64_t offset) {
		auto result = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, ring_, offset);
		if (result == MAP_FAILED)
			detail::system_failure("map IO ring");
		return result;
	}
	void setup() {
		ring_ = hooks_.setup ? hooks_.setup(2, &parameters_)
							 : static_cast<int>(::syscall(__NR_io_uring_setup, 2, &parameters_));
		if (ring_ < 0)
			detail::system_failure("create IO ring");
		if (::fcntl(ring_, F_SETFD, FD_CLOEXEC) < 0)
			detail::system_failure("protect IO ring");
		if (!parameters_.sq_entries || !parameters_.cq_entries || parameters_.sq_entries > 4096 ||
			parameters_.cq_entries > 8192)
			throw std::runtime_error("invalid IO ring capacity");
		sq_size_ = extent(parameters_.sq_off.array, parameters_.sq_entries, sizeof(unsigned));
		cq_size_ = extent(parameters_.cq_off.cqes, parameters_.cq_entries, sizeof(io_uring_cqe));
		if (parameters_.features & IORING_FEAT_SINGLE_MMAP) {
			sq_size_ = std::max(sq_size_, cq_size_);
			cq_size_ = sq_size_;
			sq_mapping_ = map(sq_size_, IORING_OFF_SQ_RING);
			cq_mapping_ = sq_mapping_;
		} else {
			sq_mapping_ = map(sq_size_, IORING_OFF_SQ_RING);
			cq_mapping_ = map(cq_size_, IORING_OFF_CQ_RING);
		}
		sqe_size_ = extent(0, parameters_.sq_entries, sizeof(io_uring_sqe));
		sqe_mapping_ = map(sqe_size_, IORING_OFF_SQES);
		sqes_ = static_cast<io_uring_sqe *>(sqe_mapping_);
		sq_head_ = field<unsigned>(sq_mapping_, sq_size_, parameters_.sq_off.head);
		sq_tail_ = field<unsigned>(sq_mapping_, sq_size_, parameters_.sq_off.tail);
		sq_mask_ = field<unsigned>(sq_mapping_, sq_size_, parameters_.sq_off.ring_mask);
		sq_array_ = field<unsigned>(sq_mapping_, sq_size_, parameters_.sq_off.array, parameters_.sq_entries);
		cq_head_ = field<unsigned>(cq_mapping_, cq_size_, parameters_.cq_off.head);
		cq_tail_ = field<unsigned>(cq_mapping_, cq_size_, parameters_.cq_off.tail);
		cq_mask_ = field<unsigned>(cq_mapping_, cq_size_, parameters_.cq_off.ring_mask);
		cqes_ = field<io_uring_cqe>(cq_mapping_, cq_size_, parameters_.cq_off.cqes, parameters_.cq_entries);
		if (*sq_mask_ != parameters_.sq_entries - 1 || *cq_mask_ != parameters_.cq_entries - 1)
			throw std::runtime_error("invalid IO ring indices");
		std::array<std::uint64_t, (sizeof(io_uring_probe) + 256 * sizeof(io_uring_probe_op) + 7) / 8>
			probe_storage{};
		auto probe = reinterpret_cast<io_uring_probe *>(probe_storage.data());
		if (::syscall(__NR_io_uring_register, ring_, IORING_REGISTER_PROBE, probe, 256) < 0)
			detail::system_failure("probe IO operations");
		for (unsigned i = 0; i < probe->ops_len; ++i)
			if (probe->ops[i].flags & IO_URING_OP_SUPPORTED)
				supported_[probe->ops[i].op] = true;
		if (!supported_[IORING_OP_READ] || !supported_[IORING_OP_WRITE] || !supported_[IORING_OP_FSYNC])
			throw std::system_error(ENOTSUP, std::generic_category(), "required native IO operations");
		if (::syscall(__NR_io_uring_register, ring_, IORING_REGISTER_EVENTFD, &wake_, 1) < 0)
			detail::system_failure("register IO eventfd");
	}
	bool prepare(io_uring_sqe &entry, IO &io, const MutationRequest &request) {
		if (ring_ < 0)
			return false;
		auto file = std::dynamic_pointer_cast<detail::PosixFile>(request.file);
		auto directory = dynamic_cast<PosixIO *>(&io);
		switch (request.kind) {
		case PrimitiveKind::Read:
		case PrimitiveKind::Write: {
			if (!file)
				return false;
			const auto length =
				request.kind == PrimitiveKind::Read ? request.destination_bytes.size() : request.bytes.size();
			if (length > UINT_MAX)
				throw std::invalid_argument("native IO length");
			entry.opcode = request.kind == PrimitiveKind::Read ? IORING_OP_READ : IORING_OP_WRITE;
			entry.fd = file->native_handle();
			entry.off = detail::checked_offset(request.offset);
			entry.addr = reinterpret_cast<std::uintptr_t>(request.kind == PrimitiveKind::Read
															  ? request.destination_bytes.data()
															  : request.bytes.data());
			entry.len = static_cast<unsigned>(length);
			return true;
		}
		case PrimitiveKind::Sync:
			if (!file)
				return false;
			entry.opcode = IORING_OP_FSYNC;
			entry.fd = file->native_handle();
			return true;
		case PrimitiveKind::DirectorySync:
			if (!directory)
				return false;
			entry.opcode = IORING_OP_FSYNC;
			entry.fd = directory->native_directory_handle();
			return true;
		case PrimitiveKind::Replace:
			if (!directory || !supported_[IORING_OP_RENAMEAT])
				return false;
			detail::validate_name(request.source);
			detail::validate_name(request.destination);
			copy_name(source_, request.source);
			copy_name(destination_, request.destination);
			entry.opcode = IORING_OP_RENAMEAT;
			entry.fd = directory->native_directory_handle();
			entry.addr = reinterpret_cast<std::uintptr_t>(source_.data());
			entry.len = static_cast<unsigned>(directory->native_directory_handle());
			entry.addr2 = reinterpret_cast<std::uintptr_t>(destination_.data());
			return true;
		case PrimitiveKind::Remove:
			if (!directory || !supported_[IORING_OP_UNLINKAT])
				return false;
			detail::validate_name(request.source);
			copy_name(source_, request.source);
			entry.opcode = IORING_OP_UNLINKAT;
			entry.fd = directory->native_directory_handle();
			entry.addr = reinterpret_cast<std::uintptr_t>(source_.data());
			return true;
		default:
			return false; // Existing composite validation stays intact.
		}
	}
	static void copy_name(std::array<char, 129> &target, std::string_view name) noexcept {
		// Both namespace primitives validate the 128-byte bound first.
		std::copy(name.begin(), name.end(), target.begin());
		target[name.size()] = '\0';
	}
	void kick() {
		if (!native_pending_ || load(sq_head_) == load(sq_tail_))
			return;
		if ((hooks_.enter ? hooks_.enter(ring_)
						  : ::syscall(__NR_io_uring_enter, ring_, 1, 0, 0, nullptr, 0)) >= 0)
			return;
		const auto error = errno;
		++stats_.ring_errors;
		// The SQE is already published. Even on error the kernel may have
		// consumed it; transient failures retry only that same ring entry.
		if (error != EINTR && error != EAGAIN && error != EBUSY)
			throw std::system_error(error, std::generic_category(), "accepted IO ring submission");
	}
	MutationCompletion error_completion(int error) const noexcept {
		MutationCompletion result;
		result.token = request_.token;
		try {
			throw std::system_error(error, std::generic_category(), "completion IO");
		} catch (...) {
			result.error = std::current_exception();
		}
		return result;
	}
	void fallback_loop() noexcept {
		for (;;) {
			work_.acquire();
			if (stop_)
				return;
			result_ = detail::execute_primitive(pending_->io(), request_);
			ready_.store(true, std::memory_order_release);
			const std::uint64_t one = 1;
			if (::write(wake_, &one, sizeof(one)) < 0)
				wake_errors_.fetch_add(1, std::memory_order_relaxed);
		}
	}
	void retire_ring() noexcept {
		if (sqe_mapping_)
			::munmap(sqe_mapping_, sqe_size_);
		if (cq_mapping_ && cq_mapping_ != sq_mapping_)
			::munmap(cq_mapping_, cq_size_);
		if (sq_mapping_)
			::munmap(sq_mapping_, sq_size_);
		if (ring_ >= 0)
			::close(ring_);
		sqe_mapping_ = cq_mapping_ = sq_mapping_ = nullptr;
		ring_ = -1;
	}
	LinuxCompletionHooks hooks_;
	int ring_ = -1, wake_ = -1, setup_error_ = 0;
	io_uring_params parameters_{};
	void *sq_mapping_ = nullptr, *cq_mapping_ = nullptr, *sqe_mapping_ = nullptr;
	std::size_t sq_size_ = 0, cq_size_ = 0, sqe_size_ = 0;
	unsigned *sq_head_ = nullptr, *sq_tail_ = nullptr, *sq_mask_ = nullptr, *sq_array_ = nullptr;
	unsigned *cq_head_ = nullptr, *cq_tail_ = nullptr, *cq_mask_ = nullptr;
	io_uring_sqe *sqes_ = nullptr;
	io_uring_cqe *cqes_ = nullptr;
	std::array<bool, 256> supported_{};
	std::uint64_t identifier_ = 0;
	std::thread worker_;
	std::binary_semaphore work_{0};
	bool stop_ = false, native_pending_ = false;
	std::atomic<bool> ready_{false};
	std::atomic<std::uint64_t> wake_errors_{0};
	std::shared_ptr<IOOperation> pending_;
	MutationRequest request_;
	MutationCompletion result_;
	std::array<char, 129> source_, destination_;
	CompletionStats stats_;
};
} // namespace kronuz::journal
