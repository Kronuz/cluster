#pragma once

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#error "BSD completion IO requires macOS or FreeBSD"
#endif

#include "operation.h"
#include "completion.h"
#include "posix.h"
#include <aio.h>
#include <atomic>
#include <semaphore>
#include <sys/event.h>
#include <thread>

namespace kronuz::journal {

// One funded primitive and one reserved completion slot. The owning executor
// submits and polls; the fallback worker performs IO only, never state changes.
// macOS uses native AIO reads/writes with bounded owner-side completion polling.
// FreeBSD also uses native aio_fsync and SIGEV_KEVENT notification.
class BsdCompletionQueue {
  public:
	BsdCompletionQueue() {
		queue_ = ::kqueue();
		if (queue_ < 0) {
			detail::system_failure("create IO notification queue");
		}
		try {
			if (::fcntl(queue_, F_SETFD, FD_CLOEXEC) < 0) {
				detail::system_failure("protect IO notification queue");
			}
			struct kevent event{};
			EV_SET(&event, 1, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
			if (::kevent(queue_, &event, 1, nullptr, 0, nullptr) < 0) {
				detail::system_failure("register IO notification");
			}
			worker_ = std::thread([this] { fallback_loop(); });
		} catch (...) {
			::close(queue_);
			throw;
		}
	}
	BsdCompletionQueue(const BsdCompletionQueue &) = delete;
	BsdCompletionQueue &operator=(const BsdCompletionQueue &) = delete;
	~BsdCompletionQueue() {
		// Shutdown must drain accepted work before destroying backend state.
		if (pending_) {
			std::terminate();
		}
		stop_ = true;
		work_.release();
		worker_.join();
		::close(queue_);
	}
	int notification_descriptor() const noexcept { return queue_; }
	bool needs_native_polling() const noexcept {
#ifdef __APPLE__
		return true;
#else
		return false;
#endif
	}
	CompletionStats stats() const noexcept {
		auto result = stats_;
		result.notification_errors += wake_errors_.load(std::memory_order_relaxed);
		return result;
	}
	bool busy() const noexcept { return static_cast<bool>(pending_); }
	bool submit(const std::shared_ptr<IOOperation> &operation) {
		if (pending_) {
			return false;
		} // No IO, no ownership change on rejection.
		if (!operation || operation->done() || operation->in_flight()) {
			throw std::invalid_argument("invalid IO operation submission");
		}
		auto request = operation->request();
		if (!CompletionStats::supported(request.kind)) {
			throw std::invalid_argument("invalid IO primitive");
		}
		// Validate every native argument before marking this primitive submitted.
		auto file = std::dynamic_pointer_cast<detail::PosixFile>(request.file);
		bool native = file && (request.kind == PrimitiveKind::Write || request.kind == PrimitiveKind::Read);
#ifdef __FreeBSD__
		native = native || (file && request.kind == PrimitiveKind::Sync);
#endif
		if (native) {
			control_ = {};
			control_.aio_fildes = file->native_handle();
			control_.aio_offset = detail::checked_offset(request.offset);
			control_.aio_buf = request.kind == PrimitiveKind::Read ? request.destination_bytes.data() : const_cast<char *>(request.bytes.data());
			control_.aio_nbytes = request.kind == PrimitiveKind::Read ? request.destination_bytes.size() : request.bytes.size();
#ifdef __FreeBSD__
			control_.aio_sigevent.sigev_notify = SIGEV_KEVENT;
			control_.aio_sigevent.sigev_notify_kqueue = queue_;
			control_.aio_sigevent.sigev_value.sival_ptr = this;
#else
			// Darwin 24 rejects SIGEV_KEVENT even with a newer SDK. Avoid
			// installing a process-global signal handler to observe completion.
			control_.aio_sigevent.sigev_notify = SIGEV_NONE;
#endif
		}
		operation->submitted();
		pending_ = operation;
		request_ = std::move(request);
		stats_.maximum_outstanding = 1;
		if (native) {
			int result;
			if (request_.kind == PrimitiveKind::Read) { result = ::aio_read(&control_); }
			else if (request_.kind == PrimitiveKind::Write) { result = ::aio_write(&control_); }
			else { result = ::aio_fsync(O_SYNC, &control_); }
			if (result == 0) {
				native_pending_ = true;
				++stats_.native_submitted;
				stats_.submitted(request_.kind, true);
				return true;
			}
			auto error = errno;
			++stats_.native_rejected;
			// A failed submission has not queued the request. Unsupported or
			// unavailable admission can use the already-reserved fallback slot.
			if (error != ENOSYS && error != ENOTSUP && error != EAGAIN) {
				result_ = error_completion(error);
				ready_.store(true, std::memory_order_release);
				return true;
			}
		}
		++stats_.fallback_submitted;
		stats_.submitted(request_.kind, false);
		fallback_pending_ = true;
		work_.release();
		return true;
	}
	std::optional<OwnedCompletion> poll() {
		if (!pending_) {
			return std::nullopt;
		}
		// Consume bounded wakeups. Readiness is only a notification; original
		// AIO status/reaping, or the fallback result slot, supplies completion.
		struct kevent events[2]{};
		timespec immediate{};
		if (::kevent(queue_, nullptr, 0, events, 2, &immediate) < 0 && errno != EINTR) {
			++stats_.notification_errors; // Watchdog polling still reaps original IO.
		}
		if (native_pending_) {
			auto error = ::aio_error(&control_);
			if (error == EINPROGRESS) {
				return std::nullopt;
			}
			if (error < 0) {
				detail::system_failure("inspect accepted AIO request");
			}
			auto count = ::aio_return(&control_); // Reap the original exactly once.
			native_pending_ = false;
			++stats_.native_completed;
			result_ = {};
			result_.token = request_.token;
			if (error || count < 0) {
				result_ = error_completion(error ? error : EIO);
			} else {
				result_.count = static_cast<std::size_t>(count);
			}
		} else if (!ready_.load(std::memory_order_acquire)) {
			return std::nullopt;
		}
		if (fallback_pending_) {
			++stats_.fallback_completed;
			fallback_pending_ = false;
		}
		if (request_.kind == PrimitiveKind::Write) {
			stats_.file_write_bytes += result_.count;
		} else if (request_.kind == PrimitiveKind::Read) {
			stats_.file_read_bytes += result_.count;
		}
		OwnedCompletion completed{std::move(pending_), std::move(result_)};
		request_ = {};
		ready_.store(false, std::memory_order_relaxed);
		return completed;
	}

  private:
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
			if (stop_) {
				return;
			}
			result_ = detail::execute_primitive(pending_->io(), request_);
			ready_.store(true, std::memory_order_release);
			struct kevent event{};
			EV_SET(&event, 1, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
			// A lost wakeup cannot lose the reserved result; owner-side polling
			// remains a shutdown/watchdog obligation on every platform.
			if (::kevent(queue_, &event, 1, nullptr, 0, nullptr) < 0) {
				wake_errors_.fetch_add(1, std::memory_order_relaxed);
			}
		}
	}
	int queue_ = -1;
	std::thread worker_;
	std::binary_semaphore work_{0};
	bool stop_ = false, native_pending_ = false, fallback_pending_ = false;
	std::atomic<bool> ready_{false};
	std::atomic<std::uint64_t> wake_errors_{0};
	std::shared_ptr<IOOperation> pending_;
	MutationRequest request_;
	MutationCompletion result_;
	aiocb control_{};
	CompletionStats stats_;
};
} // namespace kronuz::journal
