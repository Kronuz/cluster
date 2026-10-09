#pragma once

#include "completion.h"
#include "metrics.h"
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>

namespace kronuz::journal {

inline constexpr std::size_t completion_primitive_count = static_cast<std::size_t>(PrimitiveKind::Remove) + 1;
inline constexpr std::size_t completion_extension_count =
	std::tuple_size_v<decltype(CompletionStats::native_extensions)>;
struct CompletionDriverStats {
	std::array<kronuz::metrics::DurationSnapshot, completion_primitive_count> submitted_to_reaped{},
		failed_submitted_to_reaped{};
	std::array<kronuz::metrics::DurationSnapshot, completion_extension_count> extension_submitted_to_reaped{},
		extension_failed_submitted_to_reaped{};
	kronuz::metrics::DurationSnapshot original_drive;
};
struct CompletionDriverMetrics {
	std::array<kronuz::metrics::DurationHistogram<>, completion_primitive_count> submitted_to_reaped{},
		failed_submitted_to_reaped{};
	std::array<kronuz::metrics::DurationHistogram<>, completion_extension_count>
		extension_submitted_to_reaped{}, extension_failed_submitted_to_reaped{};
	kronuz::metrics::DurationHistogram<> original_drive;
	CompletionDriverStats snapshot() const noexcept {
		CompletionDriverStats result;
		for (std::size_t i = 0; i < completion_primitive_count; ++i) {
			result.submitted_to_reaped[i] = submitted_to_reaped[i].snapshot();
			result.failed_submitted_to_reaped[i] = failed_submitted_to_reaped[i].snapshot();
		}
		for (std::size_t i = 0; i < completion_extension_count; ++i) {
			result.extension_submitted_to_reaped[i] = extension_submitted_to_reaped[i].snapshot();
			result.extension_failed_submitted_to_reaped[i] =
				extension_failed_submitted_to_reaped[i].snapshot();
		}
		result.original_drive = original_drive.snapshot();
		return result;
	}
};

// Optional Asio adapter. The generic journal and completion backend do not
// depend on Asio. A single owner executor invokes one run() at a time.
template <class Queue>
class AsioCompletionDriver : public std::enable_shared_from_this<AsioCompletionDriver<Queue>> {
  public:
	AsioCompletionDriver(asio::any_io_executor executor, std::shared_ptr<Queue> queue,
						 bool collect_metrics = false)
		: executor_(std::move(executor)), queue_(std::move(queue)), notification_(executor_),
		  timer_(executor_) {
		if (!queue_) {
			throw std::invalid_argument("null completion queue");
		}
		if (collect_metrics) {
			metrics_ = std::make_unique<CompletionDriverMetrics>();
		}
		int fd = ::dup(queue_->notification_descriptor());
		if (fd < 0) {
			throw std::system_error(errno, std::generic_category(), "duplicate IO notification");
		}
		if (::fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) {
			auto error = errno;
			::close(fd);
			throw std::system_error(error, std::generic_category(), "protect IO notification");
		}
		asio::error_code error;
		notification_.assign(fd, error);
		if (error) {
			::close(fd);
			throw std::system_error(error);
		}
	}
	CompletionDriverStats stats() const noexcept {
		return metrics_ ? metrics_->snapshot() : CompletionDriverStats{};
	}
	asio::awaitable<void> run(std::shared_ptr<IOOperation> operation) {
		return drive(std::move(operation), false);
	}
	// Reap exactly one original primitive, then return control to the owner.
	// This supports operations with explicit paused scheduling boundaries.
	asio::awaitable<void> run_one(std::shared_ptr<IOOperation> operation) {
		return drive(std::move(operation), true);
	}

  private:
	asio::awaitable<void> drive(std::shared_ptr<IOOperation> operation, bool single) {
		using namespace asio::experimental::awaitable_operators;
		auto lifetime = this->shared_from_this();
		auto current_executor = co_await asio::this_coro::executor;
		if (current_executor != executor_) {
			throw std::logic_error("completion driver requires its owner executor");
		}
		if (active_ || !operation || operation->in_flight()) {
			throw std::logic_error("completion driver is not available");
		}
		// Request cancellation releases interest above this driver. It cannot
		// destroy accepted IO buffers or skip the remaining durable barriers.
		co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
		active_ = true;
		auto started = metrics_ ? Clock::now() : Clock::time_point{};
		struct Reset {
			bool &active;
			~Reset() { active = false; }
		} reset{active_};
		std::size_t immediate_steps = 0;
		while (!operation->done()) {
			// A fast backend still yields bounded owner turns to sockets and timers.
			if (++immediate_steps == 16) {
				immediate_steps = 0;
				co_await asio::post(executor_, asio::use_awaitable);
			}
			auto primitive = operation->request().kind;
			auto kind = static_cast<std::size_t>(primitive);
			if (!CompletionStats::supported(primitive)) {
				throw std::invalid_argument("invalid completion primitive kind");
			}
			auto submitted = metrics_ ? Clock::now() : Clock::time_point{};
			if (!queue_->submit(operation)) {
				throw std::logic_error("reserved completion queue is busy");
			}
			for (;;) {
				if (auto result = queue_->poll()) {
					auto latency = metrics_ ? elapsed_ns(submitted) : 0;
					bool failed = static_cast<bool>(result->completion.error);
					if (result->operation != operation ||
						!operation->complete(std::move(result->completion))) {
						throw std::logic_error("completion ownership mismatch");
					}
					if (metrics_) {
						auto &histogram =
							kind < completion_primitive_count
								? (failed ? metrics_->failed_submitted_to_reaped[kind]
										  : metrics_->submitted_to_reaped[kind])
								: (failed
									   ? metrics_->extension_failed_submitted_to_reaped
											 [kind - completion_primitive_count]
									   : metrics_->extension_submitted_to_reaped[kind -
																				 completion_primitive_count]);
						histogram.observe(latency);
					}
					break;
				}
				// Bounded polling also protects against a lost fallback wakeup.
				// The descriptor provides prompt readiness where supported.
				timer_.expires_after(std::chrono::milliseconds(1));
				asio::error_code notification_error;
				if (notification_failed_) {
					co_await timer_.async_wait(asio::use_awaitable);
				} else {
					co_await (notification_.async_wait(
								  asio::posix::stream_descriptor::wait_read,
								  asio::redirect_error(asio::use_awaitable, notification_error)) ||
							  timer_.async_wait(asio::use_awaitable));
					if (notification_error && notification_error != asio::error::operation_aborted) {
						notification_failed_ = true;
					}
				}
			}
			if (single)
				break;
		}
		if (metrics_) {
			metrics_->original_drive.observe(elapsed_ns(started));
		}
	}

  private:
	using Clock = std::chrono::steady_clock;
	static std::uint64_t elapsed_ns(Clock::time_point start) noexcept {
		auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
		return elapsed > 0 ? static_cast<std::uint64_t>(elapsed) : 0;
	}
	std::unique_ptr<CompletionDriverMetrics> metrics_;
	asio::any_io_executor executor_;
	std::shared_ptr<Queue> queue_;
	asio::posix::stream_descriptor notification_;
	asio::steady_timer timer_;
	bool active_ = false, notification_failed_ = false;
};
} // namespace kronuz::journal
