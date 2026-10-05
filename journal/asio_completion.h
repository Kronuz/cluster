#pragma once

#include "mutation.h"
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <fcntl.h>
#include <unistd.h>

namespace kronuz::journal {

// Optional Asio adapter. The generic journal and completion backend do not
// depend on Asio. A single owner executor invokes one run() at a time.
template <class Queue>
class AsioCompletionDriver : public std::enable_shared_from_this<AsioCompletionDriver<Queue>> {
  public:
	AsioCompletionDriver(asio::any_io_executor executor, std::shared_ptr<Queue> queue)
		: executor_(std::move(executor)), queue_(std::move(queue)), notification_(executor_),
		  timer_(executor_) {
		if (!queue_) {
			throw std::invalid_argument("null completion queue");
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
	asio::awaitable<void> run(std::shared_ptr<IOOperation> operation) {
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
		struct Reset {
			bool &active;
			~Reset() { active = false; }
		} reset{active_};
		while (!operation->done()) {
			if (!queue_->submit(operation)) {
				throw std::logic_error("reserved completion queue is busy");
			}
			for (;;) {
				if (auto result = queue_->poll()) {
					if (result->operation != operation ||
						!operation->complete(std::move(result->completion))) {
						throw std::logic_error("completion ownership mismatch");
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
		}
	}

  private:
	asio::any_io_executor executor_;
	std::shared_ptr<Queue> queue_;
	asio::posix::stream_descriptor notification_;
	asio::steady_timer timer_;
	bool active_ = false, notification_failed_ = false;
};
} // namespace kronuz::journal
