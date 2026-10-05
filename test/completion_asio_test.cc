#include "journal/asio_completion.h"
#include "journal/bsd_completion.h"
#include "journal/journal.h"
#include <filesystem>
#include <iostream>

using namespace kronuz::journal;
using Clock = std::chrono::steady_clock;
namespace {
// Hold the first reaped primitive for 100 ms to prove executor progress and
// delayed frontier settlement independently of local drive flush latency.
class DelayedQueue {
  public:
	int notification_descriptor() const noexcept { return queue_.notification_descriptor(); }
	bool submit(const std::shared_ptr<IOOperation> &operation) { return !held_ && queue_.submit(operation); }
	std::optional<OwnedCompletion> poll() {
		if (held_) {
			if (Clock::now() < release_) {
				return std::nullopt;
			}
			auto result = std::move(held_);
			held_.reset();
			return result;
		}
		auto result = queue_.poll();
		if (result && delay_) {
			delay_ = false;
			held_ = std::move(result);
			release_ = Clock::now() + std::chrono::milliseconds(100);
			return std::nullopt;
		}
		return result;
	}
	CompletionStats stats() const { return queue_.stats(); }

  private:
	BsdCompletionQueue queue_;
	bool delay_ = true;
	std::optional<OwnedCompletion> held_;
	Clock::time_point release_;
};
asio::awaitable<void> exercise(Journal &journal,
							   const std::shared_ptr<AsioCompletionDriver<DelayedQueue>> &driver, bool &done,
							   std::size_t &ticks) {
	auto executor = co_await asio::this_coro::executor;
	auto operation = journal.begin_append("native coroutine append");
	co_await driver->run(operation);
	if (ticks < 20 || journal.frontier().sequence != 0) {
		throw std::runtime_error("owner executor stalled or acknowledged early");
	}
	if (journal.finish_append(operation).sequence != 1) {
		throw std::runtime_error("completed frontier mismatch");
	}
	done = true;
}
asio::awaitable<void> observe(Journal &journal, bool &done, std::size_t &ticks) {
	auto executor = co_await asio::this_coro::executor;
	asio::steady_timer timer(executor);
	asio::ip::udp::socket socket(executor, asio::ip::udp::endpoint(asio::ip::address_v4::loopback(), 0));
	const char ping = 'p';
	char reply = 0;
	auto endpoint = socket.local_endpoint();
	while (!done) {
		co_await socket.async_send_to(asio::buffer(&ping, 1), endpoint, asio::use_awaitable);
		co_await socket.async_receive(asio::buffer(&reply, 1), asio::use_awaitable);
		if (reply != ping) {
			throw std::runtime_error("reactor socket progress failed");
		}
		timer.expires_after(std::chrono::milliseconds(1));
		co_await timer.async_wait(asio::use_awaitable);
		++ticks;
		if (ticks == 20 && journal.frontier().sequence != 0) {
			throw std::runtime_error("delayed completion published early");
		}
	}
}
} // namespace
int main() {
	auto directory =
		std::filesystem::current_path() / ".scratch" / ("completion-asio-test-" + std::to_string(::getpid()));
	int failed = 0;
	try {
		std::filesystem::create_directories(directory.parent_path());
		if (!std::filesystem::create_directory(directory)) {
			throw std::runtime_error("completion test directory already exists");
		}
		::chmod(directory.c_str(), 0700);
		auto io = std::make_shared<PosixIO>(directory);
		Journal journal(io, 1024);
		Identity id{};
		id[0] = 'c';
		journal.create(id);
		asio::io_context context;
		auto queue = std::make_shared<DelayedQueue>();
		auto driver = std::make_shared<AsioCompletionDriver<DelayedQueue>>(context.get_executor(), queue);
		bool done = false;
		std::size_t ticks = 0;
		std::exception_ptr error;
		asio::cancellation_signal cancellation;
		asio::steady_timer cancel(context, std::chrono::milliseconds(10));
		cancel.async_wait([&](asio::error_code) { cancellation.emit(asio::cancellation_type::terminal); });
		asio::co_spawn(context, exercise(journal, driver, done, ticks),
					   asio::bind_cancellation_slot(cancellation.slot(), [&](std::exception_ptr failure) {
						   error = failure;
						   if (failure) {
							   done = true;
						   }
					   }));
		asio::co_spawn(context, observe(journal, done, ticks), [&](std::exception_ptr failure) {
			if (failure) {
				error = failure;
				done = true;
			}
		});
		context.run();
		if (error) {
			std::rethrow_exception(error);
		}
		if (!done || queue->stats().native_completed < 3) {
			throw std::runtime_error("native backend did not complete");
		}
		std::cout << "owner_timer_ticks=" << ticks << " native=" << queue->stats().native_completed << '\n';
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		failed = 1;
	}
	std::filesystem::remove_all(directory);
	return failed;
}
