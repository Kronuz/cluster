#include "consensus/worker.h"
#include "journal/asio_completion.h"
#include "journal/journal.h"
#include "journal/native_completion.h"
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
	void delay_next() { delay_ = true; }
	CompletionStats stats() const { return queue_.stats(); }

  private:
	NativeCompletionQueue queue_;
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
asio::awaitable<void> worker_exercise(cluster::consensus::Worker &worker,
									  const std::shared_ptr<AsioCompletionDriver<DelayedQueue>> &driver,
									  const std::shared_ptr<DelayedQueue> &queue) {
	using namespace cluster::consensus;
	auto executor = co_await asio::this_coro::executor;
	asio::steady_timer timer(executor);
	asio::ip::udp::socket socket(executor, asio::ip::udp::endpoint(asio::ip::address_v4::loopback(), 0));
	std::optional<CheckpointId> capture;
	bool started = false, delayed = false, final_accepted = false;
	unsigned pending_ticks = 0, published = 0;
	std::exception_ptr io_error, failure;
	bool active = false;
	try {
		for (unsigned turn = 0; turn < 3000 && !published; ++turn) {
			worker.run_one({100, 100});
			if (auto job = worker.take_io_operation()) {
				if (active) {
					throw std::logic_error("Worker dispatched overlapping native jobs");
				}
				if (capture && !delayed && std::dynamic_pointer_cast<StoreArtifactOperation>(job)) {
					queue->delay_next();
					delayed = true;
				}
				active = true;
				asio::co_spawn(executor, driver->run(job), [&, job](std::exception_ptr error) {
					if (error) {
						io_error = error;
						worker.storage_operation_failed(job, "native Worker driver failed");
					}
					active = false;
				});
			}
			for (const auto &action : worker.take_actions()) {
				if (auto batch = std::get_if<Committed>(&action)) {
					worker.applied({batch->entries.back().index});
				}
				published += std::holds_alternative<CheckpointPublished>(action);
			}
			if (io_error) {
				std::rethrow_exception(io_error);
			}
			if (worker.fenced()) {
				throw std::runtime_error("native completion Worker fenced");
			}
			if (worker.ready() && !started) {
				worker.try_submit(Start{});
				started = true;
			}
			if (!capture && worker.applied_index() == 1) {
				capture = worker.reserve_checkpoint(65553);
				if (capture) {
					worker.attach_capture(*capture, {90, 1, worker.term()});
					worker.offer_application_chunk(*capture, std::string(65536, 'a'));
				}
			}
			if (capture && !final_accepted && worker.checkpoint_active(*capture)) {
				final_accepted = worker.offer_application_chunk(*capture, std::string(17, 'b'), true) ==
								 SubmitResult::Accepted;
			}
			if (delayed && active && pending_ticks < 20) {
				const char sent = 'w';
				char received = 0;
				co_await socket.async_send_to(asio::buffer(&sent, 1), socket.local_endpoint(),
											  asio::use_awaitable);
				co_await socket.async_receive(asio::buffer(&received, 1), asio::use_awaitable);
				if (received != sent || worker.base_index() != 0) {
					throw std::runtime_error("held Worker IO stalled sockets or published early");
				}
				++pending_ticks;
			}
			timer.expires_after(std::chrono::milliseconds(1));
			co_await timer.async_wait(asio::use_awaitable);
		}
	} catch (...) {
		failure = std::current_exception();
	}
	while (active) {
		timer.expires_after(std::chrono::milliseconds(1));
		co_await timer.async_wait(asio::use_awaitable);
	}
	if (failure) {
		std::rethrow_exception(failure);
	}
	if (io_error) {
		std::rethrow_exception(io_error);
	}
	if (pending_ticks < 20 || published != 1 || worker.base_index() != 1 ||
		!queue->stats().native_submitted) {
		std::cerr << "Worker native state: ready=" << worker.ready() << " role=" << int(worker.role())
				  << " applied=" << worker.applied_index() << " base=" << worker.base_index()
				  << " capture=" << bool(capture) << " delayed=" << delayed << " ticks=" << pending_ticks
				  << " published=" << published << " native=" << queue->stats().native_submitted << "\n";
		throw std::runtime_error("native Worker checkpoint did not reach exact durable publication");
	}
}
void native_worker(const std::filesystem::path &directory) {
	std::filesystem::create_directory(directory);
	::chmod(directory.c_str(), 0700);
	auto io = std::make_shared<PosixIO>(directory);
	cluster::consensus::FixedConfiguration fixed;
	fixed.local = 1;
	fixed.voters = {1};
	fixed.cluster[0] = 'W';
	fixed.configuration[0] = 'V';
	cluster::consensus::Limits limits;
	limits.command_bytes = 1024;
	limits.rpc_bytes = 2048;
	limits.log_bytes = 4096;
	limits.log_entries = 32;
	limits.control_entries = 4;
	limits.rpc_entries = 4;
	cluster::consensus::Worker worker(io, fixed, {{1024 * 1024, 1024}, {65536, 3}, {512 * 1024, 4}, 8, 3},
									  limits, {}, {16, 128, false, true});
	Identity identity{};
	identity[0] = 'W';
	worker.create(identity);
	asio::io_context context;
	auto queue = std::make_shared<DelayedQueue>();
	auto driver = std::make_shared<AsioCompletionDriver<DelayedQueue>>(context.get_executor(), queue, true);
	std::exception_ptr error;
	asio::co_spawn(context, worker_exercise(worker, driver, queue), [&](std::exception_ptr e) { error = e; });
	context.run();
	if (error) {
		std::rethrow_exception(error);
	}
}
} // namespace
int main() {
	auto directory =
		std::filesystem::current_path() / ".scratch" / ("completion-asio-test-" + std::to_string(::getpid()));
	int failed = 0;
	try {
#ifdef __linux__
		LinuxCompletionQueue qualification(LinuxCompletionPolicy::RequireNative);
#endif
		std::filesystem::create_directories(directory.parent_path());
		if (!std::filesystem::create_directory(directory)) {
			throw std::runtime_error("completion test directory already exists");
		}
		::chmod(directory.c_str(), 0700);
		native_worker(directory / "worker");
		auto io = std::make_shared<PosixIO>(directory);
		Journal journal(io, 1024);
		Identity id{};
		id[0] = 'c';
		journal.create(id);
		asio::io_context context;
		auto queue = std::make_shared<DelayedQueue>();
		auto driver =
			std::make_shared<AsioCompletionDriver<DelayedQueue>>(context.get_executor(), queue, true);
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
		auto timings = driver->stats();
		std::uint64_t reaped = 0;
		for (const auto &primitive : timings.submitted_to_reaped) {
			reaped += primitive.count;
		}
		if (timings.original_drive.count != 1 || timings.original_drive.maximum_ns < 100000000 ||
			reaped != queue->stats().native_completed + queue->stats().fallback_completed) {
			throw std::runtime_error("original completion timing omitted ownership or held observation");
		}
		std::cout << "owner_timer_ticks=" << ticks << " native=" << queue->stats().native_completed << '\n';
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		failed = 1;
	}
	std::filesystem::remove_all(directory);
	return failed;
}
