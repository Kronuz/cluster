// Native completion checkpoint qualification. Seed IO and recovery are excluded
// from the measured interval; all live Worker and driver callbacks are observed.
#include "../journal/asio_completion.h"
#include "../journal/native_completion.h"
#include "../journal/posix.h"
#include "worker.h"
#include <charconv>
#include <ctime>
#include <iostream>
#include <sys/resource.h>
#include <thread>

using namespace cluster::consensus;
namespace {
using Clock = std::chrono::steady_clock;
void require(bool condition, const char *message) {
	if (!condition)
		throw std::runtime_error(message);
}
double thread_cpu() {
	timespec value{};
	require(::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) == 0, "thread CPU clock");
	return value.tv_sec + value.tv_nsec / 1e9;
}
std::uint64_t peak_rss() {
	rusage value{};
	require(::getrusage(RUSAGE_SELF, &value) == 0, "RSS accounting");
#if defined(__APPLE__)
	return value.ru_maxrss;
#else
	return static_cast<std::uint64_t>(value.ru_maxrss) * 1024;
#endif
}
double seconds(Clock::time_point start, Clock::time_point end) {
	return std::chrono::duration<double>(end - start).count();
}
void run(std::size_t count) {
	FixedConfiguration configuration;
	configuration.local = 1;
	configuration.voters = {1};
	configuration.cluster[0] = 'C';
	configuration.configuration[0] = 'V';
	auto directory =
		std::filesystem::current_path() / ".scratch" /
		("completion-checkpoint-bench-" + std::to_string(::getpid()) + "-" + std::to_string(count));
	require(!std::filesystem::exists(directory), "fresh benchmark directory required");
	std::filesystem::create_directories(directory);
	require(::chmod(directory.c_str(), 0700) == 0, "private benchmark directory");
	struct Cleanup {
		std::filesystem::path path;
		~Cleanup() { std::filesystem::remove_all(path); }
	} cleanup{directory};
	{
		kronuz::journal::PosixIO io(directory);
		kronuz::journal::Journal journal(io, 4 * 1024 * 1024);
		kronuz::journal::Identity identity{};
		identity[0] = 'B';
		journal.create(identity);
		journal.append_batch(encode_initialization(configuration));
		journal.append_batch(
			encode_storage_batch({HardState{1, 1, 1}, LogMutation{1, {{1, 1, EntryKind::NoOp, {}}}}}));
		for (std::size_t first = 0; first < count;) {
			auto size = std::min(count - first, std::size_t{256});
			StorageBatch batch{HardState{1, 1, first + size + 1}, LogMutation{first + 2, {}}};
			for (std::size_t index = 0; index < size; ++index)
				batch.log->entries.push_back(
					{first + index + 2, 1, EntryKind::Command, std::string(1024, 'x')});
			journal.append_batch(encode_storage_batch(batch));
			first += size;
		}
	}
	asio::io_context executor(1);
	auto io = std::make_shared<kronuz::journal::PosixIO>(directory);
	Worker worker(io, configuration,
				  {{512ull * 1024 * 1024, 4096}, {16 * 1024, 3}, {80ull * 1024 * 1024, 4}, 8, 3}, {}, {},
				  {16, 128, true, true});
	worker.recover([](auto &) { throw std::runtime_error("unexpected initial checkpoint"); });
	auto queue = std::make_shared<kronuz::journal::NativeCompletionQueue>();
	auto driver =
		std::make_shared<kronuz::journal::AsioCompletionDriver<kronuz::journal::NativeCompletionQueue>>(
			executor.get_executor(), queue, true);
	bool active = false, finished = false, started = false;
	std::exception_ptr failure;
	std::optional<CheckpointId> checkpoint;
	std::string image(1024 * 1024, 'b');
	std::size_t offset = 0, publications = 0;
	Clock::time_point measured_start{}, freeze_start{};
	double measured_cpu = 0, worst_callback_cpu = 0, worst_callback_wall = 0;
	std::size_t observed_callbacks = 0;
	bool measuring = false, freezing = false;
	auto deadline = Clock::now() + std::chrono::minutes(3);
	asio::co_spawn(
		executor,
		[&]() -> asio::awaitable<void> {
			for (;;) {
				require(Clock::now() < deadline, "benchmark deadline");
				worker.run_one({0, 100});
				require(!worker.fenced(), "benchmark worker fenced");
				if (!active)
					if (auto operation = worker.take_io_operation()) {
						require(!active, "overlapping original IO");
						active = true;
						asio::co_spawn(executor, driver->run(operation), [&](std::exception_ptr error) {
							active = false;
							if (error)
								failure = error;
						});
					}
				if (failure)
					std::rethrow_exception(failure);
				if (worker.ready() && !started) {
					require(worker.try_submit(Start{}) == SubmitResult::Accepted, "start admission");
					started = true;
				}
				for (const auto &action : worker.take_actions()) {
					if (auto range = std::get_if<Committed>(&action))
						worker.applied({range->entries.back().index});
					if (std::holds_alternative<CheckpointPublished>(action)) {
						++publications;
						checkpoint.reset();
						offset = 0;
					}
				}
				if (!checkpoint && worker.ready() && worker.applied_index() == count + 1 &&
					publications < 2) {
					checkpoint = worker.reserve_checkpoint(image.size());
					if (checkpoint) {
						worker.attach_capture(*checkpoint,
											  {publications + 1, publications ? count + 1 : 1, 1});
						if (!measuring) {
							measured_start = Clock::now();
							measured_cpu = thread_cpu();
							measuring = true;
						}
					}
				}
				if (checkpoint && offset < image.size()) {
					auto size = std::min(image.size() - offset, std::size_t{65536});
					if (worker.offer_application_chunk(
							*checkpoint, std::string_view(image).substr(offset, size),
							offset + size == image.size()) == SubmitResult::Accepted)
						offset += size;
				}
				if (measuring && worker.busy() && !freezing) {
					freezing = true;
					freeze_start = Clock::now();
				}
				if (publications == 2 && !active && !worker.retired_log_entries()) {
					require(worker.base_index() == count + 1, "prefix did not compact");
					finished = true;
					co_return;
				}
				asio::steady_timer timer(executor);
				timer.expires_after(std::chrono::milliseconds(1));
				co_await timer.async_wait(asio::use_awaitable);
			}
		},
		[&](std::exception_ptr error) {
			if (error)
				failure = error;
		});
	while (!finished && !failure) {
		const auto before = Clock::now();
		const auto cpu = thread_cpu();
		const auto dispatched = executor.poll_one();
		const auto end_cpu = thread_cpu();
		const auto after = Clock::now();
		if (dispatched && measuring) {
			++observed_callbacks;
			worst_callback_cpu = std::max(worst_callback_cpu, end_cpu - cpu);
			worst_callback_wall = std::max(worst_callback_wall, seconds(before, after));
		}
		if (!dispatched)
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		if (executor.stopped())
			executor.restart();
	}
	if (failure)
		std::rethrow_exception(failure);
	require(!active && !queue->busy(), "original IO retained at measurement endpoint");
	std::cout << count << ',' << seconds(measured_start, Clock::now()) << ',' << thread_cpu() - measured_cpu
			  << ',' << seconds(freeze_start, Clock::now()) << ',' << worst_callback_cpu << ','
			  << worst_callback_wall << ',' << observed_callbacks << ',' << peak_rss() << ','
			  << queue->stats().native_completed << ',' << queue->stats().fallback_completed << '\n';
}
} // namespace
int main(int argc, char **argv) {
	try {
		std::cout << "suffix_commands,two_checkpoint_seconds,owner_cpu_seconds,first_freeze_to_retirement_"
					 "seconds,max_callback_cpu_seconds,max_callback_wall_seconds,callbacks,peak_rss_bytes,"
					 "native_primitives,fallback_primitives\n";
		if (argc == 2) {
			std::size_t count = 0;
			std::string_view input(argv[1]);
			auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), count);
			require(error == std::errc{} && end == input.data() + input.size() && count && count <= 65535,
					"invalid suffix count");
			run(count);
		} else {
			require(argc == 1, "usage: cluster_worker_completion_bench [suffix_count]");
			for (auto count : {std::size_t{64}, std::size_t{4096}, std::size_t{65535}})
				run(count);
		}
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
