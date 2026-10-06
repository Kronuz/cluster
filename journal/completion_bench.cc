#include "journal.h"
#include "linux_completion.h"
#include <algorithm>
#include <charconv>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <poll.h>
#include <sys/resource.h>
#include <vector>

using namespace kronuz::journal;
using Clock = std::chrono::steady_clock;
int main(int argc, char **argv) {
	if (argc != 3 || (std::string_view(argv[1]) != "native" && std::string_view(argv[1]) != "fallback")) {
		std::cerr << "usage: completion_bench native|fallback append-count\n";
		return 1;
	}
	unsigned count = 0;
	const auto argument = std::string_view(argv[2]);
	const auto parsed = std::from_chars(argument.data(), argument.data() + argument.size(), count);
	if (parsed.ec != std::errc{} || parsed.ptr != argument.data() + argument.size() || !count ||
		count > 100000)
		return 1;
	const auto directory =
		std::filesystem::current_path() / ".scratch" / ("completion-bench-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory);
	::chmod(directory.c_str(), 0700);
	try {
		auto io = std::make_shared<PosixIO>(directory);
		Journal journal(io, 65536);
		Identity identity{};
		identity[0] = 'b';
		journal.create(identity);
		LinuxCompletionQueue queue(std::string_view(argv[1]) == "native"
									   ? LinuxCompletionPolicy::RequireNative
									   : LinuxCompletionPolicy::Fallback);
		const std::string payload(4096, 'x');
		std::vector<double> latency;
		latency.reserve(count);
		rusage owner_before{};
		::getrusage(RUSAGE_THREAD, &owner_before);
		const auto cpu = std::clock();
		const auto began = Clock::now();
		auto timer_due = began + std::chrono::milliseconds(1);
		double maximum_timer_delay = 0;
		for (unsigned i = 0; i < count; ++i) {
			const auto start = Clock::now();
			auto operation = journal.begin_append(payload);
			while (!operation->done()) {
				if (!queue.submit(operation))
					throw std::runtime_error("benchmark admission");
				for (;;) {
					const auto now = Clock::now();
					if (now >= timer_due) {
						maximum_timer_delay =
							std::max(maximum_timer_delay,
									 std::chrono::duration<double, std::milli>(now - timer_due).count());
						timer_due = now + std::chrono::milliseconds(1);
					}
					if (auto result = queue.poll()) {
						if (result->operation != operation ||
							!operation->complete(std::move(result->completion)))
							throw std::runtime_error("benchmark completion ownership");
						break;
					}
					pollfd notification{queue.notification_descriptor(), POLLIN, 0};
					if (::poll(&notification, 1, 1) < 0 && errno != EINTR)
						throw std::system_error(errno, std::generic_category(), "benchmark wait");
				}
			}
			if (journal.finish_append(operation).sequence != i + 1)
				throw std::runtime_error("benchmark frontier");
			latency.push_back(std::chrono::duration<double, std::milli>(Clock::now() - start).count());
		}
		rusage owner_after{};
		::getrusage(RUSAGE_THREAD, &owner_after);
		const auto owner_cpu_ms = 1000.0 * (owner_after.ru_utime.tv_sec + owner_after.ru_stime.tv_sec -
											owner_before.ru_utime.tv_sec - owner_before.ru_stime.tv_sec) +
								  0.001 * (owner_after.ru_utime.tv_usec + owner_after.ru_stime.tv_usec -
										   owner_before.ru_utime.tv_usec - owner_before.ru_stime.tv_usec);
		const auto cpu_ms = 1000.0 * (std::clock() - cpu) / CLOCKS_PER_SEC;
		const auto wall_ms = std::chrono::duration<double, std::milli>(Clock::now() - began).count();
		std::sort(latency.begin(), latency.end());
		auto percentile = [&](double fraction) {
			return latency[static_cast<std::size_t>((latency.size() - 1) * fraction)];
		};
		auto stats = queue.stats();
		rusage usage{};
		::getrusage(RUSAGE_SELF, &usage);
		std::cout << "{\"mode\":\"" << argv[1] << "\",\"appends\":" << count
				  << ",\"payload_bytes\":4096,\"wall_ms\":" << wall_ms << ",\"process_cpu_ms\":" << cpu_ms
				  << ",\"owner_cpu_ms\":" << owner_cpu_ms << ",\"ack_p50_ms\":" << percentile(.50)
				  << ",\"ack_p95_ms\":" << percentile(.95) << ",\"ack_p99_ms\":" << percentile(.99)
				  << ",\"maximum_timer_delay_ms\":" << maximum_timer_delay
				  << ",\"peak_rss_kib\":" << usage.ru_maxrss << ",\"native\":" << stats.native_completed
				  << ",\"fallback\":" << stats.fallback_completed
				  << ",\"written_bytes\":" << stats.file_write_bytes << "}\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		std::filesystem::remove_all(directory);
		return 1;
	}
	std::filesystem::remove_all(directory);
	return 0;
}
