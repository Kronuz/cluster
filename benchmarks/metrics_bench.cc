#include "metrics.h"
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <sys/resource.h>

namespace {
using Clock = std::chrono::steady_clock;
double cpu_seconds() {
	rusage usage{};
	if (::getrusage(RUSAGE_SELF, &usage))
		throw std::runtime_error("getrusage failed");
	return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 + usage.ru_stime.tv_sec +
		   usage.ru_stime.tv_usec / 1e6;
}
template <class Histogram> void run(const char *name, bool observe, bool clocks) {
	constexpr std::uint64_t iterations = 1000000;
	Histogram histogram;
	std::uint64_t random = 12345, checksum = 0;
	auto start = Clock::now();
	auto cpu = cpu_seconds();
	for (std::uint64_t i = 0; i < iterations; ++i) {
		random ^= random << 13;
		random ^= random >> 7;
		random ^= random << 17;
		auto value = random % 100000000;
		checksum += value;
		if (observe) {
			if (clocks) {
				auto before = Clock::now();
				value = static_cast<std::uint64_t>(
					std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - before).count());
			}
			histogram.observe(value);
		}
	}
	auto consumed_cpu = cpu_seconds() - cpu;
	auto wall = std::chrono::duration<double>(Clock::now() - start).count();
	auto sample = histogram.snapshot();
	if (sample.count != (observe ? iterations : 0))
		throw std::runtime_error("sample count mismatch");
	std::cout << name << ',' << iterations << ',' << wall << ',' << consumed_cpu << ',' << sizeof(Histogram)
			  << ',' << checksum << ',' << sample.count << '\n';
}
} // namespace
int main() {
	try {
		std::cout << "mode,observations,wall_seconds,cpu_seconds,histogram_bytes,checksum,count\n";
		for (unsigned pair = 0; pair < 3; ++pair) {
			run<kronuz::metrics::DurationHistogram<>>("disabled", false, false);
			run<kronuz::metrics::DurationHistogram<>>("owner", true, false);
			run<kronuz::metrics::AtomicDurationHistogram>("atomic", true, false);
			run<kronuz::metrics::DurationHistogram<>>("owner_with_two_clocks", true, true);
		}
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
