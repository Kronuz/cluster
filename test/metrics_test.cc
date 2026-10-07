#include "metrics.h"
#include <iostream>
#include <thread>
#include <vector>

using namespace kronuz::metrics;
int main() {
	try {
		auto require = [](bool value) {
			if (!value) {
				throw std::runtime_error("bounded metric invariant");
			}
		};
		Counter counter;
		counter.add(std::numeric_limits<std::uint64_t>::max() - 1);
		counter.add(9);
		require(counter.get() == std::numeric_limits<std::uint64_t>::max());
		counter.add();
		require(counter.get() == std::numeric_limits<std::uint64_t>::max());
		DurationHistogram histogram;
		histogram.observe(0);
		for (auto edge : duration_bounds_ns) {
			histogram.observe(edge);
			histogram.observe(edge + 1);
		}
		auto snapshot = histogram.snapshot();
		require(snapshot.count == 29 && snapshot.buckets.front() == 2 && snapshot.buckets.back() == 1 &&
				snapshot.maximum_ns == duration_bounds_ns.back() + 1);
		histogram.observe(std::numeric_limits<std::uint64_t>::max());
		histogram.observe(1);
		require(histogram.snapshot().sum_ns == std::numeric_limits<std::uint64_t>::max());
		AtomicDurationHistogram concurrent;
		std::vector<std::thread> threads;
		for (unsigned thread = 0; thread < 8; ++thread) {
			threads.emplace_back([&] {
				for (unsigned i = 0; i < 10000; ++i) {
					concurrent.observe(1000);
				}
			});
		}
		for (auto &thread : threads) {
			thread.join();
		}
		snapshot = concurrent.snapshot();
		require(snapshot.count == 80000 && snapshot.sum_ns == 80000000 && snapshot.maximum_ns == 1000 &&
				snapshot.buckets.front() == 80000);
		std::cout << "fixed histogram boundaries, saturation and concurrent observations passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
