/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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
