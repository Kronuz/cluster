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

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <limits>

namespace kronuz::metrics {

class Counter {
  public:
	void add(std::uint64_t amount = 1) noexcept { value_ += std::min(amount, maximum - value_); }
	void observe_max(std::uint64_t value) noexcept { value_ = std::max(value_, value); }
	std::uint64_t get() const noexcept { return value_; }

  private:
	static constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
	std::uint64_t value_ = 0;
};
class AtomicCounter {
  public:
	void add(std::uint64_t amount = 1) noexcept {
		auto value = value_.load(std::memory_order_relaxed);
		while (!value_.compare_exchange_weak(value, value + std::min(amount, maximum - value),
											 std::memory_order_relaxed)) {
		}
	}
	void observe_max(std::uint64_t value) noexcept {
		auto current = value_.load(std::memory_order_relaxed);
		while (current < value && !value_.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
		}
	}
	std::uint64_t get() const noexcept { return value_.load(std::memory_order_relaxed); }

  private:
	static constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
	std::atomic<std::uint64_t> value_{0};
};

// Inclusive nanosecond bounds; the final bucket is the overflow bucket.
inline constexpr std::array<std::uint64_t, 14> duration_bounds_ns{
	1000,	  4000,		16000,	   64000,	   256000,	   1000000,		4000000,
	16000000, 64000000, 256000000, 1000000000, 4000000000, 16000000000, 64000000000};
struct DurationSnapshot {
	std::array<std::uint64_t, duration_bounds_ns.size() + 1> buckets{};
	std::uint64_t count = 0, sum_ns = 0, maximum_ns = 0;
};
template <class CounterType = Counter> class DurationHistogram {
  public:
	void observe(std::uint64_t nanoseconds) noexcept {
		auto bucket = std::lower_bound(duration_bounds_ns.begin(), duration_bounds_ns.end(), nanoseconds) -
					  duration_bounds_ns.begin();
		buckets_[static_cast<std::size_t>(bucket)].add();
		count_.add();
		sum_.add(nanoseconds);
		maximum_.observe_max(nanoseconds);
	}
	DurationSnapshot snapshot() const noexcept {
		DurationSnapshot result;
		for (std::size_t i = 0; i < result.buckets.size(); ++i) {
			result.buckets[i] = buckets_[i].get();
		}
		result.count = count_.get();
		result.sum_ns = sum_.get();
		result.maximum_ns = maximum_.get();
		return result;
	}

  private:
	std::array<CounterType, duration_bounds_ns.size() + 1> buckets_{};
	CounterType count_, sum_, maximum_;
};
using AtomicDurationHistogram = DurationHistogram<AtomicCounter>;

} // namespace kronuz::metrics
