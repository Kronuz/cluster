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

#include <cstdint>
#include <limits>
#include <stdexcept>

namespace kronuz::journal {
struct StorageResources {
	std::uint64_t logical_bytes = 0, entries = 0;
	bool operator==(const StorageResources&) const = default;
};
namespace detail {
inline bool resources_fit(StorageResources value, StorageResources limit) noexcept {
	return value.logical_bytes <= limit.logical_bytes && value.entries <= limit.entries;
}
inline StorageResources resources_add(StorageResources a, StorageResources b) {
	constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
	if (b.logical_bytes > maximum - a.logical_bytes || b.entries > maximum - a.entries) {
		throw std::overflow_error("storage accounting overflow");
	}
	return {a.logical_bytes + b.logical_bytes, a.entries + b.entries};
}
// Callers establish resources_fit(b, a) before subtraction.
inline StorageResources resources_subtract(StorageResources a, StorageResources b) noexcept {
	return {a.logical_bytes - b.logical_bytes, a.entries - b.entries};
}
} // namespace detail
} // namespace kronuz::journal
