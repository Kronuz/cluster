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
