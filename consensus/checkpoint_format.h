#pragma once

#include "../journal/artifact.h"
#include "storage.h"

namespace cluster::consensus {
namespace checkpoint_detail {
constexpr std::uint64_t magic = 0x31504b4342544652ull;
constexpr std::uint64_t magic_v2 = 0x32504b4342544652ull;
inline std::size_t maximum_size(Limits limits) {
	constexpr std::size_t fixed = 400;
	if (limits.log_entries > (std::numeric_limits<std::size_t>::max() - fixed) / 21 ||
		limits.log_bytes > std::numeric_limits<std::size_t>::max() - fixed - limits.log_entries * 21) {
		throw std::length_error("checkpoint encoded bound overflow");
	}
	return fixed + limits.log_entries * 21 + limits.log_bytes;
}
} // namespace checkpoint_detail
} // namespace cluster::consensus
