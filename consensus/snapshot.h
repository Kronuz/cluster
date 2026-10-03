#pragma once

#include "types.h"
#include "../journal/codec.h"
#include <algorithm>
#include <limits>

namespace cluster::consensus {

// Portable application metadata. Ballots, local identities, storage sequences
// and retained suffixes belong to the receiver's local checkpoint bundle.
struct SnapshotDescriptor {
	Identity cluster{}, configuration{};
	std::uint32_t application_format = 0;
	Index through = 0;
	Term term = 0;
	std::uint64_t application_bytes = 0;
	std::uint32_t application_crc32c = 0;
	bool operator==(const SnapshotDescriptor&) const = default;
};
struct SnapshotPolicy {
	Identity cluster{}, configuration{};
	std::uint32_t application_format = 0;
	std::uint64_t maximum_application_bytes = 0;
};
inline constexpr std::size_t snapshot_descriptor_bytes = 72;

inline void validate_snapshot(const SnapshotDescriptor& descriptor, const SnapshotPolicy& policy) {
	if (descriptor.cluster != policy.cluster || descriptor.configuration != policy.configuration ||
		descriptor.application_format != policy.application_format || descriptor.through == 0 ||
		descriptor.through == std::numeric_limits<Index>::max() || descriptor.term == 0 ||
		descriptor.application_bytes > policy.maximum_application_bytes ||
		(descriptor.application_bytes == 0 && descriptor.application_crc32c != kronuz::journal::crc32c(""))) {
		throw kronuz::journal::Corruption("invalid portable snapshot descriptor");
	}
}
inline std::string encode_snapshot(const SnapshotDescriptor& descriptor, const SnapshotPolicy& policy) {
	validate_snapshot(descriptor, policy);
	std::string result; result.reserve(snapshot_descriptor_bytes); result.append("RFTSNP01", 8);
	result.append(descriptor.cluster.data(), descriptor.cluster.size());
	result.append(descriptor.configuration.data(), descriptor.configuration.size());
	kronuz::journal::put32(result, descriptor.application_format);
	kronuz::journal::put64(result, descriptor.through); kronuz::journal::put64(result, descriptor.term);
	kronuz::journal::put64(result, descriptor.application_bytes); kronuz::journal::put32(result, descriptor.application_crc32c);
	return result;
}
inline SnapshotDescriptor decode_snapshot(std::string_view bytes, const SnapshotPolicy& policy) {
	if (bytes.size() != snapshot_descriptor_bytes || bytes.substr(0, 8) != "RFTSNP01") {
		throw kronuz::journal::Corruption("unsupported portable snapshot framing");
	}
	bytes.remove_prefix(8); SnapshotDescriptor result;
	std::copy_n(bytes.begin(), result.cluster.size(), result.cluster.begin()); bytes.remove_prefix(result.cluster.size());
	std::copy_n(bytes.begin(), result.configuration.size(), result.configuration.begin()); bytes.remove_prefix(result.configuration.size());
	result.application_format = kronuz::journal::get32(bytes);
	result.through = kronuz::journal::get64(bytes); result.term = kronuz::journal::get64(bytes);
	result.application_bytes = kronuz::journal::get64(bytes); result.application_crc32c = kronuz::journal::get32(bytes);
	validate_snapshot(result, policy); return result;
}

} // namespace cluster::consensus
