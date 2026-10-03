#pragma once

#include "storage.h"
#include "../journal/artifact.h"

namespace cluster::consensus {
struct CheckpointBundle {
	std::uint64_t storage_sequence;
	kronuz::journal::ArtifactDescriptor application;
	RecoveredState state;
};
namespace checkpoint_detail {
constexpr std::uint64_t magic = 0x31504b4342544652ull;
inline std::size_t maximum_size(Limits limits) {
	constexpr std::size_t fixed = 400;
	if (limits.log_entries > (std::numeric_limits<std::size_t>::max() - fixed) / 21 ||
		limits.log_bytes > std::numeric_limits<std::size_t>::max() - fixed - limits.log_entries * 21) {
		throw std::length_error("checkpoint encoded bound overflow");
	}
	return fixed + limits.log_entries * 21 + limits.log_bytes;
}
}
inline std::string encode_checkpoint(const RecoveredState& state, std::uint64_t storage_sequence,
	const kronuz::journal::ArtifactDescriptor& application, Limits limits = {}) {
	using namespace storage_detail;
	// Validate the typed state before producing storage bytes.
	auto maximum = checkpoint_detail::maximum_size(limits);
	if (storage_sequence == 0) { throw Corruption("invalid checkpoint storage sequence"); }
	Recovery validator(state.configuration, limits); auto payload_bytes = validator.validate_checkpoint_state(state);
	if (state.entries.size() > std::numeric_limits<std::uint32_t>::max()) { throw std::length_error("checkpoint entry count overflow"); }
	auto initialization = encode_initialization(state.configuration);
	for (const auto& entry : state.entries) {
		if (entry.payload.size() > std::numeric_limits<std::uint32_t>::max()) { throw std::length_error("checkpoint payload framing overflow"); }
	}
	auto encoded_size = std::size_t(92) + initialization.size() + state.entries.size() * 21 + payload_bytes;
	if (encoded_size > maximum) { throw std::length_error("checkpoint exceeds encoded bound"); }
	std::string result; result.reserve(encoded_size); put64(result, checkpoint_detail::magic); put64(result, storage_sequence);
	identity(result, application.identity); put64(result, application.length); put32(result, application.checksum);
	put32(result, static_cast<std::uint32_t>(initialization.size())); result.append(initialization);
	put64(result, state.base_index); put64(result, state.base_term);
	put64(result, state.hard.term); put64(result, state.hard.voted_for.value_or(0)); put64(result, state.hard.commit_index);
	put32(result, static_cast<std::uint32_t>(state.entries.size()));
	for (const auto& entry : state.entries) {
		put64(result, entry.index); put64(result, entry.term); result.push_back(static_cast<char>(entry.kind));
		put32(result, static_cast<std::uint32_t>(entry.payload.size())); result.append(entry.payload);
	}
	if (result.size() != encoded_size) { throw std::length_error("checkpoint exceeds encoded bound"); }
	return result;
}
inline std::string encode_checkpoint(const CheckpointBundle& bundle, Limits limits = {}) {
	return encode_checkpoint(bundle.state, bundle.storage_sequence, bundle.application, limits);
}
// Decode only into unpublished state. The caller also verifies the enclosing
// journal frontier and the application artifact before activating either.
inline CheckpointBundle decode_checkpoint(std::string_view bytes, const FixedConfiguration& expected, std::uint64_t covered_sequence,
	const kronuz::journal::ArtifactDescriptor& application, Limits limits = {}) {
	using namespace storage_detail;
	if (bytes.size() > checkpoint_detail::maximum_size(limits) || get64(bytes) != checkpoint_detail::magic) {
		throw Corruption("unsupported or oversized checkpoint bundle");
	}
	CheckpointBundle bundle; bundle.storage_sequence = get64(bytes);
	bundle.application.identity = identity(bytes); bundle.application.length = get64(bytes); bundle.application.checksum = get32(bytes);
	if (bundle.storage_sequence == 0 || bundle.storage_sequence != covered_sequence || bundle.application != application) { throw Corruption("checkpoint dependency or storage sequence mismatch"); }
	auto length = get32(bytes); auto initialization = encode_initialization(expected);
	if (length != initialization.size() || length > bytes.size() || bytes.substr(0, length) != initialization) {
		throw Corruption("checkpoint fixed configuration mismatch");
	}
	bytes.remove_prefix(length); bundle.state.configuration = expected;
	bundle.state.base_index = get64(bytes); bundle.state.base_term = get64(bytes); bundle.state.applied_index = bundle.state.base_index;
	bundle.state.hard.term = get64(bytes); auto vote = get64(bytes); if (vote) { bundle.state.hard.voted_for = vote; }
	bundle.state.hard.commit_index = get64(bytes); auto count = get32(bytes);
	if (count > limits.log_entries || count > bytes.size() / 21) { throw Corruption("checkpoint suffix count exceeds bound"); }
	std::size_t payload = 0;
	for (std::uint32_t i = 0; i < count; ++i) {
		Entry entry; entry.index = get64(bytes); entry.term = get64(bytes);
		if (bytes.empty() || static_cast<unsigned char>(bytes[0]) > static_cast<unsigned char>(EntryKind::NoOp)) { throw Corruption("checkpoint entry kind invalid"); }
		entry.kind = static_cast<EntryKind>(bytes[0]); bytes.remove_prefix(1); auto size = get32(bytes);
		if (size > limits.command_bytes || size > limits.log_bytes - payload || size > bytes.size()) { throw Corruption("checkpoint payload exceeds bound"); }
		entry.payload.assign(bytes.substr(0, size)); bytes.remove_prefix(size); payload += size; bundle.state.entries.push_back(std::move(entry));
	}
	end(bytes);
	Recovery validator(expected, limits); validator.validate_checkpoint_state(bundle.state);
	return bundle;
}
} // namespace cluster::consensus
