#pragma once

#include "checkpoint_encoder.h"

namespace cluster::consensus {
struct CheckpointBundle {
	std::uint64_t storage_sequence;
	kronuz::journal::ArtifactDescriptor application;
	RecoveredState state;
	std::optional<std::uint32_t> application_format{};
};
inline std::string encode_checkpoint(const RecoveredState& state, std::uint64_t storage_sequence,
	const kronuz::journal::ArtifactDescriptor& application, Limits limits = {}, std::optional<std::uint32_t> application_format = {}) {
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
	auto encoded_size = std::size_t(application_format ? 96 : 92) + initialization.size() + state.entries.size() * 21 + payload_bytes;
	if (encoded_size > maximum) { throw std::length_error("checkpoint exceeds encoded bound"); }
	std::string result; result.reserve(encoded_size);
	IncrementalCheckpointEncoder encoder(BorrowedCheckpointState{&state}, storage_sequence, application, limits, application_format);
	std::array<char, 65536> buffer{};
	while (!encoder.done()) {
		auto count = encoder.next(buffer);
		if (!count) { throw std::logic_error("checkpoint encoder made no progress"); }
		result.append(buffer.data(), count);
	}
	if (result.size() != encoded_size) { throw std::length_error("checkpoint exceeds encoded bound"); }
	return result;
}
inline std::string encode_checkpoint(const CheckpointBundle& bundle, Limits limits = {}) {
	return encode_checkpoint(bundle.state, bundle.storage_sequence, bundle.application, limits, bundle.application_format);
}
// Decode only into unpublished state. The caller also verifies the enclosing
// journal frontier and the application artifact before activating either.
inline CheckpointBundle decode_checkpoint(std::string_view bytes, const FixedConfiguration& expected, std::uint64_t covered_sequence,
	const kronuz::journal::ArtifactDescriptor& application, Limits limits = {}) {
	using namespace storage_detail;
	if (bytes.size() > checkpoint_detail::maximum_size(limits)) { throw Corruption("oversized checkpoint bundle"); }
	auto magic = get64(bytes);
	if (magic != checkpoint_detail::magic && magic != checkpoint_detail::magic_v2) {
		throw Corruption("unsupported or oversized checkpoint bundle");
	}
	CheckpointBundle bundle;
	if (magic == checkpoint_detail::magic_v2) { bundle.application_format = get32(bytes); }
	bundle.storage_sequence = get64(bytes);
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
