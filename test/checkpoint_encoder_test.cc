#include "consensus/checkpoint.h"
#include "consensus/checkpoint_encoder.h"
#include "consensus/core.h"
#include <array>
#include <iostream>

using namespace cluster::consensus;
namespace {
std::string original_encoding(const RecoveredState &state, std::uint64_t storage_sequence,
							  const kronuz::journal::ArtifactDescriptor &application, Limits limits = {},
							  std::optional<std::uint32_t> application_format = {}) {
	using namespace storage_detail;
	// Validate the typed state before producing storage bytes.
	auto maximum = checkpoint_detail::maximum_size(limits);
	if (storage_sequence == 0) {
		throw Corruption("invalid checkpoint storage sequence");
	}
	Recovery validator(state.configuration, limits);
	auto payload_bytes = validator.validate_checkpoint_state(state);
	if (state.entries.size() > std::numeric_limits<std::uint32_t>::max()) {
		throw std::length_error("checkpoint entry count overflow");
	}
	auto initialization = encode_initialization(state.configuration);
	for (const auto &entry : state.entries) {
		if (entry.payload.size() > std::numeric_limits<std::uint32_t>::max()) {
			throw std::length_error("checkpoint payload framing overflow");
		}
	}
	auto encoded_size = std::size_t(application_format ? 96 : 92) + initialization.size() +
						state.entries.size() * 21 + payload_bytes;
	if (encoded_size > maximum) {
		throw std::length_error("checkpoint exceeds encoded bound");
	}
	std::string result;
	result.reserve(encoded_size);
	put64(result, application_format ? checkpoint_detail::magic_v2 : checkpoint_detail::magic);
	if (application_format) {
		put32(result, *application_format);
	}
	put64(result, storage_sequence);
	identity(result, application.identity);
	put64(result, application.length);
	put32(result, application.checksum);
	put32(result, static_cast<std::uint32_t>(initialization.size()));
	result.append(initialization);
	put64(result, state.base_index);
	put64(result, state.base_term);
	put64(result, state.hard.term);
	put64(result, state.hard.voted_for.value_or(0));
	put64(result, state.hard.commit_index);
	put32(result, static_cast<std::uint32_t>(state.entries.size()));
	for (const auto &entry : state.entries) {
		put64(result, entry.index);
		put64(result, entry.term);
		result.push_back(static_cast<char>(entry.kind));
		put32(result, static_cast<std::uint32_t>(entry.payload.size()));
		result.append(entry.payload);
	}
	if (result.size() != encoded_size) {
		throw std::length_error("checkpoint exceeds encoded bound");
	}
	return result;
}

std::size_t checks = 0;
void require(bool condition, const char *message) {
	++checks;
	if (!condition) {
		throw std::runtime_error(message);
	}
}
struct CountedSource {
	const RecoveredState *state;
	std::size_t *accesses;
	const RecoveredState &metadata() const { return *state; }
	std::size_t size() const { return state->entries.size(); }
	const Entry &entry(std::size_t index) const {
		++*accesses;
		return state->entries.at(index);
	}
};
RecoveredState fixture(std::size_t count, std::size_t payload_size) {
	RecoveredState state;
	state.configuration.local = 1;
	state.configuration.voters = {1, 2, 3};
	state.hard = {2, 1, 1};
	state.base_index = state.applied_index = 1;
	state.base_term = 1;
	for (std::size_t i = 0; i < count; ++i) {
		state.entries.push_back(
			{i + 2, 2, payload_size ? EntryKind::Command : EntryKind::NoOp, std::string(payload_size, 'x')});
	}
	return state;
}
void compare(std::size_t count, std::size_t payload_size, std::size_t capacity,
			 std::optional<std::uint32_t> format) {
	auto state = fixture(count, payload_size);
	kronuz::journal::ArtifactDescriptor application{{}, 17, 123};
	auto expected = original_encoding(state, 7, application, {}, format);
	require(encode_checkpoint(state, 7, application, {}, format) == expected,
			"materializing adapter preserves independent format bytes");
	std::size_t accesses = 0;
	IncrementalCheckpointEncoder encoder(CountedSource{&state, &accesses}, 7, application, Limits{}, format);
	require(accesses == 0, "constructor scans no retained entry");
	std::array<char, 65536> buffer{};
	std::string actual;
	while (!encoder.done()) {
		auto before = accesses;
		auto count = encoder.next(std::span(buffer).first(capacity), 7);
		require(count > 0 && count <= capacity, "bounded encoder makes progress");
		require(accesses - before <= 8, "record budget bounds empty-entry work");
		actual.append(buffer.data(), count);
	}
	require(actual == expected, "incremental bytes equal independent original encoder");
	require(encoder.emitted_bytes() == expected.size(), "exact encoded byte count");
	auto decoded = decode_checkpoint(actual, state.configuration, 7, application);
	require(decoded.state.entries == state.entries && decoded.application_format == format,
			"existing recovery decodes both formats");
	require(encoder.next(buffer) == 0, "completed encoder emits nothing");
}
void frozen_capabilities() {
	auto state = fixture(4, 17);
	state.hard.commit_index = 4;
	Limits limits;
	limits.rpc_entries = 2;
	Core core(state.configuration, state, limits);
	core.step(Start{});
	core.step(Applied{3});
	auto result = core.step_capture(
		LocalCheckpoint{9, 7, 2, 2, state.configuration.cluster, state.configuration.configuration});
	require(result.capture && result.capture->size() == 3,
			"trusted capture retains full suffix without copying");
	require(std::get<PersistCheckpoint>(result.actions.back()).state.entries.empty(),
			"trusted persistence action materializes no suffix");
	auto token = result.capture->token();
	core.step(Persisted{token + 1});
	core.step(Applied{4});
	require(result.capture->entry(0).index == 3 && core.applied() == 4,
			"frozen range survives stale persistence and reliable Applied");
	core.step(Persisted{token});
	bool rejected = false;
	try {
		result.capture->entry(0);
	} catch (const std::logic_error &) {
		rejected = true;
	}
	require(rejected, "matching publication invalidates captured range");
	std::optional<Core::FrozenCheckpoint> escaped;
	{
		Core owner(state.configuration, state, limits);
		owner.step(Start{});
		owner.step(Applied{3});
		auto captured = owner.step_capture(
			LocalCheckpoint{9, 7, 2, 2, state.configuration.cluster, state.configuration.configuration});
		escaped = std::move(captured.capture);
	}
	rejected = false;
	try {
		escaped->metadata();
	} catch (const std::logic_error &) {
		rejected = true;
	}
	require(rejected, "destroyed owner invalidates escaped capture without dangling access");
}

void invalid_suffix() {
	auto state = fixture(20, 0);
	state.entries.back().index++;
	kronuz::journal::ArtifactDescriptor application{};
	IncrementalCheckpointEncoder encoder(BorrowedCheckpointState{&state}, 1, application);
	std::array<char, 256> buffer{};
	bool rejected = false;
	try {
		while (!encoder.done()) {
			encoder.next(buffer, 3);
		}
	} catch (const kronuz::journal::Corruption &) {
		rejected = true;
	}
	require(rejected && !encoder.done(), "late malformed entry poisons unpublished encoder");
	rejected = false;
	try {
		encoder.next(buffer);
	} catch (const std::logic_error &) {
		rejected = true;
	}
	require(rejected, "failed encoder cannot resume");
}
} // namespace
int main() {
	try {
		for (auto format : {std::optional<std::uint32_t>{}, std::optional<std::uint32_t>{1}}) {
			for (auto capacity : {std::size_t{1}, std::size_t{8}, std::size_t{65536}}) {
				compare(2, 21, capacity, format);
				compare(0, 0, capacity, format);
			}
			for (auto entries : {std::size_t{64}, std::size_t{4096}, std::size_t{65535}}) {
				compare(entries, 0, 65536, format);
			}
			compare(1, 1024 * 1024, 65536, format);
		}
		invalid_suffix();
		frozen_capabilities();
		std::cout << checks << " incremental checkpoint checks passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
