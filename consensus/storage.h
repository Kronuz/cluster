#pragma once

#include "types.h"
#include "../journal/codec.h"
#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>

namespace cluster::consensus {

namespace storage_detail {
using kronuz::journal::Corruption;
using kronuz::journal::get32;
using kronuz::journal::get64;
using kronuz::journal::put32;
using kronuz::journal::put64;
constexpr std::uint64_t initial_magic = 0x3154494e49544652ull;
constexpr std::uint64_t batch_magic = 0x3148435442544652ull;
inline void identity(std::string& bytes, Identity value) { bytes.append(value.data(), value.size()); }
inline Identity identity(std::string_view& bytes) {
	if (bytes.size() < 16) { throw Corruption("truncated consensus identity"); }
	Identity result{};
	std::copy_n(bytes.begin(), result.size(), result.begin()); bytes.remove_prefix(result.size()); return result;
}
inline bool flag(std::string_view& bytes) {
	if (bytes.empty() || static_cast<unsigned char>(bytes[0]) > 1) { throw Corruption("invalid consensus flag"); }
	bool result = bytes[0] != 0; bytes.remove_prefix(1); return result;
}
inline void end(std::string_view bytes) { if (!bytes.empty()) { throw Corruption("trailing consensus storage bytes"); } }
} // namespace storage_detail

inline std::string encode_initialization(const FixedConfiguration& configuration) {
	using namespace storage_detail;
	if (configuration.voters.size() > 31) { throw std::length_error("too many voters"); }
	std::string result;
	put64(result, initial_magic); identity(result, configuration.cluster); identity(result, configuration.configuration);
	put64(result, configuration.local); put32(result, static_cast<std::uint32_t>(configuration.voters.size()));
	for (auto voter : configuration.voters) { put64(result, voter); }
	return result;
}

inline std::string encode_storage_batch(const StorageBatch& batch) {
	using namespace storage_detail;
	std::string result; put64(result, batch_magic);
	result.push_back(batch.hard ? 1 : 0);
	if (batch.hard) {
		put64(result, batch.hard->term); put64(result, batch.hard->voted_for.value_or(0)); put64(result, batch.hard->commit_index);
	}
	result.push_back(batch.log ? 1 : 0);
	if (batch.log) {
		if (batch.log->entries.size() > std::numeric_limits<std::uint32_t>::max()) { throw std::length_error("storage entry count overflow"); }
		put64(result, batch.log->replace_from); put32(result, static_cast<std::uint32_t>(batch.log->entries.size()));
		for (const auto& entry : batch.log->entries) {
			if (entry.payload.size() > std::numeric_limits<std::uint32_t>::max()) { throw std::length_error("storage payload overflow"); }
			put64(result, entry.index); put64(result, entry.term); result.push_back(static_cast<char>(entry.kind));
			put32(result, static_cast<std::uint32_t>(entry.payload.size())); result.append(entry.payload);
		}
	}
	return result;
}

// Replay into unpublished state. Call finish only after Journal::recover has
// successfully returned the exact verified storage sequence.
class Recovery {
public:
	Recovery(FixedConfiguration configuration, Limits limits = {}) : expected_(std::move(configuration)), limits_(limits) {
		std::set<NodeId> voters(expected_.voters.begin(), expected_.voters.end());
		if (expected_.local == 0 || expected_.voters.empty() || voters.contains(0) || !voters.contains(expected_.local) ||
			voters.size() != expected_.voters.size() || voters.size() > limits.voters || limits.voters > 31 ||
			limits.command_bytes == 0 || limits.rpc_entries == 0 || limits.rpc_bytes < limits.command_bytes ||
			limits.log_bytes < limits.rpc_bytes || limits.log_entries == 0) { throw std::invalid_argument("invalid storage recovery configuration or limits"); }
		state_.configuration = expected_;
	}
	void replay(std::uint64_t sequence, std::string_view bytes) {
		using namespace storage_detail;
		if (finished_ || failed_) { throw std::logic_error("recovery unavailable"); }
		try {
			if (sequence_ == std::numeric_limits<std::uint64_t>::max() || sequence != sequence_ + 1) { throw Corruption("storage sequence mismatch"); }
			if (sequence == 1) { initialization(bytes); }
			else { apply(decode(bytes)); }
			sequence_ = sequence;
		} catch (...) { failed_ = true; throw; }
	}
	RecoveredState finish(std::uint64_t verified_sequence) {
		if (failed_ || finished_ || sequence_ == 0 || verified_sequence != sequence_) { throw std::logic_error("recovery has no verified initialized frontier"); }
		finished_ = true; return std::move(state_);
	}

private:
	void initialization(std::string_view bytes) {
		using namespace storage_detail;
		if (bytes.size() > 52 + 31 * 8 || get64(bytes) != initial_magic) { throw Corruption("invalid consensus initialization"); }
		FixedConfiguration configuration;
		configuration.cluster = identity(bytes); configuration.configuration = identity(bytes); configuration.local = get64(bytes);
		auto count = get32(bytes);
		if (count > limits_.voters || count > bytes.size() / 8) { throw Corruption("initialization voter count overflow"); }
		for (std::uint32_t i = 0; i < count; ++i) { configuration.voters.push_back(get64(bytes)); }
		end(bytes);
		if (configuration != expected_) { throw Corruption("stored fixed configuration does not match"); }
	}
	StorageBatch decode(std::string_view bytes) {
		using namespace storage_detail;
		// Metadata has its own bound; payload accounting below is independent.
		if (limits_.rpc_entries > (std::numeric_limits<std::size_t>::max() - 46) / 21 ||
			limits_.rpc_bytes > std::numeric_limits<std::size_t>::max() - 46 - limits_.rpc_entries * 21 ||
			bytes.size() > 46 + limits_.rpc_entries * 21 + limits_.rpc_bytes || get64(bytes) != batch_magic) {
			throw Corruption("invalid consensus storage batch size or version");
		}
		StorageBatch batch;
		if (flag(bytes)) {
			HardState hard; hard.term = get64(bytes); auto vote = get64(bytes); hard.commit_index = get64(bytes);
			if (vote) { hard.voted_for = vote; } batch.hard = hard;
		}
		if (flag(bytes)) {
			LogMutation mutation; mutation.replace_from = get64(bytes); auto count = get32(bytes);
			if (count > limits_.rpc_entries || count > bytes.size() / 21) { throw Corruption("storage entry count exceeds bound"); }
			std::size_t total = 0;
			for (std::uint32_t i = 0; i < count; ++i) {
				Entry entry; entry.index = get64(bytes); entry.term = get64(bytes);
				if (bytes.empty() || static_cast<unsigned char>(bytes[0]) > static_cast<unsigned char>(EntryKind::NoOp)) { throw Corruption("invalid storage entry kind"); }
				entry.kind = static_cast<EntryKind>(bytes[0]); bytes.remove_prefix(1);
				auto length = get32(bytes);
				if (length > limits_.command_bytes || length > limits_.rpc_bytes - total || length > bytes.size()) { throw Corruption("storage payload exceeds bound"); }
				entry.payload.assign(bytes.substr(0, length)); bytes.remove_prefix(length); total += length;
				mutation.entries.push_back(std::move(entry));
			}
			batch.log = std::move(mutation);
		}
		end(bytes);
		if (!batch.hard && !batch.log) { throw Corruption("empty consensus storage operation"); }
		return batch;
	}
	void apply(StorageBatch batch) {
		using storage_detail::Corruption;
		auto hard = batch.hard.value_or(state_.hard);
		if (hard.term < state_.hard.term || hard.commit_index < state_.hard.commit_index ||
			(hard.voted_for && std::find(expected_.voters.begin(), expected_.voters.end(), *hard.voted_for) == expected_.voters.end()) ||
			(hard.term == 0 && hard.voted_for) ||
			(hard.term == state_.hard.term && state_.hard.voted_for && hard.voted_for != state_.hard.voted_for)) {
			throw Corruption("invalid durable hard-state transition");
		}
		auto new_count = state_.entries.size(); auto new_bytes = bytes_;
		if (batch.log) {
			auto from = batch.log->replace_from;
			if (from == 0 || from <= state_.hard.commit_index || from - 1 > state_.entries.size() ||
				batch.log->entries.size() > limits_.log_entries - static_cast<std::size_t>(from - 1)) { throw Corruption("invalid logical log replacement"); }
			new_count = static_cast<std::size_t>(from - 1) + batch.log->entries.size();
			for (std::size_t i = static_cast<std::size_t>(from - 1); i < state_.entries.size(); ++i) { new_bytes -= state_.entries[i].payload.size(); }
			Term term = from == 1 ? 0 : state_.entries[from - 2].term;
			Index index = from;
			for (const auto& entry : batch.log->entries) {
				if (entry.index != index++ || entry.index >= std::numeric_limits<Index>::max() || entry.term == 0 || entry.term < term ||
					entry.term > hard.term || (entry.kind == EntryKind::NoOp && !entry.payload.empty()) ||
					entry.payload.size() > limits_.log_bytes - new_bytes) { throw Corruption("invalid logical replacement entry"); }
				if (entry.index <= state_.entries.size() && state_.entries[entry.index - 1].term == entry.term && state_.entries[entry.index - 1] != entry) {
					throw Corruption("same-index same-term storage content conflict");
				}
				new_bytes += entry.payload.size(); term = entry.term;
			}
		}
		if (hard.commit_index > new_count) { throw Corruption("commit beyond recovered log"); }
		if (batch.log) {
			state_.entries.resize(static_cast<std::size_t>(batch.log->replace_from - 1));
			for (auto& entry : batch.log->entries) { state_.entries.push_back(std::move(entry)); }
		}
		state_.hard = hard; bytes_ = new_bytes;
	}

	FixedConfiguration expected_;
	Limits limits_;
	RecoveredState state_;
	std::uint64_t sequence_ = 0;
	std::size_t bytes_ = 0;
	bool failed_ = false, finished_ = false;
};

} // namespace cluster::consensus
