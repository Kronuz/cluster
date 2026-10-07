#pragma once

#include "checkpoint_format.h"
#include <span>

namespace cluster::consensus {

// Borrowed sources must remain immutable until encoding finishes. Accessors
// validate their own capability on every access; no Entry reference escapes
// one synchronous next() call. Owned IO must copy the returned bytes.
struct BorrowedCheckpointState {
	const RecoveredState *state;
	const RecoveredState &metadata() const { return *state; }
	std::size_t size() const { return state->entries.size(); }
	const Entry &entry(std::size_t index) const { return state->entries.at(index); }
};

template <class Source> class IncrementalCheckpointEncoder {
  public:
	IncrementalCheckpointEncoder(Source source, std::uint64_t sequence,
								 const kronuz::journal::ArtifactDescriptor &application, Limits limits = {},
								 std::optional<std::uint32_t> format = {})
		: source_(std::move(source)), limits_(limits), maximum_(checkpoint_detail::maximum_size(limits)) {
		using namespace storage_detail;
		if (!sequence) {
			throw Corruption("invalid checkpoint storage sequence");
		}
		const auto &state = source_.metadata();
		count_ = source_.size();
		Recovery validator(state.configuration, limits);
		validator.validate_checkpoint_metadata(state, count_);
		if (count_ > std::numeric_limits<std::uint32_t>::max()) {
			throw std::length_error("checkpoint entry count overflow");
		}
		previous_ = state.base_term;
		auto initialization = encode_initialization(state.configuration);
		put64(header_, format ? checkpoint_detail::magic_v2 : checkpoint_detail::magic);
		if (format) {
			put32(header_, *format);
		}
		put64(header_, sequence);
		identity(header_, application.identity);
		put64(header_, application.length);
		put32(header_, application.checksum);
		put32(header_, static_cast<std::uint32_t>(initialization.size()));
		header_.append(initialization);
		put64(header_, state.base_index);
		put64(header_, state.base_term);
		put64(header_, state.hard.term);
		put64(header_, state.hard.voted_for.value_or(0));
		put64(header_, state.hard.commit_index);
		put32(header_, static_cast<std::uint32_t>(count_));
		if (header_.size() > maximum_ || count_ > (maximum_ - header_.size()) / 21) {
			throw std::length_error("checkpoint exceeds encoded bound");
		}
	}

	// Separate record and byte budgets bound empty-entry validation as well as
	// copying large payloads. Even a one-byte destination makes progress.
	std::size_t next(std::span<char> destination, std::size_t record_budget = 256) {
		if (failed_) {
			throw std::logic_error("checkpoint encoder failed");
		}
		if (destination.empty() || record_budget == 0) {
			throw std::invalid_argument("empty checkpoint encoding budget");
		}
		try {
			std::size_t copied = 0, records = 0;
			auto copy = [&](std::string_view bytes, std::size_t &offset) {
				auto count = std::min(bytes.size() - offset, destination.size() - copied);
				if (count > maximum_ - emitted_) {
					throw std::length_error("checkpoint exceeds encoded bound");
				}
				std::copy_n(bytes.data() + offset, count, destination.data() + copied);
				offset += count;
				copied += count;
				emitted_ += count;
			};
			if (!header_.empty()) {
				copy(header_, header_offset_);
				if (header_offset_ != header_.size()) {
					return copied;
				}
				header_.clear();
			}
			while (copied < destination.size() && index_ < count_) {
				const auto &state = source_.metadata();
				if (!active_ && records == record_budget) {
					break;
				}
				const auto &entry = source_.entry(index_);
				if (!active_) {
					++records;
					if (entry.index != state.base_index + index_ + 1 || !entry.term ||
						entry.term < previous_ || entry.term > state.hard.term ||
						entry.payload.size() > limits_.command_bytes ||
						entry.payload.size() > limits_.log_bytes - payload_bytes_ ||
						entry.payload.size() > std::numeric_limits<std::uint32_t>::max() ||
						(entry.kind != EntryKind::Command && entry.kind != EntryKind::NoOp) ||
						(entry.kind == EntryKind::NoOp && !entry.payload.empty())) {
						throw storage_detail::Corruption("invalid consensus checkpoint suffix");
					}
					payload_bytes_ += entry.payload.size();
					previous_ = entry.term;
					storage_detail::put64(record_, entry.index);
					storage_detail::put64(record_, entry.term);
					record_.push_back(static_cast<char>(entry.kind));
					storage_detail::put32(record_, static_cast<std::uint32_t>(entry.payload.size()));
					active_ = true;
				}
				copy(record_, record_offset_);
				if (record_offset_ != record_.size()) {
					break;
				}
				copy(entry.payload, payload_offset_);
				if (payload_offset_ != entry.payload.size()) {
					break;
				}
				++index_;
				active_ = false;
				record_.clear();
				record_offset_ = payload_offset_ = 0;
			}
			return copied;
		} catch (...) {
			failed_ = true;
			throw;
		}
	}
	bool done() const noexcept { return !failed_ && header_.empty() && index_ == count_; }
	std::size_t emitted_bytes() const noexcept { return emitted_; }

  private:
	Source source_;
	Limits limits_;
	std::size_t maximum_, count_ = 0, index_ = 0, payload_bytes_ = 0, emitted_ = 0;
	Term previous_ = 0;
	std::string header_, record_;
	std::size_t header_offset_ = 0, record_offset_ = 0, payload_offset_ = 0;
	bool active_ = false, failed_ = false;
};

} // namespace cluster::consensus
