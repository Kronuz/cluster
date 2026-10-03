#pragma once

#include "io.h"
#include <limits>

namespace kronuz::journal {

struct InventoryStats {
	std::uint64_t entries = 0, logical_bytes = 0, directories = 0, missing_entries = 0;
	std::optional<std::uint64_t> allocated_bytes = 0;
	bool complete = false, incomplete = false, overflow = false, failed = false;
	bool ready() const noexcept { return complete && !incomplete && !overflow && !failed; }
};

// A flat-store census, not a quota enforcer. Run after successful recovery,
// holding its stable owner lock, with ALL storage and namespace mutations
// paused until completion. The IO
// cursor must enumerate each direct entry exactly once while quiescent.
// IO outlives this object. No entry names or inode table are retained.
class Inventory {
public:
	explicit Inventory(IO& io) : io_(io) {}
	Inventory(const Inventory&) = delete;
	Inventory& operator=(const Inventory&) = delete;
	const InventoryStats& stats() const noexcept { return stats_; }
	InventoryStats step(std::size_t budget = 128) {
		if (budget == 0 || budget > 4096) { throw std::invalid_argument("invalid inventory scan budget"); }
		if (stats_.failed) { throw std::logic_error("inventory failed; restart while storage is quiescent"); }
		if (stats_.complete) { return stats_; }
		try {
			if (!cursor_) { cursor_ = io_.scan_directory(); }
			for (std::size_t scanned = 0; scanned < budget; ++scanned) {
				auto name = cursor_->next();
				if (!name) { stats_.complete = true; cursor_.reset(); break; }
				add(stats_.entries, 1);
				auto footprint = io_.entry_footprint(*name);
				if (!footprint) { add(stats_.missing_entries, 1); stats_.incomplete = true; continue; }
				add(stats_.logical_bytes, footprint->logical_bytes);
				if (stats_.allocated_bytes) {
					if (footprint->allocated_bytes) { add(*stats_.allocated_bytes, *footprint->allocated_bytes); }
					else { stats_.allocated_bytes.reset(); }
				}
				if (footprint->kind == EntryKind::Directory) { add(stats_.directories, 1); stats_.incomplete = true; }
			}
			return stats_;
		} catch (...) { stats_.failed = true; cursor_.reset(); throw; }
	}
private:
	void add(std::uint64_t& total, std::uint64_t amount) noexcept {
		if (amount > std::numeric_limits<std::uint64_t>::max() - total) {
			total = std::numeric_limits<std::uint64_t>::max(); stats_.overflow = true;
		} else { total += amount; }
	}
	IO& io_;
	std::unique_ptr<DirectoryCursor> cursor_;
	InventoryStats stats_;
};

} // namespace kronuz::journal
