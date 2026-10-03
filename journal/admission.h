#pragma once

#include "inventory.h"
#include <algorithm>
#include <memory>
#include <utility>

namespace kronuz::journal {

struct StorageResources {
	std::uint64_t logical_bytes = 0, entries = 0;
	bool operator==(const StorageResources&) const = default;
};
enum class AdmissionClass { Normal, Control, Replacement };
struct AdmissionLimits {
	StorageResources hard, control_pool, replacement_pool;
	std::size_t maximum_tickets = 64;
};
struct AdmissionStats {
	StorageResources used, control_available, replacement_available, outstanding;
	std::size_t tickets = 0;
	bool tainted = false, normal_ready = false, over_limit = false;
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
inline StorageResources resources_subtract(StorageResources a, StorageResources b) noexcept {
	return {a.logical_bytes - b.logical_bytes, a.entries - b.entries};
}
struct AdmissionState {
	AdmissionLimits limits;
	AdmissionStats value;
	StorageResources control_held, replacement_held;
	bool replacement_active = false;
	StorageResources free() const {
		auto charged = resources_add(resources_add(value.used, value.outstanding), resources_add(value.control_available, value.replacement_available));
		if (!resources_fit(charged, limits.hard)) { return {}; }
		return resources_subtract(limits.hard, charged);
	}
	void refill() {
		for (auto pair : {std::pair{&value.control_available, resources_subtract(limits.control_pool, control_held)}, std::pair{&value.replacement_available, resources_subtract(limits.replacement_pool, replacement_held)}}) {
			auto available = free(); auto needed = resources_subtract(pair.second, *pair.first);
			*pair.first = resources_add(*pair.first, {std::min(available.logical_bytes, needed.logical_bytes), std::min(available.entries, needed.entries)});
		}
	}
	AdmissionStats stats() const noexcept {
		auto result = value;
		result.over_limit = !resources_fit(value.used, limits.hard);
		result.normal_ready = !value.tainted && !result.over_limit && value.control_available == limits.control_pool && value.replacement_available == limits.replacement_pool;
		return result;
	}
};
} // namespace detail

class Admission;
// Move-only permits retain their ledger state. All operations, including
// destruction, run on the owning storage executor. No IO happens here.
class StoragePermit {
public:
	StoragePermit(const StoragePermit&) = delete;
	StoragePermit& operator=(const StoragePermit&) = delete;
	StoragePermit(StoragePermit&& other) noexcept
		: state_(std::move(other.state_)), peak_(other.peak_), class_(other.class_), started_(other.started_) {}
	StoragePermit& operator=(StoragePermit&& other) noexcept {
		if (this != &other) { release(); state_ = std::move(other.state_); peak_ = other.peak_; class_ = other.class_; started_ = other.started_; }
		return *this;
	}
	~StoragePermit() { release(); }
	StorageResources peak() const noexcept { return peak_; }
	void mark_started() {
		if (!state_ || state_->value.tainted) { throw std::logic_error("storage permit unavailable"); }
		started_ = true;
	}
	// added/removed describe final charged additions and already durable
	// removals, not cumulative transient creation traffic. Reserve the worst
	// simultaneous additions BEFORE IO. Removed resources must be charged
	// existing resources, and are never credited twice.
	void settle(StorageResources added, StorageResources removed = {}) {
		if (!state_ || !started_ || state_->value.tainted) { throw std::logic_error("storage permit cannot settle"); }
		if (!detail::resources_fit(added, peak_) || !detail::resources_fit(removed, state_->value.used)) {
			state_->value.tainted = true; throw std::invalid_argument("settlement exceeds storage reservation or charged removals");
		}
		auto state = state_;
		try {
			state_->value.used = detail::resources_add(detail::resources_subtract(state_->value.used, removed), added);
			refund(detail::resources_subtract(peak_, added));
			finish(); state_.reset(); state->refill();
		} catch (...) { state->value.tainted = true; throw; }
	}
	// Explicit uncertainty, or destruction after mark_started without settle,
	// prevents further admission. Reserved capacity is not silently refunded.
	void abandon() noexcept { if (state_) { started_ = true; release(); } }
private:
	friend class Admission;
	StoragePermit(std::shared_ptr<detail::AdmissionState> state, StorageResources peak, AdmissionClass kind)
		: state_(std::move(state)), peak_(peak), class_(kind) {}
	void refund(StorageResources unused) {
		if (class_ == AdmissionClass::Control) {
			state_->control_held = detail::resources_subtract(state_->control_held, peak_);
			state_->value.control_available = detail::resources_add(state_->value.control_available, unused);
		}
		if (class_ == AdmissionClass::Replacement) {
			state_->replacement_held = detail::resources_subtract(state_->replacement_held, peak_);
			state_->value.replacement_available = detail::resources_add(state_->value.replacement_available, unused);
		}
		state_->value.outstanding = detail::resources_subtract(state_->value.outstanding, peak_);
	}
	void finish() noexcept {
		--state_->value.tickets;
		if (class_ == AdmissionClass::Replacement) { state_->replacement_active = false; }
	}
	void release() noexcept {
		if (!state_) { return; }
		if (started_) { state_->value.tainted = true; }
		else {
			try { refund(peak_); state_->refill(); }
			catch (...) { state_->value.tainted = true; }
		}
		finish(); state_.reset();
	}
	std::shared_ptr<detail::AdmissionState> state_;
	StorageResources peak_;
	AdmissionClass class_;
	bool started_ = false;
};

// Accounting only: the host must use permits for EVERY mutation and fence
// Journal on actual IO failure. Rebuild after close/recovery/quiescent census;
// never reset a tainted live ledger or credit undurable/unperformed deletion.
class Admission {
public:
	Admission(const InventoryStats& inventory, AdmissionLimits limits) {
		if (!inventory.ready() || limits.maximum_tickets < 3 || limits.maximum_tickets > 4096 ||
			!detail::resources_fit(detail::resources_add(limits.control_pool, limits.replacement_pool), limits.hard)) {
			throw std::invalid_argument("unready census or invalid storage admission limits");
		}
		state_ = std::make_shared<detail::AdmissionState>(); state_->limits = limits;
		state_->value.used = {inventory.logical_bytes, inventory.entries}; state_->refill();
	}
	Admission(const Admission&) = delete;
	Admission& operator=(const Admission&) = delete;
	AdmissionStats stats() const noexcept { return state_->stats(); }
	std::optional<StoragePermit> reserve(AdmissionClass kind, StorageResources peak) {
		if (kind != AdmissionClass::Normal && kind != AdmissionClass::Control && kind != AdmissionClass::Replacement) {
			throw std::invalid_argument("invalid storage admission class");
		}
		if (peak == StorageResources{} || state_->value.tainted || state_->value.tickets == state_->limits.maximum_tickets) { return std::nullopt; }
		// Protect permit slots as well as bytes/entries. Normal leaves slots
		// for both protected classes; control cannot steal replacement's slot.
		if ((kind == AdmissionClass::Normal && state_->value.tickets >= state_->limits.maximum_tickets - 2) ||
			(kind == AdmissionClass::Control && state_->value.tickets >= state_->limits.maximum_tickets - (state_->replacement_active ? 0 : 1))) { return std::nullopt; }
		auto available = kind == AdmissionClass::Normal ? state_->free() :
			(kind == AdmissionClass::Control ? state_->value.control_available : state_->value.replacement_available);
		if ((kind == AdmissionClass::Normal && !state_->stats().normal_ready) ||
			(kind == AdmissionClass::Replacement && state_->replacement_active) || !detail::resources_fit(peak, available)) { return std::nullopt; }
		state_->value.outstanding = detail::resources_add(state_->value.outstanding, peak);
		if (kind == AdmissionClass::Control) {
			state_->value.control_available = detail::resources_subtract(available, peak);
			state_->control_held = detail::resources_add(state_->control_held, peak);
		}
		if (kind == AdmissionClass::Replacement) {
			state_->value.replacement_available = detail::resources_subtract(available, peak);
			state_->replacement_held = detail::resources_add(state_->replacement_held, peak); state_->replacement_active = true;
		}
		++state_->value.tickets; return StoragePermit(state_, peak, kind);
	}
	void credit_durable_reclaim(StorageResources removed) {
		if (state_->value.tainted) { throw std::logic_error("storage admission tainted"); }
		if (!detail::resources_fit(removed, state_->value.used)) {
			state_->value.tainted = true; throw std::invalid_argument("reclamation exceeds charged storage");
		}
		state_->value.used = detail::resources_subtract(state_->value.used, removed); state_->refill();
	}
	void taint() noexcept { state_->value.tainted = true; }
private:
	std::shared_ptr<detail::AdmissionState> state_;
};

} // namespace kronuz::journal
