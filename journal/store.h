#pragma once

#include "journal.h"
#include "admission.h"

namespace kronuz::journal {

enum class ArtifactPart { Application, Bundle };
class Store;
class AppendReservation {
public:
	AppendReservation(AppendReservation&&) noexcept = default;
	AppendReservation& operator=(AppendReservation&&) noexcept = default;
	AppendReservation(const AppendReservation&) = delete;
	AppendReservation& operator=(const AppendReservation&) = delete;
private:
	friend class Store;
	AppendReservation(std::shared_ptr<char> owner, StoragePermit permit, std::size_t bound)
		: owner_(std::move(owner)), permit_(std::move(permit)), bound_(bound) {}
	std::shared_ptr<char> owner_;
	StoragePermit permit_;
	std::size_t bound_;
};
class ReplacementId {
public:
	ReplacementId(const ReplacementId&) = default;
	ReplacementId& operator=(const ReplacementId&) = default;
private:
	friend class Store;
	ReplacementId(std::shared_ptr<char> owner, std::uint64_t token) : owner_(std::move(owner)), token_(token) {}
	std::shared_ptr<char> owner_;
	std::uint64_t token_;
};

// Single ordered storage executor. IO outlives Store and escaped readers and
// is used EXCLUSIVELY through Store while its session is open. No mutable
// Journal, artifact builder, or prepared handle escapes this boundary.
class Store {
public:
	Store(IO& io, AdmissionLimits limits, std::size_t maximum_batch = 64u * 1024 * 1024,
		std::uint64_t maximum_artifact = 512ull * 1024 * 1024)
		: io_(io), limits_(limits), journal_(io, maximum_batch, maximum_artifact) {
		InventoryStats empty; empty.complete = true;
		Admission validate(empty, limits); // Reject configuration before IO.
	}
	Store(const Store&) = delete;
	Store& operator=(const Store&) = delete;
	~Store() {
		if (replacement_) {
			try { cancel_replacement(ReplacementId(owner_, replacement_->token)); }
			catch (...) { fence(); replacement_.reset(); }
		}
	}
	bool fenced() const noexcept { return failed_ || journal_.fenced(); }
	void fence_storage() noexcept { fence(); }
	std::optional<AdmissionStats> accounting() const noexcept {
		if (!admission_) { return std::nullopt; }
		return admission_->stats();
	}
	InventoryStats inventory_stats() const {
		if (!inventory_) { throw std::logic_error("inventory not started"); }
		return inventory_->stats();
	}
	Frontier frontier() const { healthy(); return journal_.frontier(); }
	Frontier create(Identity identity) {
		unopened();
		if (!detail::resources_fit(Journal::bootstrap_plan().peak, limits_.hard)) {
			throw std::length_error("bootstrap exceeds storage quota");
		}
		try {
			// Trusted caller excludes external mutations during bootstrap. Reject
			// every existing name before the first owned namespace change.
			auto cursor = io_.scan_directory();
			if (cursor->next()) { throw std::invalid_argument("bootstrap requires an empty directory"); }
			cursor.reset(); auto result = journal_.create(identity);
			opened_ = true; inventory_.emplace(io_); return result;
		} catch (...) { fence(); throw; }
	}
	template <class Replay, class Restore> Frontier recover(Replay replay, Restore restore) {
		unopened();
		try {
			auto result = journal_.recover(std::move(replay), std::move(restore));
			opened_ = true; inventory_.emplace(io_); return result;
		} catch (...) { fence(); throw; }
	}
	template <class Replay> Frontier recover(Replay replay) {
		return recover(std::move(replay), [](const Frontier&, ArtifactReader&, std::span<ArtifactReader>) {
			throw Corruption("checkpoint restore callback required");
		});
	}
	InventoryStats inventory_step(std::size_t budget = 128) {
		healthy();
		if (budget == 0 || budget > 4096) { throw std::invalid_argument("invalid inventory scan budget"); }
		if (!inventory_) { throw std::logic_error("store not recovered"); }
		if (admission_) { return inventory_->stats(); }
		try {
			auto result = inventory_->step(budget);
			if (result.ready()) { admission_ = std::make_unique<Admission>(result, limits_); }
			return result;
		} catch (...) { fence(); throw; }
	}
	std::optional<AppendReservation> reserve_append(AdmissionClass kind, std::size_t maximum_encoded_bytes) {
		ready();
		if (kind == AdmissionClass::Replacement) { throw std::invalid_argument("append requires normal or control admission"); }
		auto permit = admission_->reserve(kind, journal_.append_plan(maximum_encoded_bytes).peak);
		if (!permit) { return std::nullopt; }
		return AppendReservation(owner_, std::move(*permit), maximum_encoded_bytes);
	}
	Frontier append(AppendReservation& reservation, std::string_view bytes) {
		ready();
		if (reservation.owner_ != owner_ || bytes.size() > reservation.bound_) { throw std::invalid_argument("foreign or oversized append reservation"); }
		auto plan = journal_.append_plan(bytes.size());
		reservation.permit_.mark_started();
		try {
			auto result = journal_.append_batch(bytes); reservation.permit_.settle(plan.added);
			reservation.owner_.reset(); return result;
		} catch (...) { fence(); throw; }
	}
	std::optional<ReplacementId> reserve_replacement(std::uint64_t application_cap, std::uint64_t bundle_cap) {
		ready(); if (replacement_) { return std::nullopt; }
		if (next_replacement_ == std::numeric_limits<std::uint64_t>::max()) { throw std::overflow_error("replacement token exhausted"); }
		std::array<std::uint64_t, 2> caps{bundle_cap, application_cap};
		auto permit = admission_->reserve(AdmissionClass::Replacement, journal_.checkpoint_plan(caps).peak);
		if (!permit) { return std::nullopt; }
		replacement_ = std::make_unique<Replacement>(std::move(*permit), ++next_replacement_, application_cap, bundle_cap);
		return ReplacementId(owner_, replacement_->token);
	}
	void begin_artifact(const ReplacementId& id, ArtifactPart part) {
		auto& operation = replacement(id); auto index = part_index(part);
		if (operation.builder || operation.created[index] || (index == 1 && !operation.prepared[0])) {
			throw std::logic_error("invalid artifact preparation order");
		}
		operation.permit.mark_started();
		try {
			operation.builder.emplace(journal_.prepare_artifact()); operation.active = index; operation.created[index] = true;
		} catch (...) { fence(); throw; }
	}
	void write_chunk(const ReplacementId& id, std::string_view bytes) {
		auto& operation = replacement(id);
		if (!operation.builder) { throw std::logic_error("artifact not active"); }
		auto index = operation.active;
		if (bytes.size() > detail::artifact_chunk_size || bytes.size() > operation.caps[index] - operation.lengths[index]) {
			throw std::length_error("replacement chunk or payload exceeds reservation");
		}
		try { operation.builder->append_chunk(bytes); operation.lengths[index] += bytes.size(); }
		catch (...) { fence(); throw; }
	}
	ArtifactDescriptor finish_artifact(const ReplacementId& id) {
		auto& operation = replacement(id);
		if (!operation.builder) { throw std::logic_error("artifact not active"); }
		try {
			auto index = operation.active; operation.prepared[index].emplace(operation.builder->finish()); operation.builder.reset();
			return operation.prepared[index]->descriptor();
		} catch (...) { fence(); throw; }
	}
	void verify_artifact(const ReplacementId& id, ArtifactPart part) {
		auto& operation = replacement(id); auto index = part_index(part);
		if (!operation.prepared[index]) { throw std::logic_error("artifact not sealed"); }
		try { journal_.verify_artifact(*operation.prepared[index]); } catch (...) { fence(); throw; }
	}
	Frontier publish(const ReplacementId& id, std::uint64_t expected_sequence) {
		auto& operation = replacement(id); auto current = journal_.frontier();
		if (operation.builder || !operation.prepared[0] || !operation.prepared[1] || expected_sequence != current.sequence ||
			current.generation == std::numeric_limits<std::uint64_t>::max()) { throw std::invalid_argument("unfinished or stale replacement"); }
		std::array<std::uint64_t, 2> lengths{operation.lengths[1], operation.lengths[0]};
		auto plan = journal_.checkpoint_plan(lengths);
		try {
			auto result = journal_.publish_checkpoint(*operation.prepared[1], std::span<const PreparedArtifact>(&*operation.prepared[0], 1), expected_sequence);
			operation.permit.settle(plan.added, plan.removed); replacement_.reset(); return result;
		} catch (...) { fence(); throw; }
	}
	void cancel_replacement(const ReplacementId& id) {
		auto& operation = replacement(id); StorageResources staged;
		for (unsigned index = 0; index < 2; ++index) {
			if (operation.created[index]) { staged = detail::resources_add(staged, journal_.artifact_footprint(operation.lengths[index])); }
		}
		operation.builder.reset(); operation.prepared[0].reset(); operation.prepared[1].reset();
		if (operation.created[0] || operation.created[1]) { operation.permit.settle(staged); }
		replacement_.reset(); // No IO started: ordinary pre-IO cancellation.
	}
	ReclaimStats reclaim_step(std::size_t budget = 128) {
		healthy(); if (!opened_) { throw std::logic_error("store not recovered"); }
		if (budget == 0 || budget > 4096) { throw std::invalid_argument("invalid reclamation scan budget"); }
		if (!admission_) { inventory_.reset(); } // Mutation invalidates partial census.
		try {
			auto result = journal_.reclaim_step(budget);
			if (result.logical_bytes_saturated) { throw std::overflow_error("reclamation credit overflow"); }
			if (admission_) { admission_->credit_durable_reclaim({result.logical_bytes, result.removed}); }
			else { inventory_.emplace(io_); }
			return result;
		} catch (...) { fence(); throw; }
	}
private:
	struct Replacement {
		Replacement(StoragePermit value, std::uint64_t identity, std::uint64_t app, std::uint64_t bundle)
			: permit(std::move(value)), token(identity), caps{app, bundle} {}
		StoragePermit permit;
		std::uint64_t token;
		std::array<std::uint64_t, 2> caps, lengths{};
		std::array<bool, 2> created{};
		unsigned active = 0;
		std::optional<ArtifactBuilder> builder;
		std::array<std::optional<PreparedArtifact>, 2> prepared;
	};
	static unsigned part_index(ArtifactPart part) {
		if (part != ArtifactPart::Application && part != ArtifactPart::Bundle) { throw std::invalid_argument("invalid artifact part"); }
		return part == ArtifactPart::Application ? 0 : 1;
	}
	Replacement& replacement(const ReplacementId& id) {
		ready();
		if (id.owner_ != owner_ || !replacement_ || id.token_ != replacement_->token) { throw std::invalid_argument("stale or foreign replacement"); }
		return *replacement_;
	}
	void unopened() const { healthy(); if (opened_) { throw std::logic_error("store already opened"); } }
	void healthy() const { if (fenced()) { throw std::logic_error("store fenced; close and recover"); } }
	void ready() const { healthy(); if (!admission_) { throw std::logic_error("storage inventory not ready"); } }
	void fence() noexcept { failed_ = true; journal_.fence_storage(); if (admission_) { admission_->taint(); } }
	IO& io_;
	AdmissionLimits limits_;
	std::shared_ptr<char> owner_ = std::make_shared<char>();
	Journal journal_;
	std::optional<Inventory> inventory_;
	std::unique_ptr<Admission> admission_;
	std::unique_ptr<Replacement> replacement_;
	std::uint64_t next_replacement_ = 0;
	bool opened_ = false, failed_ = false;
};
} // namespace kronuz::journal
