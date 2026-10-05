#pragma once

#include "journal.h"
#include "admission.h"

namespace kronuz::journal {

enum class ArtifactPart { Application, Bundle };
class Store;
class StoreAppend;
class AppendReservation {
public:
	AppendReservation(AppendReservation&&) noexcept = default;
	AppendReservation& operator=(AppendReservation&&) noexcept = default;
	AppendReservation(const AppendReservation&) = delete;
	AppendReservation& operator=(const AppendReservation&) = delete;
private:
	friend class Store;
	friend class StoreAppend;
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

// An admitted append owns its accounting permit through every suspension.
// Only the storage owner applies completions and settles the final frontier.
class StoreAppend final : public IOOperation {
public:
	~StoreAppend() override {
		if (started_ && !finished_) { journal_->fence_storage(); }
	}
	const MutationRequest& request() const override { return append_->request(); }
	void submitted() override {
		if (finished_ || append_->in_flight()) { throw std::logic_error("store append cannot submit"); }
		if (!started_) { reservation_.permit_.mark_started(); started_ = true; }
		append_->submitted();
	}
	bool complete(MutationCompletion completion) noexcept override {
		if (!append_->complete(std::move(completion))) { return false; }
		if (append_->done()) {
			try {
				append_->result();
				result_ = journal_->finish_append(append_);
				reservation_.permit_.settle(plan_.added); reservation_.owner_.reset();
			} catch (...) {
				error_ = std::current_exception(); journal_->fence_storage(); reservation_.permit_.abandon();
			}
			finished_ = true;
		}
		return true;
	}
	bool done() const noexcept override { return finished_; }
	bool in_flight() const noexcept override { return append_->in_flight(); }
	IO& io() const noexcept override { return append_->io(); }
	const Frontier& result() const {
		if (!finished_) { throw std::logic_error("store append has not completed"); }
		if (error_) { std::rethrow_exception(error_); }
		return *result_;
	}
private:
	friend class Store;
	StoreAppend(std::shared_ptr<Journal> journal, std::shared_ptr<AppendMutation> append,
		AppendReservation&& reservation, MutationPlan plan)
		: journal_(std::move(journal)), append_(std::move(append)), reservation_(std::move(reservation)), plan_(plan) {}
	std::shared_ptr<Journal> journal_;
	std::shared_ptr<AppendMutation> append_;
	AppendReservation reservation_;
	MutationPlan plan_;
	std::optional<Frontier> result_;
	std::exception_ptr error_;
	bool started_ = false, finished_ = false;
};

namespace detail {
struct StoreReplacement {
	StoreReplacement(StoragePermit value, std::uint64_t identity, std::uint64_t app, std::uint64_t bundle)
		: permit(std::move(value)), token(identity), caps{app, bundle} {}
	void dispose_known(Journal& journal) {
		if (disposed) { return; }
		StorageResources staged;
		for (unsigned index = 0; index < 2; ++index) {
			if (created[index]) { staged = resources_add(staged, journal.artifact_footprint(lengths[index])); }
		}
		builder.reset(); prepared[0].reset(); prepared[1].reset();
		if (created[0] || created[1]) { permit->settle(staged); }
		permit.reset(); disposed = true;
	}
	std::optional<StoragePermit> permit;
	std::uint64_t token, active_job = 0, next_job = 0;
	std::array<std::uint64_t, 2> caps, lengths{};
	std::array<bool, 2> created{};
	unsigned active = 0;
	std::optional<ArtifactBuilder> builder;
	std::array<std::optional<PreparedArtifact>, 2> prepared;
	bool detached = false, disposed = false;
};
class ReplacementJobLease {
public:
	explicit ReplacementJobLease(std::shared_ptr<StoreReplacement> cycle) : cycle_(std::move(cycle)) {
		if (cycle_->active_job || cycle_->detached || cycle_->disposed) { throw std::logic_error("replacement job unavailable"); }
		if (cycle_->next_job == std::numeric_limits<std::uint64_t>::max()) { throw std::overflow_error("replacement job sequence exhausted"); }
		token_ = ++cycle_->next_job; cycle_->active_job = token_;
	}
	ReplacementJobLease(const ReplacementJobLease&) = delete;
	ReplacementJobLease& operator=(const ReplacementJobLease&) = delete;
	ReplacementJobLease(ReplacementJobLease&& other) noexcept : cycle_(std::move(other.cycle_)), token_(other.token_) {}
	~ReplacementJobLease() { release(); }
	StoreReplacement& cycle() const noexcept { return *cycle_; }
	void release() noexcept { if (cycle_ && cycle_->active_job == token_) { cycle_->active_job = 0; } }
private:
	std::shared_ptr<StoreReplacement> cycle_;
	std::uint64_t token_;
};
} // namespace detail

// One bounded artifact quantum retains its whole replacement reservation.
// Phase visibility and accounting change only on terminal owner completion.
class StoreArtifactOperation final : public IOOperation {
public:
	StoreArtifactOperation(const StoreArtifactOperation&) = delete;
	StoreArtifactOperation& operator=(const StoreArtifactOperation&) = delete;
	~StoreArtifactOperation() override {
		if (started_ && !finished_) { journal_->fence_storage(); lease_.cycle().permit->abandon(); }
		if (!finished_ && !started_ && lease_.cycle().detached) {
			try { lease_.cycle().dispose_known(*journal_); }
			catch (...) { journal_->fence_storage(); lease_.cycle().permit->abandon(); }
		}
	}
	const MutationRequest& request() const override { return operation_->request(); }
	void submitted() override {
		if (done() || in_flight()) { throw std::logic_error("replacement primitive unavailable"); }
		if (!started_) { lease_.cycle().permit->mark_started(); started_ = true; }
		operation_->submitted();
	}
	bool complete(MutationCompletion completion) noexcept override {
		if (!operation_->complete(std::move(completion))) { return false; }
		if (operation_->done()) { settle(); }
		return true;
	}
	bool done() const noexcept override { return finished_; }
	bool in_flight() const noexcept override { return operation_->in_flight(); }
	IO& io() const noexcept override { return operation_->io(); }
	void result() const {
		if (!finished_) { throw std::logic_error("replacement quantum has not completed"); }
		if (error_) { std::rethrow_exception(error_); }
	}
	ArtifactDescriptor descriptor() const {
		result();
		if (kind_ != Kind::Seal) { throw std::logic_error("replacement quantum is not sealing"); }
		return descriptor_;
	}
private:
	friend class Store;
	enum class Kind { Create, Chunk, Seal };
	StoreArtifactOperation(std::shared_ptr<Journal> journal, std::shared_ptr<detail::StoreReplacement> cycle,
		std::shared_ptr<ArtifactMutation> operation, Kind kind, unsigned index, std::size_t bytes = 0)
		: journal_(std::move(journal)), operation_(std::move(operation)), lease_(std::move(cycle)), kind_(kind), index_(index), bytes_(bytes) {
		if (operation_->done()) { settle(); } // Empty payload quantum performs no IO.
	}
	void settle() noexcept {
		auto& cycle = lease_.cycle();
		try {
			operation_->result();
			switch (kind_) {
			case Kind::Create: cycle.builder.emplace(operation_->take_builder()); cycle.active = index_; cycle.created[index_] = true; break;
			case Kind::Chunk: cycle.lengths[index_] += bytes_; break;
			case Kind::Seal:
				cycle.prepared[index_].emplace(operation_->prepared_result()); descriptor_ = cycle.prepared[index_]->descriptor(); cycle.builder.reset(); break;
			}
			if (cycle.detached) { cycle.dispose_known(*journal_); }
		} catch (...) { error_ = std::current_exception(); journal_->fence_storage(); cycle.permit->abandon(); }
		finished_ = true; lease_.release();
	}
	std::shared_ptr<Journal> journal_;
	std::shared_ptr<ArtifactMutation> operation_;
	detail::ReplacementJobLease lease_;
	Kind kind_;
	unsigned index_;
	std::size_t bytes_;
	ArtifactDescriptor descriptor_{};
	std::exception_ptr error_;
	bool started_ = false, finished_ = false;
};

// Publication remains irreversible after submission. Detached jobs finish
// the complete frontier protocol; they never become canceled preparation.
class StorePublication final : public IOOperation {
public:
	StorePublication(const StorePublication&) = delete;
	StorePublication& operator=(const StorePublication&) = delete;
	~StorePublication() override {
		if (!finished_ && (started_ || lease_.cycle().detached)) { journal_->fence_storage(); lease_.cycle().permit->abandon(); }
	}
	const MutationRequest& request() const override { return operation_->request(); }
	void submitted() override {
		if (done() || in_flight()) { throw std::logic_error("publication primitive unavailable"); }
		if (!started_) { lease_.cycle().permit->mark_started(); started_ = true; }
		operation_->submitted();
	}
	bool complete(MutationCompletion completion) noexcept override {
		if (!operation_->complete(std::move(completion))) { return false; }
		if (operation_->done()) {
			auto& cycle = lease_.cycle();
			try {
				operation_->result(); result_.emplace(journal_->finish_checkpoint(operation_));
				cycle.permit->settle(plan_.added, plan_.removed); cycle.permit.reset();
				cycle.builder.reset(); cycle.prepared[0].reset(); cycle.prepared[1].reset(); cycle.disposed = true;
			} catch (...) { error_ = std::current_exception(); journal_->fence_storage(); cycle.permit->abandon(); }
			finished_ = true; lease_.release();
		}
		return true;
	}
	bool done() const noexcept override { return finished_; }
	bool in_flight() const noexcept override { return operation_->in_flight(); }
	IO& io() const noexcept override { return operation_->io(); }
	const Frontier& result() const {
		if (!finished_) { throw std::logic_error("publication has not completed"); }
		if (error_) { std::rethrow_exception(error_); }
		return *result_;
	}
private:
	friend class Store;
	StorePublication(std::shared_ptr<Journal> journal, std::shared_ptr<detail::StoreReplacement> cycle,
		std::shared_ptr<CheckpointMutation> operation, MutationPlan plan)
		: journal_(std::move(journal)), operation_(std::move(operation)), lease_(std::move(cycle)), plan_(plan) {}
	std::shared_ptr<Journal> journal_;
	std::shared_ptr<CheckpointMutation> operation_;
	detail::ReplacementJobLease lease_;
	MutationPlan plan_;
	std::optional<Frontier> result_;
	std::exception_ptr error_;
	bool started_ = false, finished_ = false;
};

// Single ordered storage executor. IO outlives Store and escaped readers and
// is used EXCLUSIVELY through Store while its session is open. No mutable
// Journal, artifact builder, or prepared handle escapes this boundary.
class Store {
public:
	Store(IO& io, AdmissionLimits limits, std::size_t maximum_batch = 64u * 1024 * 1024,
		std::uint64_t maximum_artifact = 512ull * 1024 * 1024)
		: io_(io), limits_(limits), journal_(std::make_shared<Journal>(io, maximum_batch, maximum_artifact)) {
		InventoryStats empty; empty.complete = true;
		Admission validate(empty, limits); // Reject configuration before IO.
	}
	Store(std::shared_ptr<IO> io, AdmissionLimits limits, std::size_t maximum_batch = 64u * 1024 * 1024,
		std::uint64_t maximum_artifact = 512ull * 1024 * 1024)
		: io_(owned_io(io)), limits_(limits), journal_(std::make_shared<Journal>(std::move(io), maximum_batch, maximum_artifact)) {
		InventoryStats empty; empty.complete = true;
		Admission validate(empty, limits);
	}
	Store(const Store&) = delete;
	Store& operator=(const Store&) = delete;
	~Store() {
		if (replacement_ && replacement_->active_job) { replacement_->detached = true; replacement_.reset(); }
		if (replacement_ && replacement_->disposed) { replacement_.reset(); }
		if (replacement_) {
			try { cancel_replacement(ReplacementId(owner_, replacement_->token)); }
			catch (...) { fence(); replacement_.reset(); }
		}
	}
	bool fenced() const noexcept { return failed_ || journal_->fenced(); }
	void fence_storage() noexcept { fence(); }
	std::optional<AdmissionStats> accounting() const noexcept {
		if (!admission_) { return std::nullopt; }
		return admission_->stats();
	}
	InventoryStats inventory_stats() const {
		if (!inventory_) { throw std::logic_error("inventory not started"); }
		return inventory_->stats();
	}
	Frontier frontier() const { healthy(); return journal_->frontier(); }
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
			cursor.reset(); auto result = journal_->create(identity);
			opened_ = true; inventory_.emplace(io_); return result;
		} catch (...) { fence(); throw; }
	}
	template <class Replay, class Restore> Frontier recover(Replay replay, Restore restore) {
		unopened();
		try {
			auto result = journal_->recover(std::move(replay), std::move(restore));
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
		auto permit = admission_->reserve(kind, journal_->append_plan(maximum_encoded_bytes).peak);
		if (!permit) { return std::nullopt; }
		return AppendReservation(owner_, std::move(*permit), maximum_encoded_bytes);
	}
	Frontier append(AppendReservation& reservation, std::string_view bytes) {
		ready();
		if (reservation.owner_ != owner_ || bytes.size() > reservation.bound_) { throw std::invalid_argument("foreign or oversized append reservation"); }
		auto operation = start_append(std::move(reservation), bytes, false);
		try {
			drive_synchronously(*operation); return operation->result();
		} catch (...) { fence(); throw; }
	}
	std::shared_ptr<StoreAppend> begin_append(AppendReservation&& reservation, std::string_view bytes) {
		return start_append(std::move(reservation), bytes, true);
	}
	std::optional<ReplacementId> reserve_replacement(std::uint64_t application_cap, std::uint64_t bundle_cap) {
		ready(); if (replacement_ && replacement_->disposed) { replacement_.reset(); } if (replacement_) { return std::nullopt; }
		if (next_replacement_ == std::numeric_limits<std::uint64_t>::max()) { throw std::overflow_error("replacement token exhausted"); }
		std::array<std::uint64_t, 2> caps{bundle_cap, application_cap};
		auto permit = admission_->reserve(AdmissionClass::Replacement, journal_->checkpoint_plan(caps).peak);
		if (!permit) { return std::nullopt; }
		replacement_ = std::make_shared<Replacement>(std::move(*permit), ++next_replacement_, application_cap, bundle_cap);
		return ReplacementId(owner_, replacement_->token);
	}
	std::shared_ptr<StoreArtifactOperation> begin_artifact_operation(const ReplacementId& id, ArtifactPart part) {
		auto& cycle = replacement(id); auto index = part_index(part);
		if (cycle.builder || cycle.created[index] || (index == 1 && !cycle.prepared[0])) { throw std::logic_error("invalid artifact preparation order"); }
		auto operation = journal_->begin_artifact_preparation();
		return std::shared_ptr<StoreArtifactOperation>(new StoreArtifactOperation(journal_, replacement_, std::move(operation), StoreArtifactOperation::Kind::Create, index));
	}
	std::shared_ptr<StoreArtifactOperation> begin_write_chunk(const ReplacementId& id, std::string_view bytes) {
		auto& cycle = replacement(id);
		if (!cycle.builder) { throw std::logic_error("artifact not active"); }
		auto index = cycle.active;
		if (bytes.size() > detail::artifact_chunk_size || bytes.size() > cycle.caps[index] - cycle.lengths[index]) { throw std::length_error("replacement chunk or payload exceeds reservation"); }
		auto operation = cycle.builder->begin_append_chunk(bytes);
		return std::shared_ptr<StoreArtifactOperation>(new StoreArtifactOperation(journal_, replacement_, std::move(operation), StoreArtifactOperation::Kind::Chunk, index, bytes.size()));
	}
	std::shared_ptr<StoreArtifactOperation> begin_finish_artifact(const ReplacementId& id) {
		auto& cycle = replacement(id);
		if (!cycle.builder) { throw std::logic_error("artifact not active"); }
		auto operation = cycle.builder->begin_finish();
		return std::shared_ptr<StoreArtifactOperation>(new StoreArtifactOperation(journal_, replacement_, std::move(operation), StoreArtifactOperation::Kind::Seal, cycle.active));
	}
	void begin_artifact(const ReplacementId& id, ArtifactPart part) {
		mutation_ready();
		auto& operation = replacement(id); auto index = part_index(part);
		if (operation.builder || operation.created[index] || (index == 1 && !operation.prepared[0])) {
			throw std::logic_error("invalid artifact preparation order");
		}
		operation.permit->mark_started();
		try {
			operation.builder.emplace(journal_->prepare_artifact()); operation.active = index; operation.created[index] = true;
		} catch (...) { fence(); throw; }
	}
	void write_chunk(const ReplacementId& id, std::string_view bytes) {
		mutation_ready();
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
		mutation_ready();
		auto& operation = replacement(id);
		if (!operation.builder) { throw std::logic_error("artifact not active"); }
		try {
			auto index = operation.active; operation.prepared[index].emplace(operation.builder->finish()); operation.builder.reset();
			return operation.prepared[index]->descriptor();
		} catch (...) { fence(); throw; }
	}
	std::optional<ArtifactVerifier> begin_artifact_verification(const ReplacementId& id, ArtifactPart part) {
		auto& operation = replacement(id); auto index = part_index(part);
		if (!operation.prepared[index]) { throw std::logic_error("artifact not sealed"); }
		try { return journal_->begin_artifact_verification(*operation.prepared[index]); }
		catch (...) { if (journal_->fenced()) { fence(); } throw; }
	}
	std::optional<PublishedArtifactSelection> select_published_dependency(std::size_t index) const {
		ready(); return journal_->select_published_dependency(index);
	}
	std::optional<ArtifactVerifier> begin_published_verification(const PublishedArtifactSelection& selection) {
		ready();
		try { return journal_->begin_published_verification(selection); }
		catch (...) { if (journal_->fenced()) { fence(); } throw; }
	}
	void verify_artifact(const ReplacementId& id, ArtifactPart part) {
		auto& operation = replacement(id); auto index = part_index(part);
		if (!operation.prepared[index]) { throw std::logic_error("artifact not sealed"); }
		try { journal_->verify_artifact(*operation.prepared[index]); } catch (...) { fence(); throw; }
	}
	Frontier publish(const ReplacementId& id, std::uint64_t expected_sequence) {
		auto operation = start_publication(id, expected_sequence, false);
		try { drive_synchronously(*operation); auto result = operation->result(); replacement_.reset(); return result; }
		catch (...) { fence(); throw; }
	}
	std::shared_ptr<StorePublication> begin_publication(const ReplacementId& id, std::uint64_t expected_sequence) {
		return start_publication(id, expected_sequence, true);
	}
	void cancel_replacement(const ReplacementId& id) {
		auto& operation = replacement(id); operation.dispose_known(*journal_); replacement_.reset();
	}
	ReclaimStats reclaim_step(std::size_t budget = 128) {
		mutation_ready();
		healthy(); if (!opened_) { throw std::logic_error("store not recovered"); }
		if (budget == 0 || budget > 4096) { throw std::invalid_argument("invalid reclamation scan budget"); }
		if (!admission_) { inventory_.reset(); } // Mutation invalidates partial census.
		try {
			auto result = journal_->reclaim_step(budget);
			if (result.logical_bytes_saturated) { throw std::overflow_error("reclamation credit overflow"); }
			if (admission_) { admission_->credit_durable_reclaim({result.logical_bytes, result.removed}); }
			else { inventory_.emplace(io_); }
			return result;
		} catch (...) { fence(); throw; }
	}
private:
	static IO& owned_io(const std::shared_ptr<IO>& io) {
		if (!io) { throw std::invalid_argument("null owned store IO"); } return *io;
	}
	std::shared_ptr<StoreAppend> start_append(AppendReservation&& reservation, std::string_view bytes, bool asynchronous) {
		ready();
		if (reservation.owner_ != owner_ || bytes.size() > reservation.bound_) { throw std::invalid_argument("foreign or oversized append reservation"); }
		auto plan = journal_->append_plan(bytes.size());
		auto append = journal_->start_append(bytes, asynchronous);
		return std::shared_ptr<StoreAppend>(new StoreAppend(journal_, std::move(append), std::move(reservation), plan));
	}
	std::shared_ptr<StorePublication> start_publication(const ReplacementId& id, std::uint64_t expected_sequence, bool asynchronous) {
		mutation_ready(); auto& cycle = replacement(id); auto current = journal_->frontier();
		if (cycle.builder || !cycle.prepared[0] || !cycle.prepared[1] || expected_sequence != current.sequence || current.generation == std::numeric_limits<std::uint64_t>::max()) { throw std::invalid_argument("unfinished or stale replacement"); }
		std::array<std::uint64_t, 2> lengths{cycle.lengths[1], cycle.lengths[0]}; auto plan = journal_->checkpoint_plan(lengths);
		auto publication = journal_->start_checkpoint(*cycle.prepared[1], std::span<const PreparedArtifact>(&*cycle.prepared[0], 1), expected_sequence, asynchronous);
		return std::shared_ptr<StorePublication>(new StorePublication(journal_, replacement_, std::move(publication), plan));
	}
	using Replacement = detail::StoreReplacement;
	static unsigned part_index(ArtifactPart part) {
		if (part != ArtifactPart::Application && part != ArtifactPart::Bundle) { throw std::invalid_argument("invalid artifact part"); }
		return part == ArtifactPart::Application ? 0 : 1;
	}
	Replacement& replacement(const ReplacementId& id) {
		ready();
		if (id.owner_ != owner_ || !replacement_ || id.token_ != replacement_->token || replacement_->disposed) { throw std::invalid_argument("stale or foreign replacement"); }
		if (replacement_->active_job) { throw std::logic_error("replacement quantum is pending"); }
		return *replacement_;
	}
	void unopened() const { healthy(); if (opened_) { throw std::logic_error("store already opened"); } }
	void healthy() const { if (fenced()) { throw std::logic_error("store fenced; close and recover"); } }
	void mutation_ready() const { ready(); if (journal_->mutation_pending()) { throw std::logic_error("storage mutation is pending"); } }
	void ready() const { healthy(); if (!admission_) { throw std::logic_error("storage inventory not ready"); } }
	void fence() noexcept { failed_ = true; journal_->fence_storage(); if (admission_) { admission_->taint(); } }
	IO& io_;
	AdmissionLimits limits_;
	std::shared_ptr<char> owner_ = std::make_shared<char>();
	std::shared_ptr<Journal> journal_;
	std::optional<Inventory> inventory_;
	std::unique_ptr<Admission> admission_;
	std::shared_ptr<Replacement> replacement_;
	std::uint64_t next_replacement_ = 0;
	bool opened_ = false, failed_ = false;
};
} // namespace kronuz::journal
