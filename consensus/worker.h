#pragma once

#include "core.h"
#include "admission.h"
#include "checkpoint.h"
#include "../journal/store.h"

namespace cluster::consensus {

enum class SubmitResult { Accepted, Busy, Pressure, Fenced };
enum class TurnResult { Idle, Blocked, Pressure, Inventory, Stored, Completed, Maintenance, Fenced };
struct WorkerLimits { std::size_t foreground_burst = 16, scan_entries = 128; };
struct CaptureMetadata { RequestId request; Index through; Term term; };
enum class CancelResult { Canceled, TooLate, Stale };
class Worker;
class CheckpointId {
public:
	CheckpointId(const CheckpointId&) = default;
	CheckpointId& operator=(const CheckpointId&) = default;
private:
	friend class Worker;
	CheckpointId(std::shared_ptr<char> owner, Token token) : owner_(std::move(owner)), token_(token) {}
	std::shared_ptr<char> owner_;
	Token token_;
};

// One owning executor. IO is exclusive to this worker and outlives it.
// There is no input queue; retryable work remains with the caller.
class Worker {
public:
	Worker(kronuz::journal::IO& io, FixedConfiguration configuration, kronuz::journal::AdmissionLimits admission,
		Limits limits = {}, Timing timing = {}, WorkerLimits scheduling = {})
		: configuration_(std::move(configuration)), limits_(limits), timing_(timing), scheduling_(scheduling),
		store_(io, admission, batch_limit(configuration_, limits)) {
		Core validate(configuration_, RecoveredState{configuration_, {}, 0, 0, {}, 0}, limits_, timing_);
		if (!scheduling_.foreground_burst || scheduling_.foreground_burst > 1048576 || !scheduling_.scan_entries || scheduling_.scan_entries > 4096 ||
			admission.control_slots < 3 || admission.maximum_tickets < 5) { throw std::invalid_argument("invalid worker scheduling or control slots"); }
		auto tick = plan_event(Tick{}, false, limits_);
		kronuz::journal::StorageResources pack{};
		for (std::size_t i = 0; i < tick.count; ++i) { pack = kronuz::journal::detail::resources_add(pack, kronuz::journal::Journal::append_footprint(tick.appends[i].encoded_bytes).peak); }
		auto rpc = kronuz::journal::Journal::append_footprint(storage_batch_size(true, true, limits_.rpc_entries)).peak;
		pack.logical_bytes = std::max(pack.logical_bytes, rpc.logical_bytes);
		if (!kronuz::journal::detail::resources_fit(pack, admission.control_pool)) { throw std::invalid_argument("worker control pool cannot fund continuation pack"); }
		// Checked bounds for one complete Core action batch, including internal
		// persistence and checkpoint actions. No unbounded output queue exists.
		output_count_ = checked_add(checked_multiply(limits_.reads, 2), checked_add(checked_multiply(limits_.voters, 4), 16));
		output_payload_ = checked_add(limits_.log_bytes, checked_multiply(checked_add(limits_.voters, 2), limits_.rpc_bytes));
		output_entries_ = checked_add(limits_.log_entries, checked_multiply(checked_add(limits_.voters, 2), limits_.rpc_entries));
	}
	Worker(const Worker&) = delete;
	Worker& operator=(const Worker&) = delete;
	void create(kronuz::journal::Identity identity) {
		unopened();
		try {
			store_.create(identity); store_.inventory_step(scheduling_.scan_entries);
			// A new directory has only the bounded bootstrap names; a tiny
			// configured scan budget can require several startup slices.
			initialization_ = encode_initialization(configuration_);
			core_ = std::make_unique<Core>(configuration_, RecoveredState{configuration_, {}, 0, 0, {}, 0}, limits_, timing_);
		} catch (const std::exception& error) { fail(error.what()); throw; }
	}
	template <class Restore> void recover(Restore restore_application) {
		unopened();
		try {
			Recovery recovery(configuration_, limits_);
			auto frontier = store_.recover([&](auto sequence, auto bytes) { recovery.replay(sequence, bytes); },
				[&](const auto& selected, auto& bundle, auto dependencies) {
					if (dependencies.size() != 1 || selected.dependencies.size() != 1 || bundle.descriptor().length > checkpoint_detail::maximum_size(limits_)) {
						throw kronuz::journal::Corruption("invalid worker checkpoint dependencies or bundle bound");
					}
					std::string bytes(static_cast<std::size_t>(bundle.descriptor().length), '\0');
					for (std::size_t offset = 0; offset < bytes.size();) {
						auto count = std::min(bytes.size() - offset, std::size_t{65536});
						offset += bundle.read_at(offset, std::span<char>(bytes.data() + offset, count));
					}
					auto decoded = decode_checkpoint(bytes, configuration_, selected.base_sequence, selected.dependencies[0], limits_);
					recovery.restore(std::move(decoded.state), decoded.storage_sequence);
					restore_application(dependencies[0]); // Build unpublished application state.
				});
			if (frontier.sequence == 0 && frontier.base_sequence == 0 && frontier.version == 1 && !frontier.checkpoint && frontier.dependencies.empty()) {
				// Generic bootstrap is already durable; typed initialization may
				// have been queued when the previous process exited.
				initialization_ = encode_initialization(configuration_);
				core_ = std::make_unique<Core>(configuration_, RecoveredState{configuration_, {}, 0, 0, {}, 0}, limits_, timing_);
			} else { core_ = std::make_unique<Core>(configuration_, recovery.finish(frontier.sequence), limits_, timing_); }
		} catch (const std::exception& error) { fail(error.what()); throw; }
	}
	bool ready() const noexcept { return core_ && initialization_.empty() && store_.accounting().has_value() && !fenced(); }
	bool fenced() const noexcept { return failed_ || store_.fenced(); }
	Role role() const noexcept { return core_ ? core_->role() : Role::Follower; }
	bool busy() const noexcept { return core_ && core_->busy(); }
	Term term() const noexcept { return core_ ? core_->term() : 0; }
	Index committed() const noexcept { return core_ ? core_->committed() : 0; }
	Index applied_index() const noexcept { return core_ ? core_->applied() : 0; }
	Index base_index() const noexcept { return core_ ? core_->base_index() : 0; }
	auto storage_frontier() const { return store_.frontier(); }
	auto accounting() const noexcept { return store_.accounting(); }
	SubmitResult try_submit(Event event) { return submit(std::move(event), false); }
	Actions take_actions() {
		Actions result = std::move(output_); output_.clear();
		for (const auto& action : result) {
			if (auto committed = std::get_if<Committed>(&action)) { delivered_ = committed->entries.back().index; }
		}
		if (result.empty() && terminal_) { result.emplace_back(std::move(*terminal_)); terminal_.reset(); }
		return result;
	}
	void applied(Applied completion) {
		if (fenced() || !core_ || completion.through <= core_->applied()) { return; }
		if (application_ && application_->through == completion.through) { return; }
		if (completion.through != delivered_ || application_) { fail("invalid worker application completion"); return; }
		delivered_ = 0;
		if (core_->busy()) { ingest(core_->step(completion)); }
		else { application_ = completion; }
	}
	void application_failed(Index through, std::string reason) {
		// A stale/duplicate report cannot fence unrelated application work.
		if (!fenced() && core_ && through != 0 && through == delivered_) { fail(std::move(reason)); }
	}
	std::optional<CheckpointId> reserve_checkpoint(std::uint64_t application_cap) {
		if (!ready() || checkpoint_) { return std::nullopt; }
		if (next_checkpoint_ == std::numeric_limits<Token>::max()) { throw std::length_error("checkpoint identity exhausted"); }
		auto replacement = store_.reserve_replacement(application_cap, checkpoint_detail::maximum_size(limits_));
		if (!replacement) { return std::nullopt; }
		auto token = ++next_checkpoint_;
		try { checkpoint_ = std::make_unique<Checkpoint>(*replacement, token, application_cap); }
		catch (...) { store_.cancel_replacement(*replacement); throw; }
		return CheckpointId(checkpoint_owner_, token);
	}
	void attach_capture(const CheckpointId& id, CaptureMetadata capture) {
		auto& operation = checkpoint(id);
		if (operation.phase != Phase::Reserved || !capture.through || !capture.term) { throw std::invalid_argument("invalid checkpoint capture phase or boundary"); }
		operation.capture = capture; operation.phase = Phase::BeginApplication;
	}
	SubmitResult offer_application_chunk(const CheckpointId& id, std::string_view bytes, bool final = false) {
		auto& operation = checkpoint(id);
		if ((operation.phase != Phase::BeginApplication && operation.phase != Phase::Application) || operation.final_offered) {
			throw std::invalid_argument("checkpoint no longer accepts application chunks");
		}
		if (bytes.size() > 65536 || bytes.size() > operation.application_cap - operation.offered_bytes) { throw std::length_error("checkpoint application chunk bound"); }
		if (operation.chunk) { return SubmitResult::Busy; }
		operation.chunk.emplace(); std::copy(bytes.begin(), bytes.end(), operation.chunk->bytes.begin());
		operation.chunk->length = bytes.size(); operation.chunk->final = final;
		operation.offered_bytes += bytes.size(); operation.final_offered = final;
		return SubmitResult::Accepted;
	}
	CancelResult cancel_checkpoint(const CheckpointId& id) {
		if (id.owner_ != checkpoint_owner_ || !checkpoint_ || id.token_ != checkpoint_->token) { return CancelResult::Stale; }
		if (checkpoint_->phase >= Phase::EncodeBundle) { return CancelResult::TooLate; }
		try { store_.cancel_replacement(checkpoint_->replacement); checkpoint_.reset(); cutover_debt_ = false; return CancelResult::Canceled; }
		catch (const std::exception& error) { fail(error.what()); throw; }
	}
	TurnResult run_one(Tick current_time) {
		if (store_.fenced() && !failed_) { fail("storage fenced outside worker"); }
		if (fenced()) { return TurnResult::Fenced; }
		if (!core_) { throw std::logic_error("worker not opened"); }
		try {
			if (!store_.accounting()) { store_.inventory_step(scheduling_.scan_entries); return TurnResult::Inventory; }
			if (!initialization_.empty()) {
				auto permit = store_.reserve_append(kronuz::journal::AdmissionClass::Control, initialization_.size());
				if (!permit) { return TurnResult::Pressure; }
				store_.append(*permit, initialization_); initialization_.clear(); return TurnResult::Stored;
			}
			if (maintenance_due()) {
				if (timer_due_ && !cutover_debt_ && !core_->busy() && output_.empty() && !completion_ && !application_) {
					auto result = submit(current_time, true);
					if (result == SubmitResult::Accepted) { timer_due_ = false; return TurnResult::Idle; }
					if (result == SubmitResult::Fenced) { return TurnResult::Fenced; }
				}
				maintenance(); return fenced() ? TurnResult::Fenced : TurnResult::Maintenance;
			}
			if (persist_) {
				auto encoded = encode_storage_batch(persist_->batch);
				store_.append(*pack_[pack_next_], encoded); pack_[pack_next_++].reset();
				completion_ = Persisted{persist_->token}; persist_.reset(); ++foreground_; return TurnResult::Stored;
			}
			if (!output_.empty()) {
				if (preparation_runnable()) { maintenance(); return fenced() ? TurnResult::Fenced : TurnResult::Maintenance; }
				return TurnResult::Blocked;
			}
			if (application_) {
				auto value = *application_; application_.reset(); ingest(core_->step(value)); ++foreground_; return TurnResult::Completed;
			}
			if (completion_) {
				// Advance time while persistence still owns the Core slot. This
				// cannot campaign and consumes no additional reservation.
				ingest(core_->step(current_time));
				auto value = *completion_; completion_.reset();
				if (checkpoint_ && checkpoint_->phase == Phase::Completion && checkpoint_->persistence_token == value.token) {
					checkpoint_.reset(); cutover_debt_ = false;
				}
				ingest(core_->step(value)); ++foreground_; return TurnResult::Completed;
			}
			if (timer_blocked_ && !cutover_debt_) {
				auto result = submit(current_time, true);
				if (result == SubmitResult::Accepted) { timer_blocked_ = false; timer_due_ = true; return TurnResult::Idle; }
				if (result == SubmitResult::Fenced) { return TurnResult::Fenced; }
				maintenance(); return fenced() ? TurnResult::Fenced : TurnResult::Maintenance;
			}
			if (preparation_runnable()) {
				// Before freeze, sustained preparation cannot postpone protocol
				// timers. Alternate an admitted timer opportunity with maintenance.
				if (!cutover_debt_ && timer_due_) {
					auto result = submit(current_time, true);
					if (result == SubmitResult::Accepted) { timer_due_ = false; return TurnResult::Idle; }
					if (result == SubmitResult::Fenced) { return TurnResult::Fenced; }
				}
				maintenance(); return fenced() ? TurnResult::Fenced : TurnResult::Maintenance;
			}
			auto result = try_submit(current_time);
			return result == SubmitResult::Accepted ? TurnResult::Idle : result == SubmitResult::Pressure ? TurnResult::Pressure :
				result == SubmitResult::Fenced ? TurnResult::Fenced : TurnResult::Blocked;
		} catch (const std::exception& error) { fail(error.what()); return TurnResult::Fenced; }
	}
private:
	SubmitResult submit(Event event, bool maintenance_tick) {
		if (maintenance_tick && !std::holds_alternative<Tick>(event)) { throw std::logic_error("maintenance admission requires Tick"); }
		// Validate even under backpressure; internal events never enter here.
		auto plan = plan_event(event, core_ && core_->busy(), limits_);
		if (core_ && core_->role() == Role::Leader && std::holds_alternative<Tick>(event)) { plan.count = 0; }
		if (store_.fenced() && !failed_) { fail("storage fenced outside worker"); }
		if (fenced()) { return SubmitResult::Fenced; }
		if (!ready() || !output_.empty() || ((maintenance_due() || timer_blocked_) && !maintenance_tick) || cutover_debt_ || core_->busy() || completion_ || application_) { return SubmitResult::Busy; }
		try {
			std::array<std::optional<kronuz::journal::AppendReservation>, 3> acquired;
			for (std::size_t i = 0; i < plan.count; ++i) {
				acquired[i] = store_.reserve_append(plan.appends[i].kind, plan.appends[i].encoded_bytes);
				if (!acquired[i]) { return SubmitResult::Pressure; }
			}
			pack_ = std::move(acquired); pack_count_ = plan.count; pack_next_ = 0;
			ingest(core_->step(std::move(event))); if (!maintenance_tick) { ++foreground_; }
			return fenced() ? SubmitResult::Fenced : SubmitResult::Accepted;
		} catch (const std::exception& error) { fail(error.what()); return SubmitResult::Fenced; }
	}
	enum class Phase { Reserved, BeginApplication, Application, SealApplication, Cutover, EncodeBundle, BeginBundle, Bundle, SealBundle, Publish, Completion };
	struct Chunk { std::array<char, 65536> bytes{}; std::size_t length = 0; bool final = false; };
	struct Checkpoint {
		Checkpoint(kronuz::journal::ReplacementId id, Token identity, std::uint64_t cap)
			: replacement(std::move(id)), token(identity), application_cap(cap) {}
		kronuz::journal::ReplacementId replacement;
		Token token, persistence_token = 0;
		std::uint64_t application_cap, offered_bytes = 0, sequence = 0;
		Phase phase = Phase::Reserved;
		std::optional<CaptureMetadata> capture;
		std::optional<Chunk> chunk;
		std::optional<kronuz::journal::ArtifactDescriptor> application;
		std::optional<PersistCheckpoint> action;
		std::string bundle;
		std::size_t offset = 0;
		bool final_offered = false;
	};
	Checkpoint& checkpoint(const CheckpointId& id) {
		if (fenced()) { throw std::logic_error("worker fenced"); }
		if (id.owner_ != checkpoint_owner_ || !checkpoint_ || id.token_ != checkpoint_->token) { throw std::invalid_argument("stale or foreign checkpoint identity"); }
		return *checkpoint_;
	}
	bool preparation_runnable() const noexcept {
		if (!checkpoint_) { return false; }
		auto phase = checkpoint_->phase;
		if (phase == Phase::Reserved || phase == Phase::Completion) { return false; }
		if (phase == Phase::Application) { return checkpoint_->chunk.has_value(); }
		if (phase == Phase::Cutover) { return !core_->busy() && output_.empty() && !application_ && !completion_; }
		return true;
	}
	void maintenance() {
		auto owed_timer = timer_due_;
		if (preparation_next_ && preparation_runnable()) { prepare_one(); preparation_next_ = false; }
		else { store_.reclaim_step(scheduling_.scan_entries); preparation_next_ = true; }
		foreground_ = 0; // Cutover debt is independent of this fairness counter.
		timer_blocked_ = owed_timer; timer_due_ = true;
	}
	void prepare_one() {
		auto& operation = *checkpoint_;
		using kronuz::journal::ArtifactPart;
		switch (operation.phase) {
		case Phase::BeginApplication:
			store_.begin_artifact(operation.replacement, ArtifactPart::Application); operation.phase = Phase::Application; break;
		case Phase::Application: {
			auto& chunk = *operation.chunk;
			store_.write_chunk(operation.replacement, std::string_view(chunk.bytes.data(), chunk.length));
			if (chunk.final) { operation.phase = Phase::SealApplication; }
			operation.chunk.reset(); break;
		}
		case Phase::SealApplication:
			operation.application = store_.finish_artifact(operation.replacement);
			operation.phase = Phase::Cutover; cutover_debt_ = true; break;
		case Phase::Cutover: {
			auto capture = *operation.capture;
			ingest(core_->step(LocalCheckpoint{capture.request, operation.token, capture.through, capture.term, configuration_.cluster, configuration_.configuration}));
			if (fenced()) { break; }
			if (!core_->busy()) { store_.cancel_replacement(operation.replacement); checkpoint_.reset(); cutover_debt_ = false; }
			break;
		}
		case Phase::EncodeBundle:
			operation.bundle = encode_checkpoint(operation.action->state, operation.sequence, *operation.application, limits_);
			operation.action.reset(); operation.phase = Phase::BeginBundle; break;
		case Phase::BeginBundle:
			store_.begin_artifact(operation.replacement, ArtifactPart::Bundle); operation.phase = Phase::Bundle; break;
		case Phase::Bundle: {
			auto count = std::min(operation.bundle.size() - operation.offset, std::size_t{65536});
			store_.write_chunk(operation.replacement, std::string_view(operation.bundle).substr(operation.offset, count)); operation.offset += count;
			if (operation.offset == operation.bundle.size()) { operation.phase = Phase::SealBundle; }
			break;
		}
		case Phase::SealBundle:
			store_.finish_artifact(operation.replacement); operation.bundle.clear(); operation.phase = Phase::Publish; break;
		case Phase::Publish:
			if (completion_) { throw std::logic_error("checkpoint completion slot occupied"); }
			store_.publish(operation.replacement, operation.sequence); completion_ = Persisted{operation.persistence_token}; operation.phase = Phase::Completion; break;
		default: throw std::logic_error("checkpoint preparation phase not runnable");
		}
	}
	static std::size_t checked_add(std::size_t left, std::size_t right) {
		if (right > std::numeric_limits<std::size_t>::max() - left) { throw std::length_error("worker bound overflow"); }
		return left + right;
	}
	static std::size_t checked_multiply(std::size_t left, std::size_t right) {
		if (left && right > std::numeric_limits<std::size_t>::max() / left) { throw std::length_error("worker bound overflow"); }
		return left * right;
	}
	static std::size_t batch_limit(const FixedConfiguration& configuration, Limits limits) {
		return std::max(encode_initialization(configuration).size(), storage_batch_size(true, true, limits.rpc_entries, limits.rpc_bytes));
	}
	bool maintenance_due() const noexcept { return foreground_ >= scheduling_.foreground_burst; }
	void unopened() const { if (core_ || failed_) { throw std::logic_error("worker already opened or fenced"); } }
	void release_pack() { for (auto& permit : pack_) { permit.reset(); } pack_count_ = pack_next_ = 0; }
	void ingest(Actions actions) {
		std::size_t payload = 0, entries = 0;
		if (actions.size() > output_count_) { throw std::length_error("worker action count bound"); }
		for (const auto& action : actions) {
			auto count_entries = [&](const auto& values) {
				entries = checked_add(entries, values.size());
				for (const auto& entry : values) { payload = checked_add(payload, entry.payload.size()); }
			};
			if (auto value = std::get_if<Persist>(&action)) { if (value->batch.log) { count_entries(value->batch.log->entries); } }
			else if (auto value = std::get_if<Committed>(&action)) { count_entries(value->entries); }
			else if (auto value = std::get_if<Send>(&action)) { if (auto append = std::get_if<AppendRequest>(&value->message)) { count_entries(append->entries); } }
			else if (auto value = std::get_if<PersistCheckpoint>(&action)) { count_entries(value->state.entries); }
		}
		if (payload > output_payload_ || entries > output_entries_) { throw std::length_error("worker action payload bound"); }
		for (auto& action : actions) {
			if (auto value = std::get_if<Persist>(&action)) {
				if (persist_ || completion_ || pack_next_ >= pack_count_ || !pack_[pack_next_]) { throw std::logic_error("persistence lacks preowned reservation"); }
				persist_ = std::move(*value);
			} else if (auto value = std::get_if<Fenced>(&action)) { fail(value->reason); }
			else if (std::holds_alternative<PersistInstall>(action) || std::holds_alternative<ActivateInstall>(action)) {
				throw std::logic_error("snapshot installation requires worker transfer API");
			}
			else if (auto value = std::get_if<PersistCheckpoint>(&action)) {
				if (!checkpoint_ || checkpoint_->phase != Phase::Cutover || value->capture != checkpoint_->token) { throw std::logic_error("checkpoint lacks owned replacement"); }
				checkpoint_->sequence = store_.frontier().sequence; checkpoint_->persistence_token = value->token;
				checkpoint_->action = std::move(*value); checkpoint_->phase = Phase::EncodeBundle;
			}
			else {
				if (!output_.empty()) { throw std::logic_error("worker output batch occupied"); }
				// Build the one output batch below after validating all actions.
			}
		}
		Actions external;
		for (auto& action : actions) {
			if (!std::holds_alternative<Persist>(action) && !std::holds_alternative<PersistCheckpoint>(action) && !std::holds_alternative<Fenced>(action)) { external.push_back(std::move(action)); }
		}
		if (!external.empty()) { output_ = std::move(external); }
		if (!core_->busy()) { release_pack(); }
	}
	void fail(std::string reason) {
		if (failed_) { return; } failed_ = true;
		store_.fence_storage(); if (core_) { core_->step(StorageFault{reason}); }
		persist_.reset(); completion_.reset(); application_.reset(); checkpoint_.reset(); release_pack(); terminal_ = Fenced{std::move(reason)};
	}
	FixedConfiguration configuration_;
	Limits limits_;
	Timing timing_;
	WorkerLimits scheduling_;
	kronuz::journal::Store store_;
	std::unique_ptr<Core> core_;
	std::string initialization_;
	std::array<std::optional<kronuz::journal::AppendReservation>, 3> pack_;
	std::size_t pack_count_ = 0, pack_next_ = 0, foreground_ = 0, output_count_ = 0, output_payload_ = 0, output_entries_ = 0;
	std::optional<Persist> persist_;
	std::optional<Persisted> completion_;
	std::optional<Applied> application_;
	std::optional<Fenced> terminal_;
	std::shared_ptr<char> checkpoint_owner_ = std::make_shared<char>();
	std::unique_ptr<Checkpoint> checkpoint_;
	Token next_checkpoint_ = 0;
	Actions output_;
	Index delivered_ = 0;
	bool failed_ = false, cutover_debt_ = false, preparation_next_ = true, timer_due_ = true, timer_blocked_ = false;
};

} // namespace cluster::consensus
