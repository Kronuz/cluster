#pragma once

#include "core.h"
#include "admission.h"
#include "checkpoint.h"
#include "snapshot.h"
#include "../journal/store.h"

namespace cluster::consensus {

enum class SubmitResult { Accepted, Busy, Pressure, Fenced };
enum class TurnResult { Idle, Blocked, Pressure, Inventory, Stored, Completed, Maintenance, Fenced };
struct WorkerLimits { std::size_t foreground_burst = 16, scan_entries = 128; };
struct CaptureMetadata { RequestId request; Index through; Term term; };
struct SnapshotLimits { std::uint32_t application_format; std::uint64_t application_bytes; };
struct SnapshotContext { NodeId authenticated_peer; Term leader_term; RequestId request; Token transfer; SnapshotDescriptor descriptor; };
enum class ValidationAck { Accepted, Stale, NotReady, Fenced };
enum class SnapshotReason { Canceled, InvalidImage, InvalidApplication, Rejected, Installed };
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
class SnapshotId {
public:
	SnapshotId(const SnapshotId&) = default;
	SnapshotId& operator=(const SnapshotId&) = default;
	bool operator==(const SnapshotId&) const = default;
private:
	friend class Worker;
	SnapshotId(std::shared_ptr<char> owner, Token token) : owner_(std::move(owner)), token_(token) {}
	std::shared_ptr<char> owner_;
	Token token_;
};
class SourceId {
public:
	SourceId(const SourceId&) = default;
	SourceId& operator=(const SourceId&) = default;
	bool operator==(const SourceId&) const = default;
private:
	friend class Worker;
	SourceId(std::shared_ptr<char> owner, Token token) : owner_(std::move(owner)), token_(token) {}
	std::shared_ptr<char> owner_; Token token_;
};
struct SourceInfo { SourceId source; NodeId peer; SnapshotKey key; SnapshotDescriptor descriptor; };
struct SourceChunk { SourceId source; Token chunk; std::uint64_t offset; std::span<const char> bytes; bool final; };
struct SnapshotOffer { SubmitResult result; std::uint64_t next_offset; };
struct ValidationView { SnapshotId snapshot; Token chunk; std::uint64_t offset; std::span<const char> bytes; bool verified_eof; };
struct SnapshotResult { SnapshotId snapshot; SnapshotContext context; SnapshotReason reason; std::optional<InstallRejectReason> rejection{}; std::optional<Term> receiver_term{}; };
struct SnapshotActivation { SnapshotId snapshot; Token token; SnapshotDescriptor descriptor; };

// One owning executor. IO is exclusive to this worker and outlives it.
// There is no input queue; retryable work remains with the caller.
class Worker {
public:
	Worker(kronuz::journal::IO& io, FixedConfiguration configuration, kronuz::journal::AdmissionLimits admission,
		Limits limits = {}, Timing timing = {}, WorkerLimits scheduling = {}, std::optional<SnapshotLimits> snapshots = {})
		: configuration_(std::move(configuration)), limits_(limits), timing_(timing), scheduling_(scheduling),
		snapshot_limits_(snapshots), store_(io, admission, batch_limit(configuration_, limits), artifact_limit(snapshots)) {
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
					if (decoded.application_format && snapshot_limits_ && *decoded.application_format != snapshot_limits_->application_format) { throw kronuz::journal::Corruption("checkpoint application format conflicts with snapshot policy"); }
					published_ = Publication{{decoded.state.base_index, decoded.state.base_term}, decoded.application_format, selected.generation};
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
		if (!ready() || checkpoint_ || snapshot_result_) { return std::nullopt; }
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
		std::get<Local>(operation.mode).capture = capture; operation.phase = Phase::BeginApplication;
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
		if (id.owner_ != checkpoint_owner_ || !checkpoint_ || id.token_ != checkpoint_->token || !std::holds_alternative<Local>(checkpoint_->mode)) { return CancelResult::Stale; }
		if (publication_started(checkpoint_->phase)) { return CancelResult::TooLate; }
		try { store_.cancel_replacement(checkpoint_->replacement); checkpoint_.reset(); cutover_debt_ = false; return CancelResult::Canceled; }
		catch (const std::exception& error) { fail(error.what()); throw; }
	}
	std::optional<SnapshotId> reserve_snapshot(SnapshotContext context) {
		if (!snapshot_limits_) { throw std::invalid_argument("snapshot reception is disabled"); }
		validate_snapshot(context.descriptor, {configuration_.cluster, configuration_.configuration, snapshot_limits_->application_format, snapshot_limits_->application_bytes});
		if (!context.request || !context.transfer || !context.leader_term || context.descriptor.term > context.leader_term ||
			context.authenticated_peer == configuration_.local || std::find(configuration_.voters.begin(), configuration_.voters.end(), context.authenticated_peer) == configuration_.voters.end() ||
			(core_ && context.leader_term < core_->term())) { throw std::invalid_argument("invalid snapshot sender context"); }
		if (!ready() || checkpoint_ || snapshot_result_) { return std::nullopt; }
		if (next_checkpoint_ == std::numeric_limits<Token>::max()) { throw std::length_error("snapshot identity exhausted"); }
		auto replacement = store_.reserve_replacement(context.descriptor.application_bytes, checkpoint_detail::maximum_size(limits_));
		if (!replacement) { return std::nullopt; }
		auto token = ++next_checkpoint_;
		try {
			checkpoint_ = std::make_unique<Checkpoint>(*replacement, token, context.descriptor.application_bytes);
			checkpoint_->mode.emplace<Incoming>(std::move(context)); checkpoint_->phase = Phase::BeginApplication;
		} catch (...) { checkpoint_.reset(); store_.cancel_replacement(*replacement); throw; }
		return SnapshotId(checkpoint_owner_, token);
	}
	SnapshotOffer offer_snapshot_chunk(const SnapshotId& id, std::uint64_t offset, std::string_view bytes, bool final = false) {
		auto& operation = snapshot(id);
		if ((operation.phase != Phase::BeginApplication && operation.phase != Phase::Application) || operation.final_offered) { throw std::invalid_argument("snapshot no longer accepts input"); }
		if (offset != operation.offered_bytes) { throw std::invalid_argument("snapshot input offset is not sequential"); }
		if (bytes.size() > 65536 || bytes.size() > operation.application_cap - offset) { throw std::length_error("snapshot chunk or image bound"); }
		if ((!final && bytes.empty()) || (final && offset + bytes.size() != operation.application_cap)) { throw std::invalid_argument("invalid snapshot final marker"); }
		if (operation.chunk) { return {SubmitResult::Busy, operation.offered_bytes}; }
		operation.chunk.emplace(); std::copy(bytes.begin(), bytes.end(), operation.chunk->bytes.begin());
		operation.chunk->length = bytes.size(); operation.chunk->final = final;
		operation.offered_bytes += bytes.size(); operation.final_offered = final;
		return {SubmitResult::Accepted, operation.offered_bytes};
	}
	std::optional<ValidationView> validation_chunk() const {
		if (fenced() || !checkpoint_) { return std::nullopt; }
		auto incoming = std::get_if<Incoming>(&checkpoint_->mode);
		if (!incoming || !incoming->view) { return std::nullopt; }
		const auto& chunk = *checkpoint_->chunk;
		return ValidationView{SnapshotId(checkpoint_owner_, checkpoint_->token), chunk.token, chunk.offset, std::span<const char>(chunk.bytes.data(), chunk.length), chunk.final};
	}
	ValidationAck consume_validation(const SnapshotId& id, Token token) {
		if (fenced()) { return ValidationAck::Fenced; }
		auto operation = find_snapshot(id); if (!operation) { return ValidationAck::Stale; }
		auto& incoming = std::get<Incoming>(operation->mode);
		if (!incoming.view || operation->chunk->token != token) { return ValidationAck::Stale; }
		incoming.view = false;
		if (operation->chunk->final) { incoming.eof = token; operation->phase = Phase::AwaitSemantic; }
		else if (incoming.verifier->offset() == operation->application_cap) { operation->phase = Phase::FinishVerification; }
		operation->chunk.reset(); return ValidationAck::Accepted;
	}
	ValidationAck validation_succeeded(const SnapshotId& id, Token eof) {
		if (fenced()) { return ValidationAck::Fenced; }
		auto operation = find_snapshot(id); if (!operation) { return ValidationAck::Stale; }
		auto& incoming = std::get<Incoming>(operation->mode);
		if (!incoming.eof || incoming.view) { return ValidationAck::NotReady; }
		if (incoming.eof != eof || operation->phase != Phase::AwaitSemantic) { return ValidationAck::Stale; }
		operation->phase = Phase::Validated; return ValidationAck::Accepted;
	}
	bool snapshot_validated(const SnapshotId& id) const noexcept {
		return !fenced() && checkpoint_ && id.owner_ == checkpoint_owner_ && id.token_ == checkpoint_->token &&
			std::holds_alternative<Incoming>(checkpoint_->mode) && checkpoint_->phase == Phase::Validated;
	}
	CancelResult reject_snapshot_validation(const SnapshotId& id) { return cancel_snapshot(id, SnapshotReason::InvalidApplication); }
	CancelResult cancel_snapshot(const SnapshotId& id) { return cancel_snapshot(id, SnapshotReason::Canceled); }
	const SnapshotResult* pending_snapshot_result() const noexcept { return snapshot_result_ ? &*snapshot_result_ : nullptr; }
	std::optional<SnapshotPolicy> snapshot_policy() const { return snapshot_limits_ ? std::optional<SnapshotPolicy>{{configuration_.cluster, configuration_.configuration, snapshot_limits_->application_format, snapshot_limits_->application_bytes}} : std::nullopt; }
	const FixedConfiguration& configuration() const noexcept { return configuration_; }
	std::optional<SnapshotResult> take_snapshot_result() { auto result = std::move(snapshot_result_); snapshot_result_.reset(); return result; }
	SubmitResult request_install(const SnapshotId& id) {
		if (fenced()) { return SubmitResult::Fenced; }
		auto& operation = snapshot(id); auto& incoming = std::get<Incoming>(operation.mode);
		if (incoming.requested) { return SubmitResult::Accepted; }
		if (operation.phase != Phase::Validated) { return SubmitResult::Busy; }
		try {
			auto permit = store_.reserve_append(kronuz::journal::AdmissionClass::Control, storage_batch_size(true, false));
			if (!permit) { return SubmitResult::Pressure; }
			incoming.cutover_permit = std::move(permit); incoming.requested = true; operation.phase = Phase::Cutover; cutover_debt_ = true;
			return SubmitResult::Accepted;
		} catch (const std::exception& error) { fail(error.what()); return SubmitResult::Fenced; }
	}
	std::optional<SnapshotActivation> snapshot_activation() const {
		if (fenced() || !checkpoint_ || delivered_ || application_ || activation_completion_) { return std::nullopt; }
		auto incoming = std::get_if<Incoming>(&checkpoint_->mode);
		if (!incoming || !incoming->activation) { return std::nullopt; }
		return SnapshotActivation{SnapshotId(checkpoint_owner_, checkpoint_->token), incoming->activation->token, incoming->context.descriptor};
	}
	ValidationAck snapshot_activated(const SnapshotId& id, Token token) {
		if (fenced()) { return ValidationAck::Fenced; }
		auto operation = find_snapshot(id); if (!operation) { return ValidationAck::Stale; }
		auto& incoming = std::get<Incoming>(operation->mode);
		if (!incoming.activation) { return ValidationAck::NotReady; }
		if (incoming.activation->token != token) { return ValidationAck::Stale; }
		if (activation_completion_) { return ValidationAck::Stale; }
		if (!snapshot_activation()) { return ValidationAck::NotReady; }
		activation_completion_ = InstallActivated{token}; return ValidationAck::Accepted;
	}
	ValidationAck snapshot_activation_failed(const SnapshotId& id, Token token) {
		if (fenced()) { return ValidationAck::Fenced; }
		auto operation = find_snapshot(id); if (!operation) { return ValidationAck::Stale; }
		auto& incoming = std::get<Incoming>(operation->mode);
		if (!incoming.activation) { return ValidationAck::NotReady; }
		if (incoming.activation->token != token) { return ValidationAck::Stale; }
		fail("snapshot application activation failed"); return ValidationAck::Accepted;
	}

	std::optional<SourceInfo> snapshot_source() const {
		if (fenced() || !source_ || (source_->phase != SourcePhase::Streaming && source_->phase != SourcePhase::AwaitResult) || !core_->snapshot_sending(source_->peer, source_->key)) { return std::nullopt; }
		return SourceInfo{SourceId(source_owner_, source_->token), source_->peer, source_->key, source_->descriptor};
	}
	std::optional<SourceChunk> source_chunk() const {
		if (!snapshot_source() || !source_->view) { return std::nullopt; }
		return SourceChunk{SourceId(source_owner_, source_->token), source_->view, source_->offset, std::span<const char>(source_->buffer.data(), source_->count), source_->final};
	}
	ValidationAck consume_source(const SourceId& id, Token chunk) {
		if (fenced()) { return ValidationAck::Fenced; }
		refresh_source();
		if (!source_ || id.owner_ != source_owner_ || id.token_ != source_->token || !source_->view || source_->view != chunk) { return ValidationAck::Stale; }
		source_->offset += source_->count; source_->view = 0;
		if (source_->final) { source_->phase = SourcePhase::AwaitResult; }
		return ValidationAck::Accepted;
	}
	std::size_t snapshot_buffer_capacity() const noexcept { return (source_ ? 65536 : 0) + (checkpoint_ ? 65536 : 0); }
	ValidationAck source_peer_failed(NodeId peer) {
		if (fenced()) { return ValidationAck::Fenced; }
		refresh_source(); if (!source_ || source_->peer != peer) { return ValidationAck::NotReady; }
		return source_failed(SourceId(source_owner_, source_->token));
	}
	ValidationAck source_failed(const SourceId& id) {
		if (fenced()) { return ValidationAck::Fenced; }
		refresh_source();
		if (!source_ || id.owner_ != source_owner_ || id.token_ != source_->token) { return ValidationAck::Stale; }
		source_->verifier.reset(); source_->reader.reset(); source_->view = 0; source_->phase = SourcePhase::Failed;
		return ValidationAck::Accepted;
	}
	TurnResult run_one(Tick current_time) {
		if (store_.fenced() && !failed_) { fail("storage fenced outside worker"); }
		if (fenced()) { return TurnResult::Fenced; }
		if (!core_) { throw std::logic_error("worker not opened"); }
		refresh_source();
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
				if (preparation_runnable() || source_ || installation_waiting()) { maintenance(); return fenced() ? TurnResult::Fenced : TurnResult::Maintenance; }
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
				if (checkpoint_ && checkpoint_->phase == Phase::Completion && checkpoint_->persistence_token == value.token && std::holds_alternative<Local>(checkpoint_->mode)) {
					pending_publication_ = Publication{LogBoundary{std::get<Local>(checkpoint_->mode).capture->through, std::get<Local>(checkpoint_->mode).capture->term}, snapshot_limits_ ? std::optional<std::uint32_t>{snapshot_limits_->application_format} : std::nullopt, store_.frontier().generation};
					checkpoint_.reset(); cutover_debt_ = false;
				}
				ingest(core_->step(value));
				if (pending_publication_ && !fenced()) { published_ = pending_publication_; pending_publication_.reset(); }
				++foreground_; return TurnResult::Completed;
			}
			if (activation_completion_) {
				ingest(core_->step(current_time)); auto value = *activation_completion_; activation_completion_.reset();
				ingest(core_->step(value)); ++foreground_; return TurnResult::Completed;
			}
			if (installation_waiting()) { maintenance(); return fenced() ? TurnResult::Fenced : TurnResult::Maintenance; }
			if (timer_blocked_ && !cutover_debt_) {
				auto result = submit(current_time, true);
				if (result == SubmitResult::Accepted) { timer_blocked_ = false; timer_due_ = true; return TurnResult::Idle; }
				if (result == SubmitResult::Fenced) { return TurnResult::Fenced; }
				maintenance(); return fenced() ? TurnResult::Fenced : TurnResult::Maintenance;
			}
			if (preparation_runnable() || source_runnable()) {
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
	enum class Phase { Reserved, BeginApplication, Application, SealApplication, OpenVerification, Readback, FinishVerification, AwaitVerifiedEOF, AwaitSemantic, Validated, AwaitRejection, Cutover, EncodeBundle, BeginBundle, Bundle, SealBundle, Publish, Completion };
	struct Chunk { std::array<char, 65536> bytes{}; std::size_t length = 0; bool final = false; std::uint64_t offset = 0; Token token = 0; };
	struct Local { std::optional<CaptureMetadata> capture; };
	enum class SourcePhase { Opening, Verifying, Ready, Streaming, AwaitResult, Failed };
	struct Publication { LogBoundary boundary; std::optional<std::uint32_t> format; std::uint64_t generation; };
	struct Source {
		Source(kronuz::journal::PublishedArtifactSelection selected, NodeId destination, SnapshotKey flight, Token identity, SnapshotDescriptor image)
			: selection(std::move(selected)), peer(destination), key(flight), token(identity), descriptor(image) {}
		kronuz::journal::PublishedArtifactSelection selection;
		NodeId peer; SnapshotKey key; Token token; SnapshotDescriptor descriptor;
		SourcePhase phase = SourcePhase::Opening;
		std::optional<kronuz::journal::ArtifactVerifier> verifier;
		std::optional<kronuz::journal::ArtifactReader> reader;
		std::array<char, 65536> buffer{};
		Token view = 0; std::uint64_t offset = 0; std::size_t count = 0; bool final = false;
	};
	struct Incoming {
		explicit Incoming(SnapshotContext value) : context(std::move(value)) {}
		SnapshotContext context;
		std::optional<kronuz::journal::ArtifactVerifier> verifier;
		std::optional<kronuz::journal::ArtifactReader> reader;
		Token eof = 0; bool view = false, requested = false, admitted = false;
		std::optional<kronuz::journal::AppendReservation> cutover_permit;
		std::optional<ActivateInstall> activation;
	};
	struct Checkpoint {
		Checkpoint(kronuz::journal::ReplacementId id, Token identity, std::uint64_t cap)
			: replacement(std::move(id)), token(identity), application_cap(cap) {}
		kronuz::journal::ReplacementId replacement;
		Token token, persistence_token = 0;
		std::uint64_t application_cap, offered_bytes = 0, sequence = 0;
		Phase phase = Phase::Reserved;
		std::variant<Local, Incoming> mode;
		std::optional<Chunk> chunk;
		std::optional<kronuz::journal::ArtifactDescriptor> application;
		std::optional<RecoveredState> captured;
		std::string bundle;
		std::size_t offset = 0;
		bool final_offered = false;
	};
	Checkpoint& checkpoint(const CheckpointId& id) {
		if (fenced()) { throw std::logic_error("worker fenced"); }
		if (id.owner_ != checkpoint_owner_ || !checkpoint_ || id.token_ != checkpoint_->token || !std::holds_alternative<Local>(checkpoint_->mode)) { throw std::invalid_argument("stale or foreign checkpoint identity"); }
		return *checkpoint_;
	}
	Checkpoint* find_snapshot(const SnapshotId& id) {
		return checkpoint_ && id.owner_ == checkpoint_owner_ && id.token_ == checkpoint_->token && std::holds_alternative<Incoming>(checkpoint_->mode) ? checkpoint_.get() : nullptr;
	}
	Checkpoint& snapshot(const SnapshotId& id) {
		if (fenced()) { throw std::logic_error("worker fenced"); }
		auto operation = find_snapshot(id); if (!operation) { throw std::invalid_argument("stale or foreign snapshot identity"); } return *operation;
	}
	static bool publication_started(Phase phase) {
		return phase == Phase::EncodeBundle || phase == Phase::BeginBundle || phase == Phase::Bundle || phase == Phase::SealBundle || phase == Phase::Publish || phase == Phase::Completion;
	}
	CancelResult cancel_snapshot(const SnapshotId& id, SnapshotReason reason) {
		auto operation = find_snapshot(id); if (!operation) { return CancelResult::Stale; }
		if (publication_started(operation->phase) || std::get<Incoming>(operation->mode).admitted) { return CancelResult::TooLate; }
		try {
			auto context = std::get<Incoming>(operation->mode).context;
			if (snapshot_result_) { throw std::logic_error("snapshot result slot occupied"); }
			store_.cancel_replacement(operation->replacement); checkpoint_.reset(); cutover_debt_ = false;
			snapshot_result_.emplace(SnapshotResult{id, std::move(context), reason}); return CancelResult::Canceled;
		} catch (const std::exception& error) { fail(error.what()); throw; }
	}
	Token validation_token() {
		if (next_validation_ == std::numeric_limits<Token>::max()) { throw std::overflow_error("validation identity exhausted"); }
		return ++next_validation_;
	}
	bool installation_waiting() const noexcept {
		if (!checkpoint_ || checkpoint_->phase != Phase::Completion) { return false; }
		auto incoming = std::get_if<Incoming>(&checkpoint_->mode); return incoming && incoming->activation.has_value();
	}

	bool preparation_runnable() const noexcept {
		if (!checkpoint_) { return false; }
		auto phase = checkpoint_->phase;
		if (phase == Phase::Reserved || phase == Phase::Completion || phase == Phase::AwaitVerifiedEOF || phase == Phase::AwaitSemantic || phase == Phase::Validated || phase == Phase::AwaitRejection) { return false; }
		if (phase == Phase::Application) { return checkpoint_->chunk.has_value(); }
		if (phase == Phase::Readback) { return !std::get<Incoming>(checkpoint_->mode).view; }
		if (phase == Phase::Cutover) { return !core_->busy() && output_.empty() && !application_ && !completion_ && !persist_ && pack_count_ == 0; }
		return true;
	}
	void maintenance() {
		auto owed_timer = timer_due_;
		for (unsigned attempt = 0; attempt < 3; ++attempt) {
			auto kind = maintenance_next_; maintenance_next_ = (maintenance_next_ + 1) % 3;
			if (kind == 0) { store_.reclaim_step(scheduling_.scan_entries); break; }
			if (kind == 1 && preparation_runnable()) { prepare_one(); break; }
			if (kind == 2 && source_runnable()) { source_one(); break; }
		}
		foreground_ = 0; // Cutover debt is independent of this fairness counter.
		timer_blocked_ = owed_timer; timer_due_ = true;
	}
	void refresh_source() {
		if (source_ && (!core_ || !core_->snapshot_active(source_->peer, source_->key))) { source_.reset(); }
	}
	bool source_runnable() const {
		if (!snapshot_limits_ || !core_ || core_->role() != Role::Leader) { return false; }
		if (source_) {
			if (source_->phase == SourcePhase::AwaitResult || (source_->phase == SourcePhase::Streaming && source_->view)) { return false; }
			if (source_->phase == SourcePhase::Ready || source_->phase == SourcePhase::Failed) { return !core_->busy() && output_.empty() && !completion_ && !persist_ && !application_ && !cutover_debt_; }
			return true;
		}
		if (core_->busy() || !output_.empty() || completion_ || persist_ || application_ || cutover_debt_) { return false; }
		for (auto peer : configuration_.voters) { if (core_->awaiting_snapshot(peer)) { return true; } }
		return false;
	}
	void source_one() {
		refresh_source();
		if (!source_) {
			for (std::size_t checked = 0; checked < configuration_.voters.size(); ++checked) {
				auto peer = configuration_.voters[source_next_]; source_next_ = (source_next_ + 1) % configuration_.voters.size(); auto key = core_->awaiting_snapshot(peer); if (!key) { continue; }
				if (core_->busy() || !output_.empty() || completion_ || persist_ || application_ || cutover_debt_) { return; }
				auto selected = store_.select_published_dependency(0);
				if (!published_ || !published_->format || *published_->format != snapshot_limits_->application_format || published_->boundary != key->boundary || core_->base_boundary() != key->boundary || !selected || selected->generation() != published_->generation || selected->descriptor().length > snapshot_limits_->application_bytes) { ingest(core_->step(SnapshotTransferFailed{peer, *key})); return; }
				if (next_source_ == std::numeric_limits<Token>::max()) { throw std::length_error("source identity exhausted"); }
				const auto& artifact = selected->descriptor(); SnapshotDescriptor descriptor{configuration_.cluster, configuration_.configuration, *published_->format, key->boundary.index, key->boundary.term, artifact.length, artifact.checksum};
				source_ = std::make_unique<Source>(std::move(*selected), peer, *key, ++next_source_, descriptor); return;
			}
			return;
		}
		auto& source = *source_;
		if (source.phase == SourcePhase::Opening) {
			source.verifier = store_.begin_published_verification(source.selection);
			if (source.verifier) { source.phase = SourcePhase::Verifying; } else { source.phase = SourcePhase::Failed; }
		} else if (source.phase == SourcePhase::Verifying) {
			if (source.verifier->offset() < source.descriptor.application_bytes) { source.verifier->read_next(source.buffer); }
			else { source.reader.emplace(std::move(*source.verifier).finish()); source.verifier.reset(); source.phase = SourcePhase::Ready; }
		} else if (source.phase == SourcePhase::Ready || source.phase == SourcePhase::Failed) {
			if (core_->busy() || !output_.empty() || completion_ || persist_ || application_ || cutover_debt_) { return; }
			if (source.phase == SourcePhase::Failed || source.selection.generation() != store_.frontier().generation || core_->base_boundary() != source.key.boundary) { auto peer = source.peer; auto key = source.key; source_.reset(); ingest(core_->step(SnapshotTransferFailed{peer, key})); }
			else { source.phase = SourcePhase::Streaming; ingest(core_->step(SnapshotSourceReady{source.peer, source.key})); refresh_source(); }
		} else if (source.phase == SourcePhase::Streaming && !source.view) {
			if (!core_->snapshot_sending(source.peer, source.key)) { return; }
			if (next_source_chunk_ == std::numeric_limits<Token>::max()) { throw std::length_error("source chunk identity exhausted"); }
			source.final = source.offset == source.descriptor.application_bytes;
			source.count = source.final ? 0 : source.reader->read_at(source.offset, std::span<char>(source.buffer.data(), static_cast<std::size_t>(std::min<std::uint64_t>(65536, source.descriptor.application_bytes - source.offset))));
			source.view = ++next_source_chunk_;
		}
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
			if (auto incoming = std::get_if<Incoming>(&operation.mode)) {
				const auto& descriptor = incoming->context.descriptor;
				if (operation.application->length != descriptor.application_bytes || operation.application->checksum != descriptor.application_crc32c) {
					cancel_snapshot(SnapshotId(checkpoint_owner_, operation.token), SnapshotReason::InvalidImage); break;
				}
				operation.phase = Phase::OpenVerification;
			} else { operation.phase = Phase::Cutover; cutover_debt_ = true; }
			break;
		case Phase::OpenVerification: {
			auto& incoming = std::get<Incoming>(operation.mode);
			incoming.verifier = store_.begin_artifact_verification(operation.replacement, ArtifactPart::Application);
			if (incoming.verifier) { operation.phase = operation.application_cap ? Phase::Readback : Phase::FinishVerification; }
			break;
		}
		case Phase::Readback: {
			auto& incoming = std::get<Incoming>(operation.mode); operation.chunk.emplace(); auto& chunk = *operation.chunk;
			chunk.offset = incoming.verifier->offset(); chunk.length = incoming.verifier->read_next(chunk.bytes); chunk.token = validation_token(); incoming.view = true; break;
		}
		case Phase::FinishVerification: {
			auto& incoming = std::get<Incoming>(operation.mode);
			incoming.reader.emplace(std::move(*incoming.verifier).finish()); incoming.verifier.reset(); operation.chunk.emplace();
			auto& chunk = *operation.chunk; chunk.offset = operation.application_cap; chunk.final = true; chunk.token = validation_token();
			incoming.view = true; operation.phase = Phase::AwaitVerifiedEOF; break;
		}
		case Phase::Cutover: {
			if (auto incoming = std::get_if<Incoming>(&operation.mode)) {
				if (!incoming->cutover_permit || pack_count_) { throw std::logic_error("installation cutover lacks dedicated control permit"); }
				pack_[0] = std::move(incoming->cutover_permit); incoming->cutover_permit.reset(); pack_count_ = 1; pack_next_ = 0;
				auto context = incoming->context;
				ingest(core_->step(InstallPrepared{context.request, operation.token, context.authenticated_peer, context.leader_term,
					configuration_.cluster, configuration_.configuration, {context.descriptor.through, context.descriptor.term}}));
				break;
			}
			auto capture = *std::get<Local>(operation.mode).capture;
			ingest(core_->step(LocalCheckpoint{capture.request, operation.token, capture.through, capture.term, configuration_.cluster, configuration_.configuration}));
			if (fenced()) { break; }
			if (!core_->busy()) { store_.cancel_replacement(operation.replacement); checkpoint_.reset(); cutover_debt_ = false; }
			break;
		}
		case Phase::EncodeBundle:
			operation.bundle = encode_checkpoint(*operation.captured, operation.sequence, *operation.application, limits_, std::holds_alternative<Incoming>(operation.mode) ? std::optional<std::uint32_t>{std::get<Incoming>(operation.mode).context.descriptor.application_format} : (snapshot_limits_ ? std::optional<std::uint32_t>{snapshot_limits_->application_format} : std::nullopt));
			operation.captured.reset(); operation.phase = Phase::BeginBundle; break;
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
	static std::uint64_t artifact_limit(std::optional<SnapshotLimits> limits) {
		return std::max(std::uint64_t{512ull * 1024 * 1024}, limits ? limits->application_bytes : 0);
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
			else if (auto value = std::get_if<PersistInstall>(&action)) { count_entries(value->state.entries); }
		}
		if (payload > output_payload_ || entries > output_entries_) { throw std::length_error("worker action payload bound"); }
		for (auto& action : actions) {
			if (auto value = std::get_if<Persist>(&action)) {
				if (persist_ || completion_ || pack_next_ >= pack_count_ || !pack_[pack_next_]) { throw std::logic_error("persistence lacks preowned reservation"); }
				persist_ = std::move(*value);
				if (checkpoint_ && checkpoint_->phase == Phase::Cutover) {
					if (auto incoming = std::get_if<Incoming>(&checkpoint_->mode); incoming && !incoming->cutover_permit) { incoming->admitted = true; checkpoint_->phase = Phase::AwaitRejection; }
				}
			} else if (auto value = std::get_if<Fenced>(&action)) { fail(value->reason); }
			else if (auto value = std::get_if<PersistInstall>(&action)) {
				if (!checkpoint_ || !std::holds_alternative<Incoming>(checkpoint_->mode) || checkpoint_->phase != Phase::Cutover || value->prepared != checkpoint_->token) { throw std::logic_error("installation lacks owned replacement"); }
				std::get<Incoming>(checkpoint_->mode).admitted = true;
				checkpoint_->sequence = store_.frontier().sequence; checkpoint_->persistence_token = value->token;
				checkpoint_->captured = std::move(value->state); checkpoint_->phase = Phase::EncodeBundle; release_pack();
			}
			else if (auto value = std::get_if<ActivateInstall>(&action)) {
				if (!checkpoint_ || !std::holds_alternative<Incoming>(checkpoint_->mode) || checkpoint_->phase != Phase::Completion || value->prepared != checkpoint_->token || value->token != checkpoint_->persistence_token || value->boundary.index != std::get<Incoming>(checkpoint_->mode).context.descriptor.through || value->boundary.term != std::get<Incoming>(checkpoint_->mode).context.descriptor.term) { throw std::logic_error("activation lacks published image"); }
				auto& incoming = std::get<Incoming>(checkpoint_->mode); if (incoming.activation) { throw std::logic_error("activation slot occupied"); } incoming.activation = *value;
			}
			else if (auto value = std::get_if<InstallRejected>(&action)) {
				finish_snapshot(value->request, value->prepared, SnapshotReason::Rejected, value->reason);
			}
			else if (auto value = std::get_if<InstallCompleted>(&action)) {
				if (!checkpoint_ || !std::holds_alternative<Incoming>(checkpoint_->mode) || checkpoint_->phase != Phase::Completion || !std::get<Incoming>(checkpoint_->mode).activation || value->boundary.index != std::get<Incoming>(checkpoint_->mode).context.descriptor.through || value->boundary.term != std::get<Incoming>(checkpoint_->mode).context.descriptor.term) { throw std::logic_error("installation completion boundary mismatch"); }
				finish_snapshot(value->request, value->prepared, SnapshotReason::Installed);
			}
			else if (auto value = std::get_if<PersistCheckpoint>(&action)) {
				if (!checkpoint_ || !std::holds_alternative<Local>(checkpoint_->mode) || checkpoint_->phase != Phase::Cutover || value->capture != checkpoint_->token) { throw std::logic_error("checkpoint lacks owned replacement"); }
				checkpoint_->sequence = store_.frontier().sequence; checkpoint_->persistence_token = value->token;
				checkpoint_->captured = std::move(value->state); checkpoint_->phase = Phase::EncodeBundle;
			}
			else {
				if (!output_.empty()) { throw std::logic_error("worker output batch occupied"); }
				// Build the one output batch below after validating all actions.
			}
		}
		Actions external;
		for (auto& action : actions) {
			if (!std::holds_alternative<Persist>(action) && !std::holds_alternative<PersistCheckpoint>(action) && !std::holds_alternative<Fenced>(action) && !std::holds_alternative<PersistInstall>(action) && !std::holds_alternative<ActivateInstall>(action) && !std::holds_alternative<InstallRejected>(action) && !std::holds_alternative<InstallCompleted>(action)) { external.push_back(std::move(action)); }
		}
		if (!external.empty()) { output_ = std::move(external); }
		if (!core_->busy()) { release_pack(); }
		refresh_source();
	}
	void finish_snapshot(RequestId request, Token prepared, SnapshotReason reason, std::optional<InstallRejectReason> rejection = {}) {
		if (!checkpoint_ || !std::holds_alternative<Incoming>(checkpoint_->mode) || checkpoint_->token != prepared || snapshot_result_) { throw std::logic_error("snapshot terminal correlation mismatch"); }
		auto context = std::get<Incoming>(checkpoint_->mode).context;
		if (reason == SnapshotReason::Installed) { published_ = Publication{{context.descriptor.through, context.descriptor.term}, context.descriptor.application_format, store_.frontier().generation}; }
		if (context.request != request) { throw std::logic_error("snapshot request correlation mismatch"); }
		SnapshotId id(checkpoint_owner_, prepared);
		if (reason != SnapshotReason::Installed) { store_.cancel_replacement(checkpoint_->replacement); }
		checkpoint_.reset(); cutover_debt_ = false; release_pack();
		snapshot_result_.emplace(SnapshotResult{id, std::move(context), reason, rejection, core_->durable_hard_state().term});
	}

	void fail(std::string reason) {
		if (failed_) { return; } failed_ = true;
		store_.fence_storage(); if (core_) { core_->step(StorageFault{reason}); }
		persist_.reset(); completion_.reset(); application_.reset(); activation_completion_.reset(); checkpoint_.reset(); source_.reset(); published_.reset(); pending_publication_.reset(); release_pack(); terminal_ = Fenced{std::move(reason)};
	}
	FixedConfiguration configuration_;
	Limits limits_;
	Timing timing_;
	WorkerLimits scheduling_;
	std::optional<SnapshotLimits> snapshot_limits_;
	kronuz::journal::Store store_;
	std::unique_ptr<Core> core_;
	std::string initialization_;
	std::array<std::optional<kronuz::journal::AppendReservation>, 3> pack_;
	std::size_t pack_count_ = 0, pack_next_ = 0, foreground_ = 0, output_count_ = 0, output_payload_ = 0, output_entries_ = 0;
	std::optional<Persist> persist_;
	std::optional<Persisted> completion_;
	std::optional<Applied> application_;
	std::optional<InstallActivated> activation_completion_;
	std::optional<Fenced> terminal_;
	std::shared_ptr<char> checkpoint_owner_ = std::make_shared<char>();
	std::unique_ptr<Checkpoint> checkpoint_;
	Token next_checkpoint_ = 0;
	Token next_validation_ = 0;
	std::optional<SnapshotResult> snapshot_result_;
	Actions output_;
	Index delivered_ = 0;
	std::optional<Publication> published_, pending_publication_;
	std::shared_ptr<char> source_owner_ = std::make_shared<char>();
	std::unique_ptr<Source> source_;
	Token next_source_ = 0, next_source_chunk_ = 0;
	std::size_t source_next_ = 0; unsigned maintenance_next_ = 1;
	bool failed_ = false, cutover_debt_ = false, timer_due_ = true, timer_blocked_ = false;
};

} // namespace cluster::consensus
