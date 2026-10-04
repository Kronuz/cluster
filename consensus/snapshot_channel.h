#pragma once

#include "worker.h"
#include "snapshot_transfer.h"

namespace cluster::consensus {

// Supplied by an authenticated, reliable, ordered transport. Identities must
// be fresh on every connection; frame fields alone authenticate nothing.
struct TrustedSession { NodeId peer; Identity local, remote; bool operator==(const TrustedSession&) const = default; };
enum class TransferReceive { Accepted, Busy, Closed };
struct TransferInput { TransferReceive result; std::size_t consumed; };
struct TransferOutput { TrustedSession session; Token token; std::span<const char> bytes; };

class SnapshotChannel {
public:
	explicit SnapshotChannel(Worker& worker, std::uint64_t timeout = 60000) : worker_(worker), timeout_(timeout) {
		if (!timeout) { throw std::invalid_argument("invalid transfer timeout"); }
		channels_.reserve(worker.configuration().voters.size() - 1);
		for (auto peer : worker.configuration().voters) { if (peer != worker.configuration().local) { channels_.emplace_back(peer); } }
	}
	SnapshotChannel(const SnapshotChannel&) = delete;
	SnapshotChannel& operator=(const SnapshotChannel&) = delete;
	void register_session(TrustedSession session) {
		auto channel = find(session.peer);
		if (!channel || session.local == Identity{} || session.remote == Identity{}) { throw std::invalid_argument("invalid trusted snapshot session"); }
		if (channel->session) { session_closed(*channel->session); }
		channel->session = session; channel->used = 0;
	}
	bool session_open(const TrustedSession& session) const { auto channel = find(session.peer); return channel && channel->session == session; }
	void session_closed(const TrustedSession& session) {
		auto channel = find(session.peer); if (!channel || channel->session != session) { return; }
		worker_.source_peer_failed(session.peer);
		auto selected = selected_frame(); auto closes_selected = selected && selected->peer == session.peer;
		if (sending_ && sending_->info.peer == session.peer) { worker_.source_failed(sending_->info.source); sending_.reset(); }
		if (receiving_ && receiving_->session == session) { receiving_->connected = false; worker_.cancel_snapshot(receiving_->id); }
		if (data_ && data_->peer == session.peer) { data_.reset(); }
		channel->control.reset(); channel->completed.reset(); channel->used = 0; channel->session.reset(); if (closes_selected) { selected_ = 0; }
	}
	TransferInput receive(const TrustedSession& session, std::span<const char> bytes) {
		auto channel = find(session.peer); if (!channel || channel->session != session) { return {TransferReceive::Closed, 0}; }
		if (worker_.fenced()) { close_fenced(); return {TransferReceive::Closed, 0}; }
		std::size_t consumed = 0;
		try {
			if (channel->used >= 12 && channel->used == frame_length(*channel)) {
				if (!process(*channel)) { return {TransferReceive::Busy, 0}; } channel->used = 0; return {TransferReceive::Accepted, 0};
			}
			while (consumed < bytes.size()) {
				auto target = channel->used < 12 ? std::size_t{12} : frame_length(*channel);
				auto count = std::min(target - channel->used, bytes.size() - consumed);
				std::copy_n(bytes.data() + consumed, count, channel->input.data() + channel->used); channel->used += count; consumed += count;
				if (channel->used >= 12 && channel->used == frame_length(*channel)) {
					if (!process(*channel)) { return {TransferReceive::Busy, consumed}; } channel->used = 0; return {TransferReceive::Accepted, consumed};
				}
			}
			return {TransferReceive::Accepted, consumed};
		} catch (const kronuz::journal::Corruption&) { session_closed(session); return {TransferReceive::Closed, consumed}; }
		catch (const std::invalid_argument&) { session_closed(session); return {TransferReceive::Closed, consumed}; }
	}
	std::optional<TransferOutput> outbound() {
		if (worker_.fenced()) { close_fenced(); return std::nullopt; }
		if (!selected_) {
			for (auto& channel : channels_) { if (channel.control) { selected_ = channel.control->token; break; } }
			if (!selected_ && data_) { selected_ = data_->token; }
		}
		auto frame = selected_frame(); if (!frame) { selected_ = 0; return std::nullopt; }
		auto channel = find(frame->peer); if (!channel || !channel->session) { throw std::logic_error("output without trusted session"); }
		return TransferOutput{*channel->session, frame->token, std::span<const char>(frame->bytes.data() + frame->offset, frame->bytes.size() - frame->offset)};
	}
	void consume_output(Token token, std::size_t bytes) {
		auto frame = selected_frame(); if (!frame || token != frame->token || !bytes || bytes > frame->bytes.size() - frame->offset) { throw std::invalid_argument("invalid transfer output consumption"); }
		frame->offset += bytes;
		if (frame->offset != frame->bytes.size()) { return; }
		if (data_ && data_->token == token) { data_.reset(); }
		else { for (auto& channel : channels_) { if (channel.control && channel.control->token == token) { channel.control.reset(); break; } } }
		selected_ = 0;
	}
	void run_one(Tick tick) {
		if (worker_.fenced()) { close_fenced(); return; }
		if (tick.now < now_) { return; } now_ = tick.now;
		for (auto& channel : channels_) {
			if (channel.session && channel.used >= 12) {
				auto session = *channel.session;
				try { if (channel.used == frame_length(channel) && process(channel)) { channel.used = 0; } }
				catch (const kronuz::journal::Corruption&) { session_closed(session); }
				catch (const std::invalid_argument&) { session_closed(session); }
			}
		}
		if (receiving_) {
			if (receiving_->connected && now_ - receiving_->started >= timeout_) { session_closed(receiving_->session); }
			if (worker_.snapshot_validated(receiving_->id)) { worker_.request_install(receiving_->id); }
			if (auto result = worker_.pending_snapshot_result(); result && result->snapshot == receiving_->id) {
				auto channel = find(receiving_->session.peer);
				if (!receiving_->connected || !channel->session || channel->session != receiving_->session) { worker_.take_snapshot_result(); receiving_.reset(); }
				else if (!channel->control) {
					auto terminal = worker_.take_snapshot_result(); auto outcome = terminal->reason == SnapshotReason::Installed ? TransferOutcome::Installed : terminal->rejection == InstallRejectReason::CaughtUp ? TransferOutcome::CaughtUp : (terminal->receiver_term && *terminal->receiver_term) ? TransferOutcome::Rejected : TransferOutcome::Failed;
					std::string body(1, static_cast<char>(outcome)); body.append(7, '\0'); kronuz::journal::put64(body, terminal->receiver_term.value_or(0));
					channel->completed.emplace(Completed{receiving_->key, receiving_->descriptor, body});
					control(*channel, TransferKind::Result, receiving_->key, body); receiving_.reset();
				}
			}
		}
		auto source = worker_.snapshot_source();
		if (sending_ && (!source || source->source != sending_->info.source)) {
			auto channel = find(sending_->info.peer);
			if (data_ && data_->offset) { if (channel->session) { session_closed(*channel->session); } }
			else if (channel->session && !channel->control) { data_.reset(); control(*channel, TransferKind::Cancel, sending_->info.key, {}); sending_.reset(); }
			else if (!channel->session) { sending_.reset(); data_.reset(); }
			return;
		}
		if (!sending_ && source) {
			auto channel = find(source->peer); if (!channel->session) { worker_.source_failed(source->source); return; }
			if (channel->control) { return; }
			auto policy = worker_.snapshot_policy(); if (!policy) { throw std::logic_error("source without snapshot policy"); }
			control(*channel, TransferKind::Begin, source->key, encode_snapshot(source->descriptor, *policy)); sending_.emplace(*source); return;
		}
		if (sending_ && sending_->accepted && !sending_->waiting && !data_) {
			auto chunk = worker_.source_chunk(); if (!chunk) { return; }
			auto channel = find(sending_->info.peer); if (!channel->session) { return; }
			data_.emplace(make_frame(*channel, TransferKind::Chunk, sending_->info.key, transfer_chunk_body(chunk->chunk, chunk->offset, std::string_view(chunk->bytes.data(), chunk->bytes.size()), chunk->final)));
			sending_->waiting = chunk->chunk; sending_->next = chunk->offset + chunk->bytes.size(); sending_->final = chunk->final;
		}
	}
	// After closing sessions, retain this adapter and service Worker/application
	// callbacks until drained before destroying it. Worker outlives the adapter.
	bool drained() const noexcept { return !receiving_; }
	std::optional<SnapshotId> incoming_snapshot() const { return receiving_ ? std::optional<SnapshotId>{receiving_->id} : std::nullopt; }
	std::size_t retained_bytes() const {
		std::size_t result = data_ ? data_->bytes.size() : 0;
		for (const auto& channel : channels_) { result += channel.used + (channel.control ? channel.control->bytes.size() : 0); } return result;
	}
	std::size_t buffer_capacity() const {
		std::size_t result = channels_.capacity() * maximum_transfer_bytes + (data_ ? data_->bytes.capacity() : 0);
		for (const auto& channel : channels_) { result += channel.control ? channel.control->bytes.capacity() : 0; } return result;
	}
	std::size_t channel_count() const noexcept { return channels_.size(); }
private:
	struct Output { NodeId peer; Token token; std::string bytes; std::size_t offset = 0; };
	struct Completed { SnapshotKey key; SnapshotDescriptor descriptor; std::string body; };
	struct Channel {
		explicit Channel(NodeId node) : peer(node) {}
		NodeId peer; std::optional<TrustedSession> session; std::array<char, maximum_transfer_bytes> input{}; std::size_t used = 0; std::optional<Output> control; std::optional<Completed> completed;
	};
	struct Sending { explicit Sending(SourceInfo source) : info(std::move(source)) {} SourceInfo info; bool accepted = false, final = false; Token waiting = 0; std::uint64_t next = 0; };
	struct Receiving { TrustedSession session; SnapshotId id; SnapshotKey key; SnapshotDescriptor descriptor; std::uint64_t offset, started; bool connected = true, final = false; };
	void close_fenced() {
		// Worker fencing already invalidated storage and application ownership.
		for (auto& channel : channels_) { channel.session.reset(); channel.control.reset(); channel.completed.reset(); channel.used = 0; }
		receiving_.reset(); sending_.reset(); data_.reset(); selected_ = 0;
	}
	Channel* find(NodeId peer) { for (auto& channel : channels_) { if (channel.peer == peer) { return &channel; } } return nullptr; }
	const Channel* find(NodeId peer) const { for (const auto& channel : channels_) { if (channel.peer == peer) { return &channel; } } return nullptr; }
	std::size_t frame_length(const Channel& channel) const {
		std::string_view prefix(channel.input.data(), 12); if (prefix.substr(0, 8) != "RFTXFR01") { throw kronuz::journal::Corruption("invalid frame magic"); }
		prefix.remove_prefix(8); auto size = kronuz::journal::get32(prefix); if (size < transfer_header_bytes || size > maximum_transfer_bytes) { throw kronuz::journal::Corruption("invalid frame bound"); } return size;
	}
	Output make_frame(const Channel& channel, TransferKind kind, SnapshotKey key, std::string_view body) {
		if (next_output_ == std::numeric_limits<Token>::max()) { throw std::length_error("transfer output token exhausted"); }
		auto& configuration = worker_.configuration(); auto& session = *channel.session;
		return {channel.peer, ++next_output_, encode_transfer(kind, {configuration.cluster, configuration.configuration, session.local, session.remote, configuration.local, channel.peer, key}, body)};
	}
	void control(Channel& channel, TransferKind kind, SnapshotKey key, std::string_view body) { if (channel.control) { throw std::logic_error("transfer control output occupied"); } channel.control.emplace(make_frame(channel, kind, key, body)); }
	Output* selected_frame() { if (data_ && data_->token == selected_) { return &*data_; } for (auto& channel : channels_) { if (channel.control && channel.control->token == selected_) { return &*channel.control; } } return nullptr; }
	bool process(Channel& channel) {
		using namespace kronuz::journal;
		auto frame = decode_transfer(std::string_view(channel.input.data(), channel.used)); auto& envelope = frame.envelope; auto& configuration = worker_.configuration(); auto& session = *channel.session;
		if (envelope.cluster != configuration.cluster || envelope.configuration != configuration.configuration || envelope.source != session.peer || envelope.destination != configuration.local || envelope.source_session != session.remote || envelope.destination_session != session.local) { throw Corruption("unbound transfer session"); }
		auto key = envelope.key;
		switch (frame.kind) {
		case TransferKind::Begin: {
			auto policy = worker_.snapshot_policy(); if (!policy) { throw Corruption("snapshot reception disabled"); }
			auto descriptor = decode_snapshot(frame.body, *policy); if (LogBoundary{descriptor.through, descriptor.term} != key.boundary) { throw Corruption("snapshot boundary mismatch"); }
			if (channel.control) { return false; }
			if (channel.completed && channel.completed->key == key) {
				if (channel.completed->descriptor != descriptor) { throw Corruption("conflicting completed Begin"); }
				control(channel, TransferKind::Result, key, channel.completed->body); return true;
			}
			if (receiving_) {
				if (receiving_->session != session || receiving_->key != key) { return false; }
				if (receiving_->descriptor != descriptor) { throw Corruption("conflicting Begin"); }
				control(channel, TransferKind::Accepted, key, {}); return true;
			}
			auto id = worker_.reserve_snapshot({session.peer, key.leader_term, key.transfer, key.transfer, descriptor}); if (!id) { return false; }
			receiving_.emplace(Receiving{session, *id, key, descriptor, 0, now_}); control(channel, TransferKind::Accepted, key, {}); return true;
		}
		case TransferKind::Accepted:
			if (sending_ && sending_->info.peer == session.peer && sending_->info.key == key) { sending_->accepted = true; } return true;
		case TransferKind::Chunk: {
			if (!receiving_ || receiving_->session != session || receiving_->key != key || receiving_->final) { throw Corruption("unowned Chunk"); }
			auto body = frame.body; auto token = get64(body); auto offset = get64(body); auto count = get32(body); auto final = body[0] != 0; body.remove_prefix(4);
			if (offset != receiving_->offset || count > receiving_->descriptor.application_bytes - offset || (!final && !count) || (final && offset + count != receiving_->descriptor.application_bytes)) { throw Corruption("invalid sequential Chunk"); }
			if (channel.control) { return false; }
			auto offered = worker_.offer_snapshot_chunk(receiving_->id, offset, body, final); if (offered.result == SubmitResult::Busy) { return false; }
			if (offered.result != SubmitResult::Accepted) { throw Corruption("receiver cannot accept Chunk"); }
			receiving_->offset = offered.next_offset; receiving_->final = final; control(channel, TransferKind::Credit, key, transfer_chunk_body(token, offered.next_offset, {}, final, true)); return true;
		}
		case TransferKind::Credit: {
			if (!sending_ || sending_->info.peer != session.peer || sending_->info.key != key) { return true; }
			auto body = frame.body; auto token = get64(body); auto next = get64(body); get32(body); auto final = body[0] != 0;
			if (!sending_->waiting || sending_->waiting != token || sending_->next != next || sending_->final != final || data_) { throw Corruption("unmatched Credit"); }
			if (worker_.consume_source(sending_->info.source, token) != ValidationAck::Accepted) { return true; } sending_->waiting = 0; return true;
		}
		case TransferKind::Result: {
			auto outcome = static_cast<TransferOutcome>(static_cast<unsigned char>(frame.body[0])); auto body = frame.body.substr(8); auto term = get64(body);
			if (!term) { if (outcome != TransferOutcome::Failed) { throw Corruption("result has no durable term"); } if (sending_ && sending_->info.peer == session.peer && sending_->info.key == key) { worker_.source_failed(sending_->info.source); } return true; }
			if (outcome == TransferOutcome::Failed) { throw Corruption("unexpected failure term"); }
			auto reply = outcome == TransferOutcome::Installed ? SnapshotReply::Installed : outcome == TransferOutcome::CaughtUp ? SnapshotReply::CaughtUp : SnapshotReply::Rejected;
			SnapshotResponse response{term, key, reply}; if (!valid_snapshot_response(response)) { throw Corruption("invalid durable result"); }
			auto result = worker_.try_submit(Receive{session.peer, response}); return result == SubmitResult::Accepted || result == SubmitResult::Fenced;
		}
		case TransferKind::Cancel:
			if (receiving_ && receiving_->session == session && receiving_->key == key) { worker_.cancel_snapshot(receiving_->id); } return true;
		} throw Corruption("unsupported frame");
	}
	Worker& worker_; std::uint64_t timeout_, now_ = 0; Token next_output_ = 0, selected_ = 0;
	std::vector<Channel> channels_; std::optional<Output> data_; std::optional<Sending> sending_; std::optional<Receiving> receiving_;
};

} // namespace cluster::consensus
