#pragma once

#include "operation.h"
#include <algorithm>
#include <limits>
#include <memory>
#include <map>
#include <optional>
#include <utility>
#include <vector>

namespace kronuz::journal {
struct ArtifactDescriptor {
	Identity identity{};
	std::uint64_t length = 0;
	std::uint32_t checksum = 0;
	bool operator==(const ArtifactDescriptor&) const = default;
};
inline std::string artifact_name(const ArtifactDescriptor& artifact) { return "artifact-" + detail::hexadecimal(artifact.identity); }

namespace detail {
struct OwnerSession {
	std::shared_ptr<IO> io_lifetime;
	std::unique_ptr<OwnerLock> lock;
	bool failed = false;
	bool mutation_active = false;
	std::uint64_t mutation_sequence = 0;
	Identity mutation_identity = random_identity();
	unsigned preparing = 0, prepared = 0;
	unsigned verification_handles = 0;
	std::optional<Identity> preparing_identity;
	std::map<Identity, unsigned> pins;
};
constexpr unsigned maximum_prepared_artifacts = 9;
constexpr unsigned maximum_verification_handles = 9;
struct VerificationLease {
	std::shared_ptr<OwnerSession> owner;
	explicit VerificationLease(std::shared_ptr<OwnerSession> session) : owner(std::move(session)) { ++owner->verification_handles; }
	~VerificationLease() { --owner->verification_handles; }
};
struct ArtifactPin {
	std::shared_ptr<OwnerSession> owner;
	Identity identity;
	ArtifactPin(std::shared_ptr<OwnerSession> session, Identity value) : owner(std::move(session)), identity(value) { ++owner->pins[identity]; }
	~ArtifactPin() {
		auto found = owner->pins.find(identity);
		if (--found->second == 0) { owner->pins.erase(found); }
	}
};
struct ArtifactLease {
	ArtifactDescriptor descriptor;
	std::shared_ptr<OwnerSession> owner;
	ArtifactPin pin;
	ArtifactLease(ArtifactDescriptor value, std::shared_ptr<OwnerSession> session) : descriptor(value), owner(std::move(session)), pin(owner, descriptor.identity) { ++owner->prepared; }
	~ArtifactLease() { --owner->prepared; }
};
constexpr std::uint64_t artifact_magic = 0x315452415a4e524bull;
constexpr std::uint64_t artifact_v2_magic = 0x325452415a4e524bull;
constexpr std::uint64_t artifact_seal_magic = 0x314c4145535a4e4bull;
constexpr std::size_t artifact_v1_header_size = 56;
constexpr std::size_t artifact_ownership_size = 44, artifact_seal_size = 24;
constexpr std::size_t artifact_header_size = artifact_ownership_size + artifact_seal_size;
constexpr std::size_t artifact_chunk_size = 64 * 1024;
inline std::string artifact_header_v1(Identity storage, ArtifactDescriptor artifact) {
	std::string result; put64(result, artifact_magic);
	result.append(storage.data(), storage.size()); result.append(artifact.identity.data(), artifact.identity.size());
	put64(result, artifact.length); put32(result, artifact.checksum); put32(result, crc32c(result));
	return result;
}
inline std::string artifact_ownership(Identity storage, Identity artifact) {
	std::string result; put64(result, artifact_v2_magic);
	result.append(storage.data(), storage.size()); result.append(artifact.data(), artifact.size());
	put32(result, crc32c(result)); return result;
}
inline std::string artifact_seal(Identity storage, ArtifactDescriptor artifact) {
	auto prefix = artifact_ownership(storage, artifact.identity);
	std::string result; put64(result, artifact_seal_magic); put64(result, artifact.length); put32(result, artifact.checksum);
	Checksum checksum; checksum.update(prefix); checksum.update(result); put32(result, checksum.value()); return result;
}
inline std::string artifact_header(Identity storage, ArtifactDescriptor artifact) {
	return artifact_ownership(storage, artifact.identity) + artifact_seal(storage, artifact);
}
} // namespace detail

class Journal;
class ArtifactBuilder;
class ArtifactMutation;
class PreparedArtifact {
public:
	PreparedArtifact(const PreparedArtifact&) = default;
	PreparedArtifact& operator=(const PreparedArtifact&) = default;
	PreparedArtifact(PreparedArtifact&&) noexcept = default;
	PreparedArtifact& operator=(PreparedArtifact&&) noexcept = default;
	explicit operator bool() const noexcept { return static_cast<bool>(lease_); }
	const ArtifactDescriptor& descriptor() const {
		if (!lease_) { throw std::logic_error("artifact handle moved"); }
		return lease_->descriptor;
	}
private:
	friend class Journal;
	friend class ArtifactBuilder;
	friend class ArtifactMutation;
	PreparedArtifact(ArtifactDescriptor descriptor, std::shared_ptr<detail::OwnerSession> owner)
		: lease_(std::make_shared<detail::ArtifactLease>(descriptor, std::move(owner))) {}
	std::shared_ptr<detail::ArtifactLease> lease_;
};

namespace detail {
struct ArtifactBuildState {
	ArtifactBuildState(IO& backend, std::shared_ptr<OwnerSession> session, Identity store, std::uint64_t cap)
		: io(backend), owner(std::move(session)), storage(store), maximum(cap) {
		if (owner->preparing || owner->prepared >= maximum_prepared_artifacts) { throw std::logic_error("artifact preparation slots exhausted"); }
		descriptor.identity = random_identity();
		owner->preparing_identity = descriptor.identity; ++owner->preparing;
	}
	ArtifactBuildState(const ArtifactBuildState&) = delete;
	ArtifactBuildState& operator=(const ArtifactBuildState&) = delete;
	~ArtifactBuildState() {
		file.reset();
		if (active) { --owner->preparing; owner->preparing_identity.reset(); }
	}
	IO& io;
	std::shared_ptr<OwnerSession> owner;
	std::shared_ptr<File> file;
	Identity storage;
	ArtifactDescriptor descriptor;
	std::uint64_t maximum;
	Checksum checksum;
	bool active = true, initialized = false;
};
} // namespace detail

// Creation, bounded payload writes and immutable sealing share one operation
// implementation between synchronous callers and completion-based drivers.
class ArtifactMutation final : public IOOperation {
public:
	ArtifactMutation(const ArtifactMutation&) = delete;
	ArtifactMutation& operator=(const ArtifactMutation&) = delete;
	~ArtifactMutation() override {
		if (in_flight_) { std::terminate(); }
		if (started_ && gated_) { state_->owner->failed = true; }
		release_gate();
	}
	const MutationRequest& request() const override {
		if (done()) { throw std::logic_error("artifact operation has completed"); }
		return request_;
	}
	void submitted() override {
		if (done() || in_flight_ || state_->owner->failed) { throw std::logic_error("artifact operation unavailable"); }
		started_ = in_flight_ = true;
	}
	bool complete(MutationCompletion completion) noexcept override {
		if (!in_flight_ || completion.token != request_.token) { return false; }
		in_flight_ = false;
		try {
			if (state_->owner->failed) { throw std::runtime_error("storage owner fenced during artifact operation"); }
			if (completion.error) { std::rethrow_exception(completion.error); }
			switch (phase_) {
			case Phase::Create:
				if (!completion.file) { throw std::runtime_error("artifact create completed without file"); }
				state_->file = std::move(completion.file); phase_ = Phase::Write; break;
			case Phase::Write:
				if (!completion.count || completion.count > bytes_.size() - written_) { throw std::runtime_error("artifact write made invalid progress"); }
				written_ += completion.count;
				if (written_ == bytes_.size()) {
					if (kind_ == Kind::Chunk) {
						state_->checksum.update(bytes_); state_->descriptor.length += bytes_.size(); phase_ = Phase::Done;
					} else { phase_ = Phase::Sync; }
				}
				break;
			case Phase::Sync:
				if (kind_ == Kind::Create) { state_->initialized = true; phase_ = Phase::Done; }
				else { phase_ = Phase::DirectorySync; }
				break;
			case Phase::DirectorySync:
				state_->descriptor.checksum = prepared_->descriptor().checksum;
				state_->file.reset(); state_->active = false;
				--state_->owner->preparing; state_->owner->preparing_identity.reset(); phase_ = Phase::Done; break;
			case Phase::Done: throw std::logic_error("artifact completion after finish");
			}
			if (!done()) { ++request_.token.step; refresh_request(); }
		} catch (...) { error_ = std::current_exception(); state_->owner->failed = true; }
		if (done()) { release_gate(); }
		return true;
	}
	bool done() const noexcept override { return error_ || phase_ == Phase::Done; }
	bool in_flight() const noexcept override { return in_flight_; }
	IO& io() const noexcept override { return state_->io; }
	void result() const {
		if (!done() || in_flight_) { throw std::logic_error("artifact operation has not completed"); }
		if (error_) { std::rethrow_exception(error_); }
	}
	ArtifactBuilder take_builder();
	PreparedArtifact prepared_result() const {
		result(); if (kind_ != Kind::Seal) { throw std::logic_error("artifact operation is not sealing"); }
		return *prepared_;
	}
private:
	friend class ArtifactBuilder;
	friend class Journal;
	enum class Kind { Create, Chunk, Seal };
	enum class Phase { Create, Write, Sync, DirectorySync, Done };
	ArtifactMutation(std::shared_ptr<detail::ArtifactBuildState> state, Kind kind, std::string_view chunk = {})
		: state_(std::move(state)), kind_(kind), phase_(kind == Kind::Create ? Phase::Create : Phase::Write) {
		auto& owner = *state_->owner;
		if (owner.failed || owner.mutation_active || !state_->active || (kind != Kind::Create && !state_->initialized)) { throw std::logic_error("artifact preparation unavailable"); }
		if (owner.mutation_sequence == std::numeric_limits<std::uint64_t>::max()) { throw std::overflow_error("storage operation sequence exhausted"); }
		if (kind == Kind::Create) {
			name_ = artifact_name(state_->descriptor);
			bytes_ = detail::artifact_ownership(state_->storage, state_->descriptor.identity) + std::string(detail::artifact_seal_size, '\0');
		} else if (kind == Kind::Chunk) {
			if (chunk.size() > detail::artifact_chunk_size || chunk.size() > state_->maximum - state_->descriptor.length) { throw std::length_error("artifact chunk or cumulative size exceeds bound"); }
			bytes_.assign(chunk); offset_ = detail::artifact_header_size + state_->descriptor.length;
			if (bytes_.empty()) { phase_ = Phase::Done; }
		} else {
			auto descriptor = state_->descriptor; descriptor.checksum = state_->checksum.value();
			bytes_ = detail::artifact_seal(state_->storage, descriptor); offset_ = detail::artifact_ownership_size;
			// Allocate the final lease before any IO. Completion settlement cannot
			// fail allocation after removing the active preparation protection.
			prepared_.emplace(PreparedArtifact(descriptor, state_->owner));
		}
		request_.token = {owner.mutation_identity, ++owner.mutation_sequence, 1};
		if (!done()) { refresh_request(); owner.mutation_active = gated_ = true; }
	}
	void release_gate() noexcept { if (gated_) { state_->owner->mutation_active = false; gated_ = false; } }
	void refresh_request() {
		auto token = request_.token; request_ = {}; request_.token = token;
		switch (phase_) {
		case Phase::Create: request_.kind = PrimitiveKind::Create; request_.source = name_; break;
		case Phase::Write:
			request_.kind = PrimitiveKind::Write; request_.file = state_->file;
			request_.offset = offset_ + written_; request_.bytes = std::string_view(bytes_).substr(written_); break;
		case Phase::Sync: request_.kind = PrimitiveKind::Sync; request_.file = state_->file; break;
		case Phase::DirectorySync: request_.kind = PrimitiveKind::DirectorySync; break;
		case Phase::Done: break;
		}
	}
	std::shared_ptr<detail::ArtifactBuildState> state_;
	Kind kind_;
	Phase phase_;
	std::string name_, bytes_;
	std::uint64_t offset_ = 0;
	std::size_t written_ = 0;
	MutationRequest request_;
	std::optional<PreparedArtifact> prepared_;
	std::exception_ptr error_;
	bool started_ = false, in_flight_ = false, gated_ = false, builder_taken_ = false;
};

// One builder facade; an accepted operation retains its preparation state,
// backend and owner lock even if this facade disappears during IO.
class ArtifactBuilder {
public:
	ArtifactBuilder(const ArtifactBuilder&) = delete;
	ArtifactBuilder& operator=(const ArtifactBuilder&) = delete;
	ArtifactBuilder(ArtifactBuilder&&) noexcept = default;
	void append_chunk(std::string_view bytes) {
		auto operation = start(ArtifactMutation::Kind::Chunk, bytes, false);
		drive_synchronously(*operation); operation->result();
	}
	PreparedArtifact finish() {
		auto operation = start(ArtifactMutation::Kind::Seal, {}, false);
		drive_synchronously(*operation); return operation->prepared_result();
	}
	std::shared_ptr<ArtifactMutation> begin_append_chunk(std::string_view bytes) { return start(ArtifactMutation::Kind::Chunk, bytes, true); }
	std::shared_ptr<ArtifactMutation> begin_finish() { return start(ArtifactMutation::Kind::Seal, {}, true); }
private:
	friend class Journal;
	friend class ArtifactMutation;
	explicit ArtifactBuilder(std::shared_ptr<detail::ArtifactBuildState> state) : state_(std::move(state)) {}
	ArtifactBuilder(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, std::uint64_t maximum)
		: state_(std::make_shared<detail::ArtifactBuildState>(io, std::move(owner), storage, maximum)) {
		auto operation = std::shared_ptr<ArtifactMutation>(new ArtifactMutation(state_, ArtifactMutation::Kind::Create));
		drive_synchronously(*operation); operation->result();
	}
	std::shared_ptr<ArtifactMutation> start(ArtifactMutation::Kind kind, std::string_view bytes, bool asynchronous) {
		if (!state_) { throw std::logic_error("artifact builder moved"); }
		if (asynchronous && !state_->owner->io_lifetime) { throw std::logic_error("completion artifacts require owned IO"); }
		return std::shared_ptr<ArtifactMutation>(new ArtifactMutation(state_, kind, bytes));
	}
	std::shared_ptr<detail::ArtifactBuildState> state_;
};
inline ArtifactBuilder ArtifactMutation::take_builder() {
	result();
	if (kind_ != Kind::Create || builder_taken_) { throw std::logic_error("artifact builder already taken or wrong operation"); }
	builder_taken_ = true; return ArtifactBuilder(state_);
}

class ArtifactVerifier;
// Own the destination buffer and exact file/pin through original read reaping.
// The private synchronous driver borrows its caller's destination.
// One read per reader can be outstanding; a terminal old capability cannot
// release a later read's slot.
class ArtifactRead final : public IOOperation {
public:
	ArtifactRead(const ArtifactRead&) = delete;
	ArtifactRead& operator=(const ArtifactRead&) = delete;
	~ArtifactRead() override {
		if (in_flight_) { std::terminate(); }
		release();
	}
	const MutationRequest& request() const override {
		if (done_) { throw std::logic_error("artifact read has completed"); }
		return request_;
	}
	void submitted() override {
		if (done_ || in_flight_ || owner_->failed) { throw std::logic_error("artifact read submission unavailable"); }
		in_flight_ = true;
	}
	bool complete(MutationCompletion completion) noexcept override {
		if (!in_flight_ || completion.token != request_.token) { return false; }
		in_flight_ = false;
		try {
			if (owner_->failed) { throw std::logic_error("artifact owner fenced during read"); }
			if (completion.error) { std::rethrow_exception(completion.error); }
			if (!completion.count || completion.count > destination_.size()) { throw Corruption("artifact read made invalid progress"); }
			count_ = completion.count;
		} catch (...) { error_ = std::current_exception(); owner_->failed = true; }
		done_ = true; release(); return true;
	}
	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return in_flight_; }
	IO& io() const noexcept override { return *io_; }
	std::span<const char> result() const {
		if (!done_) { throw std::logic_error("artifact read has not completed"); }
		if (error_) { std::rethrow_exception(error_); }
		return std::span<const char>(destination_.data(), count_);
	}
private:
	friend class ArtifactReader;
	friend class ArtifactVerifier;
	ArtifactRead(IO& io, std::shared_ptr<detail::OwnerSession> owner, std::shared_ptr<File> file,
		std::shared_ptr<detail::ArtifactPin> pin, std::shared_ptr<detail::VerificationLease> verification,
		std::shared_ptr<bool> pending, std::uint64_t offset, std::size_t count, bool asynchronous, std::span<char> borrowed)
		: owner_(std::move(owner)), file_(std::move(file)), pin_(std::move(pin)), verification_(std::move(verification)), pending_(std::move(pending)), buffer_(asynchronous ? count : 0) {
		if (!asynchronous && borrowed.size() < count) { throw std::length_error("synchronous read destination too small"); }
		destination_ = asynchronous ? std::span<char>(buffer_) : borrowed.first(count);
		if (asynchronous && !owner_->io_lifetime) { throw std::logic_error("completion reads require owned IO"); }
		if (owner_->failed || *pending_) { throw std::logic_error("artifact read unavailable"); }
		if (owner_->mutation_sequence == std::numeric_limits<std::uint64_t>::max()) { throw std::overflow_error("IO operation token exhausted"); }
		io_ = owner_->io_lifetime ? owner_->io_lifetime : std::shared_ptr<IO>(&io, [](IO*) {});
		request_.token = {owner_->mutation_identity, ++owner_->mutation_sequence, 0};
		request_.kind = PrimitiveKind::Read; request_.file = file_; request_.offset = offset;
		request_.destination_bytes = destination_; done_ = count == 0;
		if (!done_) { *pending_ = gated_ = true; }
	}
	void release() noexcept { if (gated_) { *pending_ = false; gated_ = false; } }
	std::shared_ptr<detail::OwnerSession> owner_;
	std::shared_ptr<IO> io_;
	std::shared_ptr<File> file_;
	std::shared_ptr<detail::ArtifactPin> pin_;
	std::shared_ptr<detail::VerificationLease> verification_;
	std::shared_ptr<bool> pending_;
	std::vector<char> buffer_;
	std::span<char> destination_;
	MutationRequest request_;
	std::exception_ptr error_;
	std::size_t count_ = 0;
	bool done_ = false, in_flight_ = false, gated_ = false;
};

class ArtifactReader;
// Pin and reserve all capabilities before metadata IO. Opening never reads
// payload bytes and never turns a candidate into verified application state.
class ArtifactOpen final : public IOOperation {
public:
	ArtifactOpen(const ArtifactOpen&) = delete;
	ArtifactOpen& operator=(const ArtifactOpen&) = delete;
	~ArtifactOpen() override { if (in_flight_) { std::terminate(); } }
	const MutationRequest& request() const override {
		if (done()) { throw std::logic_error("artifact metadata open has completed"); } return request_;
	}
	void submitted() override {
		if (done() || in_flight_ || owner_->failed) { throw std::logic_error("artifact metadata submission unavailable"); } in_flight_ = true;
	}
	bool complete(MutationCompletion completion) noexcept override {
		if (!in_flight_ || completion.token != request_.token) { return false; }
		in_flight_ = false;
		try {
			if (owner_->failed) { throw std::logic_error("artifact owner fenced during metadata open"); }
			if (completion.error) { std::rethrow_exception(completion.error); }
			switch (phase_) {
			case Phase::Open:
				if (!completion.file) { throw std::runtime_error("artifact open returned no file"); }
				file_ = std::move(completion.file); phase_ = Phase::Magic; break;
			case Phase::Magic:
			case Phase::Header: {
				auto limit = phase_ == Phase::Magic ? std::size_t{8} : payload_offset_;
				if (!completion.count || completion.count > limit - written_) { throw Corruption("artifact metadata read made invalid progress"); }
				written_ += completion.count;
				if (written_ == limit) {
					if (phase_ == Phase::Magic) {
						std::string_view bytes(raw_.data(), 8); auto magic = get64(bytes);
						if (magic != detail::artifact_magic && magic != detail::artifact_v2_magic) { throw Corruption("unsupported artifact format"); }
						payload_offset_ = magic == detail::artifact_magic ? detail::artifact_v1_header_size : detail::artifact_header_size;
						phase_ = Phase::Size; written_ = 0;
					} else {
						auto& expected = payload_offset_ == detail::artifact_v1_header_size ? expected_v1_ : expected_v2_;
						if (std::string_view(raw_.data(), payload_offset_) != expected) { throw Corruption("artifact header mismatch"); }
						phase_ = Phase::Done;
					}
				}
				break;
			}
			case Phase::Size:
				if (completion.length != payload_offset_ + descriptor_.length) { throw Corruption("artifact size mismatch"); }
				phase_ = Phase::Header; break;
			case Phase::Done: break;
			}
			if (!done()) { ++request_.token.step; refresh(); }
		} catch (...) { error_ = std::current_exception(); owner_->failed = true; }
		return true;
	}
	bool done() const noexcept override { return error_ || phase_ == Phase::Done; }
	bool in_flight() const noexcept override { return in_flight_; }
	IO& io() const noexcept override { return *io_; }
	void result() const {
		if (!done()) { throw std::logic_error("artifact metadata open has not completed"); }
		if (error_) { std::rethrow_exception(error_); }
	}
	ArtifactVerifier take_verifier();
private:
	friend class Journal;
	friend class ArtifactReader;
	ArtifactOpen(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, ArtifactDescriptor descriptor,
		std::shared_ptr<detail::VerificationLease> verification, bool asynchronous)
		: owner_(std::move(owner)), verification_(std::move(verification)), descriptor_(descriptor), name_(artifact_name(descriptor)),
		  expected_v1_(detail::artifact_header_v1(storage, descriptor)), expected_v2_(detail::artifact_header(storage, descriptor)) {
		if (owner_->failed) { throw std::logic_error("artifact metadata owner fenced"); }
		if (asynchronous && !owner_->io_lifetime) { throw std::logic_error("completion metadata open requires owned IO"); }
		if (descriptor.length > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) - detail::artifact_header_size) { throw std::length_error("artifact metadata length exceeds offset bound"); }
		if (owner_->mutation_sequence == std::numeric_limits<std::uint64_t>::max()) { throw std::overflow_error("IO operation token exhausted"); }
		io_ = owner_->io_lifetime ? owner_->io_lifetime : std::shared_ptr<IO>(&io, [](IO*) {});
		pin_ = std::make_shared<detail::ArtifactPin>(owner_, descriptor.identity);
		request_.token = {owner_->mutation_identity, ++owner_->mutation_sequence, 0}; refresh();
	}
	ArtifactReader take_reader();
	void refresh() {
		auto token = request_.token; request_ = {}; request_.token = token;
		if (phase_ == Phase::Open) { request_.kind = PrimitiveKind::Open; request_.source = name_; }
		else {
			request_.file = file_;
			if (phase_ == Phase::Size) { request_.kind = PrimitiveKind::Size; }
			else {
				request_.kind = PrimitiveKind::Read; request_.offset = written_;
				auto limit = phase_ == Phase::Magic ? std::size_t{8} : payload_offset_;
				request_.destination_bytes = std::span<char>(raw_).subspan(written_, limit - written_);
			}
		}
	}
	enum class Phase { Open, Magic, Size, Header, Done };
	std::shared_ptr<detail::OwnerSession> owner_;
	std::shared_ptr<IO> io_;
	std::shared_ptr<detail::ArtifactPin> pin_;
	std::shared_ptr<detail::VerificationLease> verification_;
	std::shared_ptr<File> file_;
	std::shared_ptr<bool> pending_ = std::make_shared<bool>(false);
	ArtifactDescriptor descriptor_;
	std::string name_, expected_v1_, expected_v2_;
	std::array<char, detail::artifact_header_size> raw_{};
	MutationRequest request_;
	std::exception_ptr error_;
	std::size_t written_ = 0, payload_offset_ = 0;
	Phase phase_ = Phase::Open;
	bool in_flight_ = false, taken_ = false;
};

class ArtifactReader {
public:
	ArtifactReader(ArtifactReader&&) noexcept = default;
	ArtifactReader& operator=(ArtifactReader&& other) noexcept {
		if (this != &other) {
			file_.reset(); pin_ = std::move(other.pin_); verification_ = std::move(other.verification_); owner_ = std::move(other.owner_); file_ = std::move(other.file_); pending_ = std::move(other.pending_); io_ = other.io_; descriptor_ = other.descriptor_; payload_offset_ = other.payload_offset_;
		}
		return *this;
	}
	const ArtifactDescriptor& descriptor() const noexcept { return descriptor_; }
	std::size_t read_at(std::uint64_t offset, std::span<char> bytes) {
		auto operation = start_read(offset, bytes.size(), false, bytes);
		drive_synchronously(*operation); auto result = operation->result();
		return result.size();
	}
	std::shared_ptr<ArtifactRead> begin_read_at(std::uint64_t offset, std::size_t count) {
		return start_read(offset, count, true);
	}
private:
	friend class Journal;
	friend class ArtifactVerifier;
	friend class ArtifactOpen;
	std::shared_ptr<ArtifactRead> start_read(std::uint64_t offset, std::size_t count, bool asynchronous, std::span<char> borrowed = {}) {
		if (!owner_ || !file_ || owner_->failed) { throw std::logic_error("artifact owner fenced or reader moved"); }
		if (count > detail::artifact_chunk_size || offset > descriptor_.length || count > descriptor_.length - offset) {
			throw std::length_error("artifact read exceeds chunk or payload bound");
		}
		return std::shared_ptr<ArtifactRead>(new ArtifactRead(*io_, owner_, file_, pin_, verification_, pending_, payload_offset_ + offset, count, asynchronous, borrowed));
	}
	ArtifactReader(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, ArtifactDescriptor descriptor)
		: ArtifactReader(io, std::move(owner), storage, descriptor, {}) {
		std::array<char, detail::artifact_chunk_size> buffer{}; Checksum checksum;
		std::uint64_t offset = 0;
		while (offset < descriptor.length) {
			auto count = static_cast<std::size_t>(std::min<std::uint64_t>(buffer.size(), descriptor.length - offset));
			detail::read_all(*file_, payload_offset_ + offset, std::span<char>(buffer.data(), count));
			checksum.update(std::string_view(buffer.data(), count)); offset += count;
		}
		if (checksum.value() != descriptor.checksum) { throw Corruption("artifact payload checksum mismatch"); }
	}
	// The synchronous header path drives the same metadata operation.
	ArtifactReader(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, ArtifactDescriptor descriptor,
		std::shared_ptr<detail::VerificationLease> verification)
		: ArtifactReader(open_synchronously(io, std::move(owner), storage, descriptor, std::move(verification))) {}
	static ArtifactReader open_synchronously(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage,
		ArtifactDescriptor descriptor, std::shared_ptr<detail::VerificationLease> verification) {
		auto operation = std::shared_ptr<ArtifactOpen>(new ArtifactOpen(io, std::move(owner), storage, descriptor, std::move(verification), false));
		drive_synchronously(*operation); return operation->take_reader();
	}
	explicit ArtifactReader(ArtifactOpen& operation)
		: owner_(operation.owner_), pin_(operation.pin_), verification_(operation.verification_), file_(operation.file_), pending_(operation.pending_), io_(operation.io_.get()), descriptor_(operation.descriptor_), payload_offset_(operation.payload_offset_) {}

	std::shared_ptr<detail::OwnerSession> owner_;
	std::shared_ptr<detail::ArtifactPin> pin_;
	std::shared_ptr<detail::VerificationLease> verification_;
	std::shared_ptr<File> file_;
	std::shared_ptr<bool> pending_ = std::make_shared<bool>(false);
	IO* io_ = nullptr;
	ArtifactDescriptor descriptor_;
	std::size_t payload_offset_ = 0;
};

// Candidate bytes must remain unpublished until CRC and semantic validation
// both succeed. IO outlives this handle and its promoted reader.
class ArtifactVerifier {
public:
	ArtifactVerifier(ArtifactVerifier&&) noexcept = default;
	ArtifactVerifier& operator=(ArtifactVerifier&&) noexcept = default;
	ArtifactVerifier(const ArtifactVerifier&) = delete;
	ArtifactVerifier& operator=(const ArtifactVerifier&) = delete;
	const ArtifactDescriptor& descriptor() const noexcept { return reader_.descriptor(); }
	std::uint64_t offset() const noexcept { return offset_; }
	std::size_t read_next(std::span<char> destination) {
		auto operation = start_read_next(destination.size(), false, destination);
		drive_synchronously(*operation); return finish_read_next(operation);
	}
	std::shared_ptr<ArtifactRead> begin_read_next(std::size_t capacity) { return start_read_next(capacity, true); }
	std::size_t finish_read_next(const std::shared_ptr<ArtifactRead>& operation) {
		healthy();
		if (!operation || pending_.lock() != operation) { throw std::invalid_argument("stale or foreign verification read"); }
		auto result = operation->result();
		checksum_.update(std::string_view(result.data(), result.size())); offset_ += result.size(); pending_.reset(); return result.size();
	}
	ArtifactReader finish() && {
		healthy();
		if (pending_.lock() || offset_ != descriptor().length) { throw std::logic_error("artifact verification is incomplete"); }
		if (checksum_.value() != descriptor().checksum) {
			reader_.owner_->failed = true; throw Corruption("artifact payload checksum mismatch");
		}
		return std::move(reader_); // Preserve the verified FD, pin and lease.
	}
private:
	friend class Journal;
	friend class ArtifactOpen;
	explicit ArtifactVerifier(ArtifactReader reader) : reader_(std::move(reader)) {}
	ArtifactVerifier(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, ArtifactDescriptor descriptor,
		std::shared_ptr<detail::VerificationLease> verification)
		: reader_(io, std::move(owner), storage, descriptor, std::move(verification)) {}
	std::shared_ptr<ArtifactRead> start_read_next(std::size_t capacity, bool asynchronous, std::span<char> borrowed = {}) {
		healthy();
		if (pending_.lock()) { throw std::logic_error("verification read awaits settlement"); }
		if (capacity > detail::artifact_chunk_size || (!capacity && offset_ < descriptor().length)) { throw std::length_error("invalid verification chunk bound"); }
		auto count = static_cast<std::size_t>(std::min<std::uint64_t>(capacity, descriptor().length - offset_));
		auto operation = reader_.start_read(offset_, count, asynchronous, borrowed); pending_ = operation; return operation;
	}
	void healthy() const {
		if (!reader_.owner_ || !reader_.file_ || reader_.owner_->failed) { throw std::logic_error("artifact owner fenced or verifier moved"); }
	}
	ArtifactReader reader_;
	std::weak_ptr<ArtifactRead> pending_;
	Checksum checksum_;
	std::uint64_t offset_ = 0;
};
inline ArtifactReader ArtifactOpen::take_reader() {
	result(); if (taken_) { throw std::logic_error("artifact metadata capability already consumed"); }
	if (owner_->failed) { throw std::logic_error("artifact metadata owner fenced before consumption"); }
	ArtifactReader reader(*this); taken_ = true; return reader;
}
inline ArtifactVerifier ArtifactOpen::take_verifier() { return ArtifactVerifier(take_reader()); }
} // namespace kronuz::journal
