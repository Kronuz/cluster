#pragma once

#include "primitives.h"
#include <algorithm>
#include <limits>
#include <memory>
#include <map>
#include <optional>
#include <utility>

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
	PreparedArtifact(ArtifactDescriptor descriptor, std::shared_ptr<detail::OwnerSession> owner)
		: lease_(std::make_shared<detail::ArtifactLease>(descriptor, std::move(owner))) {}
	std::shared_ptr<detail::ArtifactLease> lease_;
};

// All builder operations run on the journal's owning storage executor, and
// may interleave with complete journal appends between bounded chunks. The
// shared session keeps the stable lock until every preparation handle closes.
class ArtifactBuilder {
public:
	ArtifactBuilder(const ArtifactBuilder&) = delete;
	ArtifactBuilder& operator=(const ArtifactBuilder&) = delete;
	ArtifactBuilder(ArtifactBuilder&& other) noexcept
		: io_(other.io_), owner_(std::move(other.owner_)), file_(std::move(other.file_)), storage_(other.storage_),
			descriptor_(other.descriptor_), maximum_(other.maximum_), checksum_(other.checksum_), active_(std::exchange(other.active_, false)) {}
	~ArtifactBuilder() { file_.reset(); if (active_) { --owner_->preparing; owner_->preparing_identity.reset(); } }
	void append_chunk(std::string_view bytes) {
		if (!active_ || owner_->failed) { throw std::logic_error("artifact preparation unavailable"); }
		if (owner_->mutation_active) { throw std::logic_error("journal mutation is pending"); }
		if (bytes.size() > detail::artifact_chunk_size || bytes.size() > maximum_ - descriptor_.length) {
			throw std::length_error("artifact chunk or cumulative size exceeds bound");
		}
		try {
			detail::write_all(*file_, detail::artifact_header_size + descriptor_.length, bytes);
			checksum_.update(bytes); descriptor_.length += bytes.size();
		} catch (...) { owner_->failed = true; throw; }
	}
	PreparedArtifact finish() {
		if (!active_ || owner_->failed) { throw std::logic_error("artifact preparation unavailable"); }
		if (owner_->mutation_active) { throw std::logic_error("journal mutation is pending"); }
		try {
			descriptor_.checksum = checksum_.value();
			detail::write_all(*file_, detail::artifact_ownership_size, detail::artifact_seal(storage_, descriptor_));
			file_->sync(); io_.sync_directory();
			file_.reset(); active_ = false; --owner_->preparing; owner_->preparing_identity.reset();
			return PreparedArtifact(descriptor_, owner_);
		} catch (...) { owner_->failed = true; throw; }
	}
private:
	friend class Journal;
	ArtifactBuilder(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, std::uint64_t maximum)
		: io_(io), owner_(std::move(owner)), storage_(storage), maximum_(maximum) {
		if (owner_->preparing || owner_->prepared >= detail::maximum_prepared_artifacts) { throw std::logic_error("artifact preparation slots exhausted"); }
		try {
			descriptor_.identity = detail::random_identity(); owner_->preparing_identity = descriptor_.identity; file_ = io_.create_exclusive(artifact_name(descriptor_));
			// Establish immutable ownership before accepting any payload. Only
			// the separate seal is replaced by finish; it starts unpublished.
			detail::write_all(*file_, 0, detail::artifact_ownership(storage_, descriptor_.identity) + std::string(detail::artifact_seal_size, '\0'));
			file_->sync();
			++owner_->preparing; active_ = true;
		} catch (...) { owner_->failed = true; throw; }
	}
	IO& io_;
	std::shared_ptr<detail::OwnerSession> owner_;
	std::unique_ptr<File> file_;
	Identity storage_;
	ArtifactDescriptor descriptor_;
	std::uint64_t maximum_;
	Checksum checksum_;
	bool active_ = false;
};

class ArtifactVerifier;
class ArtifactReader {
public:
	ArtifactReader(ArtifactReader&&) noexcept = default;
	ArtifactReader& operator=(ArtifactReader&& other) noexcept {
		if (this != &other) {
			file_.reset(); pin_ = std::move(other.pin_); verification_ = std::move(other.verification_); owner_ = std::move(other.owner_); file_ = std::move(other.file_); descriptor_ = other.descriptor_; payload_offset_ = other.payload_offset_;
		}
		return *this;
	}
	const ArtifactDescriptor& descriptor() const noexcept { return descriptor_; }
	std::size_t read_at(std::uint64_t offset, std::span<char> bytes) {
		if (!owner_ || !file_ || owner_->failed) { throw std::logic_error("artifact owner fenced or reader moved"); }
		if (bytes.size() > detail::artifact_chunk_size || offset > descriptor_.length || bytes.size() > descriptor_.length - offset) {
			throw std::length_error("artifact read exceeds chunk or payload bound");
		}
		try {
			auto count = file_->read_at(payload_offset_ + offset, bytes);
			if ((!bytes.empty() && count == 0) || count > bytes.size()) { throw Corruption("artifact read made invalid progress"); }
			return count;
		}
		catch (...) { owner_->failed = true; throw; }
	}
private:
	friend class Journal;
	friend class ArtifactVerifier;
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
	// Header-only construction is private to the incremental verifier.
	ArtifactReader(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, ArtifactDescriptor descriptor,
		std::shared_ptr<detail::VerificationLease> verification)
		: owner_(std::move(owner)), verification_(std::move(verification)), file_(io.open_existing(artifact_name(descriptor))), descriptor_(descriptor) {
		std::array<char, 8> magic_bytes{}; detail::read_all(*file_, 0, magic_bytes);
		std::string_view magic_view(magic_bytes.data(), magic_bytes.size()); auto magic = get64(magic_view);
		if (magic != detail::artifact_magic && magic != detail::artifact_v2_magic) { throw Corruption("unsupported artifact format"); }
		payload_offset_ = magic == detail::artifact_magic ? detail::artifact_v1_header_size : detail::artifact_header_size;
		if (file_->size() != payload_offset_ + descriptor.length) { throw Corruption("artifact size mismatch"); }
		std::array<char, detail::artifact_header_size> header{}; detail::read_all(*file_, 0, std::span<char>(header.data(), payload_offset_));
		auto expected = magic == detail::artifact_magic ? detail::artifact_header_v1(storage, descriptor) : detail::artifact_header(storage, descriptor);
		if (std::string_view(header.data(), payload_offset_) != expected) { throw Corruption("artifact header mismatch"); }
		pin_ = std::make_shared<detail::ArtifactPin>(owner_, descriptor.identity);
	}
	std::shared_ptr<detail::OwnerSession> owner_;
	std::shared_ptr<detail::ArtifactPin> pin_;
	std::shared_ptr<detail::VerificationLease> verification_;
	std::unique_ptr<File> file_;
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
		healthy();
		if (destination.size() > detail::artifact_chunk_size || (destination.empty() && offset_ < descriptor().length)) {
			throw std::length_error("invalid verification chunk bound");
		}
		auto count = static_cast<std::size_t>(std::min<std::uint64_t>(destination.size(), descriptor().length - offset_));
		if (!count) { return 0; }
		// One backend call. Partial progress stays visible to the scheduler.
		auto actual = reader_.read_at(offset_, destination.first(count));
		checksum_.update(std::string_view(destination.data(), actual)); offset_ += actual; return actual;
	}
	ArtifactReader finish() && {
		healthy();
		if (offset_ != descriptor().length) { throw std::logic_error("artifact verification is incomplete"); }
		if (checksum_.value() != descriptor().checksum) {
			reader_.owner_->failed = true; throw Corruption("artifact payload checksum mismatch");
		}
		return std::move(reader_); // Preserve the verified FD, pin and lease.
	}
private:
	friend class Journal;
	ArtifactVerifier(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, ArtifactDescriptor descriptor,
		std::shared_ptr<detail::VerificationLease> verification)
		: reader_(io, std::move(owner), storage, descriptor, std::move(verification)) {}
	void healthy() const {
		if (!reader_.owner_ || !reader_.file_ || reader_.owner_->failed) { throw std::logic_error("artifact owner fenced or verifier moved"); }
	}
	ArtifactReader reader_;
	Checksum checksum_;
	std::uint64_t offset_ = 0;
};
} // namespace kronuz::journal
