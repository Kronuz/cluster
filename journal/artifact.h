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
	std::unique_ptr<OwnerLock> lock;
	bool failed = false;
	unsigned preparing = 0, prepared = 0;
	std::optional<Identity> preparing_identity;
	std::map<Identity, unsigned> pins;
};
constexpr unsigned maximum_prepared_artifacts = 9;
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
constexpr std::size_t artifact_header_size = 56;
constexpr std::size_t artifact_chunk_size = 64 * 1024;
inline std::string artifact_header(Identity storage, ArtifactDescriptor artifact) {
	std::string result; put64(result, artifact_magic);
	result.append(storage.data(), storage.size()); result.append(artifact.identity.data(), artifact.identity.size());
	put64(result, artifact.length); put32(result, artifact.checksum); put32(result, crc32c(result));
	return result;
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
		try {
			descriptor_.checksum = checksum_.value();
			detail::write_all(*file_, 0, detail::artifact_header(storage_, descriptor_));
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
			// The placeholder is never authoritative and cannot validate until
			// finish seals its length and checksum.
			detail::write_all(*file_, 0, std::string(detail::artifact_header_size, '\0'));
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

class ArtifactReader {
public:
	ArtifactReader(ArtifactReader&&) noexcept = default;
	ArtifactReader& operator=(ArtifactReader&& other) noexcept {
		if (this != &other) {
			file_.reset(); pin_ = std::move(other.pin_); owner_ = std::move(other.owner_); file_ = std::move(other.file_); descriptor_ = other.descriptor_;
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
			auto count = file_->read_at(detail::artifact_header_size + offset, bytes);
			if ((!bytes.empty() && count == 0) || count > bytes.size()) { throw Corruption("artifact read made invalid progress"); }
			return count;
		}
		catch (...) { owner_->failed = true; throw; }
	}
private:
	friend class Journal;
	ArtifactReader(IO& io, std::shared_ptr<detail::OwnerSession> owner, Identity storage, ArtifactDescriptor descriptor)
		: owner_(std::move(owner)), file_(io.open_existing(artifact_name(descriptor))), descriptor_(descriptor) {
		if (file_->size() != detail::artifact_header_size + descriptor.length) { throw Corruption("artifact size mismatch"); }
		std::array<char, detail::artifact_header_size> header{}; detail::read_all(*file_, 0, header);
		if (std::string_view(header.data(), header.size()) != detail::artifact_header(storage, descriptor)) { throw Corruption("artifact header mismatch"); }
		std::array<char, detail::artifact_chunk_size> buffer{}; Checksum checksum;
		std::uint64_t offset = 0;
		while (offset < descriptor.length) {
			auto count = static_cast<std::size_t>(std::min<std::uint64_t>(buffer.size(), descriptor.length - offset));
			detail::read_all(*file_, detail::artifact_header_size + offset, std::span<char>(buffer.data(), count));
			checksum.update(std::string_view(buffer.data(), count)); offset += count;
		}
		if (checksum.value() != descriptor.checksum) { throw Corruption("artifact payload checksum mismatch"); }
		pin_ = std::make_shared<detail::ArtifactPin>(owner_, descriptor.identity);
	}
	std::shared_ptr<detail::OwnerSession> owner_;
	std::shared_ptr<detail::ArtifactPin> pin_;
	std::unique_ptr<File> file_;
	ArtifactDescriptor descriptor_;
};
} // namespace kronuz::journal
