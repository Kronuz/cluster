#pragma once

#include "artifact.h"
#include "resources.h"
#include <array>
#include <limits>
#include <random>
#include <utility>
#include <optional>
#include <vector>

namespace kronuz::journal {

struct Frontier {
	Identity identity{};
	std::uint64_t generation = 1;
	std::uint64_t offset = 28;
	std::uint64_t sequence = 0;
	std::uint32_t version = 1;
	Identity journal_identity{};
	std::uint64_t base_sequence = 0;
	std::optional<ArtifactDescriptor> checkpoint;
	std::vector<ArtifactDescriptor> dependencies;
};

// A session-bound publication selection, not a file pin or durable identifier.
class PublishedArtifactSelection {
public:
	PublishedArtifactSelection(const PublishedArtifactSelection&) = default;
	PublishedArtifactSelection& operator=(const PublishedArtifactSelection&) = default;
	PublishedArtifactSelection(PublishedArtifactSelection&&) noexcept = default;
	PublishedArtifactSelection& operator=(PublishedArtifactSelection&&) noexcept = default;
	const ArtifactDescriptor& descriptor() const noexcept { return dependency_; }
	const ArtifactDescriptor& checkpoint() const noexcept { return checkpoint_; }
	std::uint64_t base_sequence() const noexcept { return base_sequence_; }
	std::uint64_t generation() const noexcept { return generation_; }
private:
	friend class Journal;
	PublishedArtifactSelection(const Frontier& publication, std::size_t index, const std::shared_ptr<detail::OwnerSession>& owner)
		: owner_(owner), identity_(publication.identity), journal_identity_(publication.journal_identity),
		generation_(publication.generation), base_sequence_(publication.base_sequence), checkpoint_(*publication.checkpoint),
		index_(index), dependency_(publication.dependencies[index]) {}
	std::weak_ptr<detail::OwnerSession> owner_;
	Identity identity_, journal_identity_;
	std::uint64_t generation_, base_sequence_;
	ArtifactDescriptor checkpoint_;
	std::size_t index_;
	ArtifactDescriptor dependency_;
};

struct ReclaimStats {
	std::size_t scanned = 0, removed = 0, protected_files = 0, unknown_files = 0;
	std::uint64_t logical_bytes = 0;
	bool logical_bytes_saturated = false, complete = false;
};

struct MutationPlan {
	StorageResources peak, added, removed;
};

// Synchronous, single-owner opaque storage batches. IO outlives the journal
// and every artifact builder, prepared handle, and reader.
// Successful append returns only after the manifest's directory barrier.
// Recover callbacks must build unpublished state: a later batch can fail.
class Journal {
public:
	explicit Journal(IO& io, std::size_t maximum_batch = 64u * 1024 * 1024, std::uint64_t maximum_artifact = 512ull * 1024 * 1024)
		: io_(io), maximum_batch_(maximum_batch), maximum_artifact_(maximum_artifact) {
		if (maximum_batch == 0 || maximum_batch > std::numeric_limits<std::uint32_t>::max()) {
			throw std::invalid_argument("invalid journal batch bound");
		}
		if (maximum_artifact == 0 || maximum_artifact > maximum_offset - detail::artifact_header_size) {
			throw std::invalid_argument("invalid journal artifact bound");
		}
	}
	Journal(const Journal&) = delete;
	Journal& operator=(const Journal&) = delete;
	bool fenced() const noexcept { return owner_->failed; }
	// Trusted owning host: uncertainty outside a Journal method must fence
	// escaped readers and capabilities from the same storage session too.
	void fence_storage() noexcept { owner_->failed = true; ready_ = false; }
	Frontier frontier() const { available(); return frontier_; }
	// Plans live beside the format encoder. Peak includes simultaneous
	// temporary names/bytes; successful settlement credits only removed names.
	static MutationPlan bootstrap_plan() {
		auto bytes = std::uint64_t(file_header_size) + encode_manifest(Frontier{}).size();
		return {{bytes, 4}, {bytes, 3}, {}};
	}
	static MutationPlan append_footprint(std::size_t payload_bound) {
		if (payload_bound > std::numeric_limits<std::uint32_t>::max()) { throw std::length_error("journal batch wire bound"); }
		auto growth = std::uint64_t(batch_header_size) + payload_bound;
		return {{growth + maximum_manifest_size, 1}, {growth, 0}, {}};
	}
	MutationPlan append_plan(std::size_t payload_bound) const {
		available();
		if (payload_bound > maximum_batch_) { throw std::length_error("journal batch exceeds configured bound"); }
		auto plan = append_footprint(payload_bound);
		auto growth = plan.added.logical_bytes;
		if (frontier_.sequence == std::numeric_limits<std::uint64_t>::max() || frontier_.offset > maximum_offset - growth) {
			throw std::length_error("journal frontier exhausted");
		}
		// A reservation may wait across migration or dependency-count changes.
		return plan;
	}
	StorageResources artifact_footprint(std::uint64_t payload) const {
		available();
		if (payload > maximum_artifact_) { throw std::length_error("artifact exceeds configured bound"); }
		return {std::uint64_t(detail::artifact_header_size) + payload, 1};
	}
	MutationPlan checkpoint_plan(std::span<const std::uint64_t> payload_bounds) const {
		// All artifacts are new. Bounds describe peak admission, not exact
		// settlement: recompute with actual lengths immediately before publish.
		// The removed manifest footprint belongs to the CURRENT generation.
		available();
		if (payload_bounds.empty() || payload_bounds.size() > maximum_dependencies + 1) {
			throw std::invalid_argument("invalid checkpoint artifact count");
		}
		Frontier target = frontier_; target.version = 2; target.checkpoint = ArtifactDescriptor{};
		target.dependencies.assign(payload_bounds.size() - 1, ArtifactDescriptor{});
		StorageResources added{std::uint64_t(file_header_v2_size) + encode_manifest(target).size(), 2};
		for (auto bound : payload_bounds) { added = detail::resources_add(added, artifact_footprint(bound)); }
		return {added, added, {encode_manifest(frontier_).size(), 1}};
	}
	ArtifactBuilder prepare_artifact() {
		available(); return ArtifactBuilder(io_, owner_, frontier_.identity, maximum_artifact_);
	}
	PreparedArtifact pin_artifact(const ArtifactDescriptor& artifact) {
		available();
		if (owner_->prepared + owner_->preparing >= detail::maximum_prepared_artifacts ||
			(!(frontier_.checkpoint && *frontier_.checkpoint == artifact) && std::find(frontier_.dependencies.begin(), frontier_.dependencies.end(), artifact) == frontier_.dependencies.end())) {
			throw std::invalid_argument("artifact is not currently referenced or preparation slots are exhausted");
		}
		return PreparedArtifact(artifact, owner_);
	}


	std::optional<ArtifactVerifier> begin_artifact_verification(const PreparedArtifact& artifact) {
		available();
		if (!artifact.lease_ || artifact.lease_->owner != owner_) { throw std::invalid_argument("foreign or moved artifact preparation"); }
		validate_artifact_bound(artifact.descriptor());
		if (owner_->verification_handles >= detail::maximum_verification_handles) { return std::nullopt; }
		auto lease = std::make_shared<detail::VerificationLease>(owner_);
		try { return ArtifactVerifier(io_, owner_, frontier_.identity, artifact.descriptor(), std::move(lease)); }
		catch (...) { owner_->failed = true; throw; }
	}

	std::optional<PublishedArtifactSelection> select_published_dependency(std::size_t index) const {
		available();
		if (!frontier_.checkpoint || index >= frontier_.dependencies.size()) { return std::nullopt; }
		return PublishedArtifactSelection(frontier_, index, owner_);
	}
	std::optional<ArtifactVerifier> begin_published_verification(const PublishedArtifactSelection& selection) {
		available();
		if (selection.owner_.lock() != owner_) { throw std::invalid_argument("foreign, expired or moved publication selection"); }
		if (selection.identity_ != frontier_.identity || selection.generation_ != frontier_.generation ||
			selection.journal_identity_ != frontier_.journal_identity || selection.base_sequence_ != frontier_.base_sequence ||
			!frontier_.checkpoint || selection.checkpoint_ != *frontier_.checkpoint ||
			selection.index_ >= frontier_.dependencies.size() || selection.dependency_ != frontier_.dependencies[selection.index_]) {
			return std::nullopt;
		}
		if (owner_->verification_handles >= detail::maximum_verification_handles) { return std::nullopt; }
		auto lease = std::make_shared<detail::VerificationLease>(owner_);
		try { return ArtifactVerifier(io_, owner_, frontier_.identity, selection.dependency_, std::move(lease)); }
		catch (...) { owner_->failed = true; throw; }
	}

	void verify_artifact(const PreparedArtifact& artifact) {
		available();
		if (!artifact.lease_ || artifact.lease_->owner != owner_) { throw std::invalid_argument("foreign artifact preparation"); }
		try {
			validate_artifact_bound(artifact.descriptor());
			ArtifactReader verified(io_, owner_, frontier_.identity, artifact.descriptor());
		} catch (...) { owner_->failed = true; throw; }
	}


	ReclaimStats reclaim_step(std::size_t scan_budget = 128) {
		available();
		if (scan_budget == 0 || scan_budget > 4096) { throw std::invalid_argument("invalid reclamation scan budget"); }
		ReclaimStats stats;
		try {
			if (!reclaim_cursor_) { reclaim_cursor_ = io_.scan_directory(); }
			while (stats.scanned < scan_budget) {
				auto name = reclaim_cursor_->next();
				if (!name) { stats.complete = true; reclaim_cursor_.reset(); break; }
				++stats.scanned;
				if (protected_name(*name)) { ++stats.protected_files; continue; }
				if (!reclaim_name(*name)) { continue; }
				auto file = io_.open_reclaim_candidate(*name);
				if (!file) { ++stats.unknown_files; continue; }
				auto length = file->size();
				if (!owned_candidate(*name, *file, length)) { ++stats.unknown_files; continue; }
				file.reset();
				if (protected_name(*name)) { ++stats.protected_files; continue; }
				io_.remove(*name); ++stats.removed;
				if (length > std::numeric_limits<std::uint64_t>::max() - stats.logical_bytes) {
					stats.logical_bytes = std::numeric_limits<std::uint64_t>::max(); stats.logical_bytes_saturated = true;
				} else { stats.logical_bytes += length; }
			}
			if (stats.removed) { io_.sync_directory(); }
			return stats;
		} catch (...) { owner_->failed = true; throw; }
	}

	Frontier create(Identity identity) {
		unused();
		try {
			owner_->lock = io_.acquire_owner(true);
			// Reserve the final metadata name exclusively. A damaged existing
			// store with a missing lock must never be reinitialized over it.
			auto reservation = io_.create_exclusive(manifest_name);
			frontier_.identity = identity;
			data_ = io_.create_exclusive(data_name(frontier_));
			auto header = file_header(frontier_);
			write_all(*data_, 0, header);
			data_->sync();
			// Establish the generation and stable lock before referencing them.
			io_.sync_directory();
			publish(frontier_);
			ready_ = true;
			return frontier_;
		} catch (...) { owner_->failed = true; throw; }
	}

	template <class Replay>
	Frontier recover(Replay&& replay) {
		return recover(std::forward<Replay>(replay), [](const Frontier&, ArtifactReader&, std::span<ArtifactReader>) {
			throw Corruption("checkpoint recovery handler required");
		});
	}
	template <class Replay, class Restore>
	Frontier recover(Replay&& replay, Restore&& restore) {
		unused();
		try {
			owner_->lock = io_.acquire_owner(false);
			auto manifest = io_.open_existing(manifest_name);
			auto manifest_length = manifest->size();
			if (manifest_length != manifest_size && (manifest_length < manifest_v2_size || manifest_length > maximum_manifest_size)) {
				throw Corruption("invalid manifest size");
			}
			std::array<char, maximum_manifest_size> raw{};
			read_all(*manifest, 0, std::span<char>(raw.data(), static_cast<std::size_t>(manifest_length)));
			frontier_ = decode_manifest(std::string_view(raw.data(), static_cast<std::size_t>(manifest_length)));
			data_ = io_.open_existing(data_name(frontier_));
			auto size = data_->size();
			if (size < frontier_.offset) { throw Corruption("journal shorter than durable frontier"); }
			auto expected_header = file_header(frontier_);
			std::array<char, file_header_v2_size> raw_header{};
			read_all(*data_, 0, std::span<char>(raw_header.data(), expected_header.size()));
			if (std::string_view(raw_header.data(), expected_header.size()) != expected_header) {
				throw Corruption("journal identity or header mismatch");
			}
			if (frontier_.checkpoint) {
				validate_artifact_bound(*frontier_.checkpoint);
				ArtifactReader checkpoint(io_, owner_, frontier_.identity, *frontier_.checkpoint);
				std::vector<ArtifactReader> dependencies;
				for (const auto& descriptor : frontier_.dependencies) {
					validate_artifact_bound(descriptor);
					dependencies.push_back(ArtifactReader(io_, owner_, frontier_.identity, descriptor));
				}
				restore(frontier_, checkpoint, std::span<ArtifactReader>(dependencies));
			}
			std::uint64_t offset = expected_header.size(), sequence = frontier_.base_sequence;
			while (offset < frontier_.offset) {
				if (frontier_.offset - offset < batch_header_size) { throw Corruption("truncated durable batch header"); }
				std::array<char, batch_header_size> raw_batch{};
				read_all(*data_, offset, raw_batch);
				std::string_view header(raw_batch.data(), raw_batch.size());
				if (crc32c(header.substr(0, batch_header_size - 4)) != checksum_at_end(header)) {
					throw Corruption("batch header checksum mismatch");
				}
				if (get32(header) != batch_magic) { throw Corruption("invalid batch magic"); }
				auto length = get32(header);
				auto next_sequence = get64(header);
				auto checksum = get32(header);
				if (sequence == std::numeric_limits<std::uint64_t>::max() || next_sequence != sequence + 1) {
					throw Corruption("batch sequence mismatch");
				}
				if (length > maximum_batch_ || length > frontier_.offset - offset - batch_header_size) {
					throw Corruption("invalid durable batch length");
				}
				std::string batch(length, '\0');
				read_all(*data_, offset + batch_header_size, std::span<char>(batch.data(), batch.size()));
				if (crc32c(batch) != checksum) { throw Corruption("batch payload checksum mismatch"); }
				// This view expires when the callback returns.
				replay(next_sequence, std::string_view(batch));
				offset += std::uint64_t(batch_header_size) + length;
				sequence = next_sequence;
			}
			if (sequence != frontier_.sequence) { throw Corruption("manifest sequence mismatch"); }
			if (size > frontier_.offset) {
				// Only bytes outside a fully verified frontier are disposable.
				data_->truncate(frontier_.offset);
				data_->sync();
			}
			// A process restart can observe a replaced manifest whose directory
			// barrier never ran. Seal that selected namespace before using it.
			io_.sync_directory();
			ready_ = true;
			return frontier_;
		} catch (...) { owner_->failed = true; throw; }
	}

	Frontier append_batch(std::string_view batch) {
		available();
		auto growth = append_plan(batch.size()).added.logical_bytes;
		Frontier next = frontier_;
		++next.sequence;
		next.offset += growth;
		std::string header;
		put32(header, batch_magic);
		put32(header, static_cast<std::uint32_t>(batch.size()));
		put64(header, next.sequence);
		put32(header, crc32c(batch));
		put32(header, crc32c(header));
		try {
			write_all(*data_, frontier_.offset, header);
			write_all(*data_, frontier_.offset + batch_header_size, batch);
			data_->sync();
			publish(next);
			frontier_ = next;
			return frontier_;
		} catch (...) { owner_->failed = true; ready_ = false; throw; }
	}

	Frontier publish_checkpoint(const PreparedArtifact& checkpoint, std::span<const PreparedArtifact> dependencies, std::uint64_t covered_sequence) {
		available();
		if (covered_sequence != frontier_.sequence || dependencies.size() > maximum_dependencies ||
			!checkpoint.lease_ || checkpoint.lease_->owner != owner_ || frontier_.generation == std::numeric_limits<std::uint64_t>::max()) {
			throw std::invalid_argument("stale checkpoint frontier, foreign preparation, or generation limit");
		}
		Frontier next = frontier_; next.version = 2; ++next.generation;
		next.journal_identity = detail::random_identity(); next.base_sequence = covered_sequence;
		next.offset = file_header_v2_size; next.checkpoint = checkpoint.descriptor(); next.dependencies.clear();
		for (const auto& dependency : dependencies) {
			if (!dependency.lease_ || dependency.lease_->owner != owner_ || dependency.descriptor().identity == checkpoint.descriptor().identity ||
				std::find_if(next.dependencies.begin(), next.dependencies.end(), [&](const auto& previous) {
					return previous.identity == dependency.descriptor().identity;
				}) != next.dependencies.end()) { throw std::invalid_argument("foreign or duplicate checkpoint dependency"); }
			next.dependencies.push_back(dependency.descriptor());
		}
		try {
			validate_artifact_bound(*next.checkpoint);
			for (const auto& descriptor : next.dependencies) {
				validate_artifact_bound(descriptor);
			}
			// Prepared capabilities prove successfully sealed immutable bytes.
			// Optional readback belongs before freezing the consensus cutover;
			// recovery always validates all referenced payloads.
			auto fresh = io_.create_exclusive(data_name(next));
			write_all(*fresh, 0, file_header(next)); fresh->sync();
			// All artifact names are already sealed; establish the new journal
			// before any manifest can refer to this immutable generation.
			io_.sync_directory(); publish(next);
			data_ = std::move(fresh); frontier_ = next; return frontier_;
		} catch (...) { owner_->failed = true; ready_ = false; throw; }
	}

private:
	static std::string data_name(const Frontier& frontier) {
		return frontier.version == 1 ? detail::generation_name(frontier.generation) : "generation-" + detail::hexadecimal(frontier.journal_identity);
	}
	static constexpr std::string_view manifest_name = "manifest";
	static constexpr std::uint64_t manifest_magic = 0x31464d4a5a4e524bull; // KRNZJMF1
	static constexpr std::uint64_t file_magic = 0x314154444a5a4e4bull;     // KNZJDTA1
	static constexpr std::uint64_t manifest_v2_magic = 0x32464d4a5a4e524bull;
	static constexpr std::uint64_t file_v2_magic = 0x324154444a5a4e4bull;
	static constexpr std::uint32_t batch_magic = 0x3142544bu;             // KTB1
	static constexpr std::size_t manifest_size = 52, file_header_size = 28, batch_header_size = 24;
	static constexpr std::size_t manifest_v2_size = 108, file_header_v2_size = 60, maximum_dependencies = 8;
	static constexpr std::size_t maximum_manifest_size = manifest_v2_size + maximum_dependencies * 28;
	static constexpr std::uint64_t maximum_offset = std::numeric_limits<std::int64_t>::max();

	static bool hexadecimal_name(std::string_view name, std::string_view prefix, std::size_t digits) {
		if (!name.starts_with(prefix) || name.size() != prefix.size() + digits) { return false; }
		name.remove_prefix(prefix.size());
		return std::all_of(name.begin(), name.end(), [](unsigned char byte) { return (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f'); });
	}
	static bool reclaim_name(std::string_view name) {
		return name == "journal-0000000000000001" || hexadecimal_name(name, "generation-", 32) ||
			hexadecimal_name(name, "artifact-", 32) || hexadecimal_name(name, "manifest.pending-", 32);
	}
	bool protected_name(std::string_view name) const {
		if (name == manifest_name || name == "owner.lock" || name == data_name(frontier_)) { return true; }
		if (frontier_.checkpoint && name == artifact_name(*frontier_.checkpoint)) { return true; }
		for (const auto& artifact : frontier_.dependencies) { if (name == artifact_name(artifact)) { return true; } }
		if (owner_->preparing_identity && name == "artifact-" + detail::hexadecimal(*owner_->preparing_identity)) { return true; }
		for (const auto& [identity, references] : owner_->pins) { if (name == "artifact-" + detail::hexadecimal(identity)) { return true; } }
		return false;
	}
	bool owned_candidate(std::string_view name, File& file, std::uint64_t length) const {
		try {
			std::array<char, maximum_manifest_size> raw{};
			if (name.starts_with("manifest.pending-")) {
				if (length != manifest_size && (length < manifest_v2_size || length > maximum_manifest_size)) { return false; }
				read_all(file, 0, std::span<char>(raw.data(), static_cast<std::size_t>(length)));
				return decode_manifest(std::string_view(raw.data(), static_cast<std::size_t>(length))).identity == frontier_.identity;
			}
			if (name.starts_with("artifact-")) {
				if (length < 8) { return false; }
				read_all(file, 0, std::span<char>(raw.data(), 8)); std::string_view version(raw.data(), 8);
				if (get64(version) == detail::artifact_v2_magic) {
					if (length < detail::artifact_ownership_size) { return false; }
					read_all(file, 0, std::span<char>(raw.data(), detail::artifact_ownership_size));
					std::string_view prefix(raw.data(), detail::artifact_ownership_size);
					if (crc32c(prefix.substr(0, 40)) != checksum_at_end(prefix)) { return false; }
					prefix.remove_prefix(8);
					if (prefix.substr(0, 16) != std::string_view(frontier_.identity.data(), 16)) { return false; }
					prefix.remove_prefix(16); Identity id{}; std::copy_n(prefix.begin(), id.size(), id.begin());
					return name == "artifact-" + detail::hexadecimal(id);
				}
			}
			auto size = name.starts_with("artifact-") ? detail::artifact_v1_header_size : (name.starts_with("generation-") ? file_header_v2_size : file_header_size);
			if (length < size) { return false; }
			read_all(file, 0, std::span<char>(raw.data(), size));
			std::string_view bytes(raw.data(), size);
			if (crc32c(bytes.substr(0, size - 4)) != checksum_at_end(bytes)) { return false; }
			auto magic = get64(bytes);
			if (bytes.substr(0, frontier_.identity.size()) != std::string_view(frontier_.identity.data(), frontier_.identity.size())) { return false; }
			bytes.remove_prefix(frontier_.identity.size());
			if (name.starts_with("artifact-")) {
				Identity id{}; std::copy_n(bytes.begin(), id.size(), id.begin()); bytes.remove_prefix(id.size());
				auto payload = get64(bytes);
				return magic == detail::artifact_magic && name == "artifact-" + detail::hexadecimal(id) &&
					payload <= maximum_offset - detail::artifact_v1_header_size && length == detail::artifact_v1_header_size + payload;
			}
			if (name.starts_with("generation-")) {
				Identity id{}; std::copy_n(bytes.begin(), id.size(), id.begin()); bytes.remove_prefix(id.size());
				return magic == file_v2_magic && name == "generation-" + detail::hexadecimal(id) && get64(bytes) >= 2;
			}
			return magic == file_magic && name == "journal-0000000000000001";
		} catch (const Corruption&) { return false; }
	}
	void unused() const {
		if (owner_->lock || ready_ || owner_->failed) { throw std::logic_error("journal already initialized or fenced"); }
	}
	void available() const { if (!ready_ || owner_->failed) { throw std::logic_error("journal unavailable"); } }
	void validate_artifact_bound(const ArtifactDescriptor& artifact) const {
		if (artifact.length > maximum_artifact_) { throw Corruption("checkpoint artifact exceeds configured bound"); }
	}
	static std::uint32_t checksum_at_end(std::string_view bytes) {
		bytes.remove_prefix(bytes.size() - 4);
		return get32(bytes);
	}
	static std::string file_header(const Frontier& frontier) {
		std::string result;
		put64(result, frontier.version == 1 ? file_magic : file_v2_magic);
		result.append(frontier.identity.data(), frontier.identity.size());
		if (frontier.version == 2) {
			result.append(frontier.journal_identity.data(), frontier.journal_identity.size());
			put64(result, frontier.generation); put64(result, frontier.base_sequence);
		}
		put32(result, crc32c(result));
		return result;
	}
	static std::string encode_manifest(const Frontier& frontier) {
		std::string result;
		put64(result, frontier.version == 1 ? manifest_magic : manifest_v2_magic);
		result.append(frontier.identity.data(), frontier.identity.size());
		put64(result, frontier.generation);
		put64(result, frontier.offset);
		put64(result, frontier.sequence);
		if (frontier.version == 2) {
			put64(result, frontier.base_sequence);
			result.append(frontier.journal_identity.data(), frontier.journal_identity.size());
			encode_artifact(result, *frontier.checkpoint);
			put32(result, static_cast<std::uint32_t>(frontier.dependencies.size()));
			for (const auto& artifact : frontier.dependencies) { encode_artifact(result, artifact); }
		}
		put32(result, crc32c(result));
		return result;
	}
	static Frontier decode_manifest(std::string_view bytes) {
		if (crc32c(bytes.substr(0, bytes.size() - 4)) != checksum_at_end(bytes)) { throw Corruption("manifest checksum mismatch"); }
		auto length = bytes.size(); auto magic = get64(bytes);
		if ((magic != manifest_magic && magic != manifest_v2_magic) ||
			(magic == manifest_magic && length != manifest_size) || (magic == manifest_v2_magic && length < manifest_v2_size)) {
			throw Corruption("unsupported journal manifest");
		}
		Frontier result;
		for (char& byte : result.identity) { byte = bytes.front(); bytes.remove_prefix(1); }
		result.generation = get64(bytes);
		result.offset = get64(bytes);
		result.sequence = get64(bytes);
		if (magic == manifest_v2_magic) {
			result.version = 2; result.base_sequence = get64(bytes);
			for (char& byte : result.journal_identity) { byte = bytes.front(); bytes.remove_prefix(1); }
			result.checkpoint = decode_artifact(bytes); auto count = get32(bytes);
			if (count > maximum_dependencies || bytes.size() != std::uint64_t(count) * 28 + 4) { throw Corruption("invalid checkpoint dependency count"); }
			for (std::uint32_t i = 0; i < count; ++i) {
				auto artifact = decode_artifact(bytes);
				if (artifact.identity == result.checkpoint->identity || std::find_if(result.dependencies.begin(), result.dependencies.end(), [&](const auto& previous) {
					return previous.identity == artifact.identity;
				}) != result.dependencies.end()) { throw Corruption("duplicate checkpoint artifact"); }
				result.dependencies.push_back(artifact);
			}
		}
		if ((result.version == 1 && result.generation != 1) || (result.version == 2 && result.generation < 2) ||
			result.offset < (result.version == 1 ? file_header_size : file_header_v2_size) || result.offset > maximum_offset ||
			result.sequence < result.base_sequence) {
			throw Corruption("unsupported generation or invalid durable offset");
		}
		return result;
	}
	static void encode_artifact(std::string& bytes, ArtifactDescriptor artifact) {
		bytes.append(artifact.identity.data(), artifact.identity.size()); put64(bytes, artifact.length); put32(bytes, artifact.checksum);
	}
	static ArtifactDescriptor decode_artifact(std::string_view& bytes) {
		if (bytes.size() < 28) { throw Corruption("truncated checkpoint descriptor"); }
		ArtifactDescriptor artifact;
		for (char& byte : artifact.identity) { byte = bytes.front(); bytes.remove_prefix(1); }
		artifact.length = get64(bytes); artifact.checksum = get32(bytes); return artifact;
	}
	static void read_all(File& file, std::uint64_t offset, std::span<char> bytes) {
		detail::read_all(file, offset, bytes);
	}
	static void write_all(File& file, std::uint64_t offset, std::string_view bytes) {
		detail::write_all(file, offset, bytes);
	}
	void publish(const Frontier& next) {
		// Exclusive creation plus a random suffix leaves interrupted publication
		// artifacts harmless. Automatic orphan cleanup is intentionally separate.
		std::string temporary = "manifest.pending-" + detail::hexadecimal(detail::random_identity());
		auto manifest = io_.create_exclusive(temporary);
		auto encoded = encode_manifest(next);
		write_all(*manifest, 0, encoded);
		manifest->sync();
		io_.replace(temporary, manifest_name);
		io_.sync_directory();
	}

	IO& io_;
	std::size_t maximum_batch_;
	std::uint64_t maximum_artifact_;
	// Data closes before the stable owner lock is released.
	std::shared_ptr<detail::OwnerSession> owner_ = std::make_shared<detail::OwnerSession>();
	std::unique_ptr<File> data_;
	Frontier frontier_;
	std::unique_ptr<DirectoryCursor> reclaim_cursor_;
	bool ready_ = false;
};

} // namespace kronuz::journal
