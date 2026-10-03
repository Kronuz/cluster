#pragma once

#include "artifact.h"
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

// Synchronous, single-owner opaque storage batches. IO outlives Journal.
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
	Frontier frontier() const { available(); return frontier_; }
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
		if (batch.size() > maximum_batch_) { throw std::length_error("journal batch exceeds configured bound"); }
		if (frontier_.sequence == std::numeric_limits<std::uint64_t>::max() ||
			frontier_.offset > maximum_offset - batch_header_size - batch.size()) {
			throw std::length_error("journal frontier exhausted");
		}
		Frontier next = frontier_;
		++next.sequence;
		next.offset += std::uint64_t(batch_header_size) + batch.size();
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
			ArtifactReader verified(io_, owner_, frontier_.identity, *next.checkpoint);
			for (const auto& descriptor : next.dependencies) {
				validate_artifact_bound(descriptor); ArtifactReader dependency(io_, owner_, frontier_.identity, descriptor);
			}
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
	bool ready_ = false;
};

} // namespace kronuz::journal
