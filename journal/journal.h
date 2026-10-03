#pragma once

#include "codec.h"
#include "io.h"
#include <array>
#include <limits>
#include <random>
#include <utility>

namespace kronuz::journal {

using Identity = std::array<char, 16>;
struct Frontier {
	Identity identity{};
	std::uint64_t generation = 1;
	std::uint64_t offset = 28;
	std::uint64_t sequence = 0;
};

// Synchronous, single-owner opaque storage batches. IO outlives Journal.
// Successful append returns only after the manifest's directory barrier.
// Recover callbacks must build unpublished state: a later batch can fail.
class Journal {
public:
	explicit Journal(IO& io, std::size_t maximum_batch = 64u * 1024 * 1024)
		: io_(io), maximum_batch_(maximum_batch) {
		if (maximum_batch == 0 || maximum_batch > std::numeric_limits<std::uint32_t>::max()) {
			throw std::invalid_argument("invalid journal batch bound");
		}
	}
	Journal(const Journal&) = delete;
	Journal& operator=(const Journal&) = delete;
	bool fenced() const noexcept { return failed_; }

	Frontier create(Identity identity) {
		unused();
		try {
			owner_ = io_.acquire_owner(true);
			// Reserve the final metadata name exclusively. A damaged existing
			// store with a missing lock must never be reinitialized over it.
			auto reservation = io_.create_exclusive(manifest_name);
			frontier_.identity = identity;
			data_ = io_.create_exclusive(data_name);
			auto header = file_header(identity);
			write_all(*data_, 0, header);
			data_->sync();
			// Establish the generation and stable lock before referencing them.
			io_.sync_directory();
			publish(frontier_);
			ready_ = true;
			return frontier_;
		} catch (...) { failed_ = true; throw; }
	}

	template <class Replay>
	Frontier recover(Replay&& replay) {
		unused();
		try {
			owner_ = io_.acquire_owner(false);
			auto manifest = io_.open_existing(manifest_name);
			if (manifest->size() != manifest_size) { throw Corruption("invalid manifest size"); }
			std::array<char, manifest_size> raw{};
			read_all(*manifest, 0, raw);
			frontier_ = decode_manifest(std::string_view(raw.data(), raw.size()));
			data_ = io_.open_existing(data_name);
			auto size = data_->size();
			if (size < frontier_.offset) { throw Corruption("journal shorter than durable frontier"); }
			std::array<char, file_header_size> raw_header{};
			read_all(*data_, 0, raw_header);
			if (std::string_view(raw_header.data(), raw_header.size()) != file_header(frontier_.identity)) {
				throw Corruption("journal identity or header mismatch");
			}
			std::uint64_t offset = file_header_size, sequence = 0;
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
		} catch (...) { failed_ = true; throw; }
	}

	Frontier append_batch(std::string_view batch) {
		if (!ready_ || failed_) { throw std::logic_error("journal unavailable"); }
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
		} catch (...) { failed_ = true; ready_ = false; throw; }
	}

private:
	static constexpr std::string_view data_name = "journal-0000000000000001";
	static constexpr std::string_view manifest_name = "manifest";
	static constexpr std::uint64_t manifest_magic = 0x31464d4a5a4e524bull; // KRNZJMF1
	static constexpr std::uint64_t file_magic = 0x314154444a5a4e4bull;     // KNZJDTA1
	static constexpr std::uint32_t batch_magic = 0x3142544bu;             // KTB1
	static constexpr std::size_t manifest_size = 52, file_header_size = 28, batch_header_size = 24;
	static constexpr std::uint64_t maximum_offset = std::numeric_limits<std::int64_t>::max();

	void unused() const {
		if (owner_ || ready_ || failed_) { throw std::logic_error("journal already initialized or fenced"); }
	}
	static std::uint32_t checksum_at_end(std::string_view bytes) {
		bytes.remove_prefix(bytes.size() - 4);
		return get32(bytes);
	}
	static std::string file_header(Identity identity) {
		std::string result;
		put64(result, file_magic);
		result.append(identity.data(), identity.size());
		put32(result, crc32c(result));
		return result;
	}
	static std::string encode_manifest(const Frontier& frontier) {
		std::string result;
		put64(result, manifest_magic);
		result.append(frontier.identity.data(), frontier.identity.size());
		put64(result, frontier.generation);
		put64(result, frontier.offset);
		put64(result, frontier.sequence);
		put32(result, crc32c(result));
		return result;
	}
	static Frontier decode_manifest(std::string_view bytes) {
		if (crc32c(bytes.substr(0, manifest_size - 4)) != checksum_at_end(bytes)) { throw Corruption("manifest checksum mismatch"); }
		if (get64(bytes) != manifest_magic) { throw Corruption("unsupported journal manifest"); }
		Frontier result;
		for (char& byte : result.identity) { byte = bytes.front(); bytes.remove_prefix(1); }
		result.generation = get64(bytes);
		result.offset = get64(bytes);
		result.sequence = get64(bytes);
		if (result.generation != 1 || result.offset < file_header_size || result.offset > maximum_offset) {
			throw Corruption("unsupported generation or invalid durable offset");
		}
		return result;
	}
	static void read_all(File& file, std::uint64_t offset, std::span<char> bytes) {
		while (!bytes.empty()) {
			auto count = file.read_at(offset, bytes);
			if (count == 0 || count > bytes.size()) { throw Corruption("short journal read"); }
			offset += count;
			bytes = bytes.subspan(count);
		}
	}
	static void write_all(File& file, std::uint64_t offset, std::string_view bytes) {
		while (!bytes.empty()) {
			auto count = file.write_at(offset, bytes);
			if (count == 0 || count > bytes.size()) { throw std::runtime_error("zero or invalid journal write progress"); }
			offset += count;
			bytes.remove_prefix(count);
		}
	}
	void publish(const Frontier& next) {
		// Exclusive creation plus a random suffix leaves interrupted publication
		// artifacts harmless. Automatic orphan cleanup is intentionally separate.
		std::random_device random;
		std::string temporary = "manifest.pending-";
		constexpr char digits[] = "0123456789abcdef";
		for (unsigned i = 0; i < 4; ++i) {
			auto value = static_cast<std::uint32_t>(random());
			for (unsigned nibble = 0; nibble < 8; ++nibble) { temporary.push_back(digits[(value >> (nibble * 4)) & 15]); }
		}
		auto manifest = io_.create_exclusive(temporary);
		auto encoded = encode_manifest(next);
		write_all(*manifest, 0, encoded);
		manifest->sync();
		io_.replace(temporary, manifest_name);
		io_.sync_directory();
	}

	IO& io_;
	std::size_t maximum_batch_;
	// Data closes before the stable owner lock is released.
	std::unique_ptr<OwnerLock> owner_;
	std::unique_ptr<File> data_;
	Frontier frontier_;
	bool ready_ = false, failed_ = false;
};

} // namespace kronuz::journal
