#pragma once

#include "snapshot.h"

namespace cluster::consensus {

enum class TransferKind : std::uint8_t { Begin = 1, Accepted, Chunk, Credit, Result, Cancel };
enum class TransferOutcome : std::uint8_t { Installed = 1, CaughtUp, Rejected, Failed };
struct TransferEnvelope {
	Identity cluster{}, configuration{}, source_session{}, destination_session{};
	NodeId source = 0, destination = 0;
	SnapshotKey key{};
};
struct TransferFrame { TransferKind kind; TransferEnvelope envelope; std::string_view body; };
inline constexpr std::size_t transfer_header_bytes = 128, maximum_transfer_bytes = transfer_header_bytes + 24 + 65536;

inline std::string encode_transfer(TransferKind kind, const TransferEnvelope& envelope, std::string_view body) {
	if (!valid_snapshot_key(envelope.key) || !envelope.source || !envelope.destination || envelope.source == envelope.destination ||
		kind < TransferKind::Begin || kind > TransferKind::Cancel || body.size() > maximum_transfer_bytes - transfer_header_bytes) {
		throw std::invalid_argument("invalid snapshot transfer framing");
	}
	std::string bytes; bytes.reserve(transfer_header_bytes + body.size()); bytes.append("RFTXFR01", 8);
	kronuz::journal::put32(bytes, static_cast<std::uint32_t>(transfer_header_bytes + body.size()));
	bytes.push_back(static_cast<char>(kind)); bytes.append(3, '\0');
	bytes.append(envelope.cluster.data(), 16); bytes.append(envelope.configuration.data(), 16);
	kronuz::journal::put64(bytes, envelope.source); kronuz::journal::put64(bytes, envelope.destination);
	bytes.append(envelope.source_session.data(), 16); bytes.append(envelope.destination_session.data(), 16);
	kronuz::journal::put64(bytes, envelope.key.leader_term); kronuz::journal::put64(bytes, envelope.key.transfer);
	kronuz::journal::put64(bytes, envelope.key.boundary.index); kronuz::journal::put64(bytes, envelope.key.boundary.term);
	bytes.append(body); return bytes;
}
inline TransferFrame decode_transfer(std::string_view bytes) {
	using namespace kronuz::journal;
	if (bytes.size() < transfer_header_bytes || bytes.size() > maximum_transfer_bytes || bytes.substr(0, 8) != "RFTXFR01") { throw Corruption("invalid snapshot transfer frame"); }
	bytes.remove_prefix(8); auto length = get32(bytes);
	if (length != bytes.size() + 12 || bytes.substr(1, 3) != std::string_view("\0\0\0", 3)) { throw Corruption("invalid snapshot transfer length or reserved fields"); }
	auto kind = static_cast<TransferKind>(static_cast<std::uint8_t>(bytes[0])); bytes.remove_prefix(4);
	if (kind < TransferKind::Begin || kind > TransferKind::Cancel) { throw Corruption("invalid snapshot transfer kind"); }
	TransferEnvelope envelope;
	auto identity = [&](Identity& value) { std::copy_n(bytes.begin(), 16, value.begin()); bytes.remove_prefix(16); };
	identity(envelope.cluster); identity(envelope.configuration); envelope.source = get64(bytes); envelope.destination = get64(bytes);
	identity(envelope.source_session); identity(envelope.destination_session);
	envelope.key.leader_term = get64(bytes); envelope.key.transfer = get64(bytes); envelope.key.boundary.index = get64(bytes); envelope.key.boundary.term = get64(bytes);
	if (!valid_snapshot_key(envelope.key) || !envelope.source || !envelope.destination || envelope.source == envelope.destination) { throw Corruption("invalid snapshot transfer envelope"); }
	switch (kind) {
	case TransferKind::Begin: if (bytes.size() != snapshot_descriptor_bytes) { throw Corruption("invalid Begin size"); } break;
	case TransferKind::Accepted: case TransferKind::Cancel: if (!bytes.empty()) { throw Corruption("unexpected control payload"); } break;
	case TransferKind::Chunk: case TransferKind::Credit: {
		if (bytes.size() < 24) { throw Corruption("truncated chunk control"); }
		auto fields = bytes; auto token = get64(fields); get64(fields); auto count = get32(fields);
		if (!token || static_cast<unsigned char>(fields[0]) > 1 || fields.substr(1, 3) != std::string_view("\0\0\0", 3) ||
			count > 65536 || (kind == TransferKind::Chunk ? bytes.size() != 24 + count : bytes.size() != 24 || count != 0)) { throw Corruption("invalid chunk control"); }
		break;
	}
	case TransferKind::Result:
		if (bytes.size() != 16 || static_cast<unsigned char>(bytes[0]) < 1 || static_cast<unsigned char>(bytes[0]) > 4 || bytes.substr(1, 7) != std::string_view("\0\0\0\0\0\0\0", 7)) { throw Corruption("invalid Result fields"); } break;
	}
	return {kind, envelope, bytes};
}
inline std::string transfer_chunk_body(Token token, std::uint64_t offset, std::string_view payload, bool final, bool credit = false) {
	if (!token || payload.size() > 65536 || (credit && !payload.empty())) { throw std::invalid_argument("invalid transfer chunk bound"); }
	std::string bytes; bytes.reserve(24 + payload.size()); kronuz::journal::put64(bytes, token); kronuz::journal::put64(bytes, offset);
	kronuz::journal::put32(bytes, credit ? 0 : static_cast<std::uint32_t>(payload.size())); bytes.push_back(final ? 1 : 0); bytes.append(3, '\0');
	if (!credit) { bytes.append(payload); } return bytes;
}

} // namespace cluster::consensus
