#pragma once

#include "codec.h"
#include "io.h"
#include <array>
#include <random>

namespace kronuz::journal {
using Identity = std::array<char, 16>;
namespace detail {
inline Identity random_identity() {
	Identity result{}; std::random_device random;
	for (unsigned part = 0; part < 4; ++part) {
		auto value = static_cast<std::uint32_t>(random());
		for (unsigned byte = 0; byte < 4; ++byte) { result[part * 4 + byte] = static_cast<char>(value >> (byte * 8)); }
	}
	return result;
}
inline std::string hexadecimal(Identity identity) {
	std::string result; result.reserve(32); constexpr char digits[] = "0123456789abcdef";
	for (unsigned char byte : identity) { result.push_back(digits[byte >> 4]); result.push_back(digits[byte & 15]); }
	return result;
}
inline std::string generation_name(std::uint64_t generation) {
	std::string result = "journal-"; constexpr char digits[] = "0123456789abcdef";
	for (int shift = 60; shift >= 0; shift -= 4) { result.push_back(digits[(generation >> shift) & 15]); }
	return result;
}
inline void read_all(File& file, std::uint64_t offset, std::span<char> bytes) {
	while (!bytes.empty()) {
		auto count = file.read_at(offset, bytes);
		if (count == 0 || count > bytes.size()) { throw Corruption("short journal read"); }
		offset += count; bytes = bytes.subspan(count);
	}
}
inline void write_all(File& file, std::uint64_t offset, std::string_view bytes) {
	while (!bytes.empty()) {
		auto count = file.write_at(offset, bytes);
		if (count == 0 || count > bytes.size()) { throw std::runtime_error("zero or invalid journal write progress"); }
		offset += count; bytes.remove_prefix(count);
	}
}
inline std::uint32_t checksum_at_end(std::string_view bytes) {
	if (bytes.size() < 4) { throw Corruption("truncated checksummed frame"); }
	bytes.remove_prefix(bytes.size() - 4); return get32(bytes);
}
} // namespace detail
} // namespace kronuz::journal
