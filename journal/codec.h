#pragma once

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kronuz::journal {

class Corruption : public std::runtime_error {
public:
	using std::runtime_error::runtime_error;
};

namespace detail {
	inline constexpr auto crc_table = [] {
		std::array<std::uint32_t, 256> result{};
		for (std::uint32_t i = 0; i < result.size(); ++i) {
			auto value = i;
			for (unsigned bit = 0; bit < 8; ++bit) {
				value = (value >> 1) ^ ((value & 1) ? 0x82f63b78u : 0);
			}
			result[i] = value;
		}
		return result;
	}();
}
class Checksum {
public:
	void update(std::string_view bytes) {
		for (unsigned char byte : bytes) { value_ = detail::crc_table[(value_ ^ byte) & 255] ^ (value_ >> 8); }
	}
	std::uint32_t value() const noexcept { return ~value_; }
private:
	std::uint32_t value_ = ~0u;
};
inline std::uint32_t crc32c(std::string_view bytes) {
	Checksum checksum; checksum.update(bytes); return checksum.value();
}

inline void put32(std::string& bytes, std::uint32_t value) {
	for (unsigned i = 0; i < 4; ++i) { bytes.push_back(static_cast<char>(value >> (i * 8))); }
}
inline void put64(std::string& bytes, std::uint64_t value) {
	for (unsigned i = 0; i < 8; ++i) { bytes.push_back(static_cast<char>(value >> (i * 8))); }
}
inline std::uint32_t get32(std::string_view& bytes) {
	if (bytes.size() < 4) { throw Corruption("truncated integer"); }
	std::uint32_t value = 0;
	for (unsigned i = 0; i < 4; ++i) { value |= std::uint32_t(static_cast<unsigned char>(bytes[i])) << (i * 8); }
	bytes.remove_prefix(4);
	return value;
}
inline std::uint64_t get64(std::string_view& bytes) {
	if (bytes.size() < 8) { throw Corruption("truncated integer"); }
	std::uint64_t value = 0;
	for (unsigned i = 0; i < 8; ++i) { value |= std::uint64_t(static_cast<unsigned char>(bytes[i])) << (i * 8); }
	bytes.remove_prefix(8);
	return value;
}

} // namespace kronuz::journal
