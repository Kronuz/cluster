/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// A varint length codec + a length-prefixed string, BYTE-COMPATIBLE with Xapiand's
// serialise_length / serialise_string (src/length.{h,cc}). Vendored here (header-only,
// no exceptions -- the unserialise side returns false on bad/short data) so the cluster
// wire format interoperates with the existing discovery protocol without pulling in a
// dependency. The encoding: a byte < 255 is the length verbatim; 0xff introduces a
// (len - 255) little-endian base-128 continuation where the final byte has 0x80 set.

#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace cluster {

inline std::string serialise_length(unsigned long long len) {
	std::string result;
	if (len < 255) {
		result += static_cast<char>(static_cast<unsigned char>(len));
	} else {
		result += '\xff';
		len -= 255;
		while (true) {
			auto b = static_cast<unsigned char>(len & 0x7f);
			len >>= 7;
			if (len == 0) {
				result += static_cast<char>(b | static_cast<unsigned char>(0x80));
				break;
			}
			result += static_cast<char>(b);
		}
	}
	return result;
}

// Read a length from [*p, end); advance *p. false on truncated/overlong input.
inline bool unserialise_length(const char** p, const char* end, unsigned long long& out) {
	if (*p == end) { return false; }
	unsigned long long len = static_cast<unsigned char>(*(*p)++);
	if (len == 0xff) {
		len = 0;
		unsigned char ch = 0;
		unsigned shift = 0;
		do {
			if (*p == end || shift > (sizeof(unsigned long long) * 8 / 7 * 7)) { return false; }
			ch = static_cast<unsigned char>(*(*p)++);
			len |= static_cast<unsigned long long>(ch & 0x7f) << shift;
			shift += 7;
		} while ((ch & 0x80) == 0);
		len += 255;
	}
	out = len;
	return true;
}

inline std::string serialise_string(std::string_view input) {
	std::string result = serialise_length(input.size());
	result.append(input.data(), input.size());
	return result;
}

// A boolean is one byte, '1' or '0' -- byte-compatible with Xapiand's serialise_bool.
inline std::string serialise_bool(bool value) {
	return value ? "1" : "0";
}

inline bool unserialise_bool(const char** p, const char* end, bool& out) {
	if (*p == end) { return false; }
	char c = *(*p)++;
	if (c < '0' || c > '1') { return false; }
	out = (c != '0');
	return true;
}

// Read a length-prefixed string; out views into the same buffer as *p. false on truncation.
inline bool unserialise_string(const char** p, const char* end, std::string_view& out) {
	unsigned long long len = 0;
	if (!unserialise_length(p, end, len)) { return false; }
	if (static_cast<unsigned long long>(end - *p) < len) { return false; }
	out = std::string_view(*p, static_cast<std::size_t>(len));
	*p += len;
	return true;
}

}  // namespace cluster
