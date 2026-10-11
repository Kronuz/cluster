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

#include "journal.h"
#include "posix.h"
#include <iostream>

int main(int argc, char** argv) {
	if (argc < 3) {
		std::cerr << "usage: journal_demo DIRECTORY create|read|append [PAYLOAD]\n";
		return 2;
	}
	try {
		kronuz::journal::PosixIO io(argv[1]);
		kronuz::journal::Journal journal(io, 1024 * 1024);
		std::string_view command(argv[2]);
		kronuz::journal::Frontier frontier;
		if (command == "create" && argc == 3) {
			kronuz::journal::Identity identity{};
			std::random_device random;
			for (char& byte : identity) { byte = static_cast<char>(random()); }
			frontier = journal.create(identity);
		} else if ((command == "read" && argc == 3) || (command == "append" && argc == 4)) {
			std::uint64_t recovered_bytes = 0;
			frontier = journal.recover([&](auto, std::string_view batch) { recovered_bytes += batch.size(); });
			// Publish inspection output only after the whole prefix validates.
			std::cout << "recovered payload bytes: " << recovered_bytes << '\n';
			if (command == "append") { frontier = journal.append_batch(argv[3]); }
		} else { throw std::invalid_argument("invalid journal command or arguments"); }
		std::cout << "durable sequence: " << frontier.sequence << ", offset: " << frontier.offset << '\n';
	} catch (const std::exception& error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
