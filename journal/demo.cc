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
