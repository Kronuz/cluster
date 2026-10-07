#include "consensus/retained_log.h"
#include <cmath>
#include <iostream>
#include <random>

using namespace cluster::consensus;
int main() {
	try {
		detail::RetainedLog log;
		std::vector<Entry> reference;
		std::mt19937_64 random(0x524554495245ull);
		std::size_t checks = 0;
		auto require = [&](bool value) {
			++checks;
			if (!value) {
				throw std::runtime_error("retained log model mismatch");
			}
		};
		for (unsigned turn = 0; turn < 20000; ++turn) {
			auto action = random() % 4;
			if (action == 0 || reference.empty()) {
				Entry entry{reference.empty() ? 1 : reference.back().index + 1, 1, EntryKind::Command,
							std::string(random() % 65, 'x')};
				log.push_back(entry);
				reference.push_back(std::move(entry));
			} else if (action == 1) {
				auto removed = random() % (reference.size() + 1);
				log.erase_prefix(removed);
				reference.erase(reference.begin(), reference.begin() + removed);
			} else if (action == 2) {
				auto kept = random() % (reference.size() + 1);
				log.resize(kept);
				reference.resize(kept);
			} else {
				auto before_count = log.retired_entries(), before_bytes = log.retired_bytes();
				auto retired = log.retire(3, 128);
				require(retired <= 3 && before_count - log.retired_entries() == retired &&
						before_bytes - log.retired_bytes() <= 128);
			}
			require(log.size() == reference.size());
			require(log.tree_height() <= 2 * std::log2(log.size() + 1) + 1);
			std::size_t payload = 0;
			for (std::size_t i = reference.size(); i-- > 0;) {
				payload += reference[i].payload.size();
				require(log.at(i) == reference[i]);
				require(log.suffix_bytes(i) == payload);
			}
			require(log.payload_bytes() == payload);
		}
		log.clear();
		while (log.retired_entries()) {
			require(log.retire(7, 512) <= 7);
		}
		require(log.charged_entries() == 0 && log.charged_bytes() == 0);
		for (std::size_t i = 0; i < 65535; ++i) {
			log.push_back({i + 1, 1, EntryKind::Command, std::string(1024, 'x')});
		}
		require(log.tree_height() <= 24);
		log.erase_prefix(65534);
		require(log.size() == 1 && log.retired_entries() == 65534 && log.at(0).index == 65535);
		auto bytes = log.charged_bytes();
		require(log.retire(256, 65536) == 64 && bytes - log.charged_bytes() == 65536);
		log.resize(0);
		log.push_back({65535, 2, EntryKind::NoOp, {}});
		require(log.at(0).term == 2 && log.retired_entries() == 65471);
		std::cout << checks << " retained log checks passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
