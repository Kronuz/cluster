#include "core.h"
#include "storage.h"
#include "../journal/journal.h"
#include "../journal/posix.h"
#include <deque>
#include <iostream>

// Single-voter integration example, not a service deployment. The caller
// provisions a dedicated directory. Production identities come from config.
int main(int argc, char** argv) {
	if (argc < 3 || argc > 4) {
		std::cerr << "usage: consensus_demo DIRECTORY create|open [COMMAND]\n";
		return 2;
	}
	try {
		using namespace cluster::consensus;
		FixedConfiguration configuration; configuration.cluster[0] = 'D'; configuration.configuration[0] = 'E';
		configuration.local = 1; configuration.voters = {1};
		kronuz::journal::PosixIO io(argv[1]); kronuz::journal::Journal journal(io, 4 * 1024 * 1024);
		RecoveredState state; state.configuration = configuration;
		std::string_view mode(argv[2]);
		if (mode == "create") {
			Identity identity{}; std::random_device random;
			for (char& byte : identity) { byte = static_cast<char>(random()); }
			journal.create(identity); journal.append_batch(encode_initialization(configuration));
		} else if (mode == "open") {
			Recovery recovery(configuration);
			auto frontier = journal.recover([&](auto sequence, std::string_view bytes) { recovery.replay(sequence, bytes); });
			state = recovery.finish(frontier.sequence);
		} else { throw std::invalid_argument("expected create or open"); }
		Core core(configuration, std::move(state));
		std::deque<Action> work;
		std::uint64_t command_count = 0;
		std::string latest;
		auto enqueue = [&](Actions actions) { for (auto& action : actions) { work.push_back(std::move(action)); } };
		auto pump = [&] {
			while (!work.empty()) {
				auto action = std::move(work.front()); work.pop_front();
				if (auto persist = std::get_if<Persist>(&action)) {
					try { journal.append_batch(encode_storage_batch(persist->batch)); }
					catch (const std::exception& error) {
						core.step(Failed{FailureSource::Storage, persist->token, error.what()}); throw;
					}
					enqueue(core.step(Persisted{persist->token}));
				} else if (auto committed = std::get_if<Committed>(&action)) {
					for (const auto& entry : committed->entries) {
						if (entry.kind == EntryKind::Command) { ++command_count; latest = entry.payload; }
					}
					enqueue(core.step(Applied{committed->entries.back().index}));
				} else if (auto fenced = std::get_if<Fenced>(&action)) { throw std::runtime_error(fenced->reason); }
				else if (std::holds_alternative<Send>(action)) { throw std::logic_error("single-voter example cannot send"); }
				else if (auto reject = std::get_if<Reject>(&action)) { throw std::runtime_error("proposal rejected: " + std::to_string(static_cast<int>(reject->reason))); }
			}
		};
		enqueue(core.step(Start{})); pump();
		// Logical ticks make this example deterministic. A service supplies its
		// actual monotonic time and randomized election delays instead.
		enqueue(core.step(Tick{100, 100})); pump();
		if (argc == 4) { enqueue(core.step(Propose{1, argv[3]})); pump(); }
		std::cout << "durable commit=" << core.committed() << ", applied=" << core.applied()
			<< ", commands=" << command_count << ", latest=" << latest << '\n';
	} catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
