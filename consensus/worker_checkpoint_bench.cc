#include "worker.h"
#include "../journal/posix.h"
#include <chrono>
#include <iostream>
#include <sys/resource.h>

using namespace cluster::consensus;
namespace {
using Clock = std::chrono::steady_clock;
double cpu() {
	rusage value{}; if (::getrusage(RUSAGE_SELF, &value)) { throw std::runtime_error("getrusage failed"); }
	return value.ru_utime.tv_sec + value.ru_utime.tv_usec / 1000000.0 + value.ru_stime.tv_sec + value.ru_stime.tv_usec / 1000000.0;
}
std::uint64_t rss() {
	rusage value{}; if (::getrusage(RUSAGE_SELF, &value)) { throw std::runtime_error("getrusage failed"); }
#if defined(__APPLE__)
	return static_cast<std::uint64_t>(value.ru_maxrss);
#else
	return static_cast<std::uint64_t>(value.ru_maxrss) * 1024;
#endif
}
double elapsed(Clock::time_point start, Clock::time_point end) { return std::chrono::duration<double>(end - start).count(); }
void run(std::size_t commands) {
	FixedConfiguration configuration; configuration.local = 1; configuration.voters = {1}; configuration.cluster[0] = 'C'; configuration.configuration[0] = 'V';
	auto directory = std::filesystem::current_path() / ".scratch" / ("worker-checkpoint-bench-" + std::to_string(::getpid()) + "-" + std::to_string(commands));
	if (std::filesystem::exists(directory)) { throw std::runtime_error("benchmark directory already exists"); }
	std::filesystem::create_directories(directory); ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	{
		// Seed a maximal legal recovered log with bounded batches. This is a
		// checkpoint fixture, not a measured proposal-throughput workload.
		kronuz::journal::PosixIO io(directory); kronuz::journal::Journal journal(io, 4 * 1024 * 1024);
		kronuz::journal::Identity identity{}; identity[0] = 'B'; journal.create(identity);
		journal.append_batch(encode_initialization(configuration));
		journal.append_batch(encode_storage_batch({HardState{1, 1, 1}, LogMutation{1, {{1, 1, EntryKind::NoOp, ""}}}}));
		for (std::size_t first = 0; first < commands;) {
			auto count = std::min(commands - first, std::size_t{256}); StorageBatch batch;
			batch.log = LogMutation{first + 2, {}};
			for (std::size_t i = 0; i < count; ++i) { batch.log->entries.push_back({first + i + 2, 1, EntryKind::Command, std::string(1024, 'x')}); }
			batch.hard = HardState{1, 1, first + count + 1}; journal.append_batch(encode_storage_batch(batch)); first += count;
		}
	}
	kronuz::journal::PosixIO io(directory);
	Worker worker(io, configuration, {{512ull * 1024 * 1024, 4096}, {16 * 1024, 3}, {80ull * 1024 * 1024, 4}, 8, 3});
	worker.recover([](auto&) { throw std::runtime_error("unexpected initial checkpoint"); });
	while (!worker.ready()) { worker.run_one({0, 100}); }
	worker.try_submit(Start{});
	auto drain = [&] {
		bool published = false;
		for (const auto& action : worker.take_actions()) {
			if (auto batch = std::get_if<Committed>(&action)) { worker.applied({batch->entries.back().index}); }
			published |= std::holds_alternative<CheckpointPublished>(action);
		}
		return published;
	};
	for (unsigned turn = 0; worker.applied_index() < commands + 1 && turn < 10000; ++turn) { drain(); worker.run_one({0, 100}); }
	if (worker.applied_index() != commands + 1) { throw std::runtime_error("fixture application did not advance"); }
	drain();
	auto id = worker.reserve_checkpoint(1024 * 1024); if (!id) { throw std::runtime_error("replacement capacity unavailable"); }
	// A fixed baseline image represents immutable application state at the
	// first no-op; all generated commands belong in the retained suffix.
	std::string image(1024 * 1024, 'b'); worker.attach_capture(*id, {1, 1, 1});
	auto start = Clock::now(); double start_cpu = cpu(), freeze_cpu = 0, worst_turn = 0, worst_cpu = 0;
	Clock::time_point freeze_start{}; bool freezing = false, published = false;
	std::size_t offset = 0, maintenance = 0;
	for (unsigned turn = 0; turn < 10000 && !published; ++turn) {
		if (offset < image.size()) {
			auto count = std::min(image.size() - offset, std::size_t{65536});
			if (worker.offer_application_chunk(*id, std::string_view(image).substr(offset, count), offset + count == image.size()) == SubmitResult::Accepted) { offset += count; }
		}
		auto before = Clock::now(); double before_cpu = cpu(); bool was_busy = worker.busy();
		auto result = worker.run_one({0, 100}); auto after = Clock::now(); double after_cpu = cpu();
		if (!was_busy && worker.busy()) { freezing = true; freeze_start = before; freeze_cpu = before_cpu; }
		if (freezing) { worst_turn = std::max(worst_turn, elapsed(before, after)); worst_cpu = std::max(worst_cpu, after_cpu - before_cpu); }
		maintenance += result == TurnResult::Maintenance; published = drain();
		if (worker.fenced()) { throw std::runtime_error("benchmark worker fenced"); }
	}
	auto end = Clock::now(); double end_cpu = cpu();
	if (!published || !freezing || worker.base_index() != 1) { throw std::runtime_error("checkpoint did not complete"); }
	std::cout << commands << ',' << worker.storage_frontier().checkpoint->length << ',' << elapsed(start, end) << ',' << end_cpu - start_cpu << ','
		<< elapsed(freeze_start, end) << ',' << end_cpu - freeze_cpu << ',' << worst_turn << ',' << worst_cpu << ',' << maintenance << ',' << rss() << '\n';
}
}
int main() {
	try {
		std::cout << "suffix_commands,bundle_bytes,checkpoint_seconds,checkpoint_cpu_seconds,freeze_seconds,freeze_cpu_seconds,worst_frozen_turn_seconds,worst_frozen_turn_cpu_seconds,maintenance_turns,process_peak_rss_bytes\n";
		for (auto commands : {std::size_t{64}, std::size_t{4096}, std::size_t{65535}}) { run(commands); }
	} catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
