#include "core.h"
#include "checkpoint.h"
#include "../journal/journal.h"
#include "../journal/posix.h"
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sys/resource.h>

static double cpu_seconds() {
	rusage usage{};
	if (::getrusage(RUSAGE_SELF, &usage)) { throw std::runtime_error("getrusage failed"); }
	return usage.ru_utime.tv_sec + usage.ru_stime.tv_sec + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
}
int main() {
	using namespace cluster::consensus;
	try {
		auto root = std::filesystem::current_path() / ".scratch"; std::filesystem::create_directories(root);
		std::cout << "retained_entries,payload_bytes,bundle_bytes,capture_wall_s,capture_cpu_s,encode_write_publish_wall_s,encode_write_publish_cpu_s,total_frozen_wall_s,peak_process_rss_bytes\n";
		for (std::size_t count : {1u, 65535u}) {
			auto directory = root / ("consensus-checkpoint-bench-" + std::to_string(::getpid()) + "-" + std::to_string(count));
			if (!std::filesystem::create_directory(directory)) { throw std::runtime_error("benchmark directory exists"); }
			::chmod(directory.c_str(), 0700);
			struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
			kronuz::journal::PosixIO io(directory); kronuz::journal::Journal journal(io);
			Identity identity{}; identity[0] = 'B'; journal.create(identity);
			FixedConfiguration configuration; configuration.cluster[0] = 'C'; configuration.configuration[0] = 'V'; configuration.local = 1; configuration.voters = {1};
			journal.append_batch(encode_initialization(configuration));
			auto app_builder = journal.prepare_artifact(); app_builder.append_chunk("immutable app at index 1"); auto application = app_builder.finish(); journal.verify_artifact(application);
			RecoveredState state; state.configuration = configuration; state.hard.term = 1; state.hard.commit_index = 1;
			state.entries.push_back(Entry{1, 1, EntryKind::NoOp, {}});
			for (Index index = 2; index <= count + 1; ++index) { state.entries.push_back(Entry{index, 1, EntryKind::Command, std::string(1024, 'x')}); }
			// Establish the recovered fixture through valid bounded storage
			// batches before measuring the checkpoint freeze.
			for (std::size_t first = 0; first < state.entries.size(); first += 256) {
				auto end = std::min(state.entries.size(), first + 256);
				StorageBatch batch;
				if (first == 0) { batch.hard = HardState{1, std::nullopt, 0}; }
				batch.log = LogMutation{state.entries[first].index, std::vector<Entry>(state.entries.begin() + static_cast<std::ptrdiff_t>(first), state.entries.begin() + static_cast<std::ptrdiff_t>(end))};
				journal.append_batch(encode_storage_batch(batch));
			}
			journal.append_batch(encode_storage_batch(StorageBatch{state.hard, std::nullopt}));
			Core core(configuration, std::move(state)); core.step(Start{}); core.step(Applied{1});
			auto cpu_begin = cpu_seconds(); auto begin = std::chrono::steady_clock::now();
			auto actions = core.step(LocalCheckpoint{1, 1, 1, 1, configuration.cluster, configuration.configuration});
			auto captured = std::chrono::steady_clock::now(); auto capture_cpu = cpu_seconds() - cpu_begin;
			const PersistCheckpoint* checkpoint = nullptr;
			for (const auto& action : actions) { if (auto value = std::get_if<PersistCheckpoint>(&action)) { checkpoint = value; } }
			if (!checkpoint) { throw std::runtime_error("benchmark capture rejected"); }
			cpu_begin = cpu_seconds();
			auto sequence = journal.frontier().sequence;
			auto bytes = encode_checkpoint(CheckpointBundle{sequence, application.descriptor(), checkpoint->state});
			auto builder = journal.prepare_artifact(); std::size_t offset = 0;
			while (offset < bytes.size()) { auto chunk = std::min(bytes.size() - offset, std::size_t(64 * 1024)); builder.append_chunk(std::string_view(bytes).substr(offset, chunk)); offset += chunk; }
			auto bundle = builder.finish(); journal.publish_checkpoint(bundle, std::array{application}, sequence);
			auto published = std::chrono::steady_clock::now(); auto write_cpu = cpu_seconds() - cpu_begin;
			core.step(Persisted{checkpoint->token});
			auto finished = std::chrono::steady_clock::now();
			rusage usage{}; if (::getrusage(RUSAGE_SELF, &usage)) { throw std::runtime_error("getrusage failed"); }
#ifdef __APPLE__
			auto peak_rss = static_cast<std::uint64_t>(usage.ru_maxrss);
#else
			auto peak_rss = static_cast<std::uint64_t>(usage.ru_maxrss) * 1024;
#endif
			std::cout << std::fixed << std::setprecision(6) << count << ',' << count * 1024 << ',' << bytes.size() << ','
				<< std::chrono::duration<double>(captured - begin).count() << ',' << capture_cpu << ','
				<< std::chrono::duration<double>(published - captured).count() << ',' << write_cpu << ','
				<< std::chrono::duration<double>(finished - begin).count() << ',' << peak_rss << '\n';
		}
	} catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
