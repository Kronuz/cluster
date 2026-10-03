#include "journal.h"
#include "posix.h"
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
static kronuz::journal::PreparedArtifact prepare(kronuz::journal::Journal& journal, std::size_t size) {
	auto builder = journal.prepare_artifact();
	const std::string chunk(64 * 1024, 'x');
	while (size) {
		auto count = std::min(size, chunk.size()); builder.append_chunk(std::string_view(chunk).substr(0, count)); size -= count;
	}
	return builder.finish();
}
int main() {
	try {
		auto root = std::filesystem::current_path() / ".scratch"; std::filesystem::create_directories(root);
		std::cout << "application_bytes,bundle_bytes,preparation_wall_s,preparation_cpu_s,publication_wall_s,publication_cpu_s\n";
		for (auto size : {1024u * 1024, 64u * 1024 * 1024}) {
			auto directory = root / ("checkpoint-bench-" + std::to_string(::getpid()) + "-" + std::to_string(size));
			if (!std::filesystem::create_directory(directory)) { throw std::runtime_error("benchmark directory exists"); }
			::chmod(directory.c_str(), 0700);
			struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
			kronuz::journal::PosixIO io(directory); kronuz::journal::Journal journal(io);
			kronuz::journal::Identity identity{}; identity[0] = 'C'; journal.create(identity); journal.append_batch("covered");
			auto cpu_begin = cpu_seconds(); auto begin = std::chrono::steady_clock::now();
			auto application = prepare(journal, size); auto bundle = prepare(journal, size);
			auto preparation_wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
			auto preparation_cpu = cpu_seconds() - cpu_begin;
			cpu_begin = cpu_seconds(); begin = std::chrono::steady_clock::now();
			journal.publish_checkpoint(bundle, std::array{application}, 1);
			std::cout << std::fixed << std::setprecision(6) << size << ',' << size << ',' << preparation_wall << ',' << preparation_cpu << ','
				<< std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count() << ',' << cpu_seconds() - cpu_begin << '\n';
		}
	} catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
