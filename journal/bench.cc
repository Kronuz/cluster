#include "journal.h"
#include "posix.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sys/resource.h>
#include <vector>

static double cpu_seconds() {
	rusage usage{};
	if (::getrusage(RUSAGE_SELF, &usage)) { throw std::runtime_error("getrusage failed"); }
	return usage.ru_utime.tv_sec + usage.ru_stime.tv_sec + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
}

int main() {
	try {
		const auto root = std::filesystem::current_path() / ".scratch";
		std::filesystem::create_directories(root);
		std::cout << "opaque operation bytes=256, groups=64; local filesystem microbenchmark\n";
		std::cout << "ops/group, payload_bytes, wall_s, cpu_s, ops/s, p50_batch_ms, p99_batch_ms\n";
		for (std::size_t operations : {1u, 32u, 256u}) {
			auto directory = root / ("journal-bench-" + std::to_string(::getpid()) + "-" + std::to_string(operations));
			if (!std::filesystem::create_directory(directory)) { throw std::runtime_error("benchmark directory already exists"); }
			::chmod(directory.c_str(), 0700);
			struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
			kronuz::journal::PosixIO io(directory);
			kronuz::journal::Journal journal(io, 1024 * 1024);
			kronuz::journal::Identity identity{}; identity[0] = 'B';
			journal.create(identity);
			std::string batch(operations * 256, 'x');
			std::vector<double> samples; samples.reserve(64);
			auto cpu_begin = cpu_seconds();
			auto begin = std::chrono::steady_clock::now();
			for (unsigned group = 0; group < 64; ++group) {
				auto start = std::chrono::steady_clock::now();
				journal.append_batch(batch);
				samples.push_back(std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count());
			}
			auto wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
			auto cpu = cpu_seconds() - cpu_begin;
			std::sort(samples.begin(), samples.end());
			std::cout << std::fixed << std::setprecision(6) << operations << ',' << batch.size() << ',' << wall << ',' << cpu << ','
				<< 64 * operations / wall << ',' << samples[31] << ',' << samples[63] << '\n';
		}
	} catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
