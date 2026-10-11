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
	auto builder = journal.prepare_artifact(); const std::string chunk(64 * 1024, 'x');
	while (size) { auto count = std::min(size, chunk.size()); builder.append_chunk(std::string_view(chunk).substr(0, count)); size -= count; }
	return builder.finish();
}
static std::uint64_t logical_bytes(const std::filesystem::path& directory) {
	std::uint64_t total = 0;
	for (const auto& entry : std::filesystem::directory_iterator(directory)) { total += entry.file_size(); }
	return total;
}
int main() {
	try {
		auto root = std::filesystem::current_path() / ".scratch"; std::filesystem::create_directories(root);
		std::cout << "artifact_bytes,logical_before,logical_after,logical_unlinked,files_removed,cleanup_wall_s,cleanup_cpu_s\n";
		for (std::size_t size : {1024u * 1024, 64u * 1024 * 1024}) {
			auto directory = root / ("journal-reclaim-bench-" + std::to_string(::getpid()) + "-" + std::to_string(size));
			if (!std::filesystem::create_directory(directory)) { throw std::runtime_error("benchmark directory exists"); }
			::chmod(directory.c_str(), 0700);
			struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
			kronuz::journal::PosixIO io(directory); kronuz::journal::Journal journal(io);
			kronuz::journal::Identity identity{}; identity[0] = 'R'; journal.create(identity); journal.append_batch("covered");
			for (unsigned generation = 0; generation < 2; ++generation) {
				auto application = prepare(journal, size); auto bundle = prepare(journal, size);
				journal.publish_checkpoint(bundle, std::array{application}, 1);
			}
			auto before = logical_bytes(directory); auto cpu_begin = cpu_seconds(); auto begin = std::chrono::steady_clock::now();
			std::uint64_t unlinked = 0; std::size_t removed = 0;
			for (;;) { auto stats = journal.reclaim_step(128); unlinked += stats.logical_bytes; removed += stats.removed; if (stats.complete) { break; } }
			auto wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count(); auto cpu = cpu_seconds() - cpu_begin;
			auto after = logical_bytes(directory);
			if (before - after != unlinked) { throw std::runtime_error("reclamation byte accounting mismatch"); }
			std::cout << std::fixed << std::setprecision(6) << size << ',' << before << ',' << after << ',' << unlinked << ',' << removed << ',' << wall << ',' << cpu << '\n';
		}
	} catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
