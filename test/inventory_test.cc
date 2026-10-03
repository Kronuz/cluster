#include "journal/inventory.h"
#include "journal/posix.h"
#include <filesystem>
#include <iostream>
#include <vector>

using namespace kronuz::journal;
namespace {
int checks = 0, failures = 0;
void check(bool result, std::string_view message) {
	++checks; if (!result) { ++failures; std::cerr << "FAIL: " << message << '\n'; }
}
template <class F> bool throws(F&& function) { try { function(); return false; } catch (const std::exception&) { return true; } }
class CensusIO final : public IO {
public:
	std::size_t count = 0, inspected = 0, emitted = 0;
	EntryFootprint footprint{EntryKind::Regular, 3, 512};
	bool missing = false, fail = false;
	std::unique_ptr<OwnerLock> acquire_owner(bool) override { throw std::logic_error("unexpected lock"); }
	std::unique_ptr<File> open_existing(std::string_view) override { throw std::logic_error("unexpected open"); }
	std::unique_ptr<File> create_exclusive(std::string_view) override { throw std::logic_error("unexpected mutation"); }
	void replace(std::string_view, std::string_view) override { throw std::logic_error("unexpected mutation"); }
	void remove(std::string_view) override { throw std::logic_error("unexpected mutation"); }
	void sync_directory() override { throw std::logic_error("unexpected mutation"); }
	std::optional<EntryFootprint> entry_footprint(std::string_view) override {
		++inspected; if (fail) { throw std::runtime_error("stat failure"); }
		return missing ? std::nullopt : std::optional{footprint};
	}
	std::unique_ptr<DirectoryCursor> scan_directory() override {
		struct Cursor final : DirectoryCursor {
			explicit Cursor(CensusIO& io) : io(io) {}
			std::optional<std::string> next() override {
				if (next_index == io.count) { return std::nullopt; }
				++io.emitted; return "foreign-" + std::to_string(next_index++);
			}
			CensusIO& io; std::size_t next_index = 0;
		};
		return std::make_unique<Cursor>(*this);
	}
};
void accounting() {
	CensusIO empty; Inventory census(empty);
	check(!census.stats().ready(), "unstarted inventory cannot admit writes");
	check(throws([&] { census.step(0); }) && throws([&] { census.step(4097); }), "invalid budgets do not start inventory");
	check(census.step(1).ready() && census.stats().entries == 0, "empty inventory completes exactly");
	CensusIO large; large.count = 100000; Inventory streaming(large);
	while (!streaming.stats().complete) {
		auto before = large.inspected; auto result = streaming.step(4096);
		check(large.inspected - before <= 4096 && large.emitted == large.inspected, "large census bounds entries and stat calls per step");
		check(result.complete || !result.ready(), "partial totals never become ready");
	}
	auto stats = streaming.stats();
	check(stats.ready() && stats.entries == large.count && stats.logical_bytes == 300000 && stats.allocated_bytes == 51200000, "unknown foreign names are included without a filename table");
	auto inspected = large.inspected; streaming.step();
	check(large.inspected == inspected, "completed census does not rescan or double count");
	CensusIO unavailable; unavailable.count = 1; unavailable.footprint.allocated_bytes.reset(); Inventory logical(unavailable);
	check(logical.step(2).ready() && !logical.stats().allocated_bytes, "missing allocation metric preserves logical census readiness");
	CensusIO missing; missing.count = 1; missing.missing = true; Inventory vanished(missing);
	check(!vanished.step(2).ready() && vanished.stats().missing_entries == 1 && vanished.stats().incomplete, "vanished entry refuses admission");
	CensusIO directory; directory.count = 1; directory.footprint.kind = EntryKind::Directory; Inventory nested(directory);
	check(!nested.step(2).ready() && nested.stats().directories == 1 && nested.stats().entries == 1, "subdirectory is counted and invalidates flat census");
	for (bool allocated : {false, true}) {
		CensusIO overflow; overflow.count = 2;
		if (allocated) { overflow.footprint.allocated_bytes = std::numeric_limits<std::uint64_t>::max(); }
		else { overflow.footprint.logical_bytes = std::numeric_limits<std::uint64_t>::max(); }
		Inventory exceeded(overflow); auto result = exceeded.step(3);
		check(result.overflow && !result.ready(), "any byte counter overflow refuses admission");
	}
	CensusIO failed; failed.count = 1; failed.fail = true; Inventory errors(failed);
	check(throws([&] { errors.step(); }) && errors.stats().failed && !errors.stats().ready(), "stat failure leaves terminal unready inventory");
	failed.fail = false; check(throws([&] { errors.step(); }), "failed census cannot silently resume with an omitted entry");
}
void posix() {
	auto root = std::filesystem::current_path() / ".scratch";
	std::filesystem::create_directories(root);
	auto path = root / ("inventory-test-" + std::to_string(::getpid()));
	if (!std::filesystem::create_directory(path)) { throw std::runtime_error("test directory exists"); }
	::chmod(path.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{path};
	PosixIO io(path);
	auto file = io.create_exclusive("file"); file->write_at(1024 * 1024, "x"); file.reset();
	std::filesystem::rename(path / "file", path / std::string(200, 'f'));
	std::filesystem::create_symlink("nonexistent-target", path / "link");
	check(::mkfifo((path / "fifo").c_str(), 0600) == 0, "create special file without opening it");
	std::uint64_t expected_logical = 0, expected_allocated = 0;
	for (const auto& entry : std::filesystem::directory_iterator(path)) {
		struct stat status{}; check(::lstat(entry.path().c_str(), &status) == 0, "reference lstat succeeds");
		expected_logical += status.st_size; expected_allocated += static_cast<std::uint64_t>(status.st_blocks) * 512;
	}
	Inventory census(io); while (!census.step(1).complete) {}
	check(census.stats().ready() && census.stats().entries == 3 && census.stats().logical_bytes == expected_logical && census.stats().allocated_bytes == expected_allocated, "POSIX census counts long foreign names sparse files symlinks and FIFO without following or opening");
	check(io.entry_footprint("link")->kind == EntryKind::Symlink && io.entry_footprint("fifo")->kind == EntryKind::Other, "POSIX footprint preserves entry kinds");
	check(!io.entry_footprint("absent") && throws([&] { io.entry_footprint("../escape"); }), "POSIX census missing and confined-name semantics");
	std::filesystem::create_directory(path / "nested"); Inventory nested(io);
	while (!nested.step(2).complete) {}
	check(!nested.stats().ready() && nested.stats().directories == 1, "real subdirectory refuses complete flat accounting");
}
}
int main() {
	try { accounting(); posix(); } catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " inventory checks, " << failures << " failures\n"; return failures ? 1 : 0;
}
