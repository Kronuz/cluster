#include "journal/journal.h"
#include "journal/posix.h"
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <map>
#include <vector>

using namespace kronuz::journal;
namespace {
int failures = 0, checks = 0;
void check(bool result, std::string_view message) {
	++checks;
	if (!result) { ++failures; std::cerr << "FAIL: " << message << '\n'; }
}
template <class Function> bool throws(Function&& function) {
	try { function(); return false; } catch (const std::exception&) { return true; }
}

struct Inode { std::string visible, durable; };
struct Model {
	using Names = std::map<std::string, std::shared_ptr<Inode>>;
	Names visible, durable;
	bool locked = false;
	std::size_t operations = 0, fail_operation = 0;
	bool fail_after = false, zero_write = false;
	std::size_t chunk = std::numeric_limits<std::size_t>::max();
	void before() {
		++operations;
		if (operations == fail_operation && !fail_after) { throw std::runtime_error("injected I/O failure before effect"); }
	}
	void after() {
		if (operations == fail_operation && fail_after) { throw std::runtime_error("injected I/O failure after effect"); }
	}
	Model clone() const {
		Model copy;
		copy.chunk = chunk;
		std::map<const Inode*, std::shared_ptr<Inode>> nodes;
		for (const auto* names : {&visible, &durable}) {
			auto& destination = names == &visible ? copy.visible : copy.durable;
			for (const auto& [name, inode] : *names) {
				auto& fresh = nodes[inode.get()];
				if (!fresh) { fresh = std::make_shared<Inode>(*inode); }
				destination.emplace(name, fresh);
			}
		}
		return copy;
	}
	void power_loss(bool keep_visible_namespace) {
		if (!keep_visible_namespace) { visible = durable; }
		for (auto& [name, inode] : visible) { inode->visible = inode->durable; }
		durable = visible;
		locked = false;
		operations = fail_operation = 0;
		zero_write = false;
	}
};

class MemoryFile final : public File {
public:
	MemoryFile(Model& model, std::shared_ptr<Inode> inode) : model_(model), inode_(std::move(inode)) {}
	std::uint64_t size() override { model_.before(); auto size = inode_->visible.size(); model_.after(); return size; }
	std::size_t read_at(std::uint64_t offset, std::span<char> bytes) override {
		model_.before();
		auto available = offset >= inode_->visible.size() ? 0 : inode_->visible.size() - static_cast<std::size_t>(offset);
		auto length = std::min({bytes.size(), available, model_.chunk});
		if (length) { std::copy_n(inode_->visible.data() + offset, length, bytes.data()); }
		model_.after(); return length;
	}
	std::size_t write_at(std::uint64_t offset, std::string_view bytes) override {
		model_.before();
		auto length = model_.zero_write ? 0 : std::min(bytes.size(), model_.chunk);
		if (length) {
			inode_->visible.resize(std::max(inode_->visible.size(), static_cast<std::size_t>(offset) + length));
			std::copy_n(bytes.data(), length, inode_->visible.data() + offset);
		}
		model_.after(); return length;
	}
	void truncate(std::uint64_t size) override { model_.before(); inode_->visible.resize(size); model_.after(); }
	void sync() override { model_.before(); inode_->durable = inode_->visible; model_.after(); }
private:
	Model& model_;
	std::shared_ptr<Inode> inode_;
};

class MemoryIO final : public IO {
public:
	explicit MemoryIO(Model& model) : model_(model) {}
	std::unique_ptr<OwnerLock> acquire_owner(bool create) override {
		model_.before();
		if (model_.locked) { throw std::runtime_error("second owner"); }
		if (create) {
			if (model_.visible.contains("owner.lock")) { throw std::runtime_error("already initialized"); }
			model_.visible["owner.lock"] = std::make_shared<Inode>();
		} else if (!model_.visible.contains("owner.lock")) { throw std::runtime_error("missing owner lock"); }
		model_.locked = true;
		auto owner = std::make_unique<Lock>(model_);
		model_.after(); return owner;
	}
	std::unique_ptr<File> open_existing(std::string_view name) override {
		model_.before();
		auto found = model_.visible.find(std::string(name));
		if (found == model_.visible.end()) { throw std::runtime_error("missing file"); }
		auto file = std::make_unique<MemoryFile>(model_, found->second);
		model_.after(); return file;
	}
	std::unique_ptr<File> create_exclusive(std::string_view name) override {
		model_.before();
		auto [found, inserted] = model_.visible.emplace(std::string(name), std::make_shared<Inode>());
		if (!inserted) { throw std::runtime_error("existing file"); }
		auto file = std::make_unique<MemoryFile>(model_, found->second);
		model_.after(); return file;
	}
	void replace(std::string_view source, std::string_view destination) override {
		model_.before();
		auto node = model_.visible.extract(std::string(source));
		if (node.empty()) { throw std::runtime_error("missing replacement"); }
		model_.visible[std::string(destination)] = std::move(node.mapped());
		model_.after();
	}
	void remove(std::string_view name) override { model_.before(); model_.visible.erase(std::string(name)); model_.after(); }
	void sync_directory() override { model_.before(); model_.durable = model_.visible; model_.after(); }
private:
	struct Lock final : OwnerLock {
		explicit Lock(Model& model) : model_(model) {}
		~Lock() override { model_.locked = false; }
		Model& model_;
	};
	Model& model_;
};

Identity identity() { Identity value{}; value[0] = 'A'; return value; }
constexpr std::string_view data_name = "journal-0000000000000001";
Model initialized(std::size_t chunk = std::numeric_limits<std::size_t>::max()) {
	Model model; model.chunk = chunk;
	MemoryIO io(model); Journal journal(io, 1024);
	journal.create(identity()); journal.append_batch("first");
	return model.clone();
}
std::vector<std::string> replay(Model& model) {
	MemoryIO io(model); Journal journal(io, 1024);
	std::vector<std::string> batches;
	auto frontier = journal.recover([&](auto sequence, std::string_view batch) {
		check(sequence == batches.size() + 1, "replay is contiguous"); batches.emplace_back(batch);
	});
	check(frontier.sequence == batches.size(), "recovered frontier matches replay");
	return batches;
}

void basics() {
	check(crc32c("123456789") == 0xe3069283u, "CRC32C standard vector");
	check(crc32c("") == 0, "CRC32C empty vector");
	auto model = initialized(3);
	check(replay(model) == std::vector<std::string>{"first"}, "short reads and writes round-trip");
	MemoryIO io(model); Journal journal(io, 1024);
	journal.recover([](auto, auto) {});
	check(throws([&] { journal.append_batch(std::string(1025, 'x')); }) && !journal.fenced(), "oversize admission has no storage side effect");
	check(journal.append_batch("").sequence == 2, "empty atomic batch is valid");
	MemoryIO other(model); Journal second(other);
	check(throws([&] { second.recover([](auto, auto) {}); }), "stable lock excludes concurrent owner");
	model.zero_write = true;
	check(throws([&] { journal.append_batch("failed"); }) && journal.fenced(), "zero-progress write fences journal");
	check(throws([&] { journal.append_batch("retry"); }), "fenced journal cannot acknowledge later batches");
	Journal third(other);
	check(throws([&] { third.recover([](auto, auto) {}); }), "fenced owner retains stable lock");
}

void append_failures(std::size_t chunk = std::numeric_limits<std::size_t>::max()) {
	auto baseline = initialized(chunk);
	std::size_t operation_count = 0;
	{
		auto model = baseline.clone(); MemoryIO io(model); Journal journal(io, 1024);
		journal.recover([](auto, auto) {}); model.operations = 0;
		journal.append_batch("second"); operation_count = model.operations;
	}
	for (std::size_t operation = 1; operation <= operation_count; ++operation) {
		for (bool after : {false, true}) {
			auto model = baseline.clone();
			{
				MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
				model.operations = 0; model.fail_operation = operation; model.fail_after = after;
				check(throws([&] { journal.append_batch("second"); }) && journal.fenced(), "each uncertain append operation fences");
				check(throws([&] { journal.append_batch("third"); }), "failure prevents subsequent acknowledgement");
			}
			for (bool keep_visible : {false, true}) {
				auto crash = model.clone(); crash.power_loss(keep_visible);
				auto batches = replay(crash);
				check(batches == std::vector<std::string>{"first"} || batches == std::vector<std::string>({"first", "second"}),
					"crash keeps the previous acknowledged prefix and at most the interrupted batch");
				if (operation == operation_count && after) {
					check(batches.size() == 2, "completed directory barrier preserves new frontier even when its call reports failure");
				}
			}
			// Process death preserves the visible namespace, unlike power loss.
			// Recovery must seal its selected frontier before a later power loss.
			model.fail_operation = 0; model.operations = 0;
			auto selected = replay(model);
			model.power_loss(false);
			check(replay(model) == selected, "restart seals selected namespace against later power loss");
		}
	}
	std::cout << "append publication operations tested: " << operation_count << '\n';
}

void recovery_failures(bool extra_tail = false) {
	auto baseline = initialized();
	if (extra_tail) {
		auto& inode = baseline.visible.at(std::string(data_name));
		inode->visible += "extra unacknowledged bytes";
		inode->durable = inode->visible;
	}
	std::size_t operation_count;
	{
		auto model = baseline.clone(); replay(model); operation_count = model.operations;
	}
	for (std::size_t operation = 1; operation <= operation_count; ++operation) {
		for (bool after : {false, true}) {
			auto model = baseline.clone(); model.fail_operation = operation; model.fail_after = after;
			{
				MemoryIO io(model); Journal journal(io, 1024);
				check(throws([&] { journal.recover([](auto, auto) {}); }) && journal.fenced(), "recovery I/O failure fences");
				check(throws([&] { journal.append_batch("after failed recovery"); }), "failed recovery remains unavailable");
			}
			model.fail_operation = 0;
			check(replay(model) == std::vector<std::string>{"first"}, "reopen after recovery failure preserves acknowledged data");
		}
	}
	{
		auto model = baseline.clone(); MemoryIO io(model); Journal journal(io, 1024);
		check(throws([&] { journal.recover([](auto, auto) { throw std::runtime_error("callback"); }); }) && journal.fenced(),
			"callback failure leaves state unpublished and journal fenced");
	}
	std::cout << "recovery operations tested: " << operation_count << '\n';
}

void corruption() {
	auto baseline = initialized();
	for (const auto name : {std::string_view("manifest"), data_name}) {
		auto length = baseline.visible.at(std::string(name))->visible.size();
		for (std::size_t byte = 0; byte < length; ++byte) {
			auto model = baseline.clone(); model.visible.at(std::string(name))->visible[byte] ^= 1;
			check(throws([&] { replay(model); }), "every corrupted durable byte fails closed");
		}
	}
	for (std::size_t length = 0; length < baseline.visible.at(std::string(data_name))->visible.size(); ++length) {
		auto model = baseline.clone(); model.visible.at(std::string(data_name))->visible.resize(length);
		check(throws([&] { replay(model); }), "every truncation of acknowledged history fails closed");
	}
	for (auto missing : {std::string("manifest"), std::string(data_name), std::string("owner.lock")}) {
		auto model = baseline.clone(); model.visible.erase(missing);
		check(throws([&] { replay(model); }), "missing initialized metadata cannot be treated as empty state");
	}
	{
		auto model = baseline.clone(); model.visible.erase("owner.lock"); model.visible.erase(std::string(data_name));
		auto previous_manifest = model.visible.at("manifest")->visible;
		MemoryIO io(model); Journal journal(io);
		check(throws([&] { journal.create(identity()); }), "existing manifest prevents initialization when lock and data are missing");
		check(model.visible.at("manifest")->visible == previous_manifest, "failed initialization preserves damaged store's metadata");
	}
	{
		auto model = baseline.clone();
		auto& bytes = model.visible.at(std::string(data_name))->visible;
		std::string header;
		put32(header, 0x3142544bu); put32(header, 1025); put64(header, 1); put32(header, crc32c("first")); put32(header, crc32c(header));
		bytes.replace(28, 24, header);
		check(throws([&] { replay(model); }), "checksummed oversized length is rejected before allocation");
	}
	{
		auto model = baseline.clone();
		auto& bytes = model.visible.at(std::string(data_name))->visible;
		std::string header;
		put32(header, 0x3142544bu); put32(header, 5); put64(header, 2); put32(header, crc32c("first")); put32(header, crc32c(header));
		bytes.replace(28, 24, header);
		check(throws([&] { replay(model); }), "checksummed noncontiguous sequence is rejected");
	}
	{
		auto model = baseline.clone(); auto& inode = model.visible.at(std::string(data_name));
		inode->visible += "unacknowledged corrupt tail";
		check(replay(model) == std::vector<std::string>{"first"}, "unacknowledged tail is discarded after frontier verification");
		model.power_loss(false);
		check(replay(model) == std::vector<std::string>{"first"}, "discarded tail is durably truncated");
	}
}

void initialization_failures() {
	Model complete;
	{
		MemoryIO io(complete); Journal journal(io, 1024); journal.create(identity());
	}
	auto count = complete.operations;
	for (std::size_t operation = 1; operation <= count; ++operation) {
		for (bool after : {false, true}) {
			Model model; model.fail_operation = operation; model.fail_after = after;
			{
				MemoryIO io(model); Journal journal(io, 1024);
				check(throws([&] { journal.create(identity()); }) && journal.fenced(), "interrupted initialization fences");
			}
			for (bool keep_visible : {false, true}) {
				auto crash = model.clone(); crash.power_loss(keep_visible);
				try { check(replay(crash).empty(), "initialized crash can recover only the zero frontier"); }
				catch (const std::exception&) { check(true, "incomplete initialization fails closed"); }
				if (crash.visible.contains("owner.lock")) {
					MemoryIO io(crash); Journal journal(io);
					check(throws([&] { journal.create(identity()); }), "creation cannot erase interrupted initialization");
				}
			}
		}
	}
	std::cout << "initialization operations tested: " << count << '\n';
}

void posix() {
	// Repository scratch is caller-created; tests own this unique directory.
	auto directory = std::filesystem::current_path() / ".scratch" / ("journal-test-" + std::to_string(::getpid()));
	std::filesystem::create_directories(directory);
	::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path directory; ~Cleanup() { std::filesystem::remove_all(directory); } } cleanup{directory};
	{
		PosixIO io(directory); Journal journal(io, 1024);
		journal.create(identity()); journal.append_batch("real filesystem");
		PosixIO second_io(directory); Journal second(second_io);
		check(throws([&] { second.recover([](auto, auto) {}); }), "POSIX lock excludes another open owner");
		check(throws([&] { io.create_exclusive("../escape"); }), "POSIX filename cannot escape directory");
	}
	{
		PosixIO io(directory); Journal journal(io, 1024);
		std::vector<std::string> batches;
		journal.recover([&](auto, std::string_view batch) { batches.emplace_back(batch); });
		check(batches == std::vector<std::string>{"real filesystem"}, "POSIX reopen replays acknowledged batch");
		check(journal.append_batch("after reopen").sequence == 2, "POSIX reopened writer advances sequence");
	}
	std::filesystem::create_symlink("manifest", directory / "symlink");
	PosixIO io(directory);
	check(throws([&] { io.open_existing("symlink"); }), "POSIX symlink file is rejected");
	std::filesystem::create_hard_link(directory / "manifest", directory / "hardlink");
	check(throws([&] { io.open_existing("hardlink"); }), "POSIX shared inode is rejected");
	check(::mkfifo((directory / "fifo").c_str(), 0600) == 0, "create FIFO fixture");
	check(throws([&] { io.open_existing("fifo"); }), "POSIX special file is rejected without blocking");
}
} // namespace

int main() {
	try { basics(); append_failures(); append_failures(3); recovery_failures(); recovery_failures(true); corruption(); initialization_failures(); posix(); }
	catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " journal checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
