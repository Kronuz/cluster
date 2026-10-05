#include "journal/journal.h"
#include "journal/posix.h"
#include "journal/store.h"
#include <set>
#include <algorithm>
#include <filesystem>
#include <functional>
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
	std::function<void()> observe;
	std::function<void(const Inode*, std::uint64_t, std::size_t)> observe_read;
	bool oversized_read = false;
	std::size_t chunk = std::numeric_limits<std::size_t>::max();
	void before() {
		++operations;
		if (operations == fail_operation && !fail_after) { throw std::runtime_error("injected I/O failure before effect"); }
	}
	void after() {
		if (observe) { observe(); }
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
		if (model_.observe_read) { model_.observe_read(inode_.get(), offset, bytes.size()); }
		model_.before();
		auto available = offset >= inode_->visible.size() ? 0 : inode_->visible.size() - static_cast<std::size_t>(offset);
		auto length = std::min({bytes.size(), available, model_.chunk});
		if (length) { std::copy_n(inode_->visible.data() + offset, length, bytes.data()); }
		model_.after(); return model_.oversized_read ? bytes.size() + 1 : length;
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
	std::unique_ptr<DirectoryCursor> scan_directory() override {
		model_.before(); auto cursor = std::make_unique<Cursor>(model_); model_.after(); return cursor;
	}
	std::unique_ptr<File> open_reclaim_candidate(std::string_view name) override {
		model_.before(); auto found = model_.visible.find(std::string(name));
		auto file = found == model_.visible.end() ? nullptr : std::make_unique<MemoryFile>(model_, found->second);
		model_.after(); return file;
	}
	std::optional<EntryFootprint> entry_footprint(std::string_view name) override {
		model_.before(); auto found = model_.visible.find(std::string(name)); std::optional<EntryFootprint> result;
		if (found != model_.visible.end()) { result = EntryFootprint{EntryKind::Regular, found->second->visible.size(), std::nullopt}; }
		model_.after(); return result;
	}
	void sync_directory() override { model_.before(); model_.durable = model_.visible; model_.after(); }
private:
	struct Cursor final : DirectoryCursor {
		explicit Cursor(Model& model) : model_(model) {}
		std::optional<std::string> next() override {
			model_.before(); auto next = model_.visible.upper_bound(last_); std::optional<std::string> result;
			if (next != model_.visible.end()) { last_ = next->first; result = last_; }
			model_.after(); return result;
		}
		Model& model_; std::string last_;
	};
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

PreparedArtifact prepare(Journal& journal, std::string_view bytes) {
	auto builder = journal.prepare_artifact();
	while (!bytes.empty()) {
		auto count = std::min(bytes.size(), detail::artifact_chunk_size);
		builder.append_chunk(bytes.substr(0, count)); bytes.remove_prefix(count);
	}
	return builder.finish();
}
std::string read_artifact(ArtifactReader& reader) {
	std::string result(static_cast<std::size_t>(reader.descriptor().length), '\0');
	std::size_t offset = 0;
	while (offset < result.size()) {
		auto count = std::min(result.size() - offset, detail::artifact_chunk_size);
		auto read = reader.read_at(offset, std::span<char>(result.data() + offset, count));
		if (!read) { throw std::runtime_error("artifact stopped reading"); }
		offset += read;
	}
	return result;
}
std::pair<Frontier, std::vector<std::string>> replay_checkpoint(Model& model) {
	MemoryIO io(model); Journal journal(io, 1024);
	std::vector<std::string> batches;
	auto frontier = journal.recover([&](auto, std::string_view batch) { batches.emplace_back(batch); },
		[&](const Frontier& selected, ArtifactReader& checkpoint, std::span<ArtifactReader> dependencies) {
			check(selected.checkpoint && dependencies.size() == selected.dependencies.size(), "manifest exposes all required dependencies");
			batches.push_back(read_artifact(checkpoint));
			for (auto& dependency : dependencies) { check(read_artifact(dependency) == "application", "application dependency restores after integrity validation"); }
		});
	return {frontier, batches};
}

void checkpoint_basics() {
	Checksum checksum; checksum.update("123"); checksum.update("456"); checksum.update("789");
	check(checksum.value() == crc32c("123456789"), "streamed checksum matches complete bytes");
	auto model = initialized(3);
	{
		MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto application = prepare(journal, "application"); auto bundle = prepare(journal, "first");
		std::array dependencies{application};
		auto frontier = journal.publish_checkpoint(bundle, dependencies, 1);
		check(frontier.version == 2 && frontier.generation == 2 && frontier.sequence == 1 && frontier.base_sequence == 1,
			"checkpoint generation preserves the covered storage sequence");
		check(journal.append_batch("second").sequence == 2, "first post-checkpoint append continues the storage sequence");
		check(throws([&] { journal.publish_checkpoint(bundle, dependencies, 1); }) && !journal.fenced(), "stale cutover frontier is rejected before I/O");
		check(throws([&] { journal.publish_checkpoint(bundle, std::array{application, application}, 2); }) && !journal.fenced(), "duplicate immutable dependencies are rejected before I/O");
	}
	model.power_loss(false);
	auto [frontier, batches] = replay_checkpoint(model);
	check(batches == std::vector<std::string>({"first", "second"}) && frontier.sequence == 2 && frontier.base_sequence == 1,
		"recovery restores opaque checkpoint then replays its journal suffix");
	check(throws([&] { replay(model); }), "checkpoint store requires an explicit restoration callback");

	auto foreign_model = initialized(); MemoryIO foreign_io(foreign_model); Journal foreign(foreign_io, 1024);
	foreign.recover([](auto, auto) {}); auto prepared = prepare(foreign, "foreign");
	MemoryIO local_io(model); Journal local(local_io, 1024);
	local.recover([](auto, auto) {}, [](auto&, auto&, auto) {});
	check(throws([&] { local.publish_checkpoint(prepared, {}, 2); }) && !local.fenced(), "foreign owner session cannot publish an artifact");
}



void artifact_verification() {
	auto model = initialized(); MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
	auto artifact = prepare(journal, "immutable"); journal.verify_artifact(artifact);
	auto& bytes = model.visible.at(artifact_name(artifact.descriptor()))->visible;
	bytes.back() ^= 1;
	check(throws([&] { journal.verify_artifact(artifact); }) && journal.fenced(), "optional preflight readback detects corruption and fences owner");
	check(throws([&] { journal.publish_checkpoint(artifact, {}, 1); }), "failed verification prevents a later cutover");
}

void checkpoint_successive_generations() {
	auto model = initialized();
	{
		MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto application = prepare(journal, "application"); auto bundle = prepare(journal, "first");
		journal.publish_checkpoint(bundle, std::array{application}, 1);
	}
	{
		MemoryIO io(model); Journal journal(io, 1024);
		auto selected = journal.recover([](auto, auto) {}, [](auto&, auto&, auto) {});
		auto application = journal.pin_artifact(selected.dependencies.at(0));
		// Reuse a referenced immutable dependency after closing and recovering.
		auto bundle = prepare(journal, "first");
		auto next = journal.publish_checkpoint(bundle, std::array{application}, 1);
		check(next.generation == 3 && next.journal_identity != selected.journal_identity, "successive publication uses an exclusive physical generation");
		check(journal.append_batch("second").sequence == 2, "reused dependency preserves sequence continuity");
	}
	model.power_loss(false);
	check(replay_checkpoint(model).second == std::vector<std::string>{"first", "second"}, "successive generation and pinned dependency recover");
	{
		MemoryIO io(model); Journal journal(io, 1024);
		check(throws([&] { journal.recover([](auto, auto) {}, [](auto&, auto&, auto) { throw std::runtime_error("restore"); }); }) && journal.fenced(), "restoration callback failure keeps recovered state unpublished");
	}
}

void preparation_ownership_and_failures() {
	auto model = initialized(); MemoryIO io(model);
	auto journal = std::make_unique<Journal>(io, 1024); journal->recover([](auto, auto) {});
	std::optional<ArtifactBuilder> builder; builder.emplace(journal->prepare_artifact()); builder->append_chunk("first");
	check(throws([&] { journal->prepare_artifact(); }) && !journal->fenced(), "only one chunked preparation is active");
	check(throws([&] { builder->append_chunk(std::string(detail::artifact_chunk_size + 1, 'x')); }) && !journal->fenced(), "oversized preparation chunk has no I/O effect");
	journal.reset();
	{
		MemoryIO another_io(model); Journal another(another_io);
		check(throws([&] { another.recover([](auto, auto) {}); }), "preparation retains stable lock after journal closes");
	}
	std::optional<PreparedArtifact> artifact; artifact.emplace(builder->finish());
	check(throws([&] { builder->append_chunk("late"); }), "sealed artifact cannot receive further writes");
	builder.reset();
	{
		MemoryIO another_io(model); Journal another(another_io);
		check(throws([&] { another.recover([](auto, auto) {}); }), "prepared handle retains its owner session");
	}
	artifact.reset(); check(replay(model) == std::vector<std::string>{"first"}, "closing preparations allows ordinary recovery");

	for (auto chunk : {std::numeric_limits<std::size_t>::max(), std::size_t{3}}) {
		auto baseline = initialized(); baseline.chunk = chunk; std::size_t operations;
		{
			auto complete = baseline.clone(); MemoryIO complete_io(complete); Journal complete_journal(complete_io, 1024);
			complete_journal.recover([](auto, auto) {}); complete.operations = 0; prepare(complete_journal, "first"); operations = complete.operations;
		}
		for (std::size_t operation = 1; operation <= operations; ++operation) {
			for (bool after : {false, true}) {
				auto failed = baseline.clone();
				{
					MemoryIO failed_io(failed); Journal failed_journal(failed_io, 1024); failed_journal.recover([](auto, auto) {});
					failed.operations = 0; failed.fail_operation = operation; failed.fail_after = after;
					check(throws([&] { prepare(failed_journal, "first"); }) && failed_journal.fenced(), "every uncertain preparation operation fences writer");
				}
				for (bool visible : {false, true}) {
					auto crashed = failed.clone(); crashed.power_loss(visible);
					check(replay(crashed) == std::vector<std::string>{"first"}, "unfinished preparation cannot replace active history");
				}
			}
		}
		std::cout << "artifact preparation operations tested: " << operations << '\n';
	}
	{
		auto bounded = initialized(); MemoryIO bounded_io(bounded); Journal writer(bounded_io, 1024, 3);
		writer.recover([](auto, auto) {});
		auto stream = writer.prepare_artifact(); stream.append_chunk("abc");
		check(throws([&] { stream.append_chunk("d"); }) && !writer.fenced(), "cumulative preparation limit rejects before I/O");
		std::vector<PreparedArtifact> handles; handles.push_back(stream.finish());
		for (unsigned i = 1; i < detail::maximum_prepared_artifacts; ++i) { handles.push_back(prepare(writer, "")); }
		check(throws([&] { writer.prepare_artifact(); }) && !writer.fenced(), "prepared artifact leases have a bounded slot count");
		handles.pop_back(); handles.push_back(prepare(writer, "new"));
		check(handles.size() == detail::maximum_prepared_artifacts, "releasing a preparation lease frees its slot");
	}
}

void checkpoint_publication_failures() {
	auto baseline = initialized(); std::size_t operations;
	{
		auto complete = baseline.clone(); MemoryIO io(complete); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto bundle = prepare(journal, "first"); std::array dependencies{prepare(journal, "application")};
		complete.operations = 0; journal.publish_checkpoint(bundle, dependencies, 1); operations = complete.operations;
	}
	for (std::size_t operation = 1; operation <= operations; ++operation) {
		for (bool after : {false, true}) {
			auto failed = baseline.clone();
			{
				MemoryIO io(failed); Journal journal(io, 1024); journal.recover([](auto, auto) {});
				auto bundle = prepare(journal, "first"); std::array dependencies{prepare(journal, "application")};
				failed.operations = 0; failed.fail_operation = operation; failed.fail_after = after;
				check(throws([&] { journal.publish_checkpoint(bundle, dependencies, 1); }) && journal.fenced(), "uncertain generation publication fences writer");
				check(throws([&] { journal.append_batch("after failure"); }), "ambiguous publication cannot resume old generation");
			}
			for (bool visible : {false, true}) {
				auto crashed = failed.clone(); crashed.power_loss(visible);
				auto [frontier, batches] = replay_checkpoint(crashed);
				check(batches == std::vector<std::string>{"first"} && frontier.sequence == 1, "either recovered generation preserves covered history");
				if (operation == operations && after) { check(frontier.generation == 2, "completed generation directory barrier survives reported failure"); }
			}
			failed.fail_operation = 0; failed.operations = 0;
			auto [selected, batches] = replay_checkpoint(failed); failed.power_loss(false);
			auto [sealed, restored] = replay_checkpoint(failed);
			check(sealed.generation == selected.generation && restored == batches, "process restart seals selected generation before later power loss");
		}
	}
	std::cout << "checkpoint publication operations tested: " << operations << '\n';
}


Model reclaim_fixture() {
	auto model = initialized();
	{
		MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto old_bundle = prepare(journal, "first"); auto old_app = prepare(journal, "application");
		journal.publish_checkpoint(old_bundle, std::array{old_app}, 1);
		auto bundle = prepare(journal, "first"); auto app = prepare(journal, "application");
		journal.publish_checkpoint(bundle, std::array{app}, 1);
	}
	return model.clone();
}
void reclaim_all(Journal& journal, std::size_t budget = 1) {
	for (std::size_t steps = 0; steps < 1000; ++steps) {
		auto stats = journal.reclaim_step(budget); check(stats.scanned <= budget, "reclamation bounds scanned entries including protected and unknown names");
		if (stats.complete) { return; }
	}
	throw std::runtime_error("reclamation pass did not complete");
}
void reclamation_roots_and_unknowns() {
	auto model = reclaim_fixture();
	MemoryIO io(model); std::optional<ArtifactReader> escaped;
	Journal journal(io, 1024);
	journal.recover([](auto, auto) {}, [&](auto&, auto& bundle, auto) { escaped.emplace(std::move(bundle)); });
	check(throws([&] { journal.reclaim_step(0); }) && !journal.fenced(), "invalid scan admission leaves journal usable");
	auto escaped_name = artifact_name(escaped->descriptor());
	std::string prepared_name, active_name;
	{
		auto held = prepare(journal, "held"); prepared_name = artifact_name(held.descriptor());
		std::set<std::string> previous_names;
		for (const auto& [name, inode] : model.visible) { previous_names.insert(name); }
		auto builder = journal.prepare_artifact(); builder.append_chunk("active");
		for (const auto& [name, inode] : model.visible) { if (!previous_names.contains(name)) { active_name = name; } }
		reclaim_all(journal);
		check(model.visible.contains(prepared_name) && model.visible.contains(active_name), "prepared handles and active builder protect their names");
		auto finished = builder.finish();
		check(model.visible.contains(artifact_name(finished.descriptor())), "protected builder remains finishable after reclamation");
	}
	auto sentinel = std::string("artifact-00000000000000000000000000000000");
	model.visible[sentinel] = std::make_shared<Inode>();
	auto paused = journal.reclaim_step(1);
	check(!paused.complete && paused.scanned == 1, "reclamation retains its cursor across a partial scan");
	{
		auto bundle = prepare(journal, "first"); auto app = prepare(journal, "application"); journal.publish_checkpoint(bundle, std::array{app}, 1);
	}
	auto newest = journal.frontier();
	reclaim_all(journal);
	check(model.visible.contains(artifact_name(*newest.checkpoint)) && model.visible.contains(artifact_name(newest.dependencies[0])) &&
		model.visible.contains("generation-" + detail::hexadecimal(newest.journal_identity)), "publication between scan steps rechecks and protects newly authoritative roots");
	io.remove(sentinel); io.sync_directory();
	check(model.visible.contains(escaped_name), "escaped recovery reader pins obsolete artifact across publication");
	escaped.reset(); reclaim_all(journal);
	check(!model.visible.contains(escaped_name) && !model.visible.contains(prepared_name) && !model.visible.contains(active_name), "released pins make only their obsolete artifacts reclaimable");
	check(model.visible.size() == 5 && model.visible.contains("manifest") && model.visible.contains("owner.lock"), "reclamation retains exactly active generation and required roots");
	// Keep partial, foreign, basename-mismatched and unrelated files intact.
	auto unclassified = "artifact-" + detail::hexadecimal(detail::random_identity());
	model.visible[unclassified] = std::make_shared<Inode>(); model.visible[unclassified]->visible.assign(56 + 1024, '\0');
	auto mismatch = "artifact-" + detail::hexadecimal(detail::random_identity());
	model.visible[mismatch] = model.visible.at(artifact_name(*journal.frontier().checkpoint));
	Model foreign;
	std::string foreign_name;
	{
		MemoryIO foreign_io(foreign); Journal other(foreign_io, 1024); auto id = identity(); id[0] = 'Z'; other.create(id);
		auto file = prepare(other, "foreign"); foreign_name = artifact_name(file.descriptor());
		model.visible[foreign_name] = foreign.visible.at(foreign_name);
	}
	model.visible["notes.txt"] = std::make_shared<Inode>();
	auto stats = journal.reclaim_step(4096);
	check(stats.complete && stats.unknown_files == 3 && stats.removed == 0 && !journal.fenced(), "unknown ownership is reported without deleting or fencing");
	check(model.visible.contains(unclassified) && model.visible.contains(mismatch) && model.visible.contains(foreign_name) && model.visible.contains("notes.txt"), "unknown and unrelated files remain untouched");
}

void durable_staging_ownership() {
	auto model = initialized(); std::string abandoned;
	{
		MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto builder = journal.prepare_artifact();
		for (const auto& [name, inode] : model.visible) { if (name.starts_with("artifact-")) { abandoned = name; } }
		auto& inode = model.visible.at(abandoned);
		check(inode->durable.size() == detail::artifact_header_size && inode->durable.substr(0, 44) == inode->visible.substr(0, 44), "builder synchronizes ownership before accepting payload");
		auto prefix = inode->visible.substr(0, 44);
		for (unsigned chunk = 0; chunk < 8; ++chunk) { builder.append_chunk(std::string(64 * 1024, 'x')); }
		check(inode->visible.substr(0, 44) == prefix, "payload writes cannot overwrite immutable ownership");
		// Model spontaneous payload writeback and a directory barrier from an
		// interleaved append, without sealing the abandoned preparation.
		inode->durable = inode->visible; journal.append_batch("second");
	}
	model.power_loss(false);
	{
		MemoryIO io(model); Journal journal(io, 1024); std::vector<std::string> replayed;
		journal.recover([&](auto, auto bytes) { replayed.emplace_back(bytes); });
		auto stats = journal.reclaim_step(4096);
		check(stats.removed == 1 && stats.logical_bytes == detail::artifact_header_size + 8 * 64 * 1024 && !model.visible.contains(abandoned), "restart reclaims large unsealed artifact from durable ownership only");
		check(replayed == std::vector<std::string>{"first", "second"}, "large orphan cleanup preserves every acknowledged append");
		auto builder = journal.prepare_artifact(); std::string name;
		for (const auto& [candidate, inode] : model.visible) { if (candidate.starts_with("artifact-")) { name = candidate; } }
		auto prefix = model.visible.at(name)->visible.substr(0, 44); builder.append_chunk("sealed"); auto artifact = builder.finish();
		check(model.visible.at(name)->visible.substr(0, 44) == prefix, "finish replaces only the seal and preserves ownership bytes");
	}
}
void mixed_artifact_formats() {
	auto model = reclaim_fixture(); auto copy = model.clone(); auto selected = replay_checkpoint(copy).first;
	auto legacy = selected.dependencies.at(0); auto legacy_name = artifact_name(legacy);
	auto& inode = model.visible.at(legacy_name); inode->visible = detail::artifact_header_v1(selected.identity, legacy) + "application"; inode->durable = inode->visible;
	{
		MemoryIO io(model); std::optional<ArtifactReader> old_reader, new_reader; Journal journal(io, 1024);
		journal.recover([](auto, auto) {}, [&](auto&, auto& bundle, auto dependencies) { new_reader.emplace(std::move(bundle)); old_reader.emplace(std::move(dependencies[0])); });
		check(read_artifact(*old_reader) == "application" && read_artifact(*new_reader) == "first", "mixed v1 dependency and v2 bundle recover with distinct payload offsets");
		*new_reader = std::move(*old_reader);
		check(read_artifact(*new_reader) == "application", "reader move assignment preserves detected legacy offset"); old_reader.reset(); new_reader.reset();
		auto pinned = journal.pin_artifact(legacy); auto bundle = prepare(journal, "first"); journal.publish_checkpoint(bundle, std::array{pinned}, 1);
	}
	model.power_loss(false); check(replay_checkpoint(model).second == std::vector<std::string>{"first"}, "new publication can reuse an immutable legacy dependency");
	{
		MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {}, [](auto&, auto&, auto) {});
		{ auto bundle = prepare(journal, "first"); auto app = prepare(journal, "application"); journal.publish_checkpoint(bundle, std::array{app}, 1); }
		reclaim_all(journal); check(!model.visible.contains(legacy_name), "obsolete sealed legacy artifact remains reclaimable");
	}
	for (std::size_t length : {44u, 67u, 68u}) {
		auto missing_seal = model.clone(); auto state = replay_checkpoint(missing_seal).first;
		auto& required = missing_seal.visible.at(artifact_name(*state.checkpoint)); required->visible.resize(length); required->durable = required->visible;
		check(throws([&] { replay_checkpoint(missing_seal); }), "referenced artifacts reject incomplete seals or payloads");
	}
}

StorageResources visible_resources(const Model& model) {
	StorageResources resources{0, model.visible.size()};
	for (const auto& [name, inode] : model.visible) { resources.logical_bytes += inode->visible.size(); }
	return resources;
}
template <class Operation> void check_plan(Model& model, MutationPlan plan, Operation operation) {
	auto before = visible_resources(model); auto peak = detail::resources_add(before, plan.peak);
	model.observe = [&] { check(detail::resources_fit(visible_resources(model), peak), "every primitive filesystem state is covered by the planned simultaneous peak"); };
	operation(); model.observe = {};
	check(visible_resources(model) == detail::resources_subtract(detail::resources_add(before, plan.added), plan.removed), "successful mutation matches planned additions and removed manifest only");
}
void footprint_plans() {
	check(Journal::append_footprint(5).peak == StorageResources{361, 1}, "format-only append bound is available before journal startup");
	if constexpr (sizeof(std::size_t) > sizeof(std::uint32_t)) {
		check(throws([] { Journal::append_footprint(static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) + 1); }), "format-only planner rejects unrepresentable payloads without IO");
	}
	for (auto chunk : {std::numeric_limits<std::size_t>::max(), std::size_t{3}}) {
		Model model; model.chunk = chunk; MemoryIO io(model); Journal journal(io, 1024);
		check_plan(model, Journal::bootstrap_plan(), [&] { journal.create(identity()); });
		check_plan(model, journal.append_plan(5), [&] { journal.append_batch("first"); });
		auto waiting_append = journal.append_plan(5);
		for (std::size_t count : {2u, 9u, 1u}) {
			std::vector<std::uint64_t> lengths(count, 3);
			auto plan = journal.checkpoint_plan(lengths);
			check_plan(model, plan, [&] {
				std::vector<PreparedArtifact> artifacts;
				for (std::size_t artifact = 0; artifact < count; ++artifact) { artifacts.push_back(prepare(journal, "app")); }
				journal.publish_checkpoint(artifacts[0], std::span<const PreparedArtifact>(artifacts.data() + 1, artifacts.size() - 1), journal.frontier().sequence);
			});
		}
		check_plan(model, waiting_append, [&] { journal.append_batch("later"); });
		auto footprint = journal.artifact_footprint(5);
		check_plan(model, {footprint, footprint, {}}, [&] { auto builder = journal.prepare_artifact(); builder.append_chunk("known"); });
		check(throws([&] { journal.append_plan(1025); }) && throws([&] { journal.checkpoint_plan({}); }) && !journal.fenced(), "invalid footprint admission is rejected before mutation without fencing");
	}
	auto model = initialized(); MemoryIO io(model);
	auto maximum = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) - detail::artifact_header_size;
	Journal journal(io, 1024, maximum); journal.recover([](auto, auto) {});
	std::array<std::uint64_t, 3> lengths{maximum, maximum, maximum};
	check(throws([&] { journal.checkpoint_plan(lengths); }) && !journal.fenced(), "aggregate checkpoint plan overflow rejects before any IO");
}

const AdmissionLimits store_limits{{4096, 32}, {512, 2}, {1024, 4}, 8};
void ready_store(Store& store) { while (!store.inventory_step(1).complete) {} check(store.accounting().has_value(), "store admits accounting only after complete inventory"); }
void store_covered(Model& model, const Store& store) {
	auto stats = store.accounting();
	if (stats) { check(detail::resources_fit(visible_resources(model), detail::resources_add(stats->used, stats->outstanding)), "every primitive Store mutation is covered by charged usage plus outstanding reservations"); }
	else { check(detail::resources_fit(visible_resources(model), store_limits.hard), "bootstrap remains within prechecked hard quota before census"); }
}
void store_exact(Model& model, const Store& store) {
	check(store.accounting()->used == visible_resources(model) && !store.fenced(), "completed Store operation exactly accounts for all existing files");
}
void store_replacement(Store& store, const ReplacementId& replacement) {
	store.begin_artifact(replacement, ArtifactPart::Application); store.write_chunk(replacement, "app"); store.finish_artifact(replacement);
	store.begin_artifact(replacement, ArtifactPart::Bundle); store.write_chunk(replacement, "bundle"); store.finish_artifact(replacement);
	store.publish(replacement, store.frontier().sequence);
}
void store_artifact_completions() {
	for (unsigned phase = 0; phase < 3; ++phase) {
		for (bool detached : {false, true}) {
			for (bool uncertain : {false, true}) {
				auto model = initialized(); auto io = std::make_shared<MemoryIO>(model);
				AdmissionLimits limits{{10000, 128}, {2048, 4}, {4096, 4}, 16, 3};
				auto store = std::make_unique<Store>(io, limits, 1024); store->recover([](auto, auto) {}); store->inventory_step(128);
				auto id = store->reserve_replacement(128, 128); auto used = store->accounting()->used;
				check(bool(id), "completion preparation reserves complete replacement cycle");
				if (!id) { return; }
				auto job = store->begin_artifact_operation(*id, ArtifactPart::Application);
				if (phase > 0) { drive_synchronously(*job); job->result(); job = store->begin_write_chunk(*id, "payload"); }
				if (phase > 1) { drive_synchronously(*job); job->result(); job = store->begin_finish_artifact(*id); }
				auto outstanding = store->accounting()->outstanding;
				job->submitted(); auto completion = detail::execute_primitive(job->io(), job->request());
				auto before = model.operations;
				check(throws([&] { store->cancel_replacement(*id); }) && throws([&] { store->write_chunk(*id, "overlap"); }) && throws([&] { store->reclaim_step(1); }) && !store->fenced() && before == model.operations, "pending quantum rejects cancellation and competing mutation without IO or fencing");
				check(store->accounting()->used == used && store->accounting()->outstanding == outstanding, "executed but undelivered artifact quantum retains entire reservation");
				MutationCompletion stale; stale.token = completion.token; ++stale.token.operation;
				check(!job->complete(std::move(stale)) && job->in_flight(), "stale artifact completion cannot settle replacement lease");
				if (uncertain) { completion.error = std::make_exception_ptr(std::runtime_error("injected artifact completion uncertainty")); }
				if (detached) { store.reset(); io.reset(); check(model.locked, "detached replacement retains backend and stable owner lock"); }
				check(job->complete(std::move(completion)), "original artifact completion reaped through admitted job");
				drive_synchronously(*job);
				if (uncertain) {
					check(throws([&] { job->result(); }), "uncertain artifact job preserves original failure");
					if (store) { check(store->fenced() && store->accounting()->tainted && store->accounting()->used == used && store->accounting()->outstanding == outstanding, "uncertain preparation fences and retains conservative capacity"); }
				} else {
					job->result();
					if (store) {
						if (phase == 0) { store->write_chunk(*id, "payload"); }
						auto descriptor = phase == 2 ? job->descriptor() : store->finish_artifact(*id);
						check(descriptor.length == 7 && descriptor.checksum == crc32c("payload"), "terminal artifact quantum transfers exact visible length and seal into cycle");
						store->cancel_replacement(*id);
						check(store->accounting()->outstanding == StorageResources{} && store->accounting()->used.logical_bytes == used.logical_bytes + detail::artifact_header_size + 7 && !store->accounting()->tainted, "known canceled preparation keeps staged bytes charged and releases unused reservation");
					}
				}
				job.reset(); store.reset(); io.reset(); check(!model.locked, "terminal artifact retirement closes handles and releases stable ownership");
				model.fail_operation = 0; check(replay(model) == std::vector<std::string>{"first"}, "detached or uncertain preparation never changes acknowledged journal history");
			}
		}
	}
	{
		auto model = initialized(); auto io = std::make_shared<MemoryIO>(model);
		AdmissionLimits limits{{10000, 128}, {2048, 4}, {4096, 4}, 16, 3}; Store store(io, limits, 1024); store.recover([](auto, auto) {}); store.inventory_step(128);
		auto id = store.reserve_replacement(128, 128); auto before = model.operations;
		{ auto canceled = store.begin_artifact_operation(*id, ArtifactPart::Application); }
		check(before == model.operations && !store.fenced(), "unsubmitted replacement job cancellation performs no IO");
		store.cancel_replacement(*id); check(store.accounting()->outstanding == StorageResources{}, "unsubmitted cycle cancellation refunds whole reservation");
		id = store.reserve_replacement(128, 128); auto creation = store.begin_artifact_operation(*id, ArtifactPart::Application); drive_synchronously(*creation); creation->result(); creation.reset();
		auto empty = store.begin_write_chunk(*id, ""); check(empty->done(), "empty preparation quantum performs no IO");
		auto payload = store.begin_write_chunk(*id, "new"); empty.reset();
		check(throws([&] { store.cancel_replacement(*id); }), "retiring old completed lease cannot release a newer preparation job");
		payload.reset(); store.cancel_replacement(*id);
	}
}

void detached_old_artifact_lease() {
	auto model = initialized(); auto io = std::make_shared<MemoryIO>(model);
	AdmissionLimits limits{{10000, 128}, {2048, 4}, {4096, 4}, 16, 3}; auto store = std::make_unique<Store>(io, limits, 1024);
	store->recover([](auto, auto) {}); store->inventory_step(128); auto id = store->reserve_replacement(128, 128);
	{ auto creation = store->begin_artifact_operation(*id, ArtifactPart::Application); drive_synchronously(*creation); creation->result(); }
	auto old = store->begin_write_chunk(*id, ""); auto payload = store->begin_write_chunk(*id, "new");
	payload->submitted(); auto original = detail::execute_primitive(payload->io(), payload->request());
	store.reset(); io.reset(); old.reset();
	check(payload->in_flight() && model.locked && payload->complete(std::move(original)), "retiring old empty job after detachment cannot dispose the active replacement lease");
	drive_synchronously(*payload); payload->result(); payload.reset();
	check(!model.locked && replay(model) == std::vector<std::string>{"first"}, "detached payload settles independently of retired old jobs");
}

void store_append_completions() {
	auto model = initialized(3); auto io = std::make_shared<MemoryIO>(model); Store store(io, store_limits, 1024);
	store.recover([](auto, auto) {}); ready_store(store); auto initial = store.accounting()->used;
	{
		auto permit = store.reserve_append(AdmissionClass::Normal, 6); auto before = model.operations;
		auto operation = store.begin_append(std::move(*permit), "second");
		check(model.operations == before && store.accounting()->tickets == 1, "owned Store append reserves before IO");
	}
	check(!store.fenced() && store.accounting()->outstanding == StorageResources{} && store.accounting()->used == initial, "unsubmitted Store append cancellation refunds without fencing");
	auto permit = store.reserve_append(AdmissionClass::Normal, 6); auto operation = store.begin_append(std::move(*permit), "second");
	auto held = store.accounting()->outstanding;
	while (!operation->done()) {
		operation->submitted(); auto completion = detail::execute_primitive(operation->io(), operation->request());
		check(store.frontier().sequence == 1 && store.accounting()->used == initial && store.accounting()->outstanding == held, "executed but undelivered Store completion cannot publish or refund");
		check(operation->complete(std::move(completion)), "Store reaps matching primitive before accounting changes");
	}
	check(operation->result().sequence == 2 && store.frontier().sequence == 2 && store.accounting()->outstanding == StorageResources{} && !store.accounting()->tainted, "terminal barrier atomically settles journal and permit");
	store_exact(model, store);

	permit = store.reserve_append(AdmissionClass::Normal, 5); auto failed = store.begin_append(std::move(*permit), "third");
	held = store.accounting()->outstanding; auto used = store.accounting()->used;
	failed->submitted(); auto completion = detail::execute_primitive(failed->io(), failed->request()); store.fence_storage();
	check(failed->complete(std::move(completion)) && failed->done() && throws([&] { failed->result(); }), "fencing during IO still reaps original completion without a success result");
	check(store.accounting()->tainted && store.accounting()->outstanding == held && store.accounting()->used == used, "uncertain Store mutation retains conservative charges without refund");
}

void store_basics() {
	Model model; model.chunk = 3; MemoryIO io(model);
	{
		Store store(io, store_limits, 1024); model.observe = [&] { store_covered(model, store); };
		store.create(identity());
		check(throws([&] { store.reserve_append(AdmissionClass::Normal, 5); }), "Store blocks writes until the census completes");
		check(throws([&] { store.inventory_step(0); }) && !store.fenced(), "invalid inventory budgets have no uncertain mutation");
		ready_store(store); auto append = store.reserve_append(AdmissionClass::Normal, 5); store.append(*append, "first"); store_exact(model, store);
		check(throws([&] { store.append(*append, "again"); }) && !store.fenced(), "settled append reservation cannot be reused");
		auto replacement = store.reserve_replacement(128, 128); check(replacement.has_value(), "Store reserves complete replacement before capture");
		auto waiting = store.reserve_append(AdmissionClass::Normal, 5);
		store.begin_artifact(*replacement, ArtifactPart::Application); store.write_chunk(*replacement, "app");
		append = store.reserve_append(AdmissionClass::Normal, 5); check(append.has_value(), "Store interleaves append admission with active preparation"); store.append(*append, "later");
		store.finish_artifact(*replacement); store.verify_artifact(*replacement, ArtifactPart::Application);
		store.begin_artifact(*replacement, ArtifactPart::Bundle); store.write_chunk(*replacement, "bundle"); store.finish_artifact(*replacement);
		check(throws([&] { store.publish(*replacement, 1); }) && !store.fenced(), "stale covered sequence rejects without fencing prepared state");
		store.publish(*replacement, 2); store_exact(model, store);
		store.append(*waiting, "after"); store_exact(model, store);
		check(throws([&] { store.cancel_replacement(*replacement); }), "completed replacement IDs cannot mutate new work");
		{
			auto canceled = store.reserve_replacement(128, 128); store.begin_artifact(*canceled, ArtifactPart::Application); store.write_chunk(*canceled, "orphan");
			check(throws([&] { store.write_chunk(*canceled, std::string(129, 'x')); }) && !store.fenced(), "chunk reservation rejects overflow before IO");
			store.cancel_replacement(*canceled); store_exact(model, store);
		}
		while (!store.reclaim_step(1).complete) {} store_exact(model, store); model.observe = {};
	}
	model.power_loss(false);
	{
		Store reopened(io, store_limits, 1024); std::string bundle, application;
		std::vector<std::string> suffix;
		reopened.recover([&](auto, auto bytes) { suffix.emplace_back(bytes); }, [&](auto&, auto& reader, auto deps) { bundle = read_artifact(reader); application = read_artifact(deps[0]); });
		ready_store(reopened); check(bundle == "bundle" && application == "app" && reopened.frontier().sequence == 3 && suffix == std::vector<std::string>{"after"}, "Store restart restores checkpoint and append reserved across manifest migration"); store_exact(model, reopened);
	}
}
void store_failures() {
	for (auto chunk : {std::numeric_limits<std::size_t>::max(), std::size_t{3}}) {
		auto baseline = initialized(); std::size_t operations;
		baseline.chunk = chunk;
		{
			auto model = baseline.clone(); MemoryIO io(model); Store store(io, store_limits, 1024); store.recover([](auto, auto) {}); ready_store(store);
			auto replacement = store.reserve_replacement(128, 128); model.operations = 0; store_replacement(store, *replacement); operations = model.operations;
		}
		for (std::size_t operation = 1; operation <= operations; ++operation) {
			for (bool after : {false, true}) {
				auto model = baseline.clone();
				{
					MemoryIO io(model); Store store(io, store_limits, 1024); store.recover([](auto, auto) {}); ready_store(store);
					auto replacement = store.reserve_replacement(128, 128); model.operations = 0; model.fail_operation = operation; model.fail_after = after;
					check(throws([&] { store_replacement(store, *replacement); }) && store.fenced() && store.accounting()->tainted, "uncertain Store preparation or publication fences storage and accounting");
				}
				for (bool visible : {false, true}) {
					auto crash = model.clone(); crash.power_loss(visible); MemoryIO io(crash); Store recovered(io, store_limits, 1024); std::vector<std::string> history;
					recovered.recover([&](auto, auto bytes) { history.emplace_back(bytes); }, [&](auto&, auto& bundle, auto deps) {
						check(read_artifact(bundle) == "bundle" && read_artifact(deps[0]) == "app", "published Store generation recovers complete required artifacts"); history.emplace_back("first");
					}); ready_store(recovered);
					check(history == std::vector<std::string>{"first"}, "Store uncertain result never loses previously acknowledged history"); store_exact(crash, recovered);
				}
			}
		}
		std::cout << "Store replacement operations tested: " << operations << '\n';
	}
}
void store_append_and_cleanup_failures() {
	for (bool cleanup : {false, true}) {
		auto baseline = reclaim_fixture(); std::size_t operations;
		{
			auto model = baseline.clone(); MemoryIO io(model); Store store(io, store_limits, 1024);
			store.recover([](auto, auto) {}, [](auto&, auto&, auto) {}); ready_store(store);
			auto append = cleanup ? std::optional<AppendReservation>{} : store.reserve_append(AdmissionClass::Normal, 4);
			model.operations = 0; if (cleanup) { store.reclaim_step(4096); } else { store.append(*append, "next"); } operations = model.operations;
		}
		for (std::size_t operation = 1; operation <= operations; ++operation) {
			for (bool after : {false, true}) {
				auto model = baseline.clone();
				{
					MemoryIO io(model); std::optional<ArtifactReader> escaped; Store store(io, store_limits, 1024);
					store.recover([](auto, auto) {}, [&](auto&, auto& bundle, auto) { escaped.emplace(std::move(bundle)); }); ready_store(store);
					auto append = cleanup ? std::optional<AppendReservation>{} : store.reserve_append(AdmissionClass::Normal, 4);
					auto charged_before = store.accounting()->used;
					model.operations = 0; model.fail_operation = operation; model.fail_after = after;
					check(throws([&] { if (cleanup) { store.reclaim_step(4096); } else { store.append(*append, "next"); } }) && store.fenced() && store.accounting()->tainted, "Store append or cleanup uncertainty fences wrapper accounting");
					check(store.accounting()->used == charged_before && throws([&] { read_artifact(*escaped); }), "uncertain cleanup never credits deletion and fencing invalidates escaped readers");
				}
				for (bool visible : {false, true}) {
					auto crash = model.clone(); crash.power_loss(visible); MemoryIO io(crash); Store recovered(io, store_limits, 1024); std::vector<std::string> suffix;
					recovered.recover([&](auto, auto bytes) { suffix.emplace_back(bytes); }, [&](auto&, auto& bundle, auto deps) { check(read_artifact(bundle) == "first" && read_artifact(deps[0]) == "application", "cleanup or append failure preserves required checkpoint"); });
					ready_store(recovered); store_exact(crash, recovered);
					check(suffix.empty() || (!cleanup && suffix == std::vector<std::string>{"next"}), "recovery observes only optional uncertain append and required old state");
				}
			}
		}
		std::cout << "Store " << (cleanup ? "cleanup" : "append") << " operations tested: " << operations << '\n';
	}
}
void store_limits_and_pins() {
	for (bool entry_limit : {false, true}) {
		Model model; MemoryIO io(model); auto limits = store_limits;
		if (entry_limit) { limits.hard.entries = 9; } else { limits.hard.logical_bytes = 1700; }
		Store store(io, limits, 1024); store.create(identity()); ready_store(store);
		check(!store.reserve_append(AdmissionClass::Normal, 1), "Store refuses admission when either hard resource leaves no normal peak");
		check(store.reserve_append(AdmissionClass::Control, 1).has_value(), "normal pressure preserves protected control capacity");
		auto replacement = store.reserve_replacement(128, 128); check(replacement.has_value(), "normal pressure preserves complete replacement capacity"); store.cancel_replacement(*replacement);
	}
	auto model = reclaim_fixture(); MemoryIO io(model); std::optional<ArtifactReader> escaped;
	{
		Store store(io, store_limits, 1024); std::string pinned;
		store.recover([](auto, auto) {}, [&](auto& frontier, auto& bundle, auto) { pinned = artifact_name(*frontier.checkpoint); escaped.emplace(std::move(bundle)); }); ready_store(store);
		auto replacement = store.reserve_replacement(128, 128); store_replacement(store, *replacement);
		while (!store.reclaim_step(1).complete) {} store_exact(model, store);
		check(model.visible.contains(pinned) && read_artifact(*escaped) == "first", "Store cleanup keeps escaped old checkpoint reader pinned and charged");
		escaped.reset(); while (!store.reclaim_step(1).complete) {} store_exact(model, store);
		check(!model.visible.contains(pinned), "Store credits old pinned artifact only after actual durable unlink");
	}
	Model other_model; MemoryIO other_io(other_model); Store other(other_io, store_limits, 1024); other.create(identity()); ready_store(other);
	auto foreign_append = other.reserve_append(AdmissionClass::Normal, 1); auto foreign_replacement = other.reserve_replacement(128, 128);
	Store store(io, store_limits, 1024); store.recover([](auto, auto) {}, [](auto&, auto&, auto) {}); ready_store(store);
	check(throws([&] { store.append(*foreign_append, "x"); }) && throws([&] { store.begin_artifact(*foreign_replacement, ArtifactPart::Application); }) && !store.fenced(), "foreign Store capabilities reject before mutation without fencing");
}
void incremental_artifact_verification() {
	for (bool legacy : {false, true}) { for (std::size_t chunk : {std::size_t{3}, std::numeric_limits<std::size_t>::max()}) {
		for (const std::string& payload : {std::string{}, std::string("candidate"), std::string(65553, 'x')}) {
			auto model = initialized(); MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
			auto artifact = prepare(journal, payload); auto inode = model.visible.at(artifact_name(artifact.descriptor()));
			auto header = legacy ? detail::artifact_v1_header_size : detail::artifact_header_size;
			if (legacy) { inode->visible = detail::artifact_header_v1(journal.frontier().identity, artifact.descriptor()) + payload; inode->durable = inode->visible; }
			model.chunk = chunk; std::size_t payload_reads = 0, largest = 0;
			model.observe_read = [&](const Inode* source, auto offset, auto count) {
				if (source == inode.get() && offset + count > header) { ++payload_reads; largest = std::max(largest, count); }
			};
			auto verifier = journal.begin_artifact_verification(artifact);
			check(verifier && payload_reads == 0 && verifier->offset() == 0, "incremental open validates fixed metadata without payload scanning");
			if (!verifier) { continue; }
			check(throws([&] { verifier->read_next(std::span<char>{}); }) == !payload.empty() && !journal.fenced(), "empty destination rejects before EOF without fencing");
			if (!payload.empty()) { check(throws([&] { std::move(*verifier).finish(); }) && !journal.fenced(), "premature promotion preserves healthy incomplete verification"); }
			std::array<char, 65536> buffer{}; std::string candidate;
			while (verifier->offset() < payload.size()) {
				auto before = payload_reads, operations = model.operations; auto offset = verifier->offset(); auto count = verifier->read_next(buffer);
				check(payload_reads == before + 1 && model.operations == operations + 1 && count > 0 && count <= 65536 && verifier->offset() == offset + count, "each verification step makes one bounded read and preserves partial progress");
				candidate.append(buffer.data(), count);
			}
			auto reads = payload_reads, operations = model.operations;
			check(verifier->read_next(buffer) == 0 && payload_reads == reads, "verified EOF makes no extra payload call");
			auto reader = std::move(*verifier).finish();
			check(model.operations == operations && payload_reads == reads && largest <= 65536 && candidate == payload, "promotion retains the verified descriptor without reopening or rereading");
			check(throws([&] { verifier->read_next(buffer); }) && !journal.fenced() && read_artifact(reader) == payload, "promoted reader reads both formats while moved verifier rejects harmlessly");
		}
	} }
	{
		auto model = initialized(); MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto original = prepare(journal, "x"), artifact = std::move(original); auto before = model.operations;
		check(throws([&] { journal.begin_artifact_verification(original); }) && model.operations == before && !journal.fenced(), "moved prepared handle rejects before IO without fencing");
		std::vector<ArtifactVerifier> handles;
		for (unsigned i = 0; i < 9; ++i) { auto handle = journal.begin_artifact_verification(artifact); if (handle) { handles.push_back(std::move(*handle)); } }
		before = model.operations;
		check(handles.size() == 9 && !journal.begin_artifact_verification(artifact) && model.operations == before && !journal.fenced(), "nine verification leases reject further opens before IO");
		std::array<char, 8> buffer{}; handles.back().read_next(buffer); std::optional<ArtifactReader> reader(std::move(handles.back()).finish()); handles.pop_back();
		check(!journal.begin_artifact_verification(artifact), "promoted reader keeps its verification lease");
		auto moved = std::move(handles.back()); handles.pop_back();
		check(!journal.begin_artifact_verification(artifact), "moving a verifier keeps its bounded lease");
		reader.reset(); auto reopened = journal.begin_artifact_verification(artifact);
		check(reopened.has_value() && !journal.begin_artifact_verification(artifact), "closing the promoted reader releases exactly one lease");
		check(moved.descriptor() == artifact.descriptor(), "moved verification keeps its exact artifact metadata");
		std::array<char, 65537> oversized{}; before = model.operations;
		check(throws([&] { moved.read_next(oversized); }) && model.operations == before && !journal.fenced(), "oversized caller buffer rejects before IO without fencing");
		Model foreign_model; MemoryIO foreign_io(foreign_model); Journal foreign(foreign_io, 1024); foreign.create(identity()); auto foreign_artifact = prepare(foreign, "foreign");
		check(throws([&] { journal.begin_artifact_verification(foreign_artifact); }) && model.operations == before && !journal.fenced(), "foreign prepared artifact rejects without IO or fencing even under slot pressure");
	}
	for (unsigned corruption = 0; corruption < 4; ++corruption) {
		auto model = initialized(); MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto artifact = prepare(journal, "candidate"); auto& bytes = model.visible.at(artifact_name(artifact.descriptor()))->visible;
		if (corruption == 0) { bytes[0] ^= 1; }
		if (corruption == 1) { bytes.back() ^= 1; }
		check(throws([&] {
			auto verifier = journal.begin_artifact_verification(artifact); std::array<char, 64> buffer{};
			if (corruption == 2) { bytes.resize(detail::artifact_header_size); }
			if (corruption == 3) { model.oversized_read = true; }
			while (verifier->offset() < verifier->descriptor().length) { verifier->read_next(buffer); }
			auto reader = std::move(*verifier).finish();
		}) && journal.fenced(), "header, checksum, zero progress and oversized backend reads fence verification");
	}
	{
		auto model = initialized(); MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto artifact = prepare(journal, "candidate"); model.chunk = 3;
		auto source = journal.begin_artifact_verification(artifact), destination = journal.begin_artifact_verification(artifact);
		std::array<char, 64> buffer{}; source->read_next(buffer); *destination = std::move(*source);
		check(destination->offset() == 3 && throws([&] { source->read_next(buffer); }) && !journal.fenced(), "move assignment preserves checksum progress and invalidates only the source");
		while (destination->offset() < destination->descriptor().length) { destination->read_next(buffer); }
		auto reader = std::move(*destination).finish(); check(read_artifact(reader) == "candidate", "move-assigned verification promotes a complete correctly checksummed image");
	}
	{
		auto model = initialized(); MemoryIO io(model); std::optional<ArtifactVerifier> escaped;
		{
			Journal journal(io, 1024); journal.recover([](auto, auto) {}); auto artifact = prepare(journal, "escaped"); escaped = journal.begin_artifact_verification(artifact);
		}
		std::array<char, 64> buffer{}; check(model.locked, "escaped verifier retains the stable owner lock after Journal destruction");
		escaped->read_next(buffer); std::optional<ArtifactReader> reader(std::move(*escaped).finish()); escaped.reset();
		check(model.locked && read_artifact(*reader) == "escaped", "promoted escaped reader retains its owner and verified file");
		reader.reset(); check(!model.locked, "final reader destruction releases the escaped owner session");
	}
}

void store_incremental_verification() {
	Model model; MemoryIO io(model); Store store(io, store_limits, 1024); store.create(identity()); ready_store(store);
	auto before = store.accounting()->used; auto replacement = store.reserve_replacement(64, 64);
	check(throws([&] { store.begin_artifact_verification(*replacement, ArtifactPart::Application); }) && !store.fenced(), "unsealed replacement artifact rejects without fencing");
	store.begin_artifact(*replacement, ArtifactPart::Application); store.write_chunk(*replacement, "candidate"); auto descriptor = store.finish_artifact(*replacement);
	auto verifier = store.begin_artifact_verification(*replacement, ArtifactPart::Application); auto name = artifact_name(descriptor);
	store.cancel_replacement(*replacement); while (!store.reclaim_step(1).complete) {}
	check(model.visible.contains(name) && store.accounting()->used.logical_bytes == before.logical_bytes + 77, "canceled staging retains verifier pin and exact charges through GC");
	std::array<char, 64> buffer{}; verifier->read_next(buffer); std::optional<ArtifactReader> reader(std::move(*verifier).finish()); verifier.reset();
	while (!store.reclaim_step(1).complete) {}
	check(model.visible.contains(name) && read_artifact(*reader) == "candidate", "promoted reader retains the canceled artifact across durable reclamation");
	reader.reset(); while (!store.reclaim_step(1).complete) {}
	check(!model.visible.contains(name) && store.accounting()->used == before, "only final handle release and durable GC credit canceled artifact resources");
	for (bool after : {false, true}) { for (unsigned fault = 1; fault <= 5; ++fault) {
		Model failed; MemoryIO backend(failed); Store owned(backend, store_limits, 1024); owned.create(identity()); ready_store(owned);
		auto id = owned.reserve_replacement(64, 64); owned.begin_artifact(*id, ArtifactPart::Application); owned.write_chunk(*id, "candidate"); owned.finish_artifact(*id);
		failed.fail_operation = failed.operations + fault; failed.fail_after = after;
		check(throws([&] { auto v = owned.begin_artifact_verification(*id, ArtifactPart::Application); v->read_next(buffer); auto r = std::move(*v).finish(); }) && owned.fenced(), "before/after verification-open and payload IO failures fence Store");
	} }
}

void published_artifact_selection() {
	for (bool legacy : {false, true}) { for (const std::string& payload : {std::string{}, std::string("published"), std::string(65553, 'p')}) {
		auto model = initialized(); MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto before = model.operations;
		check(!journal.select_published_dependency(0) && model.operations == before, "publication selection without checkpoint performs no IO");
		ArtifactDescriptor descriptor;
		{ auto app = prepare(journal, payload), bundle = prepare(journal, "bundle"); descriptor = app.descriptor(); journal.publish_checkpoint(bundle, std::array{app}, 1); }
		auto inode = model.visible.at(artifact_name(descriptor));
		if (legacy) { inode->visible = detail::artifact_header_v1(journal.frontier().identity, descriptor) + payload; inode->durable = inode->visible; }
		before = model.operations; auto selection = journal.select_published_dependency(0);
		check(selection && selection->descriptor() == descriptor && selection->checkpoint() == *journal.frontier().checkpoint && selection->base_sequence() == 1 && !journal.select_published_dependency(1) && model.operations == before, "selection binds complete current publication without IO");
		journal.append_batch("ordinary"); std::size_t reads = 0;
		model.observe_read = [&](const Inode* source, auto offset, auto count) { if (source == inode.get() && offset + count > (legacy ? detail::artifact_v1_header_size : detail::artifact_header_size)) { ++reads; } };
		auto verifier = journal.begin_published_verification(*selection);
		check(verifier && reads == 0, "ordinary append preserves selection and opening reads only fixed metadata");
		model.chunk = 3; std::array<char, 65536> buffer{}; std::string actual;
		if (!payload.empty()) { check(throws([&] { auto reader = std::move(*verifier).finish(); }) && !journal.fenced(), "published premature promotion is a healthy caller error"); }
		while (verifier->offset() < descriptor.length) {
			before = model.operations; auto count = verifier->read_next(buffer);
			check(count > 0 && count <= 65536 && model.operations == before + 1, "published verification makes one bounded partial payload read"); actual.append(buffer.data(), count);
		}
		before = model.operations; auto reader = std::move(*verifier).finish();
		check(actual == payload && model.operations == before, "published verification promotes both formats without IO");
		auto app = journal.pin_artifact(descriptor), next_bundle = prepare(journal, "next"); journal.publish_checkpoint(next_bundle, std::array{app}, 2);
		before = model.operations;
		check(!journal.begin_published_verification(*selection) && model.operations == before && !journal.fenced(), "replacement invalidates selection even when dependency is reused");
	} }
	{
		auto model = initialized(); MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		{ auto app = prepare(journal, "app"), bundle = prepare(journal, "bundle"); journal.publish_checkpoint(bundle, std::array{app}, 1); }
		auto selection = journal.select_published_dependency(0); auto artifact = journal.pin_artifact(selection->descriptor());
		auto staged = journal.begin_artifact_verification(artifact); std::vector<ArtifactVerifier> handles;
		for (unsigned i = 0; i < 8; ++i) { auto handle = journal.begin_published_verification(*selection); handles.push_back(std::move(*handle)); }
		auto before = model.operations;
		check(!journal.begin_published_verification(*selection) && !journal.begin_artifact_verification(artifact) && model.operations == before && !journal.fenced(), "staged and published verification share nine slots before IO");
		Model other_model; MemoryIO other_io(other_model); Journal other(other_io, 1024); other.create(identity());
		{ auto app = prepare(other, "app"), bundle = prepare(other, "bundle"); other.publish_checkpoint(bundle, std::array{app}, 0); }
		auto foreign = other.select_published_dependency(0); before = model.operations;
		check(throws([&] { journal.begin_published_verification(*foreign); }) && model.operations == before && !journal.fenced(), "foreign selection rejects before shared verification pressure and IO");
		std::array<char, 8> buffer{}; handles.back().read_next(buffer); std::optional<ArtifactReader> reader(std::move(handles.back()).finish()); handles.pop_back();
		check(!journal.begin_published_verification(*selection), "promoted published reader retains its verification slot");
		auto moved = std::move(handles.back()); handles.pop_back(); check(!journal.begin_published_verification(*selection), "moving published verifier retains its slot");
		reader.reset(); check(journal.begin_published_verification(*selection).has_value(), "closing published reader releases its verification slot");
		staged.reset(); handles.clear(); auto fresh = journal.begin_published_verification(*selection); moved = std::move(*fresh);
		std::vector<PreparedArtifact> prepared; for (unsigned i = 1; i < 9; ++i) { prepared.push_back(journal.pin_artifact(selection->descriptor())); }
		check(throws([&] { journal.pin_artifact(selection->descriptor()); }) && journal.begin_published_verification(*selection).has_value(), "published open does not consume an exhausted prepared-artifact slot");
		auto copied = *selection; auto moved_selection = std::move(copied); before = model.operations;
		check(throws([&] { journal.begin_published_verification(copied); }) && model.operations == before && !journal.fenced(), "moved selection rejects before IO without fencing");
		check(journal.begin_published_verification(moved_selection).has_value(), "moved-to selection preserves session provenance");
	}
}

void store_published_selection() {
	Model model; MemoryIO io(model); std::optional<PublishedArtifactSelection> escaped;
	{
		Store store(io, store_limits, 1024); store.create(identity()); ready_store(store);
		auto replacement = store.reserve_replacement(128, 128); store_replacement(store, *replacement);
		escaped = store.select_published_dependency(0); auto selected = *escaped; auto old_name = artifact_name(selected.descriptor()); auto before = store.accounting()->used;
		auto verifier = store.begin_published_verification(selected);
		check(store.accounting()->used == before && store.accounting()->outstanding == StorageResources{}, "published opening reserves no Replacement bytes or tickets");
		replacement = store.reserve_replacement(128, 128); store_replacement(store, *replacement);
		while (!store.reclaim_step(1).complete) {}
		check(model.visible.contains(old_name), "published verifier pins old dependency after replacement and reclamation");
		std::array<char, 8> buffer{}; verifier->read_next(buffer); std::optional<ArtifactReader> reader(std::move(*verifier).finish()); verifier.reset();
		while (!store.reclaim_step(1).complete) {}
		check(model.visible.contains(old_name) && read_artifact(*reader) == "app", "promoted published reader retains old dependency pin");
		before = store.accounting()->used; reader.reset(); while (!store.reclaim_step(1).complete) {}
		check(!model.visible.contains(old_name) && store.accounting()->used.logical_bytes + 71 == before.logical_bytes, "selection alone does not pin and durable GC credits the exact old file"); store_exact(model, store);
		escaped = store.select_published_dependency(0);
	}
	check(!model.locked, "escaped publication selection does not retain the owner lock");
	{
		Store store(io, store_limits, 1024); store.recover([](auto, auto) {}, [](auto&, auto&, auto) {}); ready_store(store); auto before = model.operations;
		check(throws([&] { store.begin_published_verification(*escaped); }) && model.operations == before && !store.fenced(), "old selection rejects against identical recovered publication without IO");
		auto fresh = store.select_published_dependency(0); check(store.begin_published_verification(*fresh).has_value(), "reopened Store creates a new usable selection");
	}
	for (bool after : {false, true}) { for (unsigned fault = 1; fault <= 5; ++fault) {
		Model failed; MemoryIO backend(failed); Store owned(backend, store_limits, 1024); owned.create(identity()); ready_store(owned);
		auto id = owned.reserve_replacement(128, 128); store_replacement(owned, *id); auto selected = owned.select_published_dependency(0);
		failed.fail_operation = failed.operations + fault; failed.fail_after = after;
		check(throws([&] { auto verifier = owned.begin_published_verification(*selected); std::array<char, 8> buffer{}; verifier->read_next(buffer); auto reader = std::move(*verifier).finish(); }) && owned.fenced(), "published metadata and payload IO faults fence Store before and after effects");
	} }
	for (bool header : {false, true}) {
		Model failed; MemoryIO backend(failed); Store owned(backend, store_limits, 1024); owned.create(identity()); ready_store(owned);
		auto id = owned.reserve_replacement(128, 128); store_replacement(owned, *id); auto selected = owned.select_published_dependency(0);
		auto& bytes = failed.visible.at(artifact_name(selected->descriptor()))->visible; bytes[header ? 0 : bytes.size() - 1] ^= 1;
		check(throws([&] { auto verifier = owned.begin_published_verification(*selected); std::array<char, 8> buffer{}; verifier->read_next(buffer); auto reader = std::move(*verifier).finish(); }) && owned.fenced(), "published header and CRC corruption fence shared owner");
	}
}

void store_posix() {
	auto directory = std::filesystem::current_path() / ".scratch" / ("store-test-" + std::to_string(::getpid()));
	if (!std::filesystem::create_directory(directory)) { throw std::runtime_error("Store test directory exists"); } ::chmod(directory.c_str(), 0700);
	struct Cleanup { std::filesystem::path path; ~Cleanup() { std::filesystem::remove_all(path); } } cleanup{directory};
	std::optional<PublishedArtifactSelection> old_selection;
	{
		PosixIO io(directory); Store store(io, store_limits, 1024); store.create(identity()); ready_store(store);
		auto append = store.reserve_append(AdmissionClass::Normal, 5); store.append(*append, "first");
		auto replacement = store.reserve_replacement(128, 128); store_replacement(store, *replacement);
		auto cancel = store.reserve_replacement(128, 128); store.begin_artifact(*cancel, ArtifactPart::Application); store.write_chunk(*cancel, "orphan"); store.cancel_replacement(*cancel);
		while (!store.reclaim_step(1).complete) {}
		Inventory census(io); while (!census.step(1).complete) {}
		check(store.accounting()->used == StorageResources{census.stats().logical_bytes, census.stats().entries}, "real POSIX Store accounts for publication cancellation and cleanup exactly");
		old_selection = store.select_published_dependency(0);
	}
	{
		PosixIO io(directory); Store store(io, store_limits, 1024); std::string bundle, application;
		store.recover([](auto, auto) {}, [&](auto&, auto& reader, auto deps) { bundle = read_artifact(reader); application = read_artifact(deps[0]); }); ready_store(store);
		check(bundle == "bundle" && application == "app" && store.frontier().sequence == 1, "actual POSIX Store reopens acknowledged replacement");
		check(throws([&] { store.begin_published_verification(*old_selection); }) && !store.fenced(), "POSIX reopen rejects previous selection while acquiring the stable lock");
		auto selected = store.select_published_dependency(0); auto source_name = artifact_name(selected->descriptor()); auto source = store.begin_published_verification(*selected);
		auto newer = store.reserve_replacement(128, 128); store_replacement(store, *newer);
		while (!store.reclaim_step(1).complete) {}
		check(std::filesystem::exists(directory / source_name), "POSIX published verifier pins old dependency across replacement");
		std::array<char, 3> published_buffer{}; source->read_next(published_buffer); auto published_reader = std::move(*source).finish(); source.reset();
		check(read_artifact(published_reader) == "app", "POSIX published verifier promotes exact old image");
		{ auto released = std::move(published_reader); }
		while (!store.reclaim_step(1).complete) {}
		check(!std::filesystem::exists(directory / source_name), "POSIX published pin release permits durable old-file reclamation");
		auto id = store.reserve_replacement(128, 128); store.begin_artifact(*id, ArtifactPart::Application); store.write_chunk(*id, "verified POSIX image"); auto descriptor = store.finish_artifact(*id);
		auto verifier = store.begin_artifact_verification(*id, ArtifactPart::Application); store.cancel_replacement(*id);
		while (!store.reclaim_step(1).complete) {}
		check(std::filesystem::exists(directory / artifact_name(descriptor)), "POSIX incremental verifier pins canceled staging");
		std::array<char, 3> buffer{}; std::string candidate;
		while (verifier->offset() < descriptor.length) { auto count = verifier->read_next(buffer); candidate.append(buffer.data(), count); }
		std::optional<ArtifactReader> reader(std::move(*verifier).finish()); verifier.reset();
		check(candidate == "verified POSIX image" && read_artifact(*reader) == candidate, "POSIX partial verification promotes the same complete application payload");
		reader.reset(); while (!store.reclaim_step(1).complete) {}
		check(!std::filesystem::exists(directory / artifact_name(descriptor)), "POSIX durable GC removes staging only after verified reader closes");
	}
}

void reclamation_failures() {
	auto baseline = reclaim_fixture(); std::size_t operations;
	auto proof = baseline.clone(); auto expected = replay_checkpoint(proof).first;
	{
		auto model = baseline.clone(); MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {}, [](auto&, auto&, auto) {});
		model.operations = 0; auto stats = journal.reclaim_step(4096); operations = model.operations;
		check(stats.complete && stats.removed == 4 && stats.logical_bytes > 0, "obsolete generations and sealed artifacts are durably reclaimed");
	}
	for (std::size_t operation = 1; operation <= operations; ++operation) {
		for (bool after : {false, true}) {
			auto failed = baseline.clone();
			{
				MemoryIO io(failed); Journal journal(io, 1024); journal.recover([](auto, auto) {}, [](auto&, auto&, auto) {});
				failed.operations = 0; failed.fail_operation = operation; failed.fail_after = after;
				check(throws([&] { journal.reclaim_step(4096); }) && journal.fenced(), "uncertain reclamation I/O fences the owner");
				check(throws([&] { journal.append_batch("unavailable"); }), "failed reclamation cannot resume writes");
			}
			failed.fail_operation = 0;
			for (bool choose_new : {false, true}) {
				auto crashed = failed.clone(); crashed.power_loss(choose_new);
				auto [selected, restored] = replay_checkpoint(crashed);
				check(restored == std::vector<std::string>{"first"} && selected.generation == expected.generation && selected.journal_identity == expected.journal_identity && selected.sequence == expected.sequence,
					"either cleanup namespace outcome preserves exact latest acknowledged generation and state");
			}
		}
	}
	std::cout << "reclamation operations tested: " << operations << '\n';
}

void checkpoint_corruption() {
	auto baseline = initialized(); Frontier frontier;
	{
		MemoryIO io(baseline); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto bundle = prepare(journal, "first"); std::array dependencies{prepare(journal, "application")};
		journal.publish_checkpoint(bundle, dependencies, 1); frontier = journal.append_batch("second");
	}
	std::vector<std::string> required{"manifest", "generation-" + detail::hexadecimal(frontier.journal_identity), artifact_name(*frontier.checkpoint)};
	for (const auto& dependency : frontier.dependencies) { required.push_back(artifact_name(dependency)); }
	for (const auto& name : required) {
		for (std::size_t byte = 0; byte < baseline.visible.at(name)->visible.size(); ++byte) {
			auto corrupted = baseline.clone(); corrupted.visible.at(name)->visible[byte] ^= 1;
			check(throws([&] { replay_checkpoint(corrupted); }), "every corrupt required-generation byte fails closed");
		}
		auto missing = baseline.clone(); missing.visible.erase(name);
		check(throws([&] { replay_checkpoint(missing); }), "missing referenced generation artifact fails closed without fallback");
	}
	{
		auto malicious = baseline.clone(); auto& manifest = malicious.visible.at("manifest")->visible;
		std::string length; put64(length, std::numeric_limits<std::uint64_t>::max());
		manifest.replace(88, 8, length); // v2 bundle length, with a valid frame CRC.
		std::string checksum; put32(checksum, crc32c(std::string_view(manifest).substr(0, manifest.size() - 4)));
		manifest.replace(manifest.size() - 4, 4, checksum);
		check(throws([&] { replay_checkpoint(malicious); }), "checksummed oversized artifact cannot overflow allocation or file offsets");
	}
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

void append_failures(std::size_t chunk = std::numeric_limits<std::size_t>::max(), bool checkpointed = false, bool suspended = false) {
	auto append = [&](Journal& journal) {
		if (!suspended) { return journal.append_batch("second"); }
		auto operation = journal.begin_append("second");
		while (!operation->done()) {
			check(journal.frontier().sequence == 1, "suspended append keeps public frontier unchanged");
			operation->submitted();
			auto completion = detail::execute_primitive(operation->io(), operation->request());
			check(journal.frontier().sequence == 1, "executed but undelivered completion cannot publish frontier");
			auto stale = MutationCompletion{}; stale.token = completion.token; ++stale.token.operation;
			check(!operation->complete(std::move(stale)) && operation->in_flight(), "foreign completion cannot reap an accepted primitive");
			auto token = completion.token;
			check(operation->complete(std::move(completion)), "matching completion is reaped exactly once");
			auto repeated = MutationCompletion{}; repeated.token = token;
			check(!operation->complete(std::move(repeated)), "duplicate completion cannot advance another step");
		}
		return journal.finish_append(operation);
	};
	auto baseline = initialized(chunk);
	if (checkpointed) {
		MemoryIO io(baseline); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto bundle = prepare(journal, "first"); std::array dependencies{prepare(journal, "application")};
		journal.publish_checkpoint(bundle, dependencies, 1);
	}
	auto recover_values = [&](Model& model) { return checkpointed ? replay_checkpoint(model).second : replay(model); };
	std::size_t operation_count = 0;
	{
		auto model = baseline.clone(); auto io = std::make_shared<MemoryIO>(model); Journal journal(io, 1024);
		journal.recover([](auto, auto) {}, [](auto&, auto&, auto) {}); model.operations = 0;
		append(journal); operation_count = model.operations;
	}
	for (std::size_t operation = 1; operation <= operation_count; ++operation) {
		for (bool after : {false, true}) {
			auto model = baseline.clone();
			{
				auto io = std::make_shared<MemoryIO>(model); Journal journal(io, 1024); journal.recover([](auto, auto) {}, [](auto&, auto&, auto) {});
				model.operations = 0; model.fail_operation = operation; model.fail_after = after;
				check(throws([&] { append(journal); }) && journal.fenced(), "each uncertain append operation fences");
				check(throws([&] { journal.append_batch("third"); }), "failure prevents subsequent acknowledgement");
			}
			std::optional<Model> synchronous;
			if (suspended) {
				synchronous = baseline.clone();
				MemoryIO backend(*synchronous); Journal reference(backend, 1024);
				reference.recover([](auto, auto) {}, [](auto&, auto&, auto) {});
				synchronous->operations = 0; synchronous->fail_operation = operation; synchronous->fail_after = after;
				check(throws([&] { reference.append_batch("second"); }) && reference.fenced(), "reference driver receives identical injected fault");
			}
			for (bool keep_visible : {false, true}) {
				auto crash = model.clone(); crash.power_loss(keep_visible);
				auto batches = recover_values(crash);
				if (synchronous) {
					auto comparison = synchronous->clone(); comparison.power_loss(keep_visible);
					check(batches == recover_values(comparison), "synchronous and suspended drivers recover identical histories under identical faults");
				}
				check(batches == std::vector<std::string>{"first"} || batches == std::vector<std::string>({"first", "second"}),
					"crash keeps the previous acknowledged prefix and at most the interrupted batch");
				if (operation == operation_count && after) {
					check(batches.size() == 2, "completed directory barrier preserves new frontier even when its call reports failure");
				}
			}
			// Process death preserves the visible namespace, unlike power loss.
			// Recovery must seal its selected frontier before a later power loss.
			model.fail_operation = 0; model.operations = 0;
			auto selected = recover_values(model);
			model.power_loss(false);
			check(recover_values(model) == selected, "restart seals selected namespace against later power loss");
		}
	}
	std::cout << "append publication operations tested: " << operation_count << '\n';
}

void suspended_artifact_operations() {
	auto baseline = initialized();
	for (auto chunk : {std::size_t{3}, std::numeric_limits<std::size_t>::max()}) {
		baseline.chunk = chunk;
		auto prepare_owned = [](Journal& journal, bool suspended) {
			if (!suspended) { return prepare(journal, "artifact-payload"); }
			auto drive = [&](const std::shared_ptr<ArtifactMutation>& operation) {
				while (!operation->done()) {
					auto before = operation->request().token;
					operation->submitted(); auto completion = detail::execute_primitive(operation->io(), operation->request());
					check(throws([&] { operation->result(); }), "executed artifact primitive cannot report completion before delivery");
					MutationCompletion stale; stale.token = before; ++stale.token.step;
					check(!operation->complete(std::move(stale)) && operation->in_flight(), "stale artifact completion retains original primitive");
					check(operation->complete(std::move(completion)), "matching artifact primitive reaped");
					MutationCompletion duplicate; duplicate.token = before;
					check(!operation->complete(std::move(duplicate)), "duplicate artifact completion cannot advance another phase");
				}
				operation->result();
			};
			auto creation = journal.begin_artifact_preparation(); drive(creation);
			auto builder = creation->take_builder(); creation.reset();
			auto payload = builder.begin_append_chunk("artifact-payload"); drive(payload); payload.reset();
			auto seal = builder.begin_finish(); drive(seal); return seal->prepared_result();
		};
		std::size_t count = 0;
		{
			auto model = baseline.clone(); auto io = std::make_shared<MemoryIO>(model); Journal journal(io, 1024);
			journal.recover([](auto, auto) {}); model.operations = 0;
			auto prepared = prepare_owned(journal, true); count = model.operations;
			check(prepared.descriptor().length == 16 && prepared.descriptor().checksum == crc32c("artifact-payload"), "completion artifact settles exact descriptor after seal barriers");
			journal.verify_artifact(prepared);
		}
		for (std::size_t fault = 1; fault <= count; ++fault) {
			for (bool after : {false, true}) {
				std::array<Model, 2> outcomes{baseline.clone(), baseline.clone()};
				for (unsigned driver = 0; driver < 2; ++driver) {
					auto& model = outcomes[driver];
					auto io = std::make_shared<MemoryIO>(model); Journal journal(io, 1024); journal.recover([](auto, auto) {});
					model.operations = 0; model.fail_operation = fault; model.fail_after = after;
					check(throws([&] { prepare_owned(journal, driver == 1); }) && journal.fenced(), "both artifact drivers fence each uncertain primitive outcome");
					check(model.operations == fault, "artifact failure never executes a later primitive");
				}
				auto footprints = [](const Model& model) {
					std::vector<std::pair<std::size_t, std::size_t>> sizes;
					for (const auto& [name, inode] : model.visible) { sizes.emplace_back(inode->visible.size(), inode->durable.size()); }
					std::sort(sizes.begin(), sizes.end()); return sizes;
				};
				check(footprints(outcomes[0]) == footprints(outcomes[1]), "both artifact drivers leave identical visible/durable file footprints under the same fault");
				for (bool visible : {false, true}) {
					for (auto& outcome : outcomes) {
						auto crash = outcome.clone(); crash.power_loss(visible);
						check(replay(crash) == std::vector<std::string>{"first"}, "artifact faults preserve acknowledged history under both namespace outcomes");
					}
				}
			}
		}
	}
}
void suspended_artifact_ownership() {
	auto model = initialized(); auto io = std::make_shared<MemoryIO>(model);
	auto journal = std::make_unique<Journal>(io, 1024); journal->recover([](auto, auto) {});
	{
		auto canceled = journal->begin_artifact_preparation();
		check(throws([&] { journal->prepare_artifact(); }), "unsubmitted creation owns the bounded preparation slot");
	}
	check(!journal->fenced(), "unsubmitted artifact cancellation performs no IO and leaves owner healthy");
	auto creation = journal->begin_artifact_preparation(); creation->submitted();
	auto first = detail::execute_primitive(creation->io(), creation->request());
	auto before = model.operations;
	check(throws([&] { journal->append_batch("overlap"); }) && before == model.operations, "pending artifact primitive rejects overlapping mutations before IO");
	journal.reset(); io.reset();
	check(model.locked && creation->complete(std::move(first)), "creation keeps backend and stable lock after facade retirement");
	drive_synchronously(*creation); auto builder = creation->take_builder();
	check(throws([&] { creation->take_builder(); }), "creation exposes exactly one builder facade"); creation.reset();
	{
		auto canceled = builder.begin_append_chunk("discarded");
	}
	auto payload = builder.begin_append_chunk("retained");
	{
		auto old = payload;
		drive_synchronously(*payload); payload->result();
		auto seal = builder.begin_finish(); payload.reset(); old.reset();
		check(throws([&] { builder.begin_append_chunk("overlap"); }), "retiring completed payload cannot release the sealing gate");
		while (!seal->done()) {
			seal->submitted(); auto completion = detail::execute_primitive(seal->io(), seal->request());
			check(throws([&] { seal->prepared_result(); }), "seal capability remains hidden until original directory barrier is delivered");
			check(seal->complete(std::move(completion)), "sealing retains matching completion ownership");
		}
		auto prepared = seal->prepared_result();
		check(prepared.descriptor().length == 8 && prepared.descriptor().checksum == crc32c("retained"), "unsubmitted payload cancellation leaves checksum and length unchanged");
	}
}

void suspended_append_ownership() {
	auto model = initialized(3);
	std::weak_ptr<MemoryIO> backend;
	{
	auto io = std::make_shared<MemoryIO>(model);
	backend = io;
	auto journal = std::make_unique<Journal>(io, 1024);
	journal->recover([](auto, auto) {});
	auto before = model.operations;
	check(throws([&] { journal->begin_append(std::string(1025, 'x')); }) && model.operations == before && !journal->fenced(), "owned append validates before IO");
	{
		auto canceled = journal->begin_append("canceled");
		check(model.operations == before, "begin append performs no IO");
	}
	check(!journal->fenced(), "unsubmitted cancellation keeps journal healthy");
	auto builder = journal->prepare_artifact();
	auto operation = journal->begin_append("second"); before = model.operations;
	check(throws([&] { journal->append_batch("other"); }) && throws([&] { journal->reclaim_step(); }) && throws([&] { builder.append_chunk("other"); }) && throws([&] { builder.finish(); }) && model.operations == before && !journal->fenced(), "pending append excludes mutations before IO without fencing");
	operation->submitted();
	auto completion = detail::execute_primitive(operation->io(), operation->request());
	journal.reset(); io.reset();
	check(model.locked && !backend.expired() && operation->in_flight(), "accepted operation retains backend file and stable lock after facade closes");
	check(operation->complete(std::move(completion)), "late completion returns to retained operation");
	drive_synchronously(*operation);
	check(model.locked && !backend.expired(), "completed operation retains storage lifetime until released");
	operation.reset();
	// The escaped builder continues to own the storage session until it closes.
	check(model.locked && !backend.expired(), "escaped preparation retains owned backend lifetime");
	}
	check(!model.locked && backend.expired(), "drained operation and final pin release backend and owner lock");
	check(replay(model) == std::vector<std::string>({"first", "second"}), "owner disappearance does not interrupt accepted durable sequencing");
}

void suspended_append_retirement() {
	auto model = initialized(); auto io = std::make_shared<MemoryIO>(model); Journal journal(io, 1024);
	journal.recover([](auto, auto) {});
	auto first = journal.begin_append("second"); drive_synchronously(*first);
	check(journal.frontier().sequence == 1 && journal.finish_append(first).sequence == 2, "only explicit terminal settlement advances public frontier");
	auto second = journal.begin_append("third"); first.reset(); auto before = model.operations;
	check(throws([&] { journal.begin_append("overlap"); }) && model.operations == before && !journal.fenced(), "retiring old settled operation cannot release a newer mutation gate");
	drive_synchronously(*second); journal.finish_append(second); second.reset();
	{
		auto dropped = journal.begin_append("fourth"); drive_synchronously(*dropped);
	}
	check(journal.fenced(), "discarding durable but unsettled mutation fences stale facade metadata");

	auto abandoned = initialized(); auto backend = std::make_shared<MemoryIO>(abandoned); Journal owned(backend, 1024); owned.recover([](auto, auto) {});
	{
		auto dropped = owned.begin_append("uncertain"); dropped->submitted();
		dropped->complete(detail::execute_primitive(dropped->io(), dropped->request()));
	}
	check(owned.fenced(), "abandonment after a reaped mutating primitive fences without hidden IO");
}

void recovery_failures(bool extra_tail = false, bool checkpointed = false) {
	auto baseline = initialized();
	if (checkpointed) {
		MemoryIO io(baseline); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto bundle = prepare(journal, "first"); auto application = prepare(journal, "application");
		journal.publish_checkpoint(bundle, std::array{application}, 1);
	}
	auto recover_values = [&](Model& model) { return checkpointed ? replay_checkpoint(model).second : replay(model); };
	if (extra_tail) {
		auto name = std::string(data_name);
		if (checkpointed) {
			auto selected = replay_checkpoint(baseline).first;
			name = "generation-" + detail::hexadecimal(selected.journal_identity);
		}
		auto& inode = baseline.visible.at(name);
		inode->visible += "extra unacknowledged bytes";
		inode->durable = inode->visible;
	}
	std::size_t operation_count;
	{
		auto model = baseline.clone(); recover_values(model); operation_count = model.operations;
	}
	for (std::size_t operation = 1; operation <= operation_count; ++operation) {
		for (bool after : {false, true}) {
			auto model = baseline.clone(); model.fail_operation = operation; model.fail_after = after;
			{
				MemoryIO io(model); Journal journal(io, 1024);
				check(throws([&] { journal.recover([](auto, auto) {}, [](auto&, auto& checkpoint, auto dependencies) { read_artifact(checkpoint); for (auto& dependency : dependencies) { read_artifact(dependency); } }); }) && journal.fenced(), "recovery I/O failure fences");
				check(throws([&] { journal.append_batch("after failed recovery"); }), "failed recovery remains unavailable");
			}
			model.fail_operation = 0;
			check(recover_values(model) == std::vector<std::string>{"first"}, "reopen after recovery failure preserves acknowledged data");
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
		auto bundle = prepare(journal, "prefix image"); std::array dependencies{prepare(journal, "app")};
		journal.publish_checkpoint(bundle, dependencies, 2); journal.append_batch("tail");
	}
	{
		PosixIO io(directory); Journal journal(io, 1024); std::string image, dependency, tail;
		auto frontier = journal.recover([&](auto sequence, std::string_view batch) { check(sequence == 3, "POSIX checkpoint suffix sequence continues"); tail = batch; },
			[&](const Frontier&, ArtifactReader& bundle, std::span<ArtifactReader> dependencies) { image = read_artifact(bundle); dependency = read_artifact(dependencies[0]); });
		check(frontier.base_sequence == 2 && image == "prefix image" && dependency == "app" && tail == "tail", "POSIX checkpoint generation restores declared artifact and suffix");
		std::vector<std::string> preserved;
		auto reserved_name = [&] { auto name = "artifact-" + detail::hexadecimal(detail::random_identity()); preserved.push_back(name); return name; };
		auto symlink = reserved_name(); std::filesystem::create_symlink("manifest", directory / symlink);
		auto partial = reserved_name(); auto partial_file = io.create_exclusive(partial); detail::write_all(*partial_file, 0, std::string(1024, '\0')); partial_file->sync(); partial_file.reset();
		auto hardlink = reserved_name(); std::filesystem::create_hard_link(directory / partial, directory / hardlink);
		auto fifo = reserved_name(); check(::mkfifo((directory / fifo).c_str(), 0600) == 0, "create reserved FIFO candidate");
		std::filesystem::create_directory(directory / reserved_name());
		ArtifactDescriptor foreign{detail::random_identity(), 0, crc32c("")}; auto foreign_store = identity(); foreign_store[0] = 'Z';
		preserved.push_back(artifact_name(foreign)); auto foreign_file = io.create_exclusive(artifact_name(foreign)); detail::write_all(*foreign_file, 0, detail::artifact_header(foreign_store, foreign)); foreign_file->sync(); foreign_file.reset();
		ArtifactDescriptor corrupt{detail::random_identity(), 0, crc32c("")}; auto corrupt_header = detail::artifact_header(identity(), corrupt); corrupt_header[40] ^= 1;
		preserved.push_back(artifact_name(corrupt)); auto corrupt_file = io.create_exclusive(artifact_name(corrupt)); detail::write_all(*corrupt_file, 0, corrupt_header); corrupt_file->sync(); corrupt_file.reset();
		io.sync_directory();
		auto stats = journal.reclaim_step(4096);
		check(stats.complete && stats.removed == 1 && !std::filesystem::exists(directory / std::string(data_name)), "POSIX reclaimer removes obsolete owned generation");
		for (const auto& name : preserved) { check(std::filesystem::symlink_status(directory / name).type() != std::filesystem::file_type::not_found, "POSIX reclaimer preserves foreign unknown and unsafe candidates"); }
		check(stats.unknown_files == preserved.size(), "POSIX skipped ownership candidates are reported");
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
	try { basics(); append_failures(); append_failures(3); append_failures(std::numeric_limits<std::size_t>::max(), true); append_failures(3, true); append_failures(std::numeric_limits<std::size_t>::max(), false, true); append_failures(3, false, true); append_failures(std::numeric_limits<std::size_t>::max(), true, true); append_failures(3, true, true); suspended_artifact_operations(); suspended_artifact_ownership(); suspended_append_ownership(); suspended_append_retirement(); recovery_failures(); recovery_failures(true); recovery_failures(false, true); recovery_failures(true, true); corruption(); initialization_failures(); checkpoint_basics(); checkpoint_successive_generations(); incremental_artifact_verification(); store_incremental_verification(); published_artifact_selection(); store_published_selection(); artifact_verification(); preparation_ownership_and_failures(); checkpoint_publication_failures(); checkpoint_corruption(); reclamation_roots_and_unknowns(); reclamation_failures(); durable_staging_ownership(); mixed_artifact_formats(); footprint_plans(); store_artifact_completions(); detached_old_artifact_lease(); store_append_completions(); store_basics(); store_failures(); store_append_and_cleanup_failures(); store_limits_and_pins(); store_posix(); posix(); }
	catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " journal checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
