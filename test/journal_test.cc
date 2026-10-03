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
	std::unique_ptr<DirectoryCursor> scan_directory() override {
		model_.before(); auto cursor = std::make_unique<Cursor>(model_); model_.after(); return cursor;
	}
	std::unique_ptr<File> open_reclaim_candidate(std::string_view name) override {
		model_.before(); auto found = model_.visible.find(std::string(name));
		auto file = found == model_.visible.end() ? nullptr : std::make_unique<MemoryFile>(model_, found->second);
		model_.after(); return file;
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

	auto baseline = initialized(); std::size_t operations;
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
		auto builder = journal.prepare_artifact(); builder.append_chunk("active");
		// Only one new name is active, distinct from the sealed prepared pin.
		for (const auto& [name, inode] : model.visible) { if (name.starts_with("artifact-") && inode->visible.starts_with(std::string(56, '\0'))) { active_name = name; } }
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

void append_failures(std::size_t chunk = std::numeric_limits<std::size_t>::max(), bool checkpointed = false) {
	auto baseline = initialized(chunk);
	if (checkpointed) {
		MemoryIO io(baseline); Journal journal(io, 1024); journal.recover([](auto, auto) {});
		auto bundle = prepare(journal, "first"); std::array dependencies{prepare(journal, "application")};
		journal.publish_checkpoint(bundle, dependencies, 1);
	}
	auto recover_values = [&](Model& model) { return checkpointed ? replay_checkpoint(model).second : replay(model); };
	std::size_t operation_count = 0;
	{
		auto model = baseline.clone(); MemoryIO io(model); Journal journal(io, 1024);
		journal.recover([](auto, auto) {}, [](auto&, auto&, auto) {}); model.operations = 0;
		journal.append_batch("second"); operation_count = model.operations;
	}
	for (std::size_t operation = 1; operation <= operation_count; ++operation) {
		for (bool after : {false, true}) {
			auto model = baseline.clone();
			{
				MemoryIO io(model); Journal journal(io, 1024); journal.recover([](auto, auto) {}, [](auto&, auto&, auto) {});
				model.operations = 0; model.fail_operation = operation; model.fail_after = after;
				check(throws([&] { journal.append_batch("second"); }) && journal.fenced(), "each uncertain append operation fences");
				check(throws([&] { journal.append_batch("third"); }), "failure prevents subsequent acknowledgement");
			}
			for (bool keep_visible : {false, true}) {
				auto crash = model.clone(); crash.power_loss(keep_visible);
				auto batches = recover_values(crash);
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
		ArtifactDescriptor corrupt{detail::random_identity(), 0, crc32c("")}; auto corrupt_header = detail::artifact_header(identity(), corrupt); corrupt_header.back() ^= 1;
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
	try { basics(); append_failures(); append_failures(3); append_failures(std::numeric_limits<std::size_t>::max(), true); append_failures(3, true); recovery_failures(); recovery_failures(true); recovery_failures(false, true); recovery_failures(true, true); corruption(); initialization_failures(); checkpoint_basics(); checkpoint_successive_generations(); artifact_verification(); preparation_ownership_and_failures(); checkpoint_publication_failures(); checkpoint_corruption(); reclamation_roots_and_unknowns(); reclamation_failures(); posix(); }
	catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " journal checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
