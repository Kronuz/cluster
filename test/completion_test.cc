#include "journal/native_completion.h"
#include "journal/journal.h"
#include "journal/store.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <vector>

using namespace kronuz::journal;
namespace {
int failures = 0, checks = 0;
void check(bool value, std::string_view description) {
	++checks;
	if (!value) {
		++failures;
		std::cerr << "FAIL: " << description << '\n';
	}
}
template <class F> bool throws(F function) {
	try {
		function();
		return false;
	} catch (const std::exception &) {
		return true;
	}
}
using Clock = std::chrono::steady_clock;
OwnedCompletion await_step(NativeCompletionQueue &queue) {
	auto deadline = Clock::now() + std::chrono::seconds(5);
	while (Clock::now() < deadline) {
		if (auto result = queue.poll()) {
			return std::move(*result);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1)); // Test driver only.
	}
	throw std::runtime_error("completion test deadline");
}
void drive(NativeCompletionQueue &queue, const std::shared_ptr<IOOperation> &operation) {
	while (!operation->done()) {
		check(queue.submit(operation), "free reserved slot accepts primitive");
		check(!queue.submit(operation), "busy backend rejects without a second submission");
		auto result = await_step(queue);
		check(result.operation == operation && result.operation->in_flight(),
			  "backend retains original operation until owner reaps completion");
		check(result.operation->complete(std::move(result.completion)),
			  "original token advances shared sequencing on owner");
	}
}
void actual_reclamation(const std::filesystem::path& parent) {
	for (bool retired : {false, true}) {
		auto directory = parent / (retired ? "gc-retired" : "gc"); std::filesystem::create_directory(directory); ::chmod(directory.c_str(), 0700);
		auto io = std::make_shared<PosixIO>(directory); std::weak_ptr<PosixIO> backend = io;
		AdmissionLimits limits{{16u << 20, 256}, {1u << 20, 4}, {4u << 20, 4}, 64, 3};
		auto store = std::make_unique<Store>(io, limits, 1024); Identity id{}; id[0] = 'g'; store->create(id); while (!store->inventory_step(1).complete) {}
		auto replacement = store->reserve_replacement(128, 128); store->begin_artifact(*replacement, ArtifactPart::Application); store->write_chunk(*replacement, "orphan"); auto artifact = store->finish_artifact(*replacement); store->cancel_replacement(*replacement);
		auto before = store->accounting()->used; auto job = store->begin_reclaim(4096); NativeCompletionQueue queue;
		while (!job->done()) {
			queue.submit(job); auto completed = await_step(queue);
			if (store) { check(store->accounting()->used == before && throws([&] { job->result(); }), "physical native reclamation never credits quota before terminal owner settlement"); }
			if (retired && store) { store.reset(); io.reset(); check(!backend.expired(), "outstanding GC retains backend and accounting after facade retirement"); }
			check(job->complete(std::move(completed.completion)), "original GC completion advances on owner");
		}
		check(job->result().removed == 1 && job->result().logical_bytes == 74 && !std::filesystem::exists(directory / artifact_name(artifact)), "native GC removes exact sealed orphan after directory barrier");
		if (store) { check(store->accounting()->used.logical_bytes + 74 == before.logical_bytes && store->accounting()->used.entries + 1 == before.entries && !store->accounting()->tainted, "terminal GC credits exact bytes and entry once"); }
		check(queue.stats().native_completed >= 1 && queue.stats().file_read_bytes == 56, "native GC reads only bounded ownership metadata");
		job.reset(); store.reset(); io.reset(); check(backend.expired(), "completed GC capability releases retired owner lock and backend");
	}
}

void actual_reads(const std::filesystem::path& parent) {
	auto directory = parent / "reads"; std::filesystem::create_directory(directory); ::chmod(directory.c_str(), 0700);
	auto io = std::make_shared<PosixIO>(directory); std::weak_ptr<PosixIO> backend = io;
	auto journal = std::make_unique<Journal>(io, 1024); Identity id{}; id[0] = 'r'; journal->create(id);
	std::optional<PreparedArtifact> artifact; std::string payload(150000, 'r');
	{ auto builder = journal->prepare_artifact();
	for (std::size_t offset = 0; offset < payload.size();) { auto count = std::min(std::size_t{65536}, payload.size() - offset); builder.append_chunk(std::string_view(payload).substr(offset, count)); offset += count; }
	artifact.emplace(builder.finish()); }
	NativeCompletionQueue queue; auto opening = *journal->begin_artifact_open(*artifact); drive(queue, opening);
	std::optional<ArtifactVerifier> verifier(opening->take_verifier()); opening.reset(); auto metadata = queue.stats(); std::string candidate;
	check(metadata.native_completed >= 2 && metadata.file_read_bytes == 8 + detail::artifact_header_size && metadata.fallback_completed == 2, "actual metadata opening uses native bounded header reads and reserved open/size fallback");
	while (verifier->offset() < payload.size()) {
		auto operation = verifier->begin_read_next(65536); auto offset = verifier->offset(); drive(queue, operation);
		check(verifier->offset() == offset, "native read reaping cannot skip explicit checksum settlement");
		auto bytes = operation->result(); candidate.append(bytes.data(), bytes.size()); verifier->finish_read_next(operation);
	}
	auto stats = queue.stats(); check(stats.native_completed - metadata.native_completed >= 3 && stats.native_completed == stats.native_submitted && stats.file_read_bytes - metadata.file_read_bytes == payload.size() && stats.fallback_submitted == metadata.fallback_submitted, "actual payload verification uses exact reaped native read bytes without fallback");
	check(candidate == payload, "native verification returns exact multi-chunk payload");
	std::optional<ArtifactReader> reader(std::move(*verifier).finish()); verifier.reset(); artifact.reset();
	auto operation = reader->begin_read_at(0, 65536); queue.submit(operation); reader.reset(); journal.reset(); io.reset();
	check(!backend.expired(), "accepted read retains backend, exact file and pin after all facades disappear");
	{
		PosixIO other(directory); Journal contender(other); check(throws([&] { contender.recover([](auto, auto) {}); }), "outstanding read retains original stable owner lock");
	}
	auto completed = await_step(queue); check(operation->complete(std::move(completed.completion)), "retired reader still reaps original native completion");
	auto bytes = operation->result(); check(bytes.size() == 65536 && std::all_of(bytes.begin(), bytes.end(), [](char value) { return value == 'r'; }), "owned read buffer survives interest cancellation and facade retirement");
	operation.reset(); completed.operation.reset(); check(backend.expired(), "terminal read capability releases backend and owner lock together");
}

void actual_backend(const std::filesystem::path &directory) {
	auto io = std::make_shared<PosixIO>(directory);
	Journal journal(io, 1024 * 1024);
	Identity id{};
	id[0] = 'c';
	journal.create(id);
	NativeCompletionQueue queue;
	auto operation = journal.begin_append(std::string(150000, 'a'));
	std::uint64_t steps = 0;
	while (!operation->done()) {
		auto request = operation->request();
		if (request.kind == PrimitiveKind::Write) {
			check(request.bytes.size() <= 65536, "native writes retain bounded chunks");
		}
		check(queue.submit(operation), "native or explicit fallback admission succeeds");
		check(journal.frontier().sequence == 0, "submission cannot acknowledge append");
		auto result = await_step(queue);
		check(journal.frontier().sequence == 0,
			  "reaped backend result cannot publish frontier before owner settlement");
		check(result.operation->complete(std::move(result.completion)),
			  "native completion token advances operation");
		++steps;
	}
	check(journal.finish_append(operation).sequence == 1, "terminal barrier permits acknowledged frontier");
	auto stats = queue.stats();
	check(stats.native_submitted >= 5 && stats.native_completed == stats.native_submitted,
		  "actual journal and manifest writes use reaped native IO");
	check(stats.fallback_completed == stats.fallback_submitted && stats.fallback_submitted >= 1,
		  "validated composite operations use the bounded fallback slot");
	check(stats.file_write_bytes == 150000 + 24 + 52, "backend reports exact completed file write bytes");
	check(!queue.busy() && !queue.poll(), "drained queue has no retained result");
	std::cout << "native=" << stats.native_completed << " fallback=" << stats.fallback_completed
			  << " steps=" << steps << '\n';
}
void owner_retirement(const std::filesystem::path &directory) {
	auto io = std::make_shared<PosixIO>(directory);
	std::weak_ptr<PosixIO> backend = io;
	auto journal = std::make_unique<Journal>(io, 1024 * 1024);
	journal->recover([](auto, auto) {});
	auto operation = journal->begin_append("second");
	std::weak_ptr<AppendMutation> lifetime = operation;
	NativeCompletionQueue queue;
	check(queue.submit(operation), "backend accepts operation before caller interest disappears");
	operation.reset();
	journal.reset();
	io.reset();
	check(!lifetime.expired() && !backend.expired(),
		  "accepted backend work retains all lifetimes after caller retirement");
	{
		PosixIO other(directory);
		Journal contender(other);
		check(throws([&] { contender.recover([](auto, auto) {}); }),
			  "pending native request retains stable owner lock");
	}
	{
		auto result = await_step(queue);
		auto draining = result.operation;
		check(draining->complete(std::move(result.completion)),
			  "retired caller cannot suppress original completion");
		drive(queue, draining);
		std::dynamic_pointer_cast<AppendMutation>(draining)->result();
	}
	check(lifetime.expired() && backend.expired(),
		  "terminal drain releases operation backend and lock together");
	PosixIO recovered(directory);
	Journal reopened(recovered, 1024 * 1024);
	std::vector<std::string> batches;
	auto frontier = reopened.recover([&](auto, auto bytes) { batches.emplace_back(bytes); });
	check(frontier.sequence == 2 && batches.size() == 2 && batches[0].size() == 150000 &&
			  batches[1] == "second",
		  "retired owner still finishes original durable transaction");
}

void actual_artifacts(const std::filesystem::path& parent) {
	auto directory = parent / "artifacts"; std::filesystem::create_directory(directory); ::chmod(directory.c_str(), 0700);
	ArtifactDescriptor descriptor;
	{
		auto io = std::make_shared<PosixIO>(directory); Journal journal(io, 1024);
		Identity id{}; id[0] = 'a'; journal.create(id); NativeCompletionQueue queue;
		auto creation = journal.begin_artifact_preparation(); drive(queue, creation);
		auto builder = creation->take_builder(); creation.reset();
		auto payload = builder.begin_append_chunk(std::string(60000, 'z')); drive(queue, payload); payload->result(); payload.reset();
		auto seal = builder.begin_finish();
		while (!seal->done()) {
			check(queue.submit(seal), "artifact seal reserves one primitive"); auto completed = await_step(queue);
			check(throws([&] { seal->prepared_result(); }), "native seal cannot expose capability before original barrier completion");
			check(seal->complete(std::move(completed.completion)), "native seal applies matching completion");
		}
		auto prepared = seal->prepared_result(); descriptor = prepared.descriptor();
		check(descriptor.length == 60000 && descriptor.checksum == crc32c(std::string(60000, 'z')), "native artifact preserves exact payload checksum and descriptor");
		journal.publish_checkpoint(prepared, {}, 0);
		auto stats = queue.stats(); check(stats.native_completed >= 3 && stats.fallback_completed >= 1, "artifact creation/payload/seal use native writes and explicit flush/namespace fallback");
	}
	{
		PosixIO io(directory); Journal journal(io, 1024); bool restored = false;
		journal.recover([](auto, auto) {}, [&](const Frontier& frontier, ArtifactReader& reader, auto) {
			std::string bytes(60000, '\0'); std::size_t consumed = 0;
			while (consumed < bytes.size()) { consumed += reader.read_at(consumed, std::span<char>(bytes).subspan(consumed)); }
			restored = frontier.checkpoint == descriptor && bytes == std::string(60000, 'z');
		});
		check(restored, "completion-created artifact survives real checkpoint publication and recovery");
	}
}
void admitted_artifacts(const std::filesystem::path& parent) {
	auto directory = parent / "admitted-artifacts"; std::filesystem::create_directory(directory); ::chmod(directory.c_str(), 0700);
	{
		auto io = std::make_shared<PosixIO>(directory); AdmissionLimits limits{{16u << 20, 256}, {1u << 20, 4}, {4u << 20, 4}, 64, 3};
		Store store(io, limits, 1024); Identity id{}; id[0] = 'A'; store.create(id); store.inventory_step(128);
		auto replacement = store.reserve_replacement(60000, 16); NativeCompletionQueue queue;
		for (auto part : {ArtifactPart::Application, ArtifactPart::Bundle}) {
			auto creation = store.begin_artifact_operation(*replacement, part); drive(queue, creation); creation->result(); creation.reset();
			std::string bytes = part == ArtifactPart::Application ? std::string(60000, 'q') : std::string("bundle");
			auto payload = store.begin_write_chunk(*replacement, bytes); drive(queue, payload); payload->result(); payload.reset();
			auto seal = store.begin_finish_artifact(*replacement); drive(queue, seal);
			check(seal->descriptor().length == bytes.size(), "native admitted artifact quantum publishes exact sealed phase");
		}
		check(store.accounting()->tickets == 1 && !store.accounting()->tainted, "native preparation keeps one whole-cycle reservation through both artifacts");
		auto publication = store.begin_publication(*replacement, 0); drive(queue, publication);
		check(publication->result().generation == 2, "native publication owns the complete generation and manifest protocol");
		check(store.accounting()->tickets == 0 && store.accounting()->outstanding == StorageResources{}, "publication settles the admitted completion-prepared replacement");
	}
	PosixIO io(directory); Journal journal(io, 1024); bool restored = false;
	journal.recover([](auto, auto) {}, [&](const Frontier&, ArtifactReader&, std::span<ArtifactReader> dependencies) {
		std::string bytes(60000, '\0'); std::size_t count = 0;
		while (count < bytes.size()) { count += dependencies[0].read_at(count, std::span<char>(bytes).subspan(count)); }
		restored = bytes == std::string(60000, 'q');
	});
	check(restored, "native admitted preparation recovers exact application after publication");
}
void retired_publication(const std::filesystem::path& parent) {
	auto directory = parent / "retired-publication"; std::filesystem::create_directory(directory); ::chmod(directory.c_str(), 0700);
	auto io = std::make_shared<PosixIO>(directory); AdmissionLimits limits{{16u << 20, 256}, {1u << 20, 4}, {4u << 20, 4}, 64, 3};
	auto store = std::make_unique<Store>(io, limits, 1024); Identity id{}; id[0] = 'P'; store->create(id); store->inventory_step(128);
	auto replacement = store->reserve_replacement(128, 128);
	for (auto part : {ArtifactPart::Application, ArtifactPart::Bundle}) { store->begin_artifact(*replacement, part); store->write_chunk(*replacement, part == ArtifactPart::Application ? "application" : "first"); store->finish_artifact(*replacement); }
	NativeCompletionQueue queue; auto publication = store->begin_publication(*replacement, 0);
	check(queue.submit(publication), "native publication accepted before facade retirement"); store.reset(); io.reset();
	{ auto original = await_step(queue); check(publication->complete(std::move(original.completion)), "original native generation creation reaped after facade retirement"); }
	drive(queue, publication); check(publication->result().generation == 2, "native detached publication finishes full protocol"); publication.reset();
	PosixIO recovered(directory); Journal journal(recovered, 1024); bool restored = false;
	journal.recover([](auto, auto) {}, [&](const Frontier& frontier, ArtifactReader& reader, std::span<ArtifactReader> dependencies) {
		std::array<char, 5> bytes{}; reader.read_at(0, bytes); restored = frontier.generation == 2 && std::string_view(bytes.data(), bytes.size()) == "first" && dependencies.size() == 1;
	});
	check(restored, "native detached publication recovers full selected checkpoint instead of staged orphans");
}
void admitted_store(const std::filesystem::path& parent) {
	auto directory = parent / "store"; std::filesystem::create_directory(directory); ::chmod(directory.c_str(), 0700);
	auto io = std::make_shared<PosixIO>(directory);
	AdmissionLimits limits{{16u << 20, 256}, {1u << 20, 4}, {4u << 20, 4}, 64, 3};
	auto store = std::make_unique<Store>(io, limits, 1024); Identity id{}; id[0] = 's'; store->create(id);
	while (!store->inventory_step(1).complete) {}
	auto before = store->accounting()->used;
	auto reservation = store->reserve_append(AdmissionClass::Normal, 6);
	auto operation = store->begin_append(std::move(*reservation), "stored");
	NativeCompletionQueue queue;
	while (!operation->done()) {
		queue.submit(operation); auto completed = await_step(queue);
		check(store->frontier().sequence == 0 && store->accounting()->used == before && store->accounting()->tickets == 1, "real backend cannot settle Store before owner applies completion");
		operation->complete(std::move(completed.completion));
	}
	check(operation->result().sequence == 1 && store->accounting()->outstanding == StorageResources{} && !store->accounting()->tainted, "real completion atomically publishes Store and settles admitted resources");
	reservation = store->reserve_append(AdmissionClass::Normal, 7);
	operation = store->begin_append(std::move(*reservation), "retired");
	queue.submit(operation); auto draining = operation; operation.reset(); store.reset(); io.reset();
	auto completed = await_step(queue); draining->complete(std::move(completed.completion));
	drive(queue, draining); check(draining->result().sequence == 2, "owned Store operation survives facade and accounting facade retirement");
	draining.reset(); completed.operation.reset();
	PosixIO recovered(directory); Journal journal(recovered, 1024); std::vector<std::string> batches;
	journal.recover([&](auto, auto bytes) { batches.emplace_back(bytes); });
	check(batches == std::vector<std::string>({"stored", "retired"}), "Store retirement preserves both terminal durable appends");
}
} // namespace
int main() {
	auto directory =
		std::filesystem::current_path() / ".scratch" / ("completion-test-" + std::to_string(::getpid()));
	try {
#ifdef __linux__
		LinuxCompletionQueue qualification(LinuxCompletionPolicy::RequireNative);
		check(qualification.native_available(), "qualification requires real io_uring");
#endif
		std::filesystem::create_directories(directory.parent_path());
		if (!std::filesystem::create_directory(directory)) {
			throw std::runtime_error("completion test directory already exists");
		}
		::chmod(directory.c_str(), 0700);
		actual_reclamation(directory);
		actual_reads(directory);
		actual_backend(directory);
		owner_retirement(directory);
		admitted_store(directory);
		actual_artifacts(directory);
		admitted_artifacts(directory);
		retired_publication(directory);
	} catch (const std::exception &error) {
		check(false, error.what());
	}
	std::filesystem::remove_all(directory);
	std::cout << checks << " completion checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
