#include "journal/bsd_completion.h"
#include "journal/journal.h"
#include "journal/store.h"
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
OwnedCompletion await_step(BsdCompletionQueue &queue) {
	auto deadline = Clock::now() + std::chrono::seconds(5);
	while (Clock::now() < deadline) {
		if (auto result = queue.poll()) {
			return std::move(*result);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1)); // Test driver only.
	}
	throw std::runtime_error("completion test deadline");
}
void drive(BsdCompletionQueue &queue, const std::shared_ptr<IOOperation> &operation) {
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
void actual_backend(const std::filesystem::path &directory) {
	auto io = std::make_shared<PosixIO>(directory);
	Journal journal(io, 1024 * 1024);
	Identity id{};
	id[0] = 'c';
	journal.create(id);
	BsdCompletionQueue queue;
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
		  "actual journal and manifest writes use reaped native AIO");
	check(stats.fallback_completed == stats.fallback_submitted && stats.fallback_submitted >= 3,
		  "namespace and unsupported barriers use the bounded fallback slot");
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
	BsdCompletionQueue queue;
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
		Identity id{}; id[0] = 'a'; journal.create(id); BsdCompletionQueue queue;
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
		auto stats = queue.stats(); check(stats.native_completed >= 3 && stats.fallback_completed >= 4, "artifact creation/payload/seal use native writes and explicit flush/namespace fallback");
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
		auto replacement = store.reserve_replacement(60000, 16); BsdCompletionQueue queue;
		for (auto part : {ArtifactPart::Application, ArtifactPart::Bundle}) {
			auto creation = store.begin_artifact_operation(*replacement, part); drive(queue, creation); creation->result(); creation.reset();
			std::string bytes = part == ArtifactPart::Application ? std::string(60000, 'q') : std::string("bundle");
			auto payload = store.begin_write_chunk(*replacement, bytes); drive(queue, payload); payload->result(); payload.reset();
			auto seal = store.begin_finish_artifact(*replacement); drive(queue, seal);
			check(seal->descriptor().length == bytes.size(), "native admitted artifact quantum publishes exact sealed phase");
		}
		check(store.accounting()->tickets == 1 && !store.accounting()->tainted, "native preparation keeps one whole-cycle reservation through both artifacts");
		store.publish(*replacement, 0);
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
void admitted_store(const std::filesystem::path& parent) {
	auto directory = parent / "store"; std::filesystem::create_directory(directory); ::chmod(directory.c_str(), 0700);
	auto io = std::make_shared<PosixIO>(directory);
	AdmissionLimits limits{{16u << 20, 256}, {1u << 20, 4}, {4u << 20, 4}, 64, 3};
	auto store = std::make_unique<Store>(io, limits, 1024); Identity id{}; id[0] = 's'; store->create(id);
	while (!store->inventory_step(1).complete) {}
	auto before = store->accounting()->used;
	auto reservation = store->reserve_append(AdmissionClass::Normal, 6);
	auto operation = store->begin_append(std::move(*reservation), "stored");
	BsdCompletionQueue queue;
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
		std::filesystem::create_directories(directory.parent_path());
		if (!std::filesystem::create_directory(directory)) {
			throw std::runtime_error("completion test directory already exists");
		}
		::chmod(directory.c_str(), 0700);
		actual_backend(directory);
		owner_retirement(directory);
		admitted_store(directory);
		actual_artifacts(directory);
		admitted_artifacts(directory);
	} catch (const std::exception &error) {
		check(false, error.what());
	}
	std::filesystem::remove_all(directory);
	std::cout << checks << " completion checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
