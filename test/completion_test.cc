#include "journal/bsd_completion.h"
#include "journal/journal.h"
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
	} catch (const std::exception &error) {
		check(false, error.what());
	}
	std::filesystem::remove_all(directory);
	std::cout << checks << " completion checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
