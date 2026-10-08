#include "completion/api.h"
#include "completion/native_completion.h"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

namespace c = kronuz::io::completion;
namespace {
void check(bool value) {
	if (!value)
		throw std::runtime_error("inspection/lease invariant failed");
}
template <class Action> void rejects(Action action) {
	bool rejected = false;
	try {
		action();
	} catch (const std::exception &) {
		rejected = true;
	}
	check(rejected);
}
class LegacyFile final : public c::File {
  public:
	std::uint64_t size() override { return 0; }
	std::size_t read_at(std::uint64_t, std::span<char>) override { return 0; }
	std::size_t write_at(std::uint64_t, std::string_view) override { return 0; }
	void truncate(std::uint64_t) override {}
	void sync() override {}
};
class Original final : public c::Operation {
  public:
	Original(std::shared_ptr<c::IO> io, c::Kind kind, std::shared_ptr<c::File> file = {},
			 std::string_view name = {})
		: io_(std::move(io)) {
		request_.kind = kind;
		request_.file = std::move(file);
		request_.source = name;
		request_.token.operation = 1;
	}
	const c::Request &request() const override { return request_; }
	void submitted() override {
		if (submitted_)
			throw std::logic_error("duplicate original");
		submitted_ = true;
	}
	bool complete(c::Result result) noexcept override {
		if (!submitted_ || done_ || result.token != request_.token)
			return false;
		result_ = std::move(result);
		done_ = true;
		return true;
	}
	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return submitted_ && !done_; }
	c::IO &io() const noexcept override { return *io_; }
	c::Result result_;

  private:
	std::shared_ptr<c::IO> io_;
	c::Request request_;
	bool submitted_ = false, done_ = false;
};
c::Result original(c::NativeQueue &queue, std::shared_ptr<c::IO> io, c::Kind kind,
				   std::shared_ptr<c::File> file = {}, std::string_view name = {}) {
	auto operation = std::make_shared<Original>(std::move(io), kind, std::move(file), name);
	std::weak_ptr<Original> weak = operation;
	check(queue.submit(operation));
	operation.reset();
	check(!weak.expired());
	for (;;) {
		if (auto completed = queue.poll()) {
			auto held = std::static_pointer_cast<Original>(completed->operation);
			auto wrong = c::Result{};
			wrong.token = held->request().token;
			++wrong.token.step;
			check(!held->complete(std::move(wrong)) && held->in_flight());
			check(held->complete(std::move(completed->completion)));
			return std::move(held->result_);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}
} // namespace
int main(int argc, char **argv) {
	try {
		if (argc != 2)
			throw std::invalid_argument("scratch directory required");
		std::filesystem::path directory(argv[1]);
		check(std::filesystem::create_directory(directory));
		std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
		auto io = std::make_shared<c::PosixIO>(directory);
		check(io->managed_capabilities().complete());
		auto first = std::shared_ptr<c::File>(io->create_exclusive("anchor"));
		check(first->write_at(0, "proof") == 5);
		auto named = io->inspect_entry("anchor");
		check(named && first->inspect() == *named && named->footprint.logical_bytes == 5 &&
			  named->links == 1 && named->owner == ::geteuid() && named->permissions == 0600 &&
			  named->footprint.kind == c::EntryKind::Regular);
		check(!io->inspect_entry("absent"));
		std::filesystem::create_hard_link(directory / "anchor", directory / "hard");
		check(io->inspect_entry("hard")->links == 2 && first->inspect().links == 2);
		check(!io->open_reclaim_candidate("hard"));
		std::filesystem::remove(directory / "hard");
		std::filesystem::create_symlink("anchor", directory / "link");
		check(io->inspect_entry("link")->footprint.kind == c::EntryKind::Symlink &&
			  io->inspect_entry("link")->inode != named->inode);
		std::filesystem::create_directory(directory / "subdir");
		check(io->inspect_entry("subdir")->footprint.kind == c::EntryKind::Directory);
		check(::mkfifo((directory / "fifo").c_str(), 0600) == 0);
		check(io->inspect_entry("fifo")->footprint.kind == c::EntryKind::Other);
		rejects([&] { io->inspect_entry("../anchor"); });
		LegacyFile legacy;
		rejects([&] { legacy.inspect(); });
		rejects([&] { legacy.try_lease(c::LeaseMode::Shared); });
		rejects([&] { legacy.close(); });
		c::NativeQueue queue;
		auto inspection = original(queue, io, c::Kind::InspectEntry, {}, "anchor");
		check(!inspection.error && inspection.inspection == named);
		auto held_inspection = original(queue, io, c::Kind::InspectFile, first);
		check(!held_inspection.error && held_inspection.inspection == named);
		auto reader = original(queue, io, c::Kind::SharedLease, first);
		check(!reader.error && reader.count == 1);
		rejects([&] { first->try_lease(c::LeaseMode::Exclusive); }); // Never convert an acquired lock.
		auto second = std::shared_ptr<c::File>(io->open_existing("anchor"));
		auto busy = original(queue, io, c::Kind::ExclusiveLease, second);
		check(!busy.error && busy.count == 0);
		auto closed = original(queue, io, c::Kind::CloseFile, first);
		check(!closed.error);
		rejects([&] { first->inspect(); });
		rejects([&] { first->close(); });
		auto exclusive = original(queue, io, c::Kind::ExclusiveLease, second);
		check(!exclusive.error && exclusive.count == 1);
		auto third = std::shared_ptr<c::File>(io->open_existing("anchor"));
		check(!third->try_lease(c::LeaseMode::Shared));
		rejects([&] { third->try_lease(static_cast<c::LeaseMode>(999)); });
		check(!original(queue, io, c::Kind::CloseFile, second).error);
		check(third->try_lease(c::LeaseMode::Shared));
		third->close();
		// Default/invalid operands yield authentic errors, never false success.
		for (auto kind :
			 {c::Kind::InspectFile, c::Kind::SharedLease, c::Kind::ExclusiveLease, c::Kind::CloseFile})
			check(static_cast<bool>(original(queue, io, kind).error));
		check(static_cast<bool>(
			original(queue, io, c::Kind::InspectFile, std::make_shared<LegacyFile>()).error));
		auto invalid = std::make_shared<Original>(io, static_cast<c::Kind>(999));
		rejects([&] { queue.submit(invalid); });
		check(!invalid->in_flight() && !queue.busy());
		auto stats = queue.stats();
		for (std::size_t at = 1; at < stats.fallback_extensions.size(); ++at)
			check(stats.fallback_extensions[at] > 0 && stats.native_extensions[at] == 0);
		check(stats.fallback_by_primitive.size() == 12);
		std::filesystem::remove_all(directory);
		std::cout << "no-follow stable inspection, nonblocking leases and retained close originals passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
