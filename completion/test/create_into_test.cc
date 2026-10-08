#include "completion/image.h"
#include "completion/native_completion.h"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <source_location>
#include <thread>
namespace c = kronuz::io::completion;
namespace {
void check(bool value, std::source_location where = std::source_location::current()) {
	if (!value)
		throw std::runtime_error("preallocated create invariant failed at line " +
								 std::to_string(where.line()));
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
class Resource final : public std::pmr::memory_resource {
  public:
	std::size_t calls = 0, bytes = 0;
	bool refusing = false;

  private:
	void *do_allocate(std::size_t size, std::size_t alignment) override {
		if (refusing)
			throw std::bad_alloc();
		auto pointer = std::pmr::new_delete_resource()->allocate(size, alignment);
		++calls;
		bytes += size;
		return pointer;
	}
	void do_deallocate(void *pointer, std::size_t size, std::size_t alignment) override {
		std::pmr::new_delete_resource()->deallocate(pointer, size, alignment);
		bytes -= size;
	}
	bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
		return this == &other;
	}
};
class FaultIO final : public c::IO {
  public:
	FaultIO(c::AllocationContext context, const std::filesystem::path &directory)
		: backend(c::make_posix_io(context, directory)) {}
	std::unique_ptr<c::OwnerLock> acquire_owner(bool create) override {
		return backend->acquire_owner(create);
	}
	std::unique_ptr<c::File> make_closed_file() override { return backend->make_closed_file(); }
	std::unique_ptr<c::File> open_existing(std::string_view name) override {
		return backend->open_existing(name);
	}
	std::unique_ptr<c::File> create_exclusive(std::string_view name) override {
		return backend->create_exclusive(name);
	}
	std::optional<c::EntryInspection> inspect_entry(std::string_view name) override {
		return backend->inspect_entry(name);
	}
	void replace(std::string_view from, std::string_view to) override { backend->replace(from, to); }
	void remove(std::string_view name) override { backend->remove(name); }
	void sync_directory() override { backend->sync_directory(); }
	bool supported = true, before = false, after = false;
	c::ManagedCapabilities managed_capabilities() const noexcept override {
		c::ManagedCapabilities caps;
		caps.file_close = true;
		caps.reusable_create = supported;
		return caps;
	}
	void create_exclusive_into(std::string_view name, c::File &file) override {
		if (before)
			throw std::runtime_error("before CreateInto");
		backend->create_exclusive_into(name, file);
		if (after) {
			file.close();
			throw std::runtime_error("after CreateInto");
		}
	}

  private:
	std::shared_ptr<c::PosixIO> backend;
};
void drive(c::Operation &job) {
	while (!job.done()) {
		job.submitted();
		check(job.complete(kronuz::journal::detail::execute_primitive(job.io(), job.request())));
	}
}
} // namespace
int main(int argc, char **argv) {
	try {
		check(argc == 2);
		const std::filesystem::path directory(argv[1]);
		check(std::filesystem::create_directory(directory));
		std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
		auto resource = std::make_shared<Resource>();
		c::AllocationContext context(resource);
		{
			auto io = c::make_posix_io(context, directory);
			auto lock = io->acquire_owner(true);
			auto lease = std::make_shared<int>(1);
			auto payload = std::make_shared<const std::string>("sealed payload");
			auto queue = c::make_native_queue(context);
			std::array<std::shared_ptr<c::ImagePreparation>, 3> jobs;
			constexpr std::array<std::string_view, 3> names{"family.lease", "family.image",
															"family.descriptor"};
			for (std::size_t at = 0; at < jobs.size(); ++at) {
				auto file = io->make_closed_file();
				jobs[at] = c::make_image_preparation(context, io, lease, c::Token{{}, at + 1, 0}, names[at],
													 payload, 64, std::move(file));
				check(jobs[at]->request().kind == c::Kind::CreateInto && jobs[at]->request().file);
			}
			auto reusable = io->make_closed_file();
			auto other = c::make_posix_io(context, directory);
			const auto calls = resource->calls, capacity = resource->bytes;
			resource->refusing = true;
			rejects([&] { other->create_exclusive_into("foreign-control", *reusable); });
			rejects([&] { io->create_exclusive_into("../outside", *reusable); });
			check(!io->inspect_entry("foreign-control"));
			for (auto &job : jobs) {
				drive(*job);
				check(!job->error());
			}
			check(resource->calls == calls && resource->bytes == capacity);
			for (unsigned pass = 0; pass < 100; ++pass) {
				io->create_exclusive_into("repeated", *reusable);
				rejects([&] { io->create_exclusive_into("live-collision", *reusable); });
				check(reusable->size() == 0 && !io->inspect_entry("live-collision"));
				reusable->close();
				rejects([&] { io->create_exclusive_into("repeated", *reusable); });
				check(dynamic_cast<kronuz::journal::detail::PosixFile *>(reusable.get())->native_handle() <
					  0);
				io->remove("repeated");
			}
			check(resource->calls == calls && resource->bytes == capacity);
			std::cout << "preallocated create fixture: " << capacity
					  << " PMR bytes; no new managed allocations\n";
			resource->refusing = false;
			// Retain authentic native CreateInto and CloseFile originals after facade loss.
			auto job = c::make_image_preparation(context, io, lease, c::Token{{}, 1000, 0}, "cancelled",
												 payload, 64, io->make_closed_file());
			check(queue->submit(job));
			job->cancel();
			std::weak_ptr<c::ImagePreparation> weak = job;
			job.reset();
			auto poll = [&] {
				const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
				for (;;) {
					auto result = queue->poll();
					if (result)
						return result;
					check(std::chrono::steady_clock::now() < deadline);
					std::this_thread::sleep_for(std::chrono::milliseconds(1));
				}
			};
			auto held = poll();
			check(!held->completion.error && !held->completion.file);
			job = std::static_pointer_cast<c::ImagePreparation>(held->operation);
			c::Result stale;
			stale.token = held->completion.token;
			++stale.token.operation;
			check(!job->complete(std::move(stale)) && job->in_flight());
			resource->refusing = true;
			check(job->complete(std::move(held->completion)) && !job->done());
			check(job->request().kind == c::Kind::CloseFile && queue->submit(job));
			job.reset();
			held.reset();
			held = poll();
			job = std::static_pointer_cast<c::ImagePreparation>(held->operation);
			check(!job->done() && job->complete(std::move(held->completion)) && job->done() && job->error());
			rejects([&] { job->prepared(); });
			job.reset();
			held.reset();
			check(weak.expired());
			weak.reset();
			resource->refusing = false;
			// A retained prepared capability keeps its closed control allocation alive.
			std::optional<c::PreparedImage> prepared = jobs[0]->prepared();
			jobs = {};
			const auto retained = resource->bytes;
			prepared.reset();
			check(resource->bytes < retained);
			queue.reset();
			reusable.reset();
			lock.reset();
		}
		check(resource->bytes == 0);
		check((c::ManagedCapabilities{true, true, true, true, true, true, true, false}).complete());
		{
			auto io = std::allocate_shared<FaultIO>(c::OwnedAllocator<FaultIO>(context), context, directory);
			auto lock = io->acquire_owner(false);
			auto lease = std::make_shared<int>(1);
			auto payload = std::make_shared<const std::string>("");
			io->supported = false;
			check(!io->managed_capabilities().reusable_create);
			rejects([&] {
				c::make_image_preparation(context, io, lease, c::Token{}, "unsupported", payload, 64,
										  io->make_closed_file());
			});
			check(!io->inspect_entry("unsupported"));
			io->supported = true;
			for (unsigned scenario = 0; scenario < 4; ++scenario) {
				io->before = scenario == 0;
				io->after = scenario == 1;
				const auto name = scenario == 0	  ? "before"
								  : scenario == 1 ? "after"
								  : scenario == 2 ? "unsubmitted"
												  : "empty";
				auto job = c::make_image_preparation(context, io, lease, c::Token{{}, scenario + 1, 0}, name,
													 payload, 64, io->make_closed_file());
				if (scenario == 2) {
					check(job->cancel() && job->done());
				} else
					drive(*job);
				check(job->done() && bool(job->error()) == (scenario != 3));
				check(bool(io->inspect_entry(name)) == (scenario == 1 || scenario == 3));
				if (scenario == 3)
					check(bool(job->prepared()));
			}
		}
		check(resource->bytes == 0);
		static_assert(static_cast<unsigned>(c::Kind::ScanInto) == 21 &&
					  static_cast<unsigned>(c::Kind::CreateInto) == 22);
		static_assert(std::tuple_size_v<decltype(c::Stats::native_by_primitive)> == 12);
		std::filesystem::remove_all(directory);
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
