#include "completion/api.h"
#include "completion/native_completion.h"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
namespace c = kronuz::io::completion;
namespace j = kronuz::journal;
namespace {
void check(bool value) {
	if (!value)
		throw std::runtime_error("reusable control invariant failed");
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
	void *do_allocate(std::size_t size, std::size_t align) override {
		if (refusing)
			throw std::bad_alloc();
		auto memory = std::pmr::new_delete_resource()->allocate(size, align);
		++calls;
		bytes += size;
		return memory;
	}
	void do_deallocate(void *memory, std::size_t size, std::size_t align) override {
		std::pmr::new_delete_resource()->deallocate(memory, size, align);
		bytes -= size;
	}
	bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
		return this == &other;
	}
};
struct State {
	std::shared_ptr<c::IO> io;
	std::array<std::unique_ptr<c::File>, 9> files;
	std::unique_ptr<c::DirectoryCursor> cursor;
	std::array<char, 255> name{};
	std::exception_ptr error;
};
class Operation final : public c::Operation {
  public:
	explicit Operation(std::shared_ptr<State> state) : state_(std::move(state)) { prepare(); }
	const c::Request &request() const override { return request_; }
	void submitted() override {
		check(!submitted_ && !done_);
		submitted_ = true;
	}
	bool complete(c::Result result) noexcept override {
		if (!submitted_ || result.token != request_.token)
			return false;
		submitted_ = false;
		try {
			if (result.error)
				std::rethrow_exception(result.error);
			check(!result.file && !result.cursor);
			if (request_.kind == c::Kind::OpenCandidateInto)
				check(result.count == 1);
			if (request_.kind == c::Kind::NextInto)
				check(result.count > 0 && result.count <= state_->name.size());
			if (++at_ == kinds_.size())
				done_ = true;
			else
				prepare();
		} catch (...) {
			state_->error = std::current_exception();
			done_ = true;
		}
		return true;
	}
	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return submitted_; }
	c::IO &io() const noexcept override { return *state_->io; }

  private:
	void prepare() {
		request_ = {};
		request_.token.operation = 1;
		request_.token.step = at_;
		request_.kind = kinds_[at_];
		request_.source = "anchor";
		request_.destination_bytes = state_->name;
		request_.file = std::shared_ptr<c::File>(state_, state_->files[0].get());
		request_.cursor = std::shared_ptr<c::DirectoryCursor>(state_, state_->cursor.get());
	}
	inline static constexpr std::array kinds_{
		c::Kind::OpenInto,	  c::Kind::CloseFile,		  c::Kind::ScanInto, c::Kind::NextInto,
		c::Kind::CloseCursor, c::Kind::OpenCandidateInto, c::Kind::CloseFile};
	std::shared_ptr<State> state_;
	c::Request request_;
	std::size_t at_ = 0;
	bool submitted_ = false, done_ = false;
};
} // namespace
int main(int argc, char **argv) {
	try {
		check(argc == 2);
		std::filesystem::path directory(argv[1]);
		check(std::filesystem::create_directory(directory));
		std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
		auto resource = std::make_shared<Resource>();
		c::AllocationContext context(resource);
		{
			auto backend = c::make_posix_io(context, directory);
			check(backend->managed_capabilities().complete());
			{
				auto file = backend->create_exclusive("anchor");
				check(file->write_at(0, "proof") == 5);
				file->close();
			}
			auto state = std::allocate_shared<State>(c::OwnedAllocator<State>(context));
			state->io = backend;
			for (auto &file : state->files)
				file = backend->make_closed_file();
			state->cursor = backend->make_closed_cursor();
			auto job = std::allocate_shared<Operation>(c::OwnedAllocator<Operation>(context), state);
			const auto calls = resource->calls, capacity = resource->bytes;
			resource->refusing = true;
			for (unsigned cycle = 0; cycle < 100; ++cycle) {
				for (auto &file : state->files) {
					auto address = file.get();
					check(backend->open_reclaim_candidate_into("anchor", *file));
					check(file.get() == address && file->inspect().footprint.logical_bytes == 5);
					check(file->try_lease(c::LeaseMode::Shared));
					rejects([&] { backend->open_existing_into("anchor", *file); });
					file->close();
					check(!backend->open_reclaim_candidate_into("missing", *file));
					rejects([&] { backend->open_existing_into("missing", *file); });
					backend->open_existing_into("anchor", *file);
					check(file->try_lease(c::LeaseMode::Exclusive));
					file->close();
				}
				backend->scan_directory_into(*state->cursor);
				rejects([&] { backend->scan_directory_into(*state->cursor); });
				check(state->cursor->next_into(state->name).value() == 6);
				check(!state->cursor->next_into(state->name));
				state->cursor->close();
			}
			check(resource->calls == calls && resource->bytes == capacity);
			c::NativeQueue queue;
			std::weak_ptr<Operation> weak = job;
			check(queue.submit(job));
			job.reset();
			backend.reset();
			check(!weak.expired());
			for (;;) {
				std::optional<j::OwnedCompletion> held;
				const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
				while (!held) {
					held = queue.poll();
					if (!held) {
						check(std::chrono::steady_clock::now() < deadline);
						std::this_thread::sleep_for(std::chrono::milliseconds(1));
					}
				}
				job = std::static_pointer_cast<Operation>(held->operation);
				c::Result stale;
				stale.token = held->completion.token;
				++stale.token.step;
				check(!job->complete(std::move(stale)) && job->in_flight());
				check(job->complete(std::move(held->completion)));
				if (job->done())
					break;
				check(queue.submit(job));
				job.reset();
			}
			if (state->error)
				std::rethrow_exception(state->error);
			check(resource->calls == calls && resource->bytes == capacity);
			job.reset();
			check(weak.expired());
			weak.reset();
			std::cout << "reusable collector controls/state/job=" << capacity
					  << " admitted bytes; no new resource calls across 100 cycles\n";
		}
		check(resource->bytes == 0);
		resource->refusing = false;
		{
			c::PosixIO first(directory), second(directory);
			auto file = first.make_closed_file();
			auto cursor = first.make_closed_cursor();
			rejects([&] { second.open_existing_into("anchor", *file); });
			rejects([&] { second.open_reclaim_candidate_into("anchor", *file); });
			rejects([&] { second.scan_directory_into(*cursor); });
			first.open_existing_into("anchor", *file);
			file->close();
			auto legacy = first.open_existing("anchor");
			rejects([&] { first.open_existing_into("anchor", *legacy); });
			legacy->close();
			std::filesystem::create_symlink("anchor", directory / "symlink");
			std::filesystem::create_hard_link(directory / "anchor", directory / "hardlink");
			check(!first.open_reclaim_candidate_into("anchor", *file));
			check(!first.open_reclaim_candidate_into("symlink", *file));
			std::filesystem::remove(directory / "hardlink");
			first.open_existing_into("anchor", *file);
			file->close();
		}
		{
			alignas(c::PosixIO) std::array<std::byte, sizeof(c::PosixIO)> storage;
			auto first = std::construct_at(reinterpret_cast<c::PosixIO *>(storage.data()), directory);
			auto file = first->make_closed_file();
			auto cursor = first->make_closed_cursor();
			std::destroy_at(first);
			auto second = std::construct_at(reinterpret_cast<c::PosixIO *>(storage.data()), directory);
			rejects([&] { second->open_existing_into("anchor", *file); });
			rejects([&] { second->scan_directory_into(*cursor); });
			std::destroy_at(second);
		}
		std::filesystem::remove_all(directory);
		std::cout
			<< "closed-control reuse, saturation, retained originals and backend-address reuse passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
