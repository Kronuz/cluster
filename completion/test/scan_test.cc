#include "completion/api.h"
#include "completion/native_completion.h"
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <new>
#include <thread>

namespace c = kronuz::io::completion;
namespace {
thread_local bool watching = false;
thread_local std::size_t allocations = 0;
void check(bool value) {
	if (!value)
		throw std::runtime_error("caller-buffer scan invariant failed");
}
class LegacyCursor final : public kronuz::journal::DirectoryCursor {
  public:
	unsigned calls = 0;
	std::optional<std::string> next() override {
		++calls;
		return "legacy";
	}
};
class InvalidCursor final : public kronuz::journal::DirectoryCursor {
  public:
	std::optional<std::string> next() override { throw std::logic_error("unused"); }
	std::optional<std::size_t> next_into(std::span<char>) override { return 0; }
};
struct Counts {
	std::atomic<std::size_t> used{0};
	std::atomic<bool> destroyed{false};
};
class Resource final : public std::pmr::memory_resource {
  public:
	explicit Resource(Counts &counts) : counts_(counts) {}
	~Resource() override { counts_.destroyed = true; }
	std::atomic<bool> fail{false};

  private:
	void *do_allocate(std::size_t bytes, std::size_t alignment) override {
		if (fail.load())
			throw std::bad_alloc();
		auto pointer = std::pmr::new_delete_resource()->allocate(bytes, alignment);
		counts_.used.fetch_add(bytes);
		return pointer;
	}
	void do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment) noexcept override {
		std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
		counts_.used.fetch_sub(bytes);
	}
	bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
		return this == &other;
	}
	Counts &counts_;
};
class NextOperation final : public c::Operation {
	struct Owner {
		std::shared_ptr<c::IO> io;
		std::unique_ptr<kronuz::journal::DirectoryCursor> cursor;
	};

  public:
	NextOperation(c::AllocationContext context, std::shared_ptr<c::IO> io)
		: owner_(std::allocate_shared<Owner>(c::OwnedAllocator<Owner>(context))) {
		owner_->io = std::move(io);
		owner_->cursor = owner_->io->scan_directory();
		request_.kind = c::Kind::NextInto;
		request_.cursor = std::shared_ptr<kronuz::journal::DirectoryCursor>(owner_, owner_->cursor.get());
		request_.destination_bytes = bytes_;
		request_.token.operation = 1;
	}
	const c::Request &request() const override { return request_; }
	void submitted() override {
		if (submitted_)
			throw std::logic_error("duplicate scan submission");
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
	c::IO &io() const noexcept override { return *owner_->io; }
	std::string_view name() const { return {bytes_.data(), result_.count}; }
	c::Result result_;

  private:
	std::shared_ptr<Owner> owner_;
	std::array<char, 255> bytes_;
	c::Request request_;
	bool submitted_ = false, done_ = false;
};
} // namespace
void *operator new(std::size_t bytes) {
	if (watching)
		++allocations;
	if (auto pointer = std::malloc(bytes ? bytes : 1))
		return pointer;
	throw std::bad_alloc();
}
void operator delete(void *pointer) noexcept { std::free(pointer); }
void operator delete(void *pointer, std::size_t) noexcept { std::free(pointer); }
int main(int argc, char **argv) {
	try {
		if (argc != 2)
			throw std::invalid_argument("scratch directory required");
		const std::filesystem::path directory(argv[1]);
		check(std::filesystem::create_directory(directory));
		std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
		const std::string long_name(255, 'x');
		{
			c::PosixIO io(directory);
			int fd = ::openat(io.native_directory_handle(), long_name.c_str(),
							  O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
			check(fd >= 0);
			::close(fd);
			auto cursor = io.scan_directory();
			std::array<char, 255> scratch;
			watching = true;
			auto size = cursor->next_into(scratch);
			auto end = cursor->next_into(scratch);
			watching = false;
			check(size == 255 && !end && allocations == 0 &&
				  std::string_view(scratch.data(), *size) == long_name);
			std::cout << "255-byte name and EOF global allocations: " << allocations << '\n';
			cursor = io.scan_directory();
			bool short_buffer = false;
			try {
				(void)cursor->next_into(std::span(scratch).first(254));
			} catch (const std::length_error &) {
				short_buffer = true;
			}
			check(short_buffer);
			c::Request request;
			request.kind = c::Kind::NextInto;
			request.destination_bytes = scratch;
			auto legacy = std::make_shared<LegacyCursor>();
			request.cursor = legacy;
			auto unsupported = kronuz::journal::detail::execute_primitive(io, request);
			check(unsupported.error && legacy->calls == 0 && !unsupported.name);
			request.cursor = std::make_shared<InvalidCursor>();
			check(static_cast<bool>(kronuz::journal::detail::execute_primitive(io, request).error));
			request.cursor.reset();
			check(static_cast<bool>(kronuz::journal::detail::execute_primitive(io, request).error));
		}
		Counts counts;
		auto resource = std::make_shared<Resource>(counts);
		c::AllocationContext context(resource);
		auto io = c::make_posix_io(context, directory);
		auto queue = c::make_native_queue(context);
		auto job =
			std::allocate_shared<NextOperation>(c::OwnedAllocator<NextOperation>(context), context, io);
		std::weak_ptr<NextOperation> weak = job;
		resource->fail = true;
		check(queue->submit(job));
		job.reset();
		io.reset();
		context = c::AllocationContext{};
		resource.reset();
		check(!weak.expired() && counts.used > 0 && !counts.destroyed);
		while (queue->busy()) {
			if (auto original = queue->poll()) {
				auto held = std::static_pointer_cast<NextOperation>(original->operation);
				auto wrong = c::Result{};
				wrong.token = held->request().token;
				++wrong.token.step;
				check(!held->complete(std::move(wrong)) && held->in_flight());
				check(held->complete(std::move(original->completion)) && !held->result_.error &&
					  held->name() == long_name);
			} else {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
		auto stats = queue->stats();
		check(stats.fallback_extensions[0] == 1 && stats.fallback_by_primitive.size() == 12);
		queue.reset();
		check(weak.expired() && counts.used > 0 && !counts.destroyed);
		weak.reset();
		check(counts.used == 0 && counts.destroyed);
		std::filesystem::remove_all(directory);
		std::cout << "caller-buffer original, resource lifetime and extension counters passed\n";
	} catch (const std::exception &error) {
		watching = false;
		std::cerr << error.what() << '\n';
		return 1;
	}
}
