#include "completion/image.h"
#include <cstdlib>
#include <iostream>
#include <new>

namespace c = kronuz::io::completion;
namespace {
thread_local bool watching = false;
thread_local std::size_t allocations = 0;
void check(bool value) {
	if (!value)
		throw std::runtime_error("completion allocation invariant failed");
}
class TrackedFile final : public c::File {
  public:
	explicit TrackedFile(unsigned &destroyed) : destroyed_(destroyed) {}
	~TrackedFile() { ++destroyed_; }
	std::uint64_t size() override { return 7; }
	std::size_t read_at(std::uint64_t, std::span<char>) override { return 0; }
	std::size_t write_at(std::uint64_t, std::string_view) override { return 0; }
	void truncate(std::uint64_t) override {}
	void sync() override {}

  private:
	unsigned &destroyed_;
};
class UnusedIO final : public c::IO {
  public:
	std::unique_ptr<c::OwnerLock> acquire_owner(bool) override { throw std::logic_error("unused"); }
	std::unique_ptr<c::File> open_existing(std::string_view) override { throw std::logic_error("unused"); }
	std::unique_ptr<c::File> create_exclusive(std::string_view) override { throw std::logic_error("unused"); }
	void replace(std::string_view, std::string_view) override { throw std::logic_error("unused"); }
	void remove(std::string_view) override { throw std::logic_error("unused"); }
	void sync_directory() override { throw std::logic_error("unused"); }
};
class ObservedResource final : public std::pmr::memory_resource {
  public:
	std::size_t calls = 0, fail_at = 0, used = 0;

  private:
	void *do_allocate(std::size_t bytes, std::size_t alignment) override {
		if (++calls == fail_at)
			throw std::bad_alloc();
		void *pointer = nullptr;
		if (posix_memalign(&pointer, std::max(alignment, sizeof(void *)), bytes ? bytes : 1))
			throw std::bad_alloc();
		used += bytes;
		return pointer;
	}
	void do_deallocate(void *pointer, std::size_t bytes, std::size_t) noexcept override {
		std::free(pointer);
		used -= bytes;
	}
	bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
		return this == &other;
	}
};
void managed_images(const std::shared_ptr<UnusedIO> &io, const std::shared_ptr<const std::string> &bytes) {
	const std::string candidate(120, 'c'), selected(120, 's');
	c::Token token;
	token.operation = 2;
	auto lease = std::make_shared<int>(1);
	auto resource = std::make_shared<ObservedResource>();
	c::AllocationContext context(resource);
	for (std::size_t failure = 1; failure <= 3; ++failure) {
		resource->calls = 0;
		resource->fail_at = failure;
		bool failed = false;
		try {
			(void)c::make_image_preparation(context, io, lease, token, std::string_view(candidate), bytes, 7);
		} catch (const std::bad_alloc &) {
			failed = true;
		}
		check(failed && resource->used == 0);
	}
	resource->fail_at = 0;
	resource->calls = 0;
	allocations = 0;
	watching = true;
	auto job = c::make_image_preparation(context, io, lease, token, std::string_view(candidate), bytes, 7);
	watching = false;
	check(allocations == 0 && resource->calls == 3 && resource->used > candidate.size());
	unsigned destroyed = 0;
	auto file = std::make_unique<TrackedFile>(destroyed);
	const auto admitted = resource->used;
	resource->fail_at = resource->calls + 1;
	while (!job->done()) {
		c::Result result;
		result.token = job->request().token;
		result.count = job->request().bytes.size();
		if (job->request().kind == c::Kind::Create)
			result.file = std::move(file);
		watching = true;
		job->submitted();
		check(job->complete(std::move(result)));
		watching = false;
		check(allocations == 0 && resource->used == admitted && resource->calls == 3);
	}
	auto prepared = job->prepared();
	check(prepared.name() == candidate && prepared.allocation_context().resource() == resource.get());
	job.reset();
	const auto prepared_bytes = resource->used;
	for (std::size_t failure = 1; failure <= 2; ++failure) {
		resource->calls = 0;
		resource->fail_at = failure;
		bool failed = false;
		try {
			(void)c::make_image_publication(prepared, selected, token);
		} catch (const std::bad_alloc &) {
			failed = true;
		}
		check(failed && resource->used == prepared_bytes);
	}
	resource->calls = 0;
	resource->fail_at = 0;
	watching = true;
	auto publication = c::make_image_publication(std::move(prepared), selected, token);
	watching = false;
	check(allocations == 0 && resource->calls == 2 && publication->request().destination == selected);
	while (!publication->done()) {
		c::Result result;
		result.token = publication->request().token;
		watching = true;
		publication->submitted();
		check(publication->complete(std::move(result)));
		watching = false;
		check(allocations == 0);
	}
	check(!publication->uncertain());
	std::weak_ptr<c::ImagePublication> weak = publication;
	publication.reset();
	check(!prepared);
	check(destroyed == 1 && weak.expired() && resource->used > 0);
	weak.reset();
	check(resource->used == 0);
	std::cout << "managed preparation allocations: 3; publication: 2; unmanaged: 0\n";
}
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
int main() {
	try {
		auto io = std::make_shared<UnusedIO>();
		auto lease = std::make_shared<int>(1);
		std::weak_ptr<int> retained_lease = lease;
		auto bytes = std::make_shared<const std::string>("payload");
		c::Token token;
		token.operation = 1;
		auto job = std::make_shared<c::ImagePreparation>(io, lease, token, "candidate", bytes, 7);
		job->submitted();
		unsigned destroyed = 0;
		c::Result completion;
		completion.token = job->request().token;
		completion.file = std::make_unique<TrackedFile>(destroyed);
		watching = true;
		const bool accepted = job->complete(std::move(completion));
		watching = false;
		std::cout << "Create completion allocations: " << allocations << '\n';
		check(accepted && !job->error() && allocations == 0);
		auto held_file = job->request().file;
		check(held_file && held_file->size() == 7 && destroyed == 0);
		lease.reset();
		job.reset();
		check(destroyed == 0 && !retained_lease.expired());
		held_file.reset();
		check(destroyed == 1 && retained_lease.expired());
		managed_images(io, bytes);
		std::cout << "file admission and aliased ownership passed\n";
	} catch (const std::exception &error) {
		watching = false;
		std::cerr << error.what() << '\n';
		return 1;
	}
}
