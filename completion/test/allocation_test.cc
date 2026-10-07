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
		std::cout << "file admission and aliased ownership passed\n";
	} catch (const std::exception &error) {
		watching = false;
		std::cerr << error.what() << '\n';
		return 1;
	}
}
