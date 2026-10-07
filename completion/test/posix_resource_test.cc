#include "completion/api.h"
#include "completion/image.h"
#include "completion/native_completion.h"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <thread>

namespace c = kronuz::io::completion;
void check(bool value) {
	if (!value)
		throw std::runtime_error("managed POSIX invariant failed");
}
struct Counts {
	std::size_t used = 0, calls = 0;
	bool destroyed = false;
};
class Resource final : public std::pmr::memory_resource {
  public:
	explicit Resource(Counts &counts) : counts_(counts) {}
	~Resource() override { counts_.destroyed = true; }
	bool fail = false;

  private:
	void *do_allocate(std::size_t bytes, std::size_t alignment) override {
		++counts_.calls;
		if (fail)
			throw std::bad_alloc();
		void *pointer = nullptr;
		if (posix_memalign(&pointer, std::max(alignment, sizeof(void *)), bytes ? bytes : 1))
			throw std::bad_alloc();
		counts_.used += bytes;
		return pointer;
	}
	void do_deallocate(void *pointer, std::size_t bytes, std::size_t) noexcept override {
		std::free(pointer);
		counts_.used -= bytes;
	}
	bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
		return this == &other;
	}
	Counts &counts_;
};
int main(int argc, char **argv) {
	try {
		if (argc != 2)
			throw std::invalid_argument("scratch directory required");
		const std::filesystem::path directory(argv[1]);
		check(std::filesystem::create_directory(directory));
		std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
		Counts counts;
		auto resource = std::make_shared<Resource>(counts);
		auto io = c::make_posix_io(c::AllocationContext(resource), directory);
		const auto baseline = counts.used;
		check(baseline > 0 && counts.calls == 1);
		struct Failing {
			Failing() { throw std::runtime_error("constructor failed"); }
			virtual ~Failing() = default;
		};
		bool failed = false;
		try {
			(void)new (c::AllocationContext(resource))
				kronuz::journal::detail::ManagedPosixControl<Failing>();
		} catch (const std::runtime_error &) {
			failed = true;
		}
		check(failed && counts.used == baseline);
		resource->fail = true;
		failed = false;
		try {
			(void)io->create_exclusive("refused");
		} catch (const std::bad_alloc &) {
			failed = true;
		}
		check(failed && !std::filesystem::exists(directory / "refused") && counts.used == baseline);
		failed = false;
		try {
			(void)io->acquire_owner(true);
		} catch (const std::bad_alloc &) {
			failed = true;
		}
		check(failed && !std::filesystem::exists(directory / "owner.lock") && counts.used == baseline);
		failed = false;
		try {
			(void)c::make_native_queue(c::AllocationContext(resource));
		} catch (const std::bad_alloc &) {
			failed = true;
		}
		check(failed && counts.used == baseline);
		resource->fail = false;
		auto owner = io->acquire_owner(true);
		check(counts.used > baseline);
		{
			auto queue = c::make_native_queue(c::AllocationContext(resource));
			auto bytes = std::make_shared<const std::string>(65537, 'x');
			auto admission = std::make_shared<int>(1);
			c::Token token;
			token.operation = 1;
			auto job = c::make_image_preparation(c::AllocationContext(resource), io, admission, token,
												 std::string_view("managed-native"), bytes, bytes->size());
			while (!job->done()) {
				check(queue->submit(job));
				while (queue->busy()) {
					if (auto original = queue->poll()) {
						check(original->operation == job && job->complete(std::move(original->completion)));
					} else {
						std::this_thread::sleep_for(std::chrono::milliseconds(1));
					}
				}
			}
			check(!job->error() && job->prepared().name() == "managed-native");
			std::weak_ptr<c::NativeQueue> weak_queue = queue;
			queue.reset();
			check(weak_queue.expired());
			weak_queue.reset();
		}
		auto file = io->create_exclusive("source");
		check(file->write_at(0, "payload") == 7);
		file->sync();
		io->replace("source", "destination");
		io->sync_directory();
		io->remove("destination");
		io->sync_directory();
		check(file->size() == 7);
		auto cursor = io->scan_directory();
		check(counts.used > baseline);
		owner.reset();
		std::weak_ptr<c::PosixIO> weak = io;
		io.reset();
		check(weak.expired() && counts.used > 0);
		weak.reset();
		resource.reset();
		check(!counts.destroyed && file->size() == 7 && cursor->next().has_value());
		cursor.reset();
		{
			std::jthread release([held = std::move(file)]() mutable { held.reset(); });
		}
		check(counts.destroyed && counts.used == 0);
		std::filesystem::remove_all(directory);
		std::cout << "managed POSIX admission and surviving controls passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
