#include "completion/allocation.h"
#include <atomic>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace c = kronuz::io::completion;
void check(bool value) {
	if (!value)
		throw std::runtime_error("owned allocation invariant failed");
}
struct Counts {
	std::atomic<std::size_t> used{0}, peak{0}, calls{0}, released{0};
	std::atomic<bool> destroyed{false};
};
class Resource final : public std::pmr::memory_resource {
  public:
	explicit Resource(Counts &counts) : counts_(counts) {}
	~Resource() { counts_.destroyed.store(true); }
	bool fail = false;

  private:
	void *do_allocate(std::size_t bytes, std::size_t alignment) override {
		counts_.calls.fetch_add(1);
		if (fail)
			throw std::bad_alloc();
		auto result = std::pmr::new_delete_resource()->allocate(bytes, alignment);
		const auto used = counts_.used.fetch_add(bytes) + bytes;
		auto peak = counts_.peak.load();
		while (peak < used && !counts_.peak.compare_exchange_weak(peak, used)) {
		}
		return result;
	}
	void do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment) override {
		std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
		counts_.used.fetch_sub(bytes);
		counts_.released.fetch_add(1);
	}
	bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
		return this == &other;
	}
	Counts &counts_;
};
int main() {
	try {
		check(!c::AllocationContext{}.managed());
		check(c::AllocationContext{}.resource() == std::pmr::new_delete_resource());
		Counts counts;
		{
			auto resource = std::make_shared<Resource>(counts);
			c::AllocationContext context(resource);
			c::OwnedAllocator<int> allocator(context);
			check(allocator == c::OwnedAllocator<char>(context));
			check(!(allocator == c::OwnedAllocator<int>{}));
			const auto before = counts.calls.load();
			bool overflow = false;
			try {
				allocator.allocate(std::numeric_limits<std::size_t>::max());
			} catch (const std::bad_array_new_length &) {
				overflow = true;
			}
			check(overflow && counts.calls == before && counts.used == 0);
			resource->fail = true;
			bool failed = false;
			try {
				allocator.allocate(1);
			} catch (const std::bad_alloc &) {
				failed = true;
			}
			check(failed && counts.used == 0);
			resource->fail = false;
			struct alignas(64) Aligned {
				char bytes[64];
			};
			c::OwnedAllocator<Aligned> aligned(context);
			auto raw = aligned.allocate(2);
			check(reinterpret_cast<std::uintptr_t>(raw) % 64 == 0 && counts.used == 128);
			aligned.deallocate(raw, 2);
			struct Failing {
				Failing() { throw std::runtime_error("constructor failure"); }
			};
			failed = false;
			try {
				(void)std::allocate_shared<Failing>(c::OwnedAllocator<Failing>(context));
			} catch (const std::runtime_error &) {
				failed = true;
			}
			check(failed && counts.used == 0);
			{
				std::vector<int, c::OwnedAllocator<int>> values(allocator);
				for (int i = 0; i < 1000; ++i)
					values.push_back(i);
				check(counts.used == values.capacity() * sizeof(int) && counts.peak > counts.used);
				auto moved = std::move(values);
				check(moved.size() == 1000 && moved[999] == 999);
			}
			check(counts.used == 0);
			{
				using String = std::basic_string<char, std::char_traits<char>, c::OwnedAllocator<char>>;
				String text(200, 'x', c::OwnedAllocator<char>(context));
				check(counts.used >= text.size() + 1 && text[199] == 'x');
			}
			check(counts.used == 0);
		}
		check(counts.destroyed && counts.used == 0);
		Counts retained;
		std::weak_ptr<int> weak;
		{
			auto resource = std::make_shared<Resource>(retained);
			auto owner =
				std::allocate_shared<int>(c::OwnedAllocator<int>(c::AllocationContext(resource)), 42);
			weak = owner;
			resource.reset();
			std::jthread releaser([held = std::move(owner)]() mutable { held.reset(); });
		}
		check(weak.expired() && !retained.destroyed && retained.used > 0);
		weak.reset();
		check(retained.destroyed && retained.used == 0 && retained.calls == retained.released);
		std::cout << "owned resources, weak controls, failure and alignment passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
