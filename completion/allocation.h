#pragma once
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <type_traits>
#include <utility>

namespace kronuz::io::completion {
// Empty contexts preserve process-lifetime new/delete behavior. Managed
// contexts retain their resource through every allocating owner and weak
// shared control, independently of the facade that created the resource.
class AllocationContext {
  public:
	AllocationContext() noexcept = default;
	explicit AllocationContext(std::shared_ptr<std::pmr::memory_resource> resource) noexcept
		: resource_(std::move(resource)) {}
	std::pmr::memory_resource *resource() const noexcept {
		return resource_ ? resource_.get() : std::pmr::new_delete_resource();
	}
	bool managed() const noexcept { return static_cast<bool>(resource_); }

  private:
	std::shared_ptr<std::pmr::memory_resource> resource_;
};

template <class T> class OwnedAllocator {
  public:
	using value_type = T;
	using propagate_on_container_copy_assignment = std::true_type;
	using propagate_on_container_move_assignment = std::true_type;
	using propagate_on_container_swap = std::true_type;
	using is_always_equal = std::false_type;
	explicit OwnedAllocator(AllocationContext context = {}) noexcept : context_(std::move(context)) {}
	template <class U> OwnedAllocator(const OwnedAllocator<U> &other) noexcept : context_(other.context_) {}
	T *allocate(std::size_t count) {
		if (count > std::numeric_limits<std::size_t>::max() / sizeof(T))
			throw std::bad_array_new_length();
		return static_cast<T *>(context_.resource()->allocate(count * sizeof(T), alignof(T)));
	}
	void deallocate(T *pointer, std::size_t count) noexcept {
		context_.resource()->deallocate(pointer, count * sizeof(T), alignof(T));
	}
	template <class U> bool operator==(const OwnedAllocator<U> &other) const noexcept {
		return context_.resource() == other.context_.resource();
	}
	const AllocationContext &context() const noexcept { return context_; }

  private:
	AllocationContext context_;
	template <class> friend class OwnedAllocator;
};
} // namespace kronuz::io::completion
