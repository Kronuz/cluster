/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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
