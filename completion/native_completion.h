#pragma once
#include "allocation.h"
#if defined(__linux__)
#include "linux_completion.h"
namespace kronuz::journal {
using NativeCompletionQueue = LinuxCompletionQueue;
}
#else
#include "bsd_completion.h"
namespace kronuz::journal {
using NativeCompletionQueue = BsdCompletionQueue;
}
#endif

namespace kronuz::io::completion {
using NativeQueue = kronuz::journal::NativeCompletionQueue;
template <class... Args>
std::shared_ptr<NativeQueue> make_native_queue(AllocationContext context, Args &&...args) {
	return std::allocate_shared<NativeQueue>(OwnedAllocator<NativeQueue>(context),
											 std::forward<Args>(args)...);
}
} // namespace kronuz::io::completion
