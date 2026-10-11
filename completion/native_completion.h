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
