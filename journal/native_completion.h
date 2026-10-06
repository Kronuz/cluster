#pragma once
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
