#pragma once

// Compatibility include; definitions remain canonical in the standalone module.
#if defined(__linux__)
#include "linux_completion.h"
#else
#include "bsd_completion.h"
#endif
#include "../completion/native_completion.h"
