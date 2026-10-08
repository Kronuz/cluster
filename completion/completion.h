#pragma once
#include "operation.h"
#include <array>

namespace kronuz::journal {
struct CompletionStats {
	std::uint64_t native_submitted = 0, native_completed = 0, native_rejected = 0;
	std::uint64_t fallback_submitted = 0, fallback_completed = 0;
	std::uint64_t file_write_bytes = 0, file_read_bytes = 0;
	std::uint64_t notification_errors = 0, ring_errors = 0;
	std::uint64_t maximum_outstanding = 0;
	std::array<std::uint64_t, 12> native_by_primitive{}, fallback_by_primitive{};
	// Keep legacy array indices/types stable. Additive primitives have their
	// own counters; drivers validate the kind before accepting an original.
	std::array<std::uint64_t, 11> native_extensions{}, fallback_extensions{};
	static bool supported(PrimitiveKind kind) noexcept {
		return static_cast<std::size_t>(kind) <= static_cast<std::size_t>(PrimitiveKind::CreateInto);
	}
	void submitted(PrimitiveKind kind, bool native) noexcept {
		auto index = static_cast<std::size_t>(kind);
		auto &legacy = native ? native_by_primitive : fallback_by_primitive;
		auto &extended = native ? native_extensions : fallback_extensions;
		if (index < legacy.size()) {
			++legacy[index];
		} else if (index - legacy.size() < extended.size()) {
			++extended[index - legacy.size()];
		}
	}
};
struct OwnedCompletion {
	std::shared_ptr<IOOperation> operation;
	MutationCompletion completion;
};

} // namespace kronuz::journal
