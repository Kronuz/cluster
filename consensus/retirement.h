#pragma once

#include "types.h"
#include <algorithm>
#include <type_traits>

namespace cluster::consensus::detail {
// One maximum legal payload fits a fresh budget. Counts bound empty entries
// and actions too; callers retain unfinished containers between owner turns.
struct RetirementBudget {
	std::size_t units = 256, bytes = 1024 * 1024;
	bool consume(std::size_t payload = 0) noexcept {
		if (!units || payload > bytes) {
			return false;
		}
		--units;
		bytes -= payload;
		return true;
	}
};
inline bool retire_entries(std::vector<Entry> &entries, RetirementBudget &budget) noexcept {
	while (!entries.empty()) {
		if (!budget.consume(entries.back().payload.size())) {
			return false;
		}
		entries.pop_back();
	}
	return true;
}
inline bool retire_action(Action &action, RetirementBudget &budget) noexcept {
	return std::visit(
		[&](auto &value) {
			using T = std::decay_t<decltype(value)>;
			if constexpr (std::is_same_v<T, Persist>) {
				if (value.batch.log && !retire_entries(value.batch.log->entries, budget)) {
					return false;
				}
			} else if constexpr (std::is_same_v<T, Committed>) {
				if (!retire_entries(value.entries, budget)) {
					return false;
				}
			} else if constexpr (std::is_same_v<T, PersistCheckpoint> || std::is_same_v<T, PersistInstall>) {
				if (!retire_entries(value.state.entries, budget)) {
					return false;
				}
			} else if constexpr (std::is_same_v<T, Send>) {
				if (auto request = std::get_if<AppendRequest>(&value.message);
					request && !retire_entries(request->entries, budget)) {
					return false;
				}
			}
			return budget.consume();
		},
		action);
}
inline bool retire_actions(Actions &actions, RetirementBudget &budget) noexcept {
	while (!actions.empty()) {
		if (!retire_action(actions.back(), budget)) {
			return false;
		}
		actions.pop_back();
	}
	return true;
}
} // namespace cluster::consensus::detail
