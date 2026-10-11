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
