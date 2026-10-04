#pragma once

#include "storage.h"
#include "../journal/admission.h"
#include <array>
#include <type_traits>

namespace cluster::consensus {

struct AppendBudget {
	kronuz::journal::AdmissionClass kind = kronuz::journal::AdmissionClass::Control;
	std::size_t encoded_bytes = 0;
};
struct EventAdmissionPlan {
	std::array<AppendBudget, 3> appends{};
	std::size_t count = 0;
};

// Conservative continuation packs for the Tick-only campaign Core. Planning
// neither reserves storage nor changes protocol state. The worker rejects
// external completions and owns checkpoint capture through a separate API.
inline EventAdmissionPlan plan_event(const Event& event, bool busy, Limits limits = {}) {
	using kronuz::journal::AdmissionClass;
	EventAdmissionPlan plan;
	auto add = [&](AdmissionClass kind, std::size_t bytes) { plan.appends.at(plan.count++) = {kind, bytes}; };
	auto hard = storage_batch_size(true, false);
	auto noop = storage_batch_size(false, true, 1);
	std::visit([&](const auto& value) {
		using T = std::decay_t<decltype(value)>;
		if constexpr (std::is_same_v<T, Persisted> || std::is_same_v<T, LocalCheckpoint> ||
			std::is_same_v<T, Applied> || std::is_same_v<T, StorageFault> || std::is_same_v<T, Failed> ||
			std::is_same_v<T, InstallPrepared> || std::is_same_v<T, InstallActivated> || std::is_same_v<T, InstallActivationFailed> || std::is_same_v<T, SnapshotSourceReady> || std::is_same_v<T, SnapshotTransferFailed>) {
			throw std::invalid_argument("internal worker event cannot be submitted");
		} else if constexpr (std::is_same_v<T, Tick>) {
			if (!busy) { add(AdmissionClass::Control, hard); add(AdmissionClass::Control, noop); add(AdmissionClass::Control, hard); }
		} else if constexpr (std::is_same_v<T, Propose>) {
			if (value.command.size() > limits.command_bytes) { throw std::length_error("proposal exceeds admission bound"); }
			if (!busy) {
				add(AdmissionClass::Normal, storage_batch_size(false, true, 1, value.command.size()));
				add(AdmissionClass::Control, hard);
			}
		} else if constexpr (std::is_same_v<T, Receive>) {
			std::visit([&](const auto& message) {
				using M = std::decay_t<decltype(message)>;
				if constexpr (std::is_same_v<M, AppendRequest>) {
					if (message.entries.size() > limits.rpc_entries) { throw std::length_error("RPC entry admission bound"); }
					std::size_t payload = 0;
					bool command = false;
					for (const auto& entry : message.entries) {
						if (entry.payload.size() > limits.command_bytes || payload > limits.rpc_bytes || entry.payload.size() > limits.rpc_bytes - payload ||
							(entry.kind != EntryKind::Command && (entry.kind != EntryKind::NoOp || !entry.payload.empty()))) {
							throw std::length_error("RPC payload admission bound");
						}
						payload += entry.payload.size(); command |= entry.kind == EntryKind::Command;
					}
					if (!busy) { add(command ? AdmissionClass::Normal : AdmissionClass::Control, storage_batch_size(true, true, message.entries.size(), payload)); }
				} else if constexpr (std::is_same_v<M, VoteResponse>) {
					if (!busy) { add(AdmissionClass::Control, noop); add(AdmissionClass::Control, hard); }
				} else if constexpr (std::is_same_v<M, VoteRequest> || std::is_same_v<M, AppendResponse> || std::is_same_v<M, SnapshotResponse>) {
					if constexpr (std::is_same_v<M, SnapshotResponse>) { if (!valid_snapshot_response(message)) { throw std::invalid_argument("invalid snapshot response"); } }
					if (!busy) { add(AdmissionClass::Control, hard); }
				} else {
					static_assert(std::is_same_v<M, void>, "new consensus message requires an admission plan");
				}
			}, value.message);
		} else if constexpr (std::is_same_v<T, Start> || std::is_same_v<T, Read>) {
			// These events emit no persistence action.
		} else {
			static_assert(std::is_same_v<T, void>, "new consensus event requires an admission plan");
		}
	}, event);
	return plan;
}

} // namespace cluster::consensus
