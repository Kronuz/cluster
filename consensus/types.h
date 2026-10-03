#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace cluster::consensus {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using Index = std::uint64_t;
using Token = std::uint64_t;
using RequestId = std::uint64_t;
using Identity = std::array<char, 16>;

struct FixedConfiguration {
	Identity cluster{}, configuration{};
	NodeId local = 0;
	std::vector<NodeId> voters;
	bool operator==(const FixedConfiguration&) const = default;
};
struct HardState {
	Term term = 0;
	std::optional<NodeId> voted_for;
	Index commit_index = 0;
	bool operator==(const HardState&) const = default;
};
enum class EntryKind : std::uint8_t { Command, NoOp };
struct Entry {
	Index index = 0;
	Term term = 0;
	EntryKind kind = EntryKind::Command;
	std::string payload;
	bool operator==(const Entry&) const = default;
};
struct RecoveredState {
	FixedConfiguration configuration;
	HardState hard;
	Index base_index = 0;
	Term base_term = 0;
	std::vector<Entry> entries;
	Index applied_index = 0;
};
struct LogMutation { Index replace_from; std::vector<Entry> entries; };
struct StorageBatch {
	std::optional<HardState> hard;
	std::optional<LogMutation> log;
};
struct Limits {
	std::size_t voters = 7;
	std::size_t command_bytes = 1024 * 1024;
	std::size_t log_bytes = 64 * 1024 * 1024;
	std::size_t log_entries = 65536;
	std::size_t control_entries = 16;
	std::size_t rpc_bytes = 2 * 1024 * 1024;
	std::size_t rpc_entries = 256;
	std::size_t reads = 64;
	std::size_t uncommitted_entries = 1024;
};
struct Timing {
	std::uint64_t heartbeat = 20, rpc_timeout = 40;
	std::uint64_t election_min = 100, election_max = 200;
};

struct VoteRequest { Term term; Index last_index; Term last_term; };
struct VoteResponse { Term term; bool granted; };
struct AppendRequest {
	Term term;
	Token rpc;
	Index previous;
	Term previous_term;
	Index commit;
	Token read_probe;
	std::vector<Entry> entries;
};
struct LogBoundary { Index index; Term term; };
struct AppendResponse {
	Term term;
	Token rpc;
	bool success;
	Index matched;
	Index next_hint;
	Token read_probe;
	std::optional<LogBoundary> compacted{};
};
using Message = std::variant<VoteRequest, VoteResponse, AppendRequest, AppendResponse>;

struct Start {};
// The host supplies absolute monotonic time and a fresh randomized election
// delay within Timing's bounds. Deliver a Tick before completions after I/O.
struct Tick { std::uint64_t now; std::uint64_t election_delay; };
struct Receive { NodeId authenticated_peer; Message message; };
struct Propose { RequestId request; std::string command; };
struct Read { RequestId request; };
struct Persisted { Token token; };
struct Applied { Index through; };
// The host owns the immutable application capture identified by capture.
struct LocalCheckpoint { RequestId request; Token capture; Index through; Term term; Identity cluster, configuration; };
// Trusted adapter: prepared identifies a staged, semantically validated image.
struct InstallPrepared {
	RequestId request; Token prepared; NodeId authenticated_peer; Term leader_term;
	Identity cluster, configuration; LogBoundary boundary;
};
struct InstallActivated { Token token; };
struct InstallActivationFailed { Token token; std::string error; };
struct StorageFault { std::string error; };
enum class FailureSource { Storage, Application };
struct Failed { FailureSource source; Token token; std::string error; };
using Event = std::variant<Start, Tick, Receive, Propose, Read, Persisted, Applied, Failed, LocalCheckpoint, StorageFault, InstallPrepared, InstallActivated, InstallActivationFailed>;

enum class Role { Follower, Candidate, Leader, Fenced };
enum class RejectReason { Busy, NotLeader, NotReady, LogFull, TooLarge, DuplicateRequest, InvalidCheckpoint };
struct Persist { Token token; StorageBatch batch; };
struct PersistCheckpoint { Token token; Token capture; RecoveredState state; };
struct PersistInstall { Token token, prepared; RecoveredState state; };
struct ActivateInstall { Token token, prepared; LogBoundary boundary; };
enum class InstallRejectReason { Busy, Invalid, StaleTerm, CaughtUp };
struct InstallRejected { RequestId request; Token prepared; InstallRejectReason reason; };
struct InstallCompleted { RequestId request; Token prepared; LogBoundary boundary; };
struct CheckpointPublished { RequestId request; Index through; };
struct SnapshotNeeded { NodeId peer; Index through; Term term; };
struct Send { NodeId peer; Message message; };
struct ProposalPlaced { RequestId request; Term term; Index index; };
struct Committed { Index first; std::vector<Entry> entries; };
struct ReadReady { RequestId request; Index index; };
struct RoleChanged { Role role; Term term; NodeId leader; };
struct Reject { RequestId request; RejectReason reason; };
struct Fenced { std::string reason; };
using Action = std::variant<Persist, Send, ProposalPlaced, Committed, ReadReady, RoleChanged, Reject, Fenced, PersistCheckpoint, CheckpointPublished, SnapshotNeeded, PersistInstall, ActivateInstall, InstallRejected, InstallCompleted>;
using Actions = std::vector<Action>;

} // namespace cluster::consensus
