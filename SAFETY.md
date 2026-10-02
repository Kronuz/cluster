# Legacy Raft safety corrections

The legacy `cluster::Raft` API and message field encodings remain compatible with Xapiand. Safety corrections are deliberate changes after the faithful extraction. This module remains volatile: restarting a voter loses its term, vote, and log. It does not provide durable acknowledgements or safe voting membership changes.

## Replication progress

Empty heartbeats verify only their preceding log prefix. They cannot acknowledge an entry at `prev_log_index + 1`, advance commitment into an unverified suffix, or substitute for data replication acknowledgements. A new leader sends its initial empty probe as `HEARTBEAT` with the actual log end. Retransmitted matching data entries receive fresh acknowledgements after response loss.

Leaders ignore heartbeat match claims as a compatibility policy because older followers report inflated progress. Data progress cannot exceed the leader's log and excludes self responses. Successful data acknowledgements are monotonic until a rejection invalidates that peer's evidence: legacy followers can restart under the same identity with an empty log. Rejections permit prefix replay without lowering the committed index. Heartbeat responses schedule replication from the last acknowledged data prefix. This conservative choice may replay more retained log entries after an election; the legacy log has no checkpoint protocol.

`test/raft_safety_test.cc` drives fixed-membership message schedules through the public API. Its initial election uses a short timer because `PeriodicTimer::again()` with a zero repeat cancels the timer; subsequent timeouts are one hour and message deliveries are controlled by the test. `cluster_raft_safety` runs active checks in Release builds.

## Election majority

A candidate requires affirmative votes from a quorum of the configured membership. A quorum of responses followed by more grants than denials is insufficient: two grants and one denial previously elected a leader in five voters. Duplicate voter responses cannot increase the affirmative count. The fixed-membership regression proves both the denied election and the successful election after a third distinct grant.

## Vote lifetime

Votes belong to terms, not roles. A candidate records its self-vote before broadcasting, and same-term candidate-to-follower, leader-to-follower, or explicit step-down transitions retain that vote. A higher term permits a new vote. The regression schedules delay self-loopback, deliver same-term leader traffic, and request another candidate's vote before checking a new-term vote.

## Inherited log entries

A new leader retains the log it inherited. An entry from an earlier term may already have been acknowledged by the previous leader's majority even when the successor has not learned its commit index. Appending a new command cannot truncate that prefix. Replicating a current-term entry commits the preceding prefix in order. Whole-command deduplication remains unchanged for Xapiand compatibility.

The regression models an old leader replicating an entry to the successor and crashing before communicating its committed index. The surviving fixed majority elects the successor in the next term; a newly replicated command commits both entries in order. The schedule failed before removal of leader-side suffix truncation.

## Term handling and message validation

Complete message decoding precedes Raft-state changes. A valid higher term is observed before role filtering, clearing the previous ballot, replication bookkeeping, and advertised leader. Tested truncated messages and unrepresentable preceding indexes do not depose a leader. Encoded-integer overflow in the shared length decoder is tracked separately until its correction lands. A missing vote-response ballot is distinct from the existing empty-node encoding. The injected node parser may still touch membership while decoding; its contract is unchanged.

Stale requests are rejected using the receiver's current term without starting elections or resetting election timeouts. Same-term valid leader traffic retains the ballot while stepping a candidate down. Response field encodings remain unchanged. Deterministic regressions cover higher-term requests and responses in different roles, stale traffic, response terms, malformed fields, and index overflow.

## Upgrade boundary

The seven logical message types and their field encodings are unchanged. Old followers can decode the new initial heartbeat. New leaders defend against old heartbeat match claims, but old leaders retain the original safety defects. Frame compatibility is not proof of safe mixed-version consensus. Upgrade all voters before relying on corrected behavior, and validate the application's discovery/bootstrap and leadership policies separately.

An additive strict consensus module will provide durable state, fixed voting configuration, point-to-point authenticated transport, and explicit persistence completions. It must use a separate protocol and voting group. Its production guarantees require deterministic invariant checks and storage/crash qualification; they do not follow from the legacy integration tests.
