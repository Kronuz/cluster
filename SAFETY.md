# Legacy Raft safety corrections

The legacy `cluster::Raft` API and message field encodings remain compatible with Xapiand. Safety corrections are deliberate changes after the faithful extraction. This module remains volatile: restarting a voter loses its term, vote, and log. It does not provide durable acknowledgements or safe voting membership changes.

## Replication progress

Empty heartbeats verify only their preceding log prefix. They cannot acknowledge an entry at `prev_log_index + 1`, advance commitment into an unverified suffix, or substitute for data replication acknowledgements. A new leader sends its initial empty probe as `HEARTBEAT` with the actual log end. Retransmitted matching data entries receive fresh acknowledgements after response loss.

Leaders ignore heartbeat match claims as a compatibility policy because older followers report inflated progress. Data progress cannot exceed the leader's log and excludes self responses. Successful data acknowledgements are monotonic until a rejection invalidates that peer's evidence: legacy followers can restart under the same identity with an empty log. Rejections permit prefix replay without lowering the committed index. Heartbeat responses schedule replication from the last acknowledged data prefix. This conservative choice may replay more retained log entries after an election; the legacy log has no checkpoint protocol.

`test/raft_safety_test.cc` drives fixed-membership message schedules through the public API. Its initial election uses a short timer because `PeriodicTimer::again()` with a zero repeat cancels the timer; subsequent timeouts are one hour and message deliveries are controlled by the test. `cluster_raft_safety` runs active checks in Release builds.

## Upgrade boundary

The seven logical message types and their field encodings are unchanged. Old followers can decode the new initial heartbeat. New leaders defend against old heartbeat match claims, but old leaders retain the original safety defects. Frame compatibility is not proof of safe mixed-version consensus. Upgrade all voters before relying on corrected behavior, and validate the application's discovery/bootstrap and leadership policies separately.

An additive strict consensus module will provide durable state, fixed voting configuration, point-to-point authenticated transport, and explicit persistence completions. It must use a separate protocol and voting group. Its production guarantees require deterministic invariant checks and storage/crash qualification; they do not follow from the legacy integration tests.
