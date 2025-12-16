# Paxos Implementation Status & Testing Guide

## Current Implementation Overview

The codebase implements **single-decree Paxos** based on the original Lamport paper (not Multi-Paxos).

### Architecture
- **Proposer**: Phase 1 sends `Prepare`, collects `Promise` messages, Phase 2 broadcasts `Accept` when quorum reached
- **Acceptor**: Phase 1 responds with `Promise` if ballot is higher; Phase 2 accepts if ballot meets min_ballot
- **Learner**: Records `Accepted` messages, tracks voting by ballot
- **Ledger**: Tracks votes per decree, determines consensus via quorum
- **Cluster**: Routes messages between nodes via tokio mpsc channels

### Core Components

#### Messages
All messages now include `from` field for proper routing:
```rust
Prepare { from, decree_num, ballot }
Promise { from, decree_num, ballot, accepted_ballot, accepted_value }
Accept { from, decree_num, ballot, value }
Accepted { from, decree_num, ballot, value }
NACK
```

#### Ballot Ordering
- Lexicographic: `(number, node_id)` where number is round, node_id is tiebreaker

#### State Management
- **Acceptor per decree**: `min_ballot`, `accepted_ballot`, `accepted_value`
- **Proposer per decree**: `ballot`, `highest_seen_ballot`, `proposed_value`, `votes`, `chosen`
- **Ledger per decree**: Vote tracking by ballot number, quorum detection

## Completed Work

✅ Message routing fixed with `from` field
✅ Proposer Phase 2 waits for quorum before broadcasting Accept
✅ Proposer value adoption from higher accepted_ballots
✅ Tests reorganized into 9 focused test files
✅ Test helpers for clean scenario setup

## Test Coverage (66 tests total, 3 failing)

### Test Files & Organization

**acceptor_tests.rs** (7 tests)
- `acceptor_rejects_lower_ballot_prepare` ✓
- `acceptor_accepts_higher_ballot_prepare` ✓
- `acceptor_rejects_accept_below_min_ballot` ✓
- `acceptor_accepts_accept_at_min_ballot` ✓
- `acceptor_accepts_accept_above_min_ballot` ✓
- `acceptor_returns_previous_accepted_value` ✓
- `acceptor_handles_equal_ballot_prepare` ✓

**proposer_tests.rs** (5 tests)
- `proposer_issues_prepare_with_correct_ballot` ✓
- `proposer_sends_accept_on_promise` ✓
- `proposer_adopts_previously_accepted_value` ✓
- `proposer_ignores_promise_for_wrong_ballot` ✓
- `proposer_picks_highest_accepted_ballot` ✓

**learner_tests.rs** (3 tests)
- `learner_receives_accepted_values` ✓
- `learner_ignores_non_accepted_messages` ✓
- `learner_learns_multiple_decrees` ✓

**tie_breaking_tests.rs** (6 tests)
- `tie_breaking_same_round_higher_node_id_wins` ✓
- `tie_breaking_lower_node_id_rejected_same_round` ✓
- `ballot_ordering_complete_comparisons` ✓
- `proposer_ballot_ordering_from_proposer_perspective` ✓
- `acceptor_rejects_lower_node_id_when_already_promised_to_higher` ✓
- `tie_breaking_affects_accept_phase` ✓

**concurrent_decrees_tests.rs** (9 tests)
- `proposer_can_track_multiple_decrees` ✓
- `acceptor_can_accept_multiple_decrees` ✓
- `learner_learns_multiple_decrees` ✓
- `multiple_decrees_with_different_ballots` ✓
- `proposer_handles_promises_for_different_decrees` ✓
- `concurrent_decrees_dont_interfere` ✓
- `sequential_decrees_same_proposer` ✓
- `learner_ledger_tracks_concurrent_decrees` ✓
- `mixed_single_and_multi_decree_flow` ✓

**integration_tests.rs** (4 tests)
- `basic_paxos_flow` ✓
- `conflicting_proposals_higher_ballot_wins` ✓
- `observer_event_tracking` ✓
- `value_adoption_across_proposals` ✓

**state_validation_tests.rs** (11 tests)
- `acceptor_ballot_monotonicity_within_decree` ✓
- `acceptor_no_promise_downgrade_same_decree` ✓
- `acceptor_monotonic_promise_progression` ✓
- `acceptor_decree_independence_for_ballots` ✓
- `proposer_ballot_monotonicity_per_decree` ✓
- `proposer_same_ballot_different_decrees` ✓
- `acceptor_never_leaks_value_on_nack` ✓
- `proposer_value_adoption_invariant` ✓
- `proposer_accept_ballot_matches_promise` ✓
- `acceptor_accept_ballot_validation` ✓
- `concurrent_proposals_ballot_isolation` ✓

**edge_case_tests.rs** (13 tests - **3 FAILING**)
- `out_of_order_promise_after_accept` ✓
- `accept_before_prepare_same_decree` ❌ **FAILING** - Acceptor should reject Accept without Promise
- `duplicate_prepare_messages` ✓
- `duplicate_accept_messages` ✓
- `learner_out_of_order_accepted` ✓
- `proposer_with_insufficient_promises` ❌ **FAILING** - Proposer should send Accept at quorum
- `large_ballot_numbers` ✓
- `accept_with_different_value_than_proposed` ❌ **FAILING** - Acceptor should not change accepted value
- `proposer_promise_from_itself` ✓
- `multiple_concurrent_proposals_same_decree` ✓
- `sparse_decree_numbering` ✓
- `learner_consensus_from_all_acceptors` ✓
- `promise_reports_higher_accepted_ballot` ✓

**basic_paxos_test.rs** (3 tests)
- `test_basic_paxos_flow` ✓
- `test_acceptor_rejects_lower_ballot` ✓
- `test_proposer_adopts_previous_value` ✓

**test_helpers.rs** (4 test helper modules)
- `RecordingObserver` captures/filters events
- `NodeBuilder` factory pattern
- `ScenarioBuilder` for multi-node setups
- `QuorumCalc` math helpers

**multi_node_tests.rs** (1 placeholder)
- Cluster tests deferred until cluster implementation complete

## Hardening Progress

### ✅ Completed
- **Tie-breaking** (6 tests) - All ballot comparisons tested
- **Concurrent Decrees** (9 tests) - Multiple decrees proven independent
- **State Validation** (11 tests) - Invariant checking for ballot monotonicity
- **Edge Cases** (13 tests) - Out-of-order messages, sparse decrees, duplicates
- **Test Infrastructure** - RecordingObserver, NodeBuilder, ScenarioBuilder
- **Message Routing** - `from` field ensures correct delivery
- **Test Reorganization** - Split monolithic 1351-line file into 9 focused test files

### ❌ Critical Issues Found (3 Failing Tests)

#### 1. **accept_before_prepare_same_decree** - SAFETY VIOLATION
- **Issue**: Acceptor accepts without prior promise
- **Spec Requirement**: Acceptor must promise ballot before accepting at that ballot
- **Impact**: Violates Paxos Phase 1 guarantee
- **Fix Location**: Acceptor.handle_message() for Accept messages

#### 2. **accept_with_different_value_than_proposed** - SAFETY VIOLATION
- **Issue**: Acceptor overwrites accepted value at same ballot
- **Spec Requirement**: Once accepted at ballot B, value is immutable at ballot B
- **Impact**: Multiple different values could be considered "accepted" for same ballot
- **Fix Location**: Acceptor state management for accepted_value

#### 3. **proposer_with_insufficient_promises** - LIVENESS ISSUE
- **Issue**: Proposer doesn't send Accept when quorum is reached
- **Spec Requirement**: Proposer must send Accept after receiving quorum of promises
- **Impact**: Consensus cannot be reached even with sufficient promises
- **Fix Location**: Proposer promise handling logic, quorum calculation

### ⏳ In Progress / Coming Next

### 1. **Learner Consensus Detection** ❌
- Ledger tracks votes but doesn't expose "chosen" status
- Need `ledger.get_chosen_value(decree_num)` → Option<PaxosCommand>
- Website needs to know when consensus is reached
- **Impact**: Can't distinguish "in-progress" from "decided"

### 2. **Partition/Failure Scenarios** ❌
- No tests for:
  - Missing acceptors (minority partition)
  - Late-arriving promises after timeout
  - Acceptor state persistence/recovery
  - Message reordering/delays
- **Impact**: Unknown behavior under network faults

### 3. **Ledger Semantics** ⚠️
- Current `next()` returns `log.len()` assuming sequential decrees
- Doesn't handle gaps (what if decree 0 and 2 chosen, but 1 rejected?)
- **Impact**: Potential issues with decree numbering in retry scenarios

## Test Execution Summary

```
Total Tests: 66
Passing: 63 ✓
Failing: 3 ❌

Test File Breakdown:
- acceptor_tests.rs: 7/7 passing
- proposer_tests.rs: 5/5 passing
- learner_tests.rs: 3/3 passing
- tie_breaking_tests.rs: 6/6 passing
- concurrent_decrees_tests.rs: 9/9 passing
- integration_tests.rs: 4/4 passing
- state_validation_tests.rs: 11/11 passing
- edge_case_tests.rs: 10/13 passing (3 failing - see Critical Issues)
- basic_paxos_test.rs: 3/3 passing
- test_helpers.rs: 4/4 passing
- multi_node_tests.rs: 1/1 passing
```

## Events for Website Visualization

Events are captured but need review:

```rust
Event::Proposal { id, decree_num, value }      // Proposer initiates
Event::Promise { id, decree_num, ballot }       // Acceptor promises
Event::Accept { id, decree_num, ballot, value } // Acceptor accepts
Event::Learn { id, decree_num, value }          // Learner learns (but no consensus signal!)
```

**Gap**: Event::Learn fires for every Accepted message, not when consensus is reached.

## Key Insights from Testing

### Concurrent Decrees Architecture
Each decree_num maintains completely independent state:
- **Acceptor**: Separate `min_ballot` and `accepted_value` per decree
- **Proposer**: Separate ballot, highest_seen_ballot, proposed_value per decree
- **Learner**: Tracks votes per decree independently
- **Ballot progression**: Each decree can advance to higher ballots independently

Example: Decree 0 at ballot (5,1) can move to (6,1) while decree 1 still at (5,2). No interference.

### Tie-Breaking Fully Tested
Ballot ordering is lexicographic: (number, node_id)
- Lower number always loses
- Same number: lower node_id loses
- Tested from proposer and acceptor perspectives
- All comparison combinations verified

## Roadmap to Hardening (Pre-Retry)

**Priority 1 (BLOCKING - Fix Critical Bugs)**

1. **Fix Acceptor Promise Validation** (HIGH) 
   - Issue: Accept without prior Promise breaks Phase 1
   - Fix: Validate min_ballot is set before accepting
   - Tests: `accept_before_prepare_same_decree`
   - Estimated effort: Low

2. **Fix Acceptor Value Immutability** (HIGH)
   - Issue: Acceptor overwrites accepted value at same ballot
   - Fix: Once value accepted at ballot, don't overwrite
   - Tests: `accept_with_different_value_than_proposed`
   - Estimated effort: Low

3. **Fix Proposer Quorum Logic** (HIGH)
   - Issue: Doesn't send Accept when quorum reached
   - Fix: Send Accept immediately upon reaching quorum
   - Tests: `proposer_with_insufficient_promises`
   - Estimated effort: Medium

**Priority 2 (AFTER BUG FIXES)**

4. **Consensus Detection** - Make ledger expose chosen values
   - Required for: website to show decided decrees
   - Estimated effort: Low (add method to Ledger)

5. **Partition Tests** - Test majority/minority scenarios
   - Required for: understanding quorum safety
   - Scenarios: 3-node with 1 down, 5-node with 2 down
   - Estimated effort: Medium (use ScenarioBuilder)

6. **Ledger Gap Handling** - Support sparse decree numbering
   - Required for: retry scenarios with gaps
   - Current issue: `next()` breaks with gaps
   - Estimated effort: Medium (redesign decree tracking)

## Website Requirements

- **Single node**: Show one proposer/acceptor/learner loop
- **3-5 nodes**: Show all nodes, consensus via quorum
- **10 nodes**: Show scalability, minority rejections
- **Event timeline**: Replay all events from any node's perspective
- **State view**: Show each node's current ballot, accepted values, learned values

## For Retries (Future)

Once hardened:
- Proposer NACKs will trigger ballot increment
- New Prepare with (ballot+1, node_id)
- Same decree_num will be retried
- Ledger.next() returns next unclaimed decree for initial proposals
