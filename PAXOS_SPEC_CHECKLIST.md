# Paxos Specification Compliance Checklist

## Core Paxos Safety Properties

### Property 1: Safety - Only one value can be chosen for each decree
- **Spec**: For a given decree, if a value is chosen, no other value can be chosen for that decree
- **Tests covering this**:
  - `conflicting_proposals_higher_ballot_wins` - Only highest ballot value accepted
  - `acceptor_returns_previous_accepted_value` - Value persists across ballots
  - `multiple_concurrent_proposals_same_decree` - Only highest ballot succeeds
  - **MISSING**: Test that once a value is learned (quorum accepts it), no other value can be chosen

### Property 2: Acceptor Promise Invariant
- **Spec**: Once an acceptor promises ballot B, it must not promise any ballot < B for the same decree
- **Tests covering this**:
  - `acceptor_rejects_lower_ballot_prepare` ✓
  - `acceptor_monotonic_promise_progression` ✓
  - `acceptor_ballot_monotonicity_within_decree` ✓

### Property 3: Acceptor Accept Invariant  
- **Spec**: An acceptor accepts Accept(b,v) for decree d only if it has promised ballot >= b for decree d
- **Tests covering this**:
  - `acceptor_rejects_accept_below_min_ballot` ✓
  - `acceptor_accepts_accept_at_min_ballot` ✓
  - `acceptor_accepts_accept_above_min_ballot` ✓
  - `accept_before_prepare_same_decree` - **FAILING** (should reject Accept without prior promise)

### Property 4: Value Adoption (Critical for Safety)
- **Spec**: If a proposer receives promises where some acceptor has accepted value V in ballot B, proposer MUST propose the value from the highest such ballot
- **Tests covering this**:
  - `proposer_adopts_previously_accepted_value` ✓
  - `proposer_picks_highest_accepted_ballot` ✓
  - `proposer_value_adoption_invariant` ✓
  - `promise_reports_higher_accepted_ballot` ✓

### Property 5: Immutability of Accepted Values
- **Spec**: Once an acceptor accepts a value at ballot B, it cannot accept a different value at ballot B
- **Tests covering this**:
  - `accept_with_different_value_than_proposed` - **FAILING** (acceptor overwrites value)
  - **MISSING**: Duplicate Accept should be idempotent

## Paxos Phase Requirements

### Phase 1: Prepare (Proposer -> Acceptor)
- **Spec**: Proposer sends Prepare with ballot number, acceptor responds with Promise
- **Tests covering this**:
  - `proposer_issues_prepare_with_correct_ballot` ✓
  - `basic_paxos_flow` ✓
  - All acceptor ballot tests ✓

### Phase 2: Accept (Proposer -> Acceptor)
- **Spec**: Proposer sends Accept only after collecting quorum of Promises
- **Tests covering this**:
  - `proposer_sends_accept_on_promise` ✓ (quorum=1)
  - `proposer_with_insufficient_promises` - **FAILING** (doesn't send Accept at quorum)
  - **MISSING**: Test with quorum > 1, verify Accept sent at exactly quorum size

### Phase 3: Learn (Acceptor -> Learner)
- **Spec**: Acceptor sends Accepted to learners after accepting
- **Tests covering this**:
  - `learner_receives_accepted_values` ✓
  - `learner_learns_multiple_decrees` ✓
  - `learner_consensus_from_all_acceptors` ✓

## Multi-Decree Requirements

### Independent Decree Handling
- **Spec**: Each decree should have independent ballot state across acceptors, proposers, learners
- **Tests covering this**:
  - `proposer_can_track_multiple_decrees` ✓
  - `acceptor_can_accept_multiple_decrees` ✓
  - `acceptor_decree_independence_for_ballots` ✓
  - `concurrent_decrees_dont_interfere` ✓

## Edge Cases & Robustness

### Out-of-Order Messages
- **Spec**: Paxos must handle messages arriving out of order
- **Tests covering this**:
  - `learner_out_of_order_accepted` ✓
  - `out_of_order_promise_after_accept` ✓ (handles gracefully)
  - **MISSING**: Accept arrives before Prepare (should NACK) - currently failing

### Ballot Ordering (Tie-Breaking)
- **Spec**: Ballots are (round, node_id) pairs, compared lexicographically
- **Tests covering this**:
  - `tie_breaking_same_round_higher_node_id_wins` ✓
  - `tie_breaking_lower_node_id_rejected_same_round` ✓
  - `ballot_ordering_complete_comparisons` ✓
  - `tie_breaking_affects_accept_phase` ✓

### Duplicate Handling
- **Spec**: Paxos must handle duplicate messages safely
- **Tests covering this**:
  - `duplicate_prepare_messages` ✓ (rejects equal ballot)
  - `duplicate_accept_messages` ✓

## MISSING CRITICAL TESTS

1. **Quorum Safety**: 
   - Test that quorum determines consensus, not just any acceptor
   - Test 3-node cluster: 1 down, 2 forming quorum
   - Test 5-node cluster: 2 down, 3 forming quorum

2. **Value Immutability After Acceptance**:
   - Acceptor must never change accepted value at same ballot
   - Currently **FAILING** - acceptor overwrites value

3. **Promise Before Accept**:
   - Acceptor must reject Accept without prior Prepare/Promise
   - Currently **FAILING** - acceptor accepts without promise

4. **Quorum-Driven Accept**:
   - Proposer must wait for quorum before sending Accept
   - Currently **FAILING** - doesn't send Accept at quorum threshold

5. **Consensus Detection**:
   - **MISSING**: Test that learner correctly detects when quorum has accepted
   - Ledger needs `get_chosen_value()` method

6. **Ballot Monotonicity**:
   - Acceptor state validation (accepted_ballot <= min_ballot)
   - **MISSING**: Explicit invariant enforcement tests

7. **Partition Scenarios**:
   - Majority partition can make progress
   - Minority partition cannot
   - **MISSING**: Requires cluster implementation

## Test Status Summary

**Passing Tests**: ~59  
**Failing Tests**: 3  
  - `accept_before_prepare_same_decree`
  - `accept_with_different_value_than_proposed`
  - `proposer_with_insufficient_promises`

**Critical Issues Found**:
1. Acceptor doesn't validate promise requirement before accept
2. Acceptor overwrites accepted values at same ballot
3. Proposer doesn't send Accept at quorum threshold

These failures expose real bugs in the implementation that violate Paxos safety properties.
