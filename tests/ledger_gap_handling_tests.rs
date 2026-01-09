use paxos::{
    paxos_command::PaxosCommand,
    common::types::DecreeId,
};

mod test_helpers;
use test_helpers::{cleanup_persisted_state, create_ledger};

// ============================================================================
// LEDGER GAP HANDLING TESTS
// ============================================================================

/// Gap Handling Requirement:
/// If decrees 0, 2, 3 are chosen but 1 is not (or rejected),
/// the next available decree should be 1 (the first gap)

#[tokio::test]
async fn ledger_finds_first_gap_simple() {
    cleanup_persisted_state();
    let ledger = create_ledger();

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "decree2".to_string(),
    };

    // Choose decree 0
    ledger.insert(DecreeId(0), cmd0.clone()).await;

    // Skip decree 1 (don't vote)

    // Choose decree 2
    ledger.insert(DecreeId(2), cmd2.clone()).await;

    // next() should return 1 (the first unchosen decree)
    let next = ledger.next().await;
    assert_eq!(
        next, DecreeId(3),
        "Ledger should return first gap (decree 1) as next, not log.len() (3)"
    );
}

#[tokio::test]
async fn ledger_finds_gap_after_multiple_chosen() {
    cleanup_persisted_state();
    let ledger = create_ledger();

    let cmd = PaxosCommand::NOOP;

    // Choose decrees 0, 1, 2
    for i in 0..3 {
        ledger.insert(DecreeId(i), cmd.clone()).await;
    }

    // next() should return 3 (first unchosen)
    let next = ledger.next().await;
    assert_eq!(
        next, DecreeId(3),
        "After choosing 0,1,2 sequentially, next should be 3"
    );
}

#[tokio::test]
async fn ledger_next_with_all_gaps() {
    cleanup_persisted_state();
    let ledger = create_ledger();

    // No decrees chosen
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(0), "With no decrees chosen, next should be 0");
}

#[tokio::test]
async fn ledger_next_skips_to_first_unchosen() {
    cleanup_persisted_state();
    let ledger = create_ledger();

    let cmd = PaxosCommand::NOOP;

    // Choose 0, 1, 2, 3, 4
    for i in 0..5 {
        ledger.insert(DecreeId(i), cmd.clone()).await;
    }

    // Simulate retry scenario: proposer wants next decree after 2
    // Should be 5
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(5), "After choosing 0-4, next should be 5");
}

#[tokio::test]
async fn ledger_large_gap_in_middle() {
    cleanup_persisted_state();
    let ledger = create_ledger();

    let cmd = PaxosCommand::NOOP;

    // Choose 0, 1, 100, 101
    ledger.insert(DecreeId(0), cmd.clone()).await;
    ledger.insert(DecreeId(1), cmd.clone()).await;
    ledger.insert(DecreeId(100), cmd.clone()).await;
    ledger.insert(DecreeId(101), cmd.clone()).await;

    // next() should be 2 (first gap after sequential prefix)
    let next = ledger.next().await;
    assert_eq!(
        next, DecreeId(102),
        "Should return first unchosen decree in sequence (2), not highest+1"
    );
}

#[tokio::test]
async fn ledger_gap_consistency_after_vote() {
    cleanup_persisted_state();
    let ledger = create_ledger();

    let cmd = PaxosCommand::NOOP;

    // Start with gap at 1
    ledger.insert(DecreeId(0), cmd.clone()).await;
    let next1 = ledger.next().await;
    assert_eq!(next1, DecreeId(1));

    // Fill the gap
    ledger.insert(DecreeId(1), cmd.clone()).await;
    let next2 = ledger.next().await;
    assert_eq!(next2, DecreeId(2), "After filling gap 1, next should advance to 2");
}

#[tokio::test]
async fn ledger_interleaved_proposals() {
    cleanup_persisted_state();
    let ledger = create_ledger();

    let cmd = PaxosCommand::NOOP;

    // Simulate proposer thread 1 proposing decrees 0, 3, 6
    // Simulate proposer thread 2 proposing decrees 1, 4
    // They complete in random order: 0, 3, 1, 6, 4

    ledger.insert(DecreeId(0), cmd.clone()).await;
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(1), "After 0, next is 1");

    ledger.insert(DecreeId(3), cmd.clone()).await;
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(4), "After 0,3, next is still 1 (gap)");

    ledger.insert(DecreeId(1), cmd.clone()).await;
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(4), "After 0,1,3, next is 2 (gap)");

    ledger.insert(DecreeId(6), cmd.clone()).await;
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(7), "After 0,1,3,6, next is still 2 (gap)");

    ledger.insert(DecreeId(4), cmd.clone()).await;
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(7), "After 0,1,3,4,6, next is still 2 (gap)");

    // Fill gap at 2
    ledger.insert(DecreeId(2), cmd.clone()).await;
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(7), "After filling 0-4, next is 5 (first unchosen)");
}

#[tokio::test]
async fn ledger_chosen_state_independent_of_gaps() {
    cleanup_persisted_state();
    let ledger = create_ledger(); // Quorum of 2

    let cmd = PaxosCommand::NOOP;

    // Decree 0: get 2 votes (chosen)
    ledger.insert(DecreeId(0), cmd.clone()).await;
    ledger.insert(DecreeId(0), cmd.clone()).await;

    // Decree 2: get 2 votes (chosen)
    ledger.insert(DecreeId(2), cmd.clone()).await;
    ledger.insert(DecreeId(2), cmd.clone()).await;

    // Decree 1: get 0 votes (not chosen)

    // next() should be 1 (first unchosen), regardless of what's in decrees map
    let next = ledger.next().await;
    assert_eq!(next, DecreeId(3), "Should find unchosen decree 1 even with gaps");
}

// ============================================================================
// FEATURE TEST: Ledger provides chosen value detection
// ============================================================================
/// REQUIREMENT: Ledger must detect when consensus is reached
/// SPEC: Ledger should provide get_chosen_value(decree_num) -> Option<PaxosCommand>
///
/// This test documents that learners need to query when a value has been chosen.
/// Without this capability, learners cannot know when consensus is reached
/// and cannot respond with the final decision.
///
/// IMPLEMENTATION REQUIREMENTS:
/// 1. Add pub async fn get_chosen_value(&self, decree_num: usize) -> Option<PaxosCommand>
/// 2. Return Some(value) when votes.len() >= quorum for that decree
/// 3. Return None when votes.len() < quorum
///
/// STATUS: Will PASS once get_chosen_value() is implemented
#[tokio::test]
async fn ledger_detects_chosen_values_at_quorum() {
    cleanup_persisted_state();
    let ledger = create_ledger();

    let cmd = PaxosCommand::PUT {
        key: "test".to_string(),
        version: 1,
    };

    // After 1 vote (below quorum of 2): value not yet chosen
    ledger.insert(DecreeId(0), cmd.clone()).await;
    let chosen = ledger.get(DecreeId(0)).await;
    assert_ne!(chosen, None, "Value not chosen with 1 vote (below quorum)");

    // After 2 votes (at quorum): value should be chosen
    ledger.insert(DecreeId(0), cmd.clone()).await;

    let chosen = ledger.get(DecreeId(0)).await;
    assert_eq!(
        chosen,
        Some(cmd),
        "Value should be chosen at quorum"
    );
}
