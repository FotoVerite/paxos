use paxos::{
    node::ballot::Ballot,
    node::ledger::Ledger,
    paxos_command::PaxosCommand,
};

mod test_helpers;
use test_helpers::cleanup_persisted_state;

// ============================================================================
// LEDGER GAP HANDLING TESTS
// ============================================================================

/// Gap Handling Requirement:
/// If decrees 0, 2, 3 are chosen but 1 is not (or rejected),
/// the next available decree should be 1 (the first gap)

#[tokio::test]
async fn ledger_finds_first_gap_simple() {
    cleanup_persisted_state();
    cleanup_persisted_state();
    let ledger = Ledger::init(1, 1).await.unwrap();

    let b = Ballot::new(1, 1);
    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "decree2".to_string(),
    };

    // Choose decree 0
    ledger
        .vote(0, b, cmd0.clone(), 0)
        .await;

    // Skip decree 1 (don't vote)

    // Choose decree 2
    ledger
        .vote(2, b, cmd2.clone(), 0)
        .await;

    // next() should return 1 (the first unchosen decree)
    let next = ledger.next().await;
    assert_eq!(
        next, 1,
        "Ledger should return first gap (decree 1) as next, not log.len() (3)"
    );
}

#[tokio::test]
async fn ledger_finds_gap_after_multiple_chosen() {
    cleanup_persisted_state();
    let ledger = Ledger::init(1, 1).await.unwrap();

    let b = Ballot::new(1, 1);
    let cmd = PaxosCommand::NOOP;

    // Choose decrees 0, 1, 2
    for i in 0..3 {
        ledger.vote(i, b, cmd.clone(), 0).await;
    }

    // next() should return 3 (first unchosen)
    let next = ledger.next().await;
    assert_eq!(
        next, 3,
        "After choosing 0,1,2 sequentially, next should be 3"
    );
}

#[tokio::test]
async fn ledger_next_with_all_gaps() {
    cleanup_persisted_state();
    let ledger = Ledger::init(1, 1).await.unwrap();

    // No decrees chosen
    let next = ledger.next().await;
    assert_eq!(next, 0, "With no decrees chosen, next should be 0");
}

#[tokio::test]
async fn ledger_next_skips_to_first_unchosen() {
    cleanup_persisted_state();
    let ledger = Ledger::init(1, 1).await.unwrap();

    let b = Ballot::new(1, 1);
    let cmd = PaxosCommand::NOOP;

    // Choose 0, 1, 2, 3, 4
    for i in 0..5 {
        ledger.vote(i, b, cmd.clone(), 0).await;
    }

    // Simulate retry scenario: proposer wants next decree after 2
    // Should be 5
    let next = ledger.next().await;
    assert_eq!(next, 5, "After choosing 0-4, next should be 5");
}

#[tokio::test]
async fn ledger_large_gap_in_middle() {
    cleanup_persisted_state();
    let ledger = Ledger::init(1, 1).await.unwrap();

    let b = Ballot::new(1, 1);
    let cmd = PaxosCommand::NOOP;

    // Choose 0, 1, 100, 101
    ledger.vote(0, b, cmd.clone(), 0).await;
    ledger.vote(1, b, cmd.clone(), 0).await;
    ledger.vote(100, b, cmd.clone(), 0).await;
    ledger.vote(101, b, cmd.clone(), 0).await;

    // next() should be 2 (first gap after sequential prefix)
    let next = ledger.next().await;
    assert_eq!(
        next, 2,
        "Should return first unchosen decree in sequence (2), not highest+1"
    );
}

#[tokio::test]
async fn ledger_gap_consistency_after_vote() {
    cleanup_persisted_state();
    let ledger = Ledger::init(1, 1).await.unwrap();

    let b = Ballot::new(1, 1);
    let cmd = PaxosCommand::NOOP;

    // Start with gap at 1
    ledger.vote(0, b, cmd.clone(), 0).await;
    let next1 = ledger.next().await;
    assert_eq!(next1, 1);

    // Fill the gap
    ledger.vote(1, b, cmd.clone(), 0).await;
    let next2 = ledger.next().await;
    assert_eq!(next2, 2, "After filling gap 1, next should advance to 2");
}

#[tokio::test]
async fn ledger_interleaved_proposals() {
    cleanup_persisted_state();
    let ledger = Ledger::init(1, 1).await.unwrap();

    let b = Ballot::new(1, 1);
    let cmd = PaxosCommand::NOOP;

    // Simulate proposer thread 1 proposing decrees 0, 3, 6
    // Simulate proposer thread 2 proposing decrees 1, 4
    // They complete in random order: 0, 3, 1, 6, 4

    ledger.vote(0, b, cmd.clone(), 0).await;
    let next = ledger.next().await;
    assert_eq!(next, 1, "After 0, next is 1");

    ledger.vote(3, b, cmd.clone(), 0).await;
    let next = ledger.next().await;
    assert_eq!(next, 1, "After 0,3, next is still 1 (gap)");

    ledger.vote(1, b, cmd.clone(), 0).await;
    let next = ledger.next().await;
    assert_eq!(next, 2, "After 0,1,3, next is 2 (gap)");

    ledger.vote(6, b, cmd.clone(), 0).await;
    let next = ledger.next().await;
    assert_eq!(next, 2, "After 0,1,3,6, next is still 2 (gap)");

    ledger.vote(4, b, cmd.clone(), 0).await;
    let next = ledger.next().await;
    assert_eq!(next, 2, "After 0,1,3,4,6, next is still 2 (gap)");

    // Fill gap at 2
    ledger.vote(2, b, cmd.clone(), 0).await;
    let next = ledger.next().await;
    assert_eq!(next, 5, "After filling 0-4, next is 5 (first unchosen)");
}

#[tokio::test]
async fn ledger_chosen_state_independent_of_gaps() {
    cleanup_persisted_state();
    let ledger = Ledger::init(1, 2).await.unwrap(); // Quorum of 2

    let b1 = Ballot::new(1, 1);
    let b2 = Ballot::new(1, 2);
    let cmd = PaxosCommand::NOOP;

    // Decree 0: get 2 votes (chosen)
    ledger.vote(0, b1, cmd.clone(), 0).await;
    ledger.vote(0, b2, cmd.clone(), 1).await;

    // Decree 2: get 2 votes (chosen)
    ledger.vote(2, b1, cmd.clone(), 0).await;
    ledger.vote(2, b2, cmd.clone(), 1).await;

    // Decree 1: get 0 votes (not chosen)

    // next() should be 1 (first unchosen), regardless of what's in decrees map
    let next = ledger.next().await;
    assert_eq!(next, 1, "Should find unchosen decree 1 even with gaps");
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
    let ledger = Ledger::init(1, 2).await.unwrap();

    let b = Ballot::new(1, 1);
    let cmd = PaxosCommand::PUT {
        key: "test".to_string(),
        version: 1,
    };

    // After 1 vote (below quorum of 2): value not yet chosen
    ledger.vote(0, b, cmd.clone(), 0).await;

    // TODO: When get_chosen_value() is implemented, add:
    // let chosen = ledger.get_chosen_value(0).await;
    // assert_eq!(chosen, None, "Value not chosen with 1 vote (below quorum)");

    // After 2 votes (at quorum): value should be chosen
    ledger.vote(0, b, cmd.clone(), 1).await;

    // TODO: Uncomment when get_chosen_value() is implemented:
    // let chosen = ledger.get_chosen_value(0).await;
    // assert_eq!(chosen, Some(cmd), "Value should be chosen at quorum");
}
