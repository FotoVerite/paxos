use std::sync::Arc;

use uuid::Uuid;

use crate::{
    message::Message,
    monitor::NoOpObserver,
    node::{classic_paxos::ballot::Ballot, pvalue::PValue},
    paxos_command::PaxosCommand,
};

use super::Scout;

fn cmd(value: usize) -> PaxosCommand {
    PaxosCommand::PUT {
        key: "k".to_string(),
        version: 1,
        value,
    }
}

#[tokio::test]
async fn preempts_when_acceptor_reports_higher_ballot() {
    let leader = Uuid::new_v4();
    let acceptor = Uuid::new_v4();
    let mut scout = Scout::new(leader, 2, Ballot::new(5, leader), Arc::new(NoOpObserver));

    let reply = scout
        .handle_message(Message::P1B {
            from: acceptor,
            to: leader,
            ballot: Ballot::new(6, acceptor),
            pvalues: vec![],
        })
        .await;

    assert!(
        matches!(reply, Message::PREEMPT { ballot, .. } if ballot == Ballot::new(6, acceptor)),
        "PMMC scout must preempt when any p1b carries a higher ballot"
    );
}

#[tokio::test]
async fn ignores_lower_ballot_responses() {
    let leader = Uuid::new_v4();
    let acceptor = Uuid::new_v4();
    let mut scout = Scout::new(leader, 2, Ballot::new(5, leader), Arc::new(NoOpObserver));

    let reply = scout
        .handle_message(Message::P1B {
            from: acceptor,
            to: leader,
            ballot: Ballot::new(4, acceptor),
            pvalues: vec![],
        })
        .await;

    assert!(
        matches!(reply, Message::NACK),
        "PMMC scout should ignore stale lower-ballot p1b responses"
    );
}

#[tokio::test]
async fn adopts_after_quorum_and_returns_pmax_per_slot() {
    let leader = Uuid::new_v4();
    let a1 = Uuid::new_v4();
    let a2 = Uuid::new_v4();
    let leader_ballot = Ballot::new(10, leader);
    let mut scout = Scout::new(leader, 2, leader_ballot, Arc::new(NoOpObserver));
    let lower = PValue::new(7, Ballot::new(8, a1), cmd(100));
    let higher = PValue::new(7, Ballot::new(9, a2), cmd(200));
    let other_slot = PValue::new(9, Ballot::new(7, a1), cmd(300));

    let r1 = scout
        .handle_message(Message::P1B {
            from: a1,
            to: leader,
            ballot: leader_ballot,
            pvalues: vec![lower.clone(), other_slot.clone()],
        })
        .await;
    assert!(matches!(r1, Message::NACK));

    let r2 = scout
        .handle_message(Message::P1B {
            from: a2,
            to: leader,
            ballot: leader_ballot,
            pvalues: vec![higher.clone()],
        })
        .await;

    if let Message::ADOPTED {
        ballot, pvalues, ..
    } = r2
    {
        assert_eq!(ballot, leader_ballot);
        assert_eq!(pvalues.len(), 2);
        assert!(pvalues.contains(&higher));
        assert!(pvalues.contains(&other_slot));
        assert!(!pvalues.contains(&lower));
    } else {
        panic!("expected ADOPTED after quorum");
    }
}

#[tokio::test]
async fn same_acceptor_cannot_satisfy_quorum_twice() {
    let leader = Uuid::new_v4();
    let a1 = Uuid::new_v4();
    let a2 = Uuid::new_v4();
    let ballot = Ballot::new(3, leader);
    let mut scout = Scout::new(leader, 2, ballot, Arc::new(NoOpObserver));

    let first = scout
        .handle_message(Message::P1B {
            from: a1,
            to: leader,
            ballot,
            pvalues: vec![],
        })
        .await;
    assert!(matches!(first, Message::NACK));

    let dup = scout
        .handle_message(Message::P1B {
            from: a1,
            to: leader,
            ballot,
            pvalues: vec![],
        })
        .await;
    assert!(matches!(dup, Message::NACK));

    let quorum = scout
        .handle_message(Message::P1B {
            from: a2,
            to: leader,
            ballot,
            pvalues: vec![],
        })
        .await;

    assert!(matches!(quorum, Message::ADOPTED { .. }));
}

#[tokio::test]
async fn quorum_one_adopts_on_first_matching_p1b() {
    let leader = Uuid::new_v4();
    let a1 = Uuid::new_v4();
    let ballot = Ballot::new(2, leader);
    let mut scout = Scout::new(leader, 1, ballot, Arc::new(NoOpObserver));

    let reply = scout
        .handle_message(Message::P1B {
            from: a1,
            to: leader,
            ballot,
            pvalues: vec![PValue::new(0, Ballot::new(1, a1), cmd(9))],
        })
        .await;

    assert!(
        matches!(reply, Message::ADOPTED { ballot: b, .. } if b == ballot),
        "quorum=1 scout should adopt immediately on first valid p1b"
    );
}

#[tokio::test]
async fn preempt_after_partial_adoption_with_higher_ballot() {
    let leader = Uuid::new_v4();
    let a1 = Uuid::new_v4();
    let a2 = Uuid::new_v4();
    let base = Ballot::new(4, leader);
    let higher = Ballot::new(5, a2);
    let mut scout = Scout::new(leader, 2, base, Arc::new(NoOpObserver));

    let first = scout
        .handle_message(Message::P1B {
            from: a1,
            to: leader,
            ballot: base,
            pvalues: vec![],
        })
        .await;
    assert!(matches!(first, Message::NACK));

    let preempt = scout
        .handle_message(Message::P1B {
            from: a2,
            to: leader,
            ballot: higher,
            pvalues: vec![],
        })
        .await;

    assert!(
        matches!(preempt, Message::PREEMPT { ballot, .. } if ballot == higher),
        "higher-ballot p1b should preempt even after partial progress"
    );
}
