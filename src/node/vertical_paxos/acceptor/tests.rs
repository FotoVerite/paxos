use std::sync::Arc;

use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    monitor::NoOpObserver,
    node::{pvalue::PValue, vertical_paxos::message::VerticalPaxosMessage},
    paxos_command::PaxosCommand,
};

use super::Acceptor;

type Message = VerticalPaxosMessage;

async fn new_acceptor() -> Acceptor {
    let uuid = Uuid::new_v4();
    Acceptor::new(
        uuid,
        crate::common::persistence::ClusterPersistence::for_test("pmmc_acceptor").node(uuid),
        Arc::new(NoOpObserver),
    )
    .await
    .expect("acceptor should initialize")
}

fn cmd(value: usize) -> PaxosCommand {
    PaxosCommand::PUT {
        key: "k".to_string(),
        version: 1,
        value,
    }
}

#[tokio::test]
async fn p1a_adopts_only_strictly_higher_ballots() {
    let acceptor = new_acceptor().await;
    let leader = Uuid::new_v4();
    let acceptor_id = Uuid::new_v4();
    let b5 = Ballot::new(5, leader);
    let b3 = Ballot::new(3, acceptor_id);

    let first = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot: b5,
            start_index: 0,
        })
        .await
        .expect("p1a should return p1b");
    assert!(matches!(
        first,
        VerticalPaxosMessage::P1B {
            to,
            ballot,
            pvalues,
            ..
        } if to == leader && ballot == b5 && pvalues.is_empty()
    ));

    let second = acceptor
        .handle_message(Message::P1A {
            from: acceptor_id,
            ballot: b3,
            start_index: 0,
        })
        .await
        .expect("p1a should return p1b");
    assert!(matches!(
        second,
        VerticalPaxosMessage::P1B { to, ballot, .. } if to == acceptor_id && ballot == b5
    ));
}

#[tokio::test]
async fn p2a_accepts_only_when_ballot_equals_current_ballot() {
    let acceptor = new_acceptor().await;
    let leader = Uuid::new_v4();
    let b7 = Ballot::new(7, leader);
    let b6 = Ballot::new(6, leader);
    let b8 = Ballot::new(8, leader);
    let v7 = PValue::new(1, b7, cmd(7));
    let v6 = PValue::new(2, b6, cmd(6));
    let v8 = PValue::new(3, b8, cmd(8));

    let _ = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot: b7,
            start_index: 0,
        })
        .await;

    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: v7.clone(),
        })
        .await;
    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: v6,
        })
        .await;
    let high = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: v8.clone(),
        })
        .await
        .expect("p2a should return p2b");

    assert!(
        matches!(high, VerticalPaxosMessage::P2B { ballot, .. } if ballot == b7),
        "paper rule: p2a with b > ballot_num should not advance ballot_num"
    );

    let check = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot: b7,
            start_index: 0,
        })
        .await
        .expect("p1a should return p1b");

    if let VerticalPaxosMessage::P1B {
        ballot, pvalues, ..
    } = check
    {
        assert_eq!(ballot, b7);
        assert!(
            pvalues.contains(&v7),
            "value at current ballot should be accepted"
        );
        assert!(
            !pvalues.contains(&v8),
            "paper rule: higher ballot p2a must not be accepted before adoption"
        );
    } else {
        panic!("expected P1B");
    }
}

#[tokio::test]
async fn p1a_returns_previously_accepted_pvalues() {
    let acceptor = new_acceptor().await;
    let leader = Uuid::new_v4();
    let b4 = Ballot::new(4, leader);
    let v1 = PValue::new(1, b4, cmd(10));
    let v2 = PValue::new(2, b4, cmd(20));

    let _ = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot: b4,
            start_index: 0,
        })
        .await;
    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: v1.clone(),
        })
        .await;
    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: v2.clone(),
        })
        .await;

    let reply = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot: b4,
            start_index: 0,
        })
        .await
        .expect("p1a should return p1b");

    if let VerticalPaxosMessage::P1B {
        pvalues, ballot, ..
    } = reply
    {
        assert_eq!(ballot, b4);
        assert_eq!(pvalues.len(), 2);
        assert!(pvalues.contains(&v1));
        assert!(pvalues.contains(&v2));
    } else {
        panic!("expected P1B");
    }
}

#[tokio::test]
async fn p1a_respects_start_index_filter() {
    let acceptor = new_acceptor().await;
    let leader = Uuid::new_v4();
    let b4 = Ballot::new(4, leader);
    let v1 = PValue::new(1, b4, cmd(10));
    let v2 = PValue::new(2, b4, cmd(20));
    let v4 = PValue::new(4, b4, cmd(40));

    let _ = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot: b4,
            start_index: 0,
        })
        .await;
    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: v1.clone(),
        })
        .await;
    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: v2.clone(),
        })
        .await;
    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: v4.clone(),
        })
        .await;

    let reply = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot: b4,
            start_index: 2,
        })
        .await
        .expect("p1a should return p1b");

    if let VerticalPaxosMessage::P1B { pvalues, .. } = reply {
        assert_eq!(pvalues.len(), 2);
        assert!(pvalues.contains(&v2));
        assert!(pvalues.contains(&v4));
        assert!(!pvalues.contains(&v1));
    } else {
        panic!("expected P1B");
    }
}

#[tokio::test]
async fn duplicate_p2a_is_idempotent_for_same_pvalue() {
    let acceptor = new_acceptor().await;
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(11, leader);
    let value = PValue::new(7, ballot, cmd(77));

    let _ = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot,
            start_index: 0,
        })
        .await;
    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: value.clone(),
        })
        .await;
    let _ = acceptor
        .handle_message(Message::P2A {
            from: leader,
            pvalue: value.clone(),
        })
        .await;

    let reply = acceptor
        .handle_message(Message::P1A {
            from: leader,
            ballot,
            start_index: 0,
        })
        .await
        .expect("p1a should return p1b");

    if let VerticalPaxosMessage::P1B { pvalues, .. } = reply {
        assert_eq!(pvalues.len(), 1);
        assert_eq!(pvalues[0], value);
    } else {
        panic!("expected P1B");
    }
}

#[tokio::test]
async fn unhandled_message_returns_none() {
    let acceptor = new_acceptor().await;
    let resp = acceptor
        .handle_message(Message::PROPOSE {
            from: Uuid::new_v4(),
            slot: 0,
            cmd: PaxosCommand::NOOP,
        })
        .await;
    assert!(resp.is_none());
}
