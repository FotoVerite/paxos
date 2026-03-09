use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use tokio::{sync::mpsc, time::sleep};
use uuid::Uuid;

use crate::{
    cluster::network_handle::NetworkHandle,
    message::Message,
    monitor::{NoOpObserver, PaxosObserver},
    node::{classic_paxos::ballot::Ballot, pvalue::PValue},
    paxos_command::PaxosCommand,
};

use super::Commander;

fn cmd(v: usize) -> PaxosCommand {
    PaxosCommand::PUT {
        key: "k".to_string(),
        version: 1,
        value: v,
    }
}

async fn mk_commander(
    quorum: usize,
    ballot: Ballot,
    proposals: BTreeMap<usize, PaxosCommand>,
) -> Commander {
    let id = ballot.node_id;
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let peers = Arc::new(NetworkHandle::new(id, HashMap::new(), Arc::clone(&observer)).await);
    Commander::new(id, quorum, ballot, vec![], proposals, peers, observer)
}

#[tokio::test]
async fn p2b_returns_nack_until_quorum_then_accepted() {
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(5, leader);
    let a1 = Uuid::new_v4();
    let a2 = Uuid::new_v4();
    let mut proposals = BTreeMap::new();
    proposals.insert(3, cmd(10));
    let mut commander = mk_commander(2, ballot, proposals).await;

    let first = commander
        .handle_message(Message::P2B {
            from: a1,
            to: leader,
            ballot,
            pvalue: PValue::new(3, ballot, cmd(10)),
        })
        .await;
    assert!(matches!(first, Message::NACK));

    let second = commander
        .handle_message(Message::P2B {
            from: a2,
            to: leader,
            ballot,
            pvalue: PValue::new(3, ballot, cmd(10)),
        })
        .await;
    assert!(matches!(
        second,
        Message::ACCEPTED { from, pvalue } if from == leader && pvalue.slot() == 3 && pvalue.ballot() == ballot && pvalue.cmd() == cmd(10)
    ));
}

#[tokio::test]
async fn duplicate_acceptor_ack_does_not_count_twice() {
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(7, leader);
    let a1 = Uuid::new_v4();
    let a2 = Uuid::new_v4();
    let mut proposals = BTreeMap::new();
    proposals.insert(1, cmd(1));
    let mut commander = mk_commander(2, ballot, proposals).await;

    let r1 = commander
        .handle_message(Message::P2B {
            from: a1,
            to: leader,
            ballot,
            pvalue: PValue::new(1, ballot, cmd(1)),
        })
        .await;
    assert!(matches!(r1, Message::NACK));

    let dup = commander
        .handle_message(Message::P2B {
            from: a1,
            to: leader,
            ballot,
            pvalue: PValue::new(1, ballot, cmd(1)),
        })
        .await;
    assert!(matches!(dup, Message::NACK));

    let quorum = commander
        .handle_message(Message::P2B {
            from: a2,
            to: leader,
            ballot,
            pvalue: PValue::new(1, ballot, cmd(1)),
        })
        .await;
    assert!(matches!(quorum, Message::ACCEPTED { .. }));
}

#[tokio::test]
async fn higher_ballot_p2b_preempts_commander() {
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(8, leader);
    let mut proposals = BTreeMap::new();
    proposals.insert(2, cmd(2));
    let mut commander = mk_commander(2, ballot, proposals).await;
    let higher = Ballot::new(9, Uuid::new_v4());

    let reply = commander
        .handle_message(Message::P2B {
            from: Uuid::new_v4(),
            to: leader,
            ballot: higher,
            pvalue: PValue::new(2, ballot, cmd(2)),
        })
        .await;

    assert!(
        matches!(reply, Message::PREEMPT { ballot, .. } if ballot == higher),
        "PMMC commander should preempt on a higher ballot in p2b"
    );
}

#[tokio::test]
async fn lower_ballot_p2b_is_ignored() {
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(11, leader);
    let lower = Ballot::new(10, Uuid::new_v4());
    let mut proposals = BTreeMap::new();
    proposals.insert(2, cmd(2));
    let mut commander = mk_commander(2, ballot, proposals).await;

    let reply = commander
        .handle_message(Message::P2B {
            from: Uuid::new_v4(),
            to: leader,
            ballot: lower,
            pvalue: PValue::new(2, lower, cmd(2)),
        })
        .await;

    assert!(
        matches!(reply, Message::NACK),
        "PMMC commander should ignore lower-ballot p2b responses"
    );
}

#[tokio::test]
async fn unhandled_message_returns_nack() {
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(1, leader);
    let mut proposals = BTreeMap::new();
    proposals.insert(0, cmd(0));
    let mut commander = mk_commander(1, ballot, proposals).await;

    let reply = commander
        .handle_message(Message::P1A {
            from: leader,
            ballot,
            start_index: 0,
        })
        .await;
    assert!(matches!(reply, Message::NACK));
}

#[tokio::test]
async fn unknown_slot_p2b_is_ignored() {
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(3, leader);
    let mut proposals = BTreeMap::new();
    proposals.insert(0, cmd(0));
    let mut commander = mk_commander(2, ballot, proposals).await;

    let reply = commander
        .handle_message(Message::P2B {
            from: Uuid::new_v4(),
            to: leader,
            ballot,
            pvalue: PValue::new(42, ballot, cmd(42)),
        })
        .await;
    assert!(matches!(reply, Message::NACK));
}

#[tokio::test]
async fn run_rebroadcasts_p2a_periodically_until_quorum() {
    let leader = Uuid::new_v4();
    let peer = Uuid::new_v4();
    let ballot = Ballot::new(1, leader);
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let mut proposals = BTreeMap::new();
    proposals.insert(0, cmd(1));

    let (peer_tx, mut peer_rx) = mpsc::channel(2);
    let mut peers_map = HashMap::new();
    peers_map.insert(peer, peer_tx);
    let peers = Arc::new(NetworkHandle::new(leader, peers_map, Arc::clone(&observer)).await);
    let commander = Commander::new(leader, 2, ballot, vec![peer], proposals, peers, observer);
    let runner = commander.clone();
    tokio::spawn(async move {
        runner.run().await;
    });

    sleep(Duration::from_millis(700)).await;
    let mut p2a_count = 0usize;
    while let Ok(msg) = peer_rx.try_recv() {
        if matches!(msg, Message::P2A { .. }) {
            p2a_count += 1;
        }
    }

    assert!(
        p2a_count >= 2,
        "commander should keep re-broadcasting p2a while quorum is not reached"
    );
}

#[tokio::test]
async fn run_stops_rebroadcasts_after_stop_signal() {
    let leader = Uuid::new_v4();
    let peer = Uuid::new_v4();
    let ballot = Ballot::new(1, leader);
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let mut proposals = BTreeMap::new();
    proposals.insert(0, cmd(1));

    let (peer_tx, mut peer_rx) = mpsc::channel(8);
    let mut peers_map = HashMap::new();
    peers_map.insert(peer, peer_tx);
    let peers = Arc::new(NetworkHandle::new(leader, peers_map, Arc::clone(&observer)).await);
    let commander = Commander::new(leader, 2, ballot, vec![peer], proposals, peers, observer);
    let runner = commander.clone();
    tokio::spawn(async move {
        runner.run().await;
    });

    sleep(Duration::from_millis(320)).await;
    let before_stop = std::iter::from_fn(|| peer_rx.try_recv().ok())
        .filter(|m| matches!(m, Message::P2A { .. }))
        .count();
    assert!(before_stop >= 1, "commander should emit p2a before stop");

    commander.stop();
    sleep(Duration::from_millis(380)).await;
    let after_stop = std::iter::from_fn(|| peer_rx.try_recv().ok())
        .filter(|m| matches!(m, Message::P2A { .. }))
        .count();

    assert_eq!(
        after_stop, 0,
        "commander should emit no further p2a once stop is signaled"
    );
}

#[tokio::test]
async fn run_rebroadcasts_accepted_to_only_unacked_replicas_once_decided() {
    let leader = Uuid::new_v4();
    let r1 = Uuid::new_v4();
    let r2 = Uuid::new_v4();
    let ballot = Ballot::new(1, leader);
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let mut proposals = BTreeMap::new();
    proposals.insert(0, cmd(11));

    let (r1_tx, mut r1_rx) = mpsc::channel(8);
    let (r2_tx, mut r2_rx) = mpsc::channel(8);
    let mut peers_map = HashMap::new();
    peers_map.insert(r1, r1_tx);
    peers_map.insert(r2, r2_tx);
    let peers = Arc::new(NetworkHandle::new(leader, peers_map, Arc::clone(&observer)).await);
    let commander = Commander::new(leader, 1, ballot, vec![r1, r2], proposals, peers, observer);
    let mut gate = commander.clone();

    let accepted = gate
        .handle_message(Message::P2B {
            from: Uuid::new_v4(),
            to: leader,
            ballot,
            pvalue: PValue::new(0, ballot, cmd(11)),
        })
        .await;
    assert!(matches!(accepted, Message::ACCEPTED { .. }));

    commander.record_replica_ack(r1, 0).await;

    let runner = commander.clone();
    tokio::spawn(async move {
        runner.run().await;
    });

    sleep(Duration::from_millis(450)).await;
    let r1_msgs: Vec<_> = std::iter::from_fn(|| r1_rx.try_recv().ok()).collect();
    let r2_msgs: Vec<_> = std::iter::from_fn(|| r2_rx.try_recv().ok()).collect();

    let r1_accepted = r1_msgs
        .iter()
        .filter(|m| matches!(m, Message::ACCEPTED { .. }))
        .count();
    let r2_accepted = r2_msgs
        .iter()
        .filter(|m| matches!(m, Message::ACCEPTED { .. }))
        .count();

    assert_eq!(
        r1_accepted, 0,
        "already-acked replica should not get accepted rebroadcast"
    );
    assert!(
        r2_accepted >= 1,
        "unacked replica should receive accepted rebroadcast"
    );
}

#[tokio::test]
async fn all_replica_acks_compact_decided_slot() {
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(2, leader);
    let r1 = Uuid::new_v4();
    let r2 = Uuid::new_v4();
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let peers = Arc::new(NetworkHandle::new(leader, HashMap::new(), Arc::clone(&observer)).await);
    let mut proposals = BTreeMap::new();
    proposals.insert(5, cmd(5));
    let mut commander = Commander::new(leader, 1, ballot, vec![r1, r2], proposals, peers, observer);

    let accepted = commander
        .handle_message(Message::P2B {
            from: Uuid::new_v4(),
            to: leader,
            ballot,
            pvalue: PValue::new(5, ballot, cmd(5)),
        })
        .await;
    assert!(matches!(accepted, Message::ACCEPTED { .. }));

    commander.record_replica_ack(r1, 5).await;
    commander.record_replica_ack(r2, 5).await;

    let after_compact = commander
        .handle_message(Message::P2B {
            from: Uuid::new_v4(),
            to: leader,
            ballot,
            pvalue: PValue::new(5, ballot, cmd(5)),
        })
        .await;
    assert!(
        matches!(after_compact, Message::NACK),
        "once slot is compacted by replica acks, further p2b for that slot should be ignored"
    );
}

#[tokio::test]
async fn role_split_replica_acks_compaction_stops_accepted_rebroadcasts() {
    let leader = Uuid::new_v4();
    let ballot = Ballot::new(3, leader);
    let r1 = Uuid::new_v4();
    let r2 = Uuid::new_v4();
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let mut proposals = BTreeMap::new();
    proposals.insert(0, cmd(17));

    let (r1_tx, mut r1_rx) = mpsc::channel(8);
    let (r2_tx, mut r2_rx) = mpsc::channel(8);
    let mut peers_map = HashMap::new();
    peers_map.insert(r1, r1_tx);
    peers_map.insert(r2, r2_tx);
    let peers = Arc::new(NetworkHandle::new(leader, peers_map, Arc::clone(&observer)).await);
    let mut commander = Commander::new(leader, 1, ballot, vec![r1, r2], proposals, peers, observer);

    let accepted = commander
        .handle_message(Message::P2B {
            from: Uuid::new_v4(),
            to: leader,
            ballot,
            pvalue: PValue::new(0, ballot, cmd(17)),
        })
        .await;
    assert!(matches!(accepted, Message::ACCEPTED { .. }));

    // Both replicas ACKed the decided slot; commander should compact it.
    commander.record_replica_ack(r1, 0).await;
    commander.record_replica_ack(r2, 0).await;

    let runner = commander.clone();
    tokio::spawn(async move {
        runner.run().await;
    });
    sleep(Duration::from_millis(450)).await;

    let r1_accepted = std::iter::from_fn(|| r1_rx.try_recv().ok())
        .filter(|m| matches!(m, Message::ACCEPTED { .. }))
        .count();
    let r2_accepted = std::iter::from_fn(|| r2_rx.try_recv().ok())
        .filter(|m| matches!(m, Message::ACCEPTED { .. }))
        .count();

    assert_eq!(
        r1_accepted, 0,
        "compacted slot must not be rebroadcast to replica r1"
    );
    assert_eq!(
        r2_accepted, 0,
        "compacted slot must not be rebroadcast to replica r2"
    );
}
