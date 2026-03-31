use std::sync::Arc;

use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    monitor::NoOpObserver,
    node::{
        pvalue::PValue,
        vertical_paxos::{
            leader::sync::scout_round::ScoutRound,
            message::VerticalPaxosMessage,
            transport::{VerticalPaxosHandle, new_vertical_paxos_fabric},
        },
    },
    paxos_command::PaxosCommand,
};

#[tokio::test]
async fn round_processes_messages_until_history_is_recovered() {
    let leader = Uuid::new_v4();
    let member_a = Uuid::new_v4();
    let member_b = Uuid::new_v4();
    let ballot = Ballot::new(3, leader);
    let members = vec![member_a, member_b];
    let peers = Arc::new(VerticalPaxosHandle::from_fabric(
        leader,
        Arc::new(new_vertical_paxos_fabric(Arc::new(NoOpObserver))),
    ));
    let round = ScoutRound::new(leader, peers, ballot, 0, members.iter().copied().collect());

    round
        .handle_message(&VerticalPaxosMessage::P1B {
            from: members[0],
            to: leader,
            ballot,
            pvalues: vec![PValue::new(4, ballot, PaxosCommand::NOOP)],
        })
        .await;
    round
        .handle_message(&VerticalPaxosMessage::P1B {
            from: members[1],
            to: leader,
            ballot,
            pvalues: vec![],
        })
        .await;

    assert!(round.is_complete().await);
    assert_eq!(round.settled_values().await.len(), 1);
    round.shutdown().await;
}
