use std::sync::Arc;

use uuid::Uuid;

use crate::{
    cluster::vertical::{
        activation::{ActivationSnapshot, PredecessorChain},
        configuration::{VerticalClusterConfiguration, VerticalPaxosVariant},
        quorum::Quorum,
    },
    common::ballot::Ballot,
    monitor::NoOpObserver,
    node::{
        pvalue::PValue,
        vertical_paxos::{
            leader::sync::LeaderSynchronizer,
            message::VerticalPaxosMessage,
            transport::{VerticalPaxosHandle, new_vertical_paxos_fabric},
        },
    },
    paxos_command::PaxosCommand,
};

fn snapshot() -> Arc<ActivationSnapshot> {
    let leader = Uuid::new_v4();
    let acceptor_a = Uuid::new_v4();
    let acceptor_b = Uuid::new_v4();
    let configuration = Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            leader,
            VerticalPaxosVariant::V1,
            Ballot::new(3, leader),
            4,
            vec![acceptor_a, acceptor_b],
            vec![leader],
            Quorum::new(vec![acceptor_a, acceptor_b]).expect("read quorum should build"),
            Quorum::new(vec![acceptor_a, acceptor_b]).expect("write quorum should build"),
            None,
        )
        .expect("configuration should build"),
    );
    Arc::new(ActivationSnapshot::new(
        configuration,
        Arc::new(PredecessorChain::default()),
    ))
}

#[tokio::test]
async fn p1b_updates_history_and_write_readiness_in_parallel() {
    let snapshot = snapshot();
    let peers = Arc::new(VerticalPaxosHandle::from_fabric(
        snapshot.configuration().leader(),
        Arc::new(new_vertical_paxos_fabric(Arc::new(NoOpObserver))),
    ));
    let mut synchronizer = LeaderSynchronizer::new(
        snapshot.configuration().leader(),
        peers,
        Arc::clone(&snapshot),
    );
    let ballot = snapshot.configuration().ballot();
    let members = snapshot.configuration().read_quorum().members().to_vec();

    synchronizer.start().await;
    synchronizer
        .handle_message(VerticalPaxosMessage::P1B {
            from: members[0],
            to: snapshot.configuration().leader(),
            ballot,
            pvalues: vec![PValue::new(4, ballot, PaxosCommand::NOOP)],
        })
        .await;
    assert!(synchronizer.is_in_progress());
    synchronizer
        .handle_message(VerticalPaxosMessage::P1B {
            from: members[1],
            to: snapshot.configuration().leader(),
            ballot,
            pvalues: vec![],
        })
        .await;

    assert!(synchronizer.is_ready());
    assert!(!synchronizer.is_in_progress());
}

#[tokio::test]
async fn higher_ballot_stops_synchronization() {
    let snapshot = snapshot();
    let peers = Arc::new(VerticalPaxosHandle::from_fabric(
        snapshot.configuration().leader(),
        Arc::new(new_vertical_paxos_fabric(Arc::new(NoOpObserver))),
    ));
    let mut synchronizer = LeaderSynchronizer::new(
        snapshot.configuration().leader(),
        peers,
        Arc::clone(&snapshot),
    );
    let higher = Ballot::new(snapshot.configuration().ballot().number + 1, Uuid::new_v4());

    synchronizer.start().await;
    synchronizer
        .handle_message(VerticalPaxosMessage::P1B {
            from: snapshot.configuration().read_quorum().members()[0],
            to: snapshot.configuration().leader(),
            ballot: higher,
            pvalues: vec![],
        })
        .await;

    assert_eq!(synchronizer.superseded_by(), Some(higher));
    assert!(!synchronizer.is_in_progress());
    assert!(!synchronizer.is_ready());
}
