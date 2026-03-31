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
            leader::sync::recovery::RecoverySynchronizer,
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
    let predecessor_acceptor = Uuid::new_v4();

    let current = Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            leader,
            VerticalPaxosVariant::V1,
            Ballot::new(3, leader),
            5,
            vec![acceptor_a, acceptor_b],
            vec![leader],
            Quorum::new(vec![acceptor_a, acceptor_b]).expect("read quorum should build"),
            Quorum::new(vec![acceptor_a, acceptor_b]).expect("write quorum should build"),
            Some(Ballot::new(1, predecessor_acceptor)),
        )
        .expect("configuration should build"),
    );

    let predecessor = Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            predecessor_acceptor,
            VerticalPaxosVariant::V1,
            Ballot::new(2, predecessor_acceptor),
            0,
            vec![predecessor_acceptor],
            vec![leader],
            Quorum::new(vec![predecessor_acceptor]).expect("read quorum should build"),
            Quorum::new(vec![predecessor_acceptor]).expect("write quorum should build"),
            None,
        )
        .expect("predecessor should build"),
    );

    Arc::new(ActivationSnapshot::new(
        current,
        Arc::new(PredecessorChain::new(vec![predecessor])),
    ))
}

#[tokio::test]
async fn recovery_walks_current_then_predecessors_and_merges_highest_values() {
    let snapshot = snapshot();
    let peers = Arc::new(VerticalPaxosHandle::from_fabric(
        snapshot.configuration().leader(),
        Arc::new(new_vertical_paxos_fabric(Arc::new(NoOpObserver))),
    ));
    let mut recovery = RecoverySynchronizer::new(
        snapshot.configuration().leader(),
        peers,
        Arc::clone(&snapshot),
    );
    let ballot = snapshot.configuration().ballot();
    let current_members = snapshot.configuration().read_quorum().members().to_vec();
    let predecessor_member = snapshot.predecessor_chain().entries()[0]
        .read_quorum()
        .members()[0];

    recovery.start().await;
    recovery
        .handle_message(&VerticalPaxosMessage::P1B {
            from: current_members[0],
            to: snapshot.configuration().leader(),
            ballot,
            pvalues: vec![PValue::new(
                5,
                Ballot::new(1, current_members[0]),
                PaxosCommand::NOOP,
            )],
        })
        .await;
    recovery
        .handle_message(&VerticalPaxosMessage::P1B {
            from: current_members[1],
            to: snapshot.configuration().leader(),
            ballot,
            pvalues: vec![],
        })
        .await;
    recovery
        .handle_message(&VerticalPaxosMessage::P1B {
            from: predecessor_member,
            to: snapshot.configuration().leader(),
            ballot,
            pvalues: vec![PValue::new(
                5,
                Ballot::new(2, predecessor_member),
                PaxosCommand::NOOP,
            )],
        })
        .await;

    let recovered = recovery
        .recovered_state()
        .expect("recovery should complete");
    assert_eq!(recovered.start_index(), 0);
    assert_eq!(
        recovered
            .settled_values()
            .get(&5)
            .expect("slot should be recovered")
            .ballot(),
        Ballot::new(2, predecessor_member)
    );
}

#[tokio::test]
async fn higher_ballot_supersedes_recovery() {
    let snapshot = snapshot();
    let peers = Arc::new(VerticalPaxosHandle::from_fabric(
        snapshot.configuration().leader(),
        Arc::new(new_vertical_paxos_fabric(Arc::new(NoOpObserver))),
    ));
    let mut recovery = RecoverySynchronizer::new(
        snapshot.configuration().leader(),
        peers,
        Arc::clone(&snapshot),
    );
    let higher = Ballot::new(snapshot.configuration().ballot().number + 1, Uuid::new_v4());

    recovery.start().await;
    recovery
        .handle_message(&VerticalPaxosMessage::P1B {
            from: snapshot.configuration().read_quorum().members()[0],
            to: snapshot.configuration().leader(),
            ballot: higher,
            pvalues: vec![],
        })
        .await;

    assert_eq!(recovery.superseded_by(), Some(higher));
    assert!(!recovery.is_complete());
}
