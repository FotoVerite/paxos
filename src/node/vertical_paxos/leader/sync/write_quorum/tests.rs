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
    node::vertical_paxos::{
        leader::sync::write_quorum::WriteQuorumSynchronizer,
        message::VerticalPaxosMessage,
        transport::{VerticalPaxosHandle, new_vertical_paxos_fabric},
    },
};

fn snapshot() -> Arc<ActivationSnapshot> {
    let leader = Uuid::new_v4();
    let member_a = Uuid::new_v4();
    let member_b = Uuid::new_v4();
    let configuration = Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            leader,
            VerticalPaxosVariant::V1,
            Ballot::new(4, leader),
            0,
            vec![member_a, member_b],
            vec![leader],
            Quorum::new(vec![member_a]).expect("read quorum should build"),
            Quorum::new(vec![member_a, member_b]).expect("write quorum should build"),
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
async fn p1b_from_required_members_marks_write_quorum_ready() {
    let snapshot = snapshot();
    let peers = Arc::new(VerticalPaxosHandle::from_fabric(
        snapshot.configuration().leader(),
        Arc::new(new_vertical_paxos_fabric(Arc::new(NoOpObserver))),
    ));
    let mut synchronizer = WriteQuorumSynchronizer::new(
        snapshot.configuration().leader(),
        peers,
        Arc::clone(&snapshot),
    );
    let ballot = snapshot.configuration().ballot();
    let members = snapshot.configuration().write_quorum().members().to_vec();

    synchronizer.start().await;
    synchronizer
        .handle_message(&VerticalPaxosMessage::P1B {
            from: members[0],
            to: Uuid::new_v4(),
            ballot,
            pvalues: vec![],
        })
        .await;
    synchronizer
        .handle_message(&VerticalPaxosMessage::P1B {
            from: members[1],
            to: Uuid::new_v4(),
            ballot,
            pvalues: vec![],
        })
        .await;

    assert!(synchronizer.is_ready());
}

#[tokio::test]
async fn higher_ballot_supersedes_write_quorum_synchronization() {
    let snapshot = snapshot();
    let peers = Arc::new(VerticalPaxosHandle::from_fabric(
        snapshot.configuration().leader(),
        Arc::new(new_vertical_paxos_fabric(Arc::new(NoOpObserver))),
    ));
    let mut synchronizer = WriteQuorumSynchronizer::new(
        snapshot.configuration().leader(),
        peers,
        Arc::clone(&snapshot),
    );
    let higher = Ballot::new(snapshot.configuration().ballot().number + 1, Uuid::new_v4());

    synchronizer.start().await;
    synchronizer
        .handle_message(&VerticalPaxosMessage::P1B {
            from: snapshot.configuration().write_quorum().members()[0],
            to: Uuid::new_v4(),
            ballot: higher,
            pvalues: vec![],
        })
        .await;

    assert_eq!(synchronizer.superseded_by(), Some(higher));
    assert!(!synchronizer.is_ready());
}

#[tokio::test]
async fn start_resets_previous_write_quorum_progress() {
    let snapshot = snapshot();
    let peers = Arc::new(VerticalPaxosHandle::from_fabric(
        snapshot.configuration().leader(),
        Arc::new(new_vertical_paxos_fabric(Arc::new(NoOpObserver))),
    ));
    let mut synchronizer = WriteQuorumSynchronizer::new(
        snapshot.configuration().leader(),
        peers,
        Arc::clone(&snapshot),
    );
    let members = snapshot.configuration().write_quorum().members().to_vec();
    let ballot = snapshot.configuration().ballot();

    synchronizer.start().await;
    synchronizer
        .handle_message(&VerticalPaxosMessage::P1B {
            from: members[0],
            to: Uuid::new_v4(),
            ballot,
            pvalues: vec![],
        })
        .await;
    synchronizer
        .handle_message(&VerticalPaxosMessage::P1B {
            from: members[1],
            to: Uuid::new_v4(),
            ballot,
            pvalues: vec![],
        })
        .await;
    assert!(synchronizer.is_ready());

    synchronizer.start().await;

    assert!(!synchronizer.is_ready());
    assert_eq!(synchronizer.superseded_by(), None);
}
