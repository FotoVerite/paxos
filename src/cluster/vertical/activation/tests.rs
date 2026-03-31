use std::sync::Arc;

use uuid::Uuid;

use crate::{
    cluster::vertical::{
        activation::{ActivationSnapshot, PredecessorChain},
        configuration::{VerticalClusterConfiguration, VerticalPaxosVariant},
        quorum::Quorum,
    },
    common::ballot::Ballot,
};

fn config(ballot_number: usize) -> VerticalClusterConfiguration {
    let id = Uuid::new_v4();
    let leader = Uuid::new_v4();
    let acceptors = vec![Uuid::new_v4(), Uuid::new_v4()];
    let quorum = Quorum::new(acceptors.clone()).expect("quorum should be valid");
    VerticalClusterConfiguration::new(
        id,
        leader,
        VerticalPaxosVariant::V1,
        Ballot::new(ballot_number, leader),
        0,
        acceptors,
        vec![leader],
        quorum.clone(),
        quorum,
        None,
    )
    .expect("configuration should be valid")
}

#[test]
fn configuration_wraps_leader_assignment_metadata() {
    let configuration = config(7);

    assert_eq!(configuration.ballot().number, 7);
    assert_eq!(configuration.leader(), configuration.ballot().node_id);
}

#[test]
fn activation_snapshot_keeps_configuration_and_history_separate() {
    let current = Arc::new(config(10));
    let previous = Arc::new(config(9));
    let chain = Arc::new(PredecessorChain::new(vec![Arc::clone(&previous)]));
    let snapshot = ActivationSnapshot::new(Arc::clone(&current), Arc::clone(&chain));

    assert_eq!(snapshot.configuration().id(), current.id());
    assert_eq!(
        snapshot.predecessor_chain().entries()[0].id(),
        previous.id()
    );
}
