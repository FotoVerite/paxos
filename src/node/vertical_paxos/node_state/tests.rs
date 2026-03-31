use std::sync::Arc;

use uuid::Uuid;

use crate::{
    cluster::vertical::{
        activation::{ActivationSnapshot, PredecessorChain},
        configuration::{VerticalClusterConfiguration, VerticalPaxosVariant},
        quorum::Quorum,
    },
    common::ballot::Ballot,
    node::vertical_paxos::node_state::{
        LeaderLifecycle, ReplicaLifecycle, VerticalPaxosNodeStateSketch,
    },
};

fn configuration(node_id: Uuid) -> Arc<VerticalClusterConfiguration> {
    let acceptors = vec![node_id, Uuid::new_v4()];
    let replicas = vec![node_id];
    let quorum = Quorum::new(acceptors.clone()).expect("quorum should be valid");
    Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            node_id,
            VerticalPaxosVariant::V1,
            Ballot::new(4, node_id),
            0,
            acceptors,
            replicas,
            quorum.clone(),
            quorum,
            None,
        )
        .expect("leader config should be valid"),
    )
}

#[test]
fn install_sets_membership_and_assigned_states() {
    let node_id = Uuid::new_v4();
    let configuration = configuration(node_id);
    let mut state = VerticalPaxosNodeStateSketch::new(node_id);

    state.install_configuration(Arc::clone(&configuration));

    assert!(state.installed_roles().designated_leader());
    assert!(state.installed_roles().member_of_acceptor_set());
    assert!(state.installed_roles().member_of_replica_set());
    assert!(matches!(
        state.leader(),
        LeaderLifecycle::Assigned { configuration_id } if *configuration_id == configuration.id()
    ));
    assert!(matches!(
        state.replica(),
        ReplicaLifecycle::Assigned { configuration_id } if *configuration_id == configuration.id()
    ));
}

#[test]
fn activation_and_supersession_are_separate_leader_states() {
    let node_id = Uuid::new_v4();
    let configuration = configuration(node_id);
    let snapshot = Arc::new(ActivationSnapshot::new(
        Arc::clone(&configuration),
        Arc::new(PredecessorChain::default()),
    ));
    let mut state = VerticalPaxosNodeStateSketch::new(node_id);
    state.install_configuration(Arc::clone(&configuration));

    state.start_activation(Arc::clone(&snapshot));
    assert!(matches!(
        state.leader(),
        LeaderLifecycle::Synchronizing { snapshot: active } if active.configuration().id() == configuration.id()
    ));

    let newer = Ballot::new(5, Uuid::new_v4());
    state.supersede(newer);
    assert!(matches!(
        state.leader(),
        LeaderLifecycle::Superseded { superseded_by, .. } if *superseded_by == newer
    ));
}

#[test]
fn replica_lifecycle_distinguishes_active_and_redirecting_membership() {
    let node_id = Uuid::new_v4();
    let current = configuration(node_id);
    let successor = configuration(Uuid::new_v4());
    let snapshot = Arc::new(ActivationSnapshot::new(
        Arc::clone(&current),
        Arc::new(PredecessorChain::default()),
    ));
    let mut state = VerticalPaxosNodeStateSketch::new(node_id);
    state.install_configuration(Arc::clone(&current));

    state.start_activation(snapshot);
    assert!(matches!(
        state.replica(),
        ReplicaLifecycle::Syncing { configuration_id } if *configuration_id == current.id()
    ));

    state.activate_replica_set(Arc::clone(&current));
    assert!(matches!(
        state.replica(),
        ReplicaLifecycle::Active { configuration_id } if *configuration_id == current.id()
    ));

    state.decommission_to(Arc::clone(&successor));
    assert!(matches!(
        state.replica(),
        ReplicaLifecycle::Redirecting {
            installed_configuration_id,
            active_configuration_id,
        } if *installed_configuration_id == current.id() && *active_configuration_id == successor.id()
    ));
}
