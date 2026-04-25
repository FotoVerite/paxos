use std::sync::Arc;

use uuid::Uuid;

use crate::{
    cluster::{
        synod_vertical::{master::ActivationStatus, system::SynodVerticalSystem},
        vertical::{
            activation::PredecessorChain,
            configuration::{VerticalClusterConfiguration, VerticalPaxosVariant},
            quorum::Quorum,
        },
    },
    common::{ballot::Ballot, persistence::ClusterPersistence},
    monitor::{NoOpObserver, PaxosObserver},
    node::vertical_paxos::node_state::LeaderLifecycle,
};

fn configuration(cluster_ids: &[Uuid], leader: Uuid) -> Arc<VerticalClusterConfiguration> {
    let acceptors = vec![cluster_ids[0], cluster_ids[1]];
    let replicas = vec![cluster_ids[0], cluster_ids[2]];
    let read_quorum = Quorum::new(vec![acceptors[0]]).expect("read quorum should build");
    let write_quorum = Quorum::new(acceptors.clone()).expect("write quorum should build");

    Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            leader,
            VerticalPaxosVariant::V1,
            Ballot::new(2, leader),
            4,
            acceptors,
            replicas,
            read_quorum,
            write_quorum,
            None,
        )
        .expect("configuration should build"),
    )
}

#[tokio::test]
async fn install_tracks_configuration_and_updates_runtime_nodes() {
    let ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let system = SynodVerticalSystem::new(
        ids.clone(),
        Arc::new(ClusterPersistence::for_test("vertical_master_install")),
        observer,
    )
    .await
    .expect("system should build");
    system.start().await;
    let master = system.master();
    let config = configuration(&ids, ids[0]);

    master
        .install_configuration(Arc::clone(&config))
        .await
        .expect("install should work");

    assert_eq!(
        master.configuration(config.id()).await,
        Some(Arc::clone(&config))
    );
    assert!(
        system
            .snapshot(ids[0])
            .await
            .expect("leader node should exist")
            .installed_roles
            .designated_leader()
    );
}

#[tokio::test]
async fn start_activation_records_snapshot_and_targets_designated_leader() {
    let ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let system = SynodVerticalSystem::new(
        ids.clone(),
        Arc::new(ClusterPersistence::for_test("vertical_master_activation")),
        observer,
    )
    .await
    .expect("system should build");
    system.start().await;
    let master = system.master();
    let config = configuration(&ids, ids[0]);

    master
        .install_configuration(Arc::clone(&config))
        .await
        .expect("install should work");
    let snapshot = master
        .start_activation(config.id(), Arc::new(PredecessorChain::default()))
        .await
        .expect("activation should start");

    let record = master
        .activation(config.id())
        .await
        .expect("activation record should exist");
    assert_eq!(record.snapshot(), &snapshot);
    assert_eq!(record.status(), &ActivationStatus::Synchronizing);
    assert!(matches!(
        system
            .snapshot(ids[0])
            .await
            .expect("leader node should exist")
            .leader,
        LeaderLifecycle::Synchronizing { .. }
    ));
}

#[tokio::test]
async fn preempt_leader_marks_activation_and_updates_leader_state() {
    let ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let system = SynodVerticalSystem::new(
        ids.clone(),
        Arc::new(ClusterPersistence::for_test("vertical_master_supersede")),
        observer,
    )
    .await
    .expect("system should build");
    system.start().await;
    let master = system.master();
    let config = configuration(&ids, ids[0]);
    let newer_ballot = Ballot::new(5, ids[1]);

    master
        .install_configuration(Arc::clone(&config))
        .await
        .expect("install should work");
    master
        .start_activation(config.id(), Arc::new(PredecessorChain::default()))
        .await
        .expect("activation should start");
    master
        .preempt_leader(config.id(), newer_ballot)
        .await
        .expect("preempt should work");

    let record = master
        .activation(config.id())
        .await
        .expect("activation record should exist");
    assert_eq!(
        record.status(),
        &ActivationStatus::Preempted {
            by_ballot: newer_ballot,
        }
    );
    assert!(matches!(
        system
            .snapshot(ids[0])
            .await
            .expect("leader node should exist")
            .leader,
        LeaderLifecycle::Superseded { superseded_by, .. } if superseded_by == newer_ballot
    ));
}
