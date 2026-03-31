use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, time::timeout};
use uuid::Uuid;

use crate::{
    cluster::{
        runtime::RuntimeNode,
        vertical::{
            configuration::{VerticalClusterConfiguration, VerticalPaxosVariant},
            master::VerticalMasterEvent,
            quorum::Quorum,
        },
    },
    common::{ballot::Ballot, persistence::ClusterPersistence},
    monitor::{NoOpObserver, PaxosObserver},
    node::vertical_paxos::{
        message::VerticalPaxosMessage,
        node::VerticalNode,
        node_factory::VerticalNodeFactory,
        node_state::{LeaderLifecycle, ReplicaLifecycle},
        transport::new_vertical_paxos_fabric,
    },
};

fn config_for(node_id: Uuid) -> Arc<VerticalClusterConfiguration> {
    let acceptors = vec![node_id, Uuid::new_v4()];
    let quorum = Quorum::new(acceptors.clone()).expect("quorum should build");
    Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            node_id,
            VerticalPaxosVariant::V1,
            Ballot::new(1, node_id),
            0,
            acceptors,
            vec![node_id],
            quorum.clone(),
            quorum,
            None,
        )
        .expect("configuration should build"),
    )
}

#[tokio::test]
async fn install_configuration_updates_membership_snapshot() {
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let fabric = Arc::new(new_vertical_paxos_fabric(Arc::clone(&observer)));
    let node_id = Uuid::new_v4();
    let node = VerticalNode::new(
        node_id,
        ClusterPersistence::for_test("vertical_node").node(node_id),
        fabric,
        observer,
        mpsc::channel::<VerticalMasterEvent>(8).0,
    )
    .await
    .expect("node should build");

    node.install_configuration(config_for(node_id)).await;
    let snapshot = node.snapshot().await;

    assert!(snapshot.installed_roles.designated_leader());
    assert!(snapshot.installed_roles.member_of_acceptor_set());
    assert!(snapshot.installed_roles.member_of_replica_set());
    assert!(!snapshot.acceptor_online);
    assert!(matches!(snapshot.leader, LeaderLifecycle::Assigned { .. }));
    assert!(matches!(
        snapshot.replica,
        ReplicaLifecycle::Assigned { .. }
    ));
}

#[tokio::test]
async fn started_acceptor_replies_to_p1a_when_node_is_in_acceptor_set() {
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let fabric = Arc::new(new_vertical_paxos_fabric(Arc::clone(&observer)));
    let node_id = Uuid::new_v4();
    let leader_id = Uuid::new_v4();
    let (leader_tx, mut leader_rx) = mpsc::channel(8);
    fabric.register(leader_id, leader_tx).await;

    let node = VerticalNode::new(
        node_id,
        ClusterPersistence::for_test("vertical_node").node(node_id),
        Arc::clone(&fabric),
        observer,
        mpsc::channel::<VerticalMasterEvent>(8).0,
    )
    .await
    .expect("node should build");
    node.install_configuration(config_for(node_id)).await;

    let (inbox_tx, inbox_rx) = mpsc::channel(8);
    let _task = node.start(inbox_rx).await;

    inbox_tx
        .send(VerticalPaxosMessage::P1A {
            from: leader_id,
            ballot: Ballot::new(3, leader_id),
            start_index: 0,
        })
        .await
        .expect("inbox should accept p1a");

    let reply = timeout(Duration::from_millis(200), leader_rx.recv())
        .await
        .expect("reply should arrive")
        .expect("peer channel should stay open");

    assert!(matches!(
        reply,
        VerticalPaxosMessage::P1B { from, to, ballot, .. }
            if from == node_id && to == leader_id && ballot == Ballot::new(3, leader_id)
    ));
}

#[tokio::test]
async fn factory_keeps_handle_to_built_nodes() {
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let fabric = Arc::new(new_vertical_paxos_fabric(Arc::clone(&observer)));
    let persistence = Arc::new(ClusterPersistence::for_test("vertical_factory"));
    let factory = VerticalNodeFactory::new(
        fabric,
        persistence,
        observer,
        mpsc::channel::<VerticalMasterEvent>(8).0,
    );
    let node_id = Uuid::new_v4();

    let _ = <VerticalNodeFactory as crate::cluster::runtime::RuntimeNodeFactory<
        VerticalPaxosMessage,
    >>::build(&factory, node_id)
    .await
    .expect("factory should build node");

    assert!(factory.get(node_id).await.is_some());
}
