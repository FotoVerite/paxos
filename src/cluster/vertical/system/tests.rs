use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    cluster::vertical::{
        activation::PredecessorChain,
        configuration::{VerticalClusterConfiguration, VerticalPaxosVariant},
        master::ActivationStatus,
        quorum::Quorum,
        system::VerticalSystem,
    },
    common::{ballot::Ballot, persistence::ClusterPersistence},
    monitor::{Event, MessageTrace, NoOpObserver, PaxosObserver},
    node::vertical_paxos::{
        message::VerticalPaxosMessage,
        node_state::{LeaderLifecycle, ReplicaLifecycle},
        replica::VerticalClientReply,
    },
    paxos_command::PaxosCommand,
};

#[derive(Default)]
struct RecordingObserver {
    events: Arc<Mutex<Vec<Event>>>,
}

impl RecordingObserver {
    async fn wait_for_event<F>(&self, predicate: F, timeout_duration: Duration) -> Event
    where
        F: Fn(&Event) -> bool,
    {
        tokio::time::timeout(timeout_duration, async {
            loop {
                if let Some(event) = self
                    .events
                    .lock()
                    .await
                    .iter()
                    .find(|event| predicate(event))
                    .cloned()
                {
                    return event;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for event")
    }
}

impl PaxosObserver for RecordingObserver {
    fn on_event(&self, event: Event) {
        let events = self.events.clone();
        tokio::spawn(async move {
            events.lock().await.push(event);
        });
    }

    fn on_message_trace(&self, _trace: MessageTrace) {}
}

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
async fn master_install_reaches_all_nodes_through_system() {
    let ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let system = VerticalSystem::new(
        ids.clone(),
        Arc::new(ClusterPersistence::for_test("vertical_system")),
        observer,
    )
    .await
    .expect("system should build");
    system.start().await;

    let config = configuration(&ids, ids[0]);
    system
        .master()
        .install_configuration(Arc::clone(&config))
        .await
        .expect("install should work");

    let leader_snapshot = system
        .snapshot(ids[0])
        .await
        .expect("leader node should exist");
    let acceptor_snapshot = system
        .snapshot(ids[1])
        .await
        .expect("acceptor node should exist");
    let replica_snapshot = system
        .snapshot(ids[2])
        .await
        .expect("replica node should exist");

    assert!(leader_snapshot.installed_roles.designated_leader());
    assert!(leader_snapshot.installed_roles.member_of_acceptor_set());
    assert!(leader_snapshot.installed_roles.member_of_replica_set());
    assert!(acceptor_snapshot.installed_roles.member_of_acceptor_set());
    assert!(!acceptor_snapshot.installed_roles.member_of_replica_set());
    assert!(!replica_snapshot.installed_roles.member_of_acceptor_set());
    assert!(replica_snapshot.installed_roles.member_of_replica_set());
}

#[tokio::test]
async fn master_activation_targets_only_the_designated_leader() {
    let ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let system = VerticalSystem::new(
        ids.clone(),
        Arc::new(ClusterPersistence::for_test("vertical_system_activation")),
        observer,
    )
    .await
    .expect("system should build");
    system.start().await;

    let config = configuration(&ids, ids[0]);
    system
        .master()
        .install_configuration(Arc::clone(&config))
        .await
        .expect("install should work");
    system
        .master()
        .start_activation(config.id(), Arc::new(PredecessorChain::default()))
        .await
        .expect("activation should start");

    let leader_snapshot = system
        .snapshot(ids[0])
        .await
        .expect("leader node should exist");
    let follower_snapshot = system
        .snapshot(ids[1])
        .await
        .expect("follower node should exist");

    assert!(matches!(
        leader_snapshot.leader,
        LeaderLifecycle::Synchronizing { .. }
    ));
    assert!(matches!(
        follower_snapshot.leader,
        LeaderLifecycle::Inactive
    ));
    assert!(matches!(
        follower_snapshot.replica,
        ReplicaLifecycle::Inactive
    ));
}

#[tokio::test]
async fn leader_callbacks_update_master_activation_status_through_runtime_inbox() {
    let ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    let system = VerticalSystem::new(
        ids.clone(),
        Arc::new(ClusterPersistence::for_test("vertical_system_callbacks")),
        observer,
    )
    .await
    .expect("system should build");
    system.start().await;

    let config = configuration(&ids, ids[0]);
    system
        .master()
        .install_configuration(Arc::clone(&config))
        .await
        .expect("install should work");
    system
        .master()
        .start_activation(config.id(), Arc::new(PredecessorChain::default()))
        .await
        .expect("activation should start");

    let ballot = config.ballot();
    let leader_id = config.leader();
    for from in config.acceptors().iter().copied() {
        system
            .runtime()
            .fabric()
            .send(
                from,
                leader_id,
                VerticalPaxosMessage::P1B {
                    from,
                    to: leader_id,
                    ballot,
                    pvalues: vec![],
                },
            )
            .await;
    }

    tokio::time::timeout(std::time::Duration::from_millis(250), async {
        loop {
            if system
                .master()
                .activation(config.id())
                .await
                .is_some_and(|record| record.status() == &ActivationStatus::Ready)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("master should observe ready callback");
}

#[tokio::test]
async fn active_replica_can_drive_decision_and_apply_flow() {
    let ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let observer = Arc::new(RecordingObserver::default());
    let system = VerticalSystem::new(
        ids.clone(),
        Arc::new(ClusterPersistence::for_test("vertical_system_apply_flow")),
        observer.clone() as Arc<dyn PaxosObserver>,
    )
    .await
    .expect("system should build");
    system.start().await;

    let config = configuration(&ids, ids[0]);
    system
        .install_configuration(Arc::clone(&config))
        .await
        .expect("install should work");
    system
        .start_activation(config.id(), Arc::new(PredecessorChain::default()))
        .await
        .expect("activation should start");

    observer
        .wait_for_event(
            |event| matches!(event, Event::VerticalActivationReady { configuration_id, .. } if *configuration_id == config.id()),
            Duration::from_secs(2),
        )
        .await;

    let reply = system
        .submit_client_request(
            ids[2],
            PaxosCommand::PUT {
                key: "olive".to_string(),
                version: 0,
                value: 1,
            }
            .with_client(Uuid::new_v4(), 1),
        )
        .await
        .expect("client request should be accepted");
    assert!(matches!(
        reply,
        VerticalClientReply::Accepted { configuration_id, slot: 4 } if configuration_id == config.id()
    ));

    observer
        .wait_for_event(
            |event| matches!(event, Event::VerticalReplicaApplied { slot, .. } if *slot == 4),
            Duration::from_secs(2),
        )
        .await;
}

#[tokio::test]
async fn decommissioned_replica_redirects_to_active_configuration() {
    let ids = vec![
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    ];
    let observer = Arc::new(RecordingObserver::default());
    let system = VerticalSystem::new(
        ids.clone(),
        Arc::new(ClusterPersistence::for_test("vertical_system_redirect")),
        observer.clone() as Arc<dyn PaxosObserver>,
    )
    .await
    .expect("system should build");
    system.start().await;

    let acceptors_a = vec![ids[0], ids[1], ids[2]];
    let replicas_a = vec![ids[0], ids[3]];
    let config_a = Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            ids[0],
            VerticalPaxosVariant::V1,
            Ballot::new(1, ids[0]),
            0,
            acceptors_a.clone(),
            replicas_a.clone(),
            Quorum::new(vec![ids[0], ids[1]]).expect("read quorum should build"),
            Quorum::new(vec![ids[0], ids[2]]).expect("write quorum should build"),
            None,
        )
        .expect("config a should build"),
    );
    let config_b = Arc::new(
        VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            ids[3],
            VerticalPaxosVariant::V2,
            Ballot::new(2, ids[3]),
            1,
            vec![ids[1], ids[2], ids[3]],
            vec![ids[2], ids[3]],
            Quorum::new(vec![ids[1], ids[3]]).expect("read quorum should build"),
            Quorum::new(vec![ids[2], ids[3]]).expect("write quorum should build"),
            Some(config_a.ballot()),
        )
        .expect("config b should build"),
    );

    system
        .install_configuration(Arc::clone(&config_a))
        .await
        .expect("config a should install");
    system
        .start_activation(config_a.id(), Arc::new(PredecessorChain::default()))
        .await
        .expect("config a should activate");
    observer
        .wait_for_event(
            |event| matches!(event, Event::VerticalActivationReady { configuration_id, .. } if *configuration_id == config_a.id()),
            Duration::from_secs(2),
        )
        .await;

    system
        .install_configuration(Arc::clone(&config_b))
        .await
        .expect("config b should install");
    system
        .start_activation(
            config_b.id(),
            Arc::new(PredecessorChain::new(vec![Arc::clone(&config_a)])),
        )
        .await
        .expect("config b should activate");
    observer
        .wait_for_event(
            |event| matches!(event, Event::VerticalActivationReady { configuration_id, .. } if *configuration_id == config_b.id()),
            Duration::from_secs(2),
        )
        .await;

    let reply = system
        .submit_client_request(
            ids[0],
            PaxosCommand::PUT {
                key: "citadel".to_string(),
                version: 0,
                value: 2,
            }
            .with_client(Uuid::new_v4(), 7),
        )
        .await
        .expect("redirected request should return a reply");

    assert!(matches!(
        reply,
        VerticalClientReply::Redirect {
            from_replica,
            active_configuration_id,
            leader,
            ..
        } if from_replica == ids[0] && active_configuration_id == config_b.id() && leader == config_b.leader()
    ));

    observer
        .wait_for_event(
            |event| matches!(event, Event::VerticalReplicaRedirected { from_replica, active_configuration_id, .. } if *from_replica == ids[0] && *active_configuration_id == config_b.id()),
            Duration::from_secs(2),
        )
        .await;
}
