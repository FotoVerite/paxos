use std::{collections::HashSet, net::IpAddr, path::Path, sync::Arc};

use tokio::time::{Duration, sleep, timeout};
use uuid::Uuid;

use crate::{
    cluster::cluster_configuration::reconfig_patch::ReconfigPatch,
    cluster::configuration_handler::types::{
        ConfigurationCommand, ConfigurationOperationStatus, ConfigurationReplyOutcome,
    },
    common::persistence::Persistence,
    message::ClientMessage,
    monitor::NoOpObserver,
    rsm::kv_store::ReplyOutcome,
    node::config::{PmmcNodeConfig, Roles},
    paxos_command::PaxosCommand,
};

use super::PmmcCluster;

async fn propose_via_client(cluster: &PmmcCluster, replica_idx: usize, cmd: PaxosCommand) {
    let client_id = Uuid::new_v4();
    let (tx, _rx) = cluster
        .connect_client_to(replica_idx, client_id)
        .await
        .expect("replica should accept client connection");
    tx.send(ClientMessage::PROPOSE { cmd })
        .await
        .expect("client propose send should succeed");
}

async fn wait_for_leader_index(cluster: &PmmcCluster, wait: Duration) -> usize {
    timeout(wait, async {
        loop {
            if let Some(idx) = cluster.leader_index().await {
                break idx;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("cluster should elect a leader")
}

async fn request_via_cluster(cluster: &PmmcCluster, request_id: u64, cmd: PaxosCommand) -> ReplyOutcome {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("client response timed out across all replicas");
        }

        for replica_idx in 0..cluster.num_nodes() {
            let client_id = Uuid::new_v4();
            let Some((tx, mut rx)) = cluster.connect_client_to(replica_idx, client_id).await else {
                continue;
            };

            if tx
                .send(ClientMessage::PROPOSE {
                    cmd: cmd.clone().with_client(client_id, request_id),
                })
                .await
                .is_err()
            {
                continue;
            }

            if let Ok(Some(ClientMessage::RESPONSE { response, .. })) =
                timeout(Duration::from_secs(2), rx.recv()).await
            {
                return response;
            }
        }

        sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn role_split_cluster_sets_expected_quorum_and_node_count() {
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut configs = Vec::new();
    for _ in 0..2 {
        configs.push(PmmcNodeConfig {
            roles: Roles {
                proposer: true,
                acceptor: false,
                learner: false,
            },
        });
    }
    for _ in 0..2 {
        configs.push(PmmcNodeConfig {
            roles: Roles {
                proposer: false,
                acceptor: false,
                learner: true,
            },
        });
    }
    for _ in 0..3 {
        configs.push(PmmcNodeConfig {
            roles: Roles {
                proposer: false,
                acceptor: true,
                learner: false,
            },
        });
    }

    let cluster = PmmcCluster::new_with_configs(ip, configs, Arc::new(NoOpObserver))
        .await
        .expect("role-split cluster should initialize");

    assert_eq!(cluster.num_nodes(), 7);
    assert_eq!(cluster.quorum_size(), 2, "3 acceptors => quorum 2");
    assert_eq!(cluster.get_node_uuids().len(), 7);
}

#[test]
fn node_uuid_is_stable_and_unique_per_index() {
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let u0a = PmmcCluster::node_uuid(ip, 0);
    let u0b = PmmcCluster::node_uuid(ip, 0);
    let u1 = PmmcCluster::node_uuid(ip, 1);

    assert_eq!(u0a, u0b, "same ip/index must produce stable UUID");
    assert_ne!(
        u0a, u1,
        "different node indexes must map to different UUIDs"
    );

    let mut set = HashSet::new();
    for i in 0..8 {
        set.insert(PmmcCluster::node_uuid(ip, i));
    }
    assert_eq!(set.len(), 8, "UUID mapping should be unique across indexes");
}

#[tokio::test]
async fn single_node_cluster_persists_state_under_ip_node_directory() {
    let ip = IpAddr::V4([127, 0, 0, 42].into());
    let cluster = PmmcCluster::new(ip, 1, Arc::new(NoOpObserver))
        .await
        .expect("single-node PMMC cluster should initialize");

    let uuid = cluster.get_node_uuids()[0];
    let persistence = Persistence::cluster(ip).node(uuid);
    let _ = std::fs::remove_dir_all(persistence.dir());

    cluster.start_all().await;

    timeout(Duration::from_secs(2), async {
        loop {
            if cluster.leader().await.is_some() {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("single-node cluster should elect a leader");

    propose_via_client(
        &cluster,
        0,
        PaxosCommand::PUT {
            key: "persist".to_string(),
            version: 1,
            value: 7,
        }
        .with_client(uuid, 1),
    )
    .await;

    timeout(Duration::from_secs(30), async {
        loop {
            let acceptor_path = persistence.dir().join("acceptor.bin");
            let store_path = persistence.dir().join("store.bin");
            if Path::new(&acceptor_path).exists() && Path::new(&store_path).exists() {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("PMMC durable files should be written under the node directory");

    assert!(persistence.dir().ends_with(uuid.to_string()));
    assert!(persistence.dir().join("acceptor.bin").exists());
    assert!(persistence.dir().join("store.bin").exists());
}

#[tokio::test]
async fn cleanup_removes_cluster_persistence_root() {
    let ip = IpAddr::V4([127, 0, 0, 43].into());
    let cluster = PmmcCluster::new(ip, 1, Arc::new(NoOpObserver))
        .await
        .expect("single-node PMMC cluster should initialize");

    let persistence = Persistence::cluster(ip);
    let _ = std::fs::remove_dir_all(persistence.dir());

    cluster.start_all().await;
    timeout(Duration::from_secs(2), async {
        loop {
            if cluster.leader().await.is_some() {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("single-node cluster should elect a leader");

    propose_via_client(
        &cluster,
        0,
        PaxosCommand::PUT {
            key: "cleanup".to_string(),
            version: 1,
            value: 9,
        }
        .with_client(cluster.get_node_uuids()[0], 1),
    )
    .await;

    timeout(Duration::from_secs(2), async {
        loop {
            if persistence.dir().exists() {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("cluster persistence root should exist before cleanup");

    cluster.cleanup().await.expect("cleanup should succeed");
    assert!(
        !persistence.dir().exists(),
        "cleanup should remove the entire cluster persistence root"
    );
}

#[tokio::test]
async fn configuration_endpoint_submit_and_await_roundtrip() {
    let ip = IpAddr::V4([127, 0, 0, 44].into());
    let cluster = PmmcCluster::new(ip, 1, Arc::new(NoOpObserver))
        .await
        .expect("single-node PMMC cluster should initialize");
    cluster.start_all().await;

    let node = cluster.get_node_uuids()[0];

    let emit_op = cluster
        .runtime_registry
        .submit_configuration_op(node, ConfigurationCommand::Status)
        .await
        .expect("emit op should submit");
    let emit = cluster
        .runtime_registry
        .await_configuration_op(emit_op, Duration::from_secs(2))
        .await
        .expect("emit op should complete");
    assert_eq!(emit, ConfigurationReplyOutcome::Active);

    let stop_op = cluster
        .runtime_registry
        .submit_configuration_op(node, ConfigurationCommand::Stop)
        .await
        .expect("stop op should submit");
    let stop = cluster
        .runtime_registry
        .await_configuration_op(stop_op, Duration::from_secs(6))
        .await
        .expect("stop op should complete");
    assert_eq!(stop, ConfigurationReplyOutcome::Stopped);

    let status = cluster
        .runtime_registry
        .configuration_op_status(stop_op)
        .await
        .expect("status should exist");
    assert_eq!(
        status,
        ConfigurationOperationStatus::Completed(ConfigurationReplyOutcome::Stopped)
    );
}

#[tokio::test]
async fn reconfig_remove_leader_add_replica_preserves_state_and_progresses_after_restart() {
    let ip = IpAddr::V4([127, 0, 0, 45].into());
    Persistence::cluster(ip)
        .purge_cluster_dir()
        .await
        .expect("reconfig test should start from a clean persistence directory");
    let mut cluster = PmmcCluster::new(ip, 3, Arc::new(NoOpObserver))
        .await
        .expect("three-node PMMC cluster should initialize");
    cluster.start_all().await;

    let initial_leader_idx = wait_for_leader_index(&cluster, Duration::from_secs(8)).await;
    let initial_members = cluster.get_node_uuids();
    let removed_leader_uuid = initial_members[initial_leader_idx];

    let write_before = request_via_cluster(
        &cluster,
        1,
        PaxosCommand::PUT {
            key: "reconfig_restart_key".to_string(),
            version: 1,
            value: 11,
        },
    )
    .await;
    assert!(
        matches!(write_before, ReplyOutcome::WriteOk { .. }),
        "expected write before reconfiguration to succeed, got: {:?}",
        write_before
    );

    let new_replica_uuid = PmmcCluster::node_uuid(ip, 99);
    let patch = ReconfigPatch::new()
        .add_node(
            new_replica_uuid,
            Roles {
                proposer: false,
                acceptor: false,
                learner: true,
            },
        )
        .remove_node(removed_leader_uuid);

    cluster
        .update_configuration(patch)
        .await
        .expect("reconfiguration should stop and restart cluster");
    wait_for_leader_index(&cluster, Duration::from_secs(8)).await;

    let members_after = cluster.get_node_uuids();
    assert_eq!(members_after.len(), 3);
    assert!(
        members_after.contains(&new_replica_uuid),
        "new replica UUID should be present after reconfiguration"
    );
    assert!(
        !members_after.contains(&removed_leader_uuid),
        "removed leader UUID should not be present after reconfiguration"
    );

    let read_after_restart = request_via_cluster(
        &cluster,
        2,
        PaxosCommand::GET {
            key: "reconfig_restart_key".to_string(),
        },
    )
    .await;
    match read_after_restart {
        ReplyOutcome::GetOk { value, .. } => assert_eq!(value.0, 11),
        other => panic!("expected GetOk after restart, got: {:?}", other),
    }

    let write_after_restart = request_via_cluster(
        &cluster,
        3,
        PaxosCommand::PUT {
            key: "reconfig_restart_key".to_string(),
            version: 1,
            value: 42,
        },
    )
    .await;
    assert!(
        matches!(write_after_restart, ReplyOutcome::WriteOk { .. }),
        "expected write after restart to succeed, got: {:?}",
        write_after_restart
    );

    let read_after_write = request_via_cluster(
        &cluster,
        4,
        PaxosCommand::GET {
            key: "reconfig_restart_key".to_string(),
        },
    )
    .await;
    match read_after_write {
        ReplyOutcome::GetOk { value, .. } => assert_eq!(value.0, 42),
        other => panic!("expected final GetOk with updated value, got: {:?}", other),
    }
}

#[tokio::test]
async fn update_configuration_lifecycle_finishes_within_30s_and_preserves_state() {
    let ip = IpAddr::V4([127, 0, 0, 46].into());
    Persistence::cluster(ip)
        .purge_cluster_dir()
        .await
        .expect("lifecycle test should start from a clean persistence directory");
    let mut cluster = PmmcCluster::new(ip, 3, Arc::new(NoOpObserver))
        .await
        .expect("three-node PMMC cluster should initialize");
    cluster.start_all().await;
    wait_for_leader_index(&cluster, Duration::from_secs(5)).await;

    let write_before = request_via_cluster(
        &cluster,
        1,
        PaxosCommand::PUT {
            key: "lifecycle_key".to_string(),
            version: 1,
            value: 17,
        },
    )
    .await;
    assert!(
        matches!(write_before, ReplyOutcome::WriteOk { .. }),
        "expected initial write to succeed, got: {:?}",
        write_before
    );

    let patch = ReconfigPatch::new().alpha_max_inflight(9);
    timeout(Duration::from_secs(30), cluster.update_configuration(patch))
        .await
        .expect("update_configuration timed out after 30 seconds")
        .expect("update_configuration should succeed");

    wait_for_leader_index(&cluster, Duration::from_secs(5)).await;

    let status_node = cluster.get_node_uuids()[0];
    let status = cluster
        .runtime_registry
        .configuration_operation(status_node, ConfigurationCommand::Status, Duration::from_secs(2))
        .await
        .expect("status should be queryable after update_configuration");
    assert_eq!(status, ConfigurationReplyOutcome::Active);

    let read_after = request_via_cluster(
        &cluster,
        2,
        PaxosCommand::GET {
            key: "lifecycle_key".to_string(),
        },
    )
    .await;
    match read_after {
        ReplyOutcome::GetOk { value, .. } => assert_eq!(value.0, 17),
        other => panic!("expected persisted value after lifecycle restart, got: {:?}", other),
    }
}
