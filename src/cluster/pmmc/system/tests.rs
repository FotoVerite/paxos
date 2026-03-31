use std::{collections::HashSet, net::IpAddr, path::Path};

use tokio::time::{Duration, sleep, timeout};

use crate::{
    cluster::pmmc::control::types::{
        ConfigurationCommand, ConfigurationOperationStatus, ConfigurationReplyOutcome,
    },
    cluster::pmmc::reconfiguration::reconfig_patch::ReconfigPatch,
    common::persistence::Persistence,
    node::config::{PmmcNodeConfig, Roles},
    paxos_command::PaxosCommand,
    rsm::kv_store::ReplyOutcome,
};

use super::PmmcCluster;
use super::fixtures::PmmcTestCluster;

#[tokio::test]
async fn role_split_cluster_sets_expected_quorum_and_node_count() {
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

    let fixture = PmmcTestCluster::with_configs(configs)
        .await
        .expect("role-split cluster should initialize");
    let cluster = fixture.cluster();

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
    let mut fixture = PmmcTestCluster::new(1)
        .await
        .expect("single-node PMMC cluster should initialize");
    let uuid = fixture.cluster().get_node_uuids()[0];
    let persistence = Persistence::cluster(fixture.ip()).node(uuid);
    let _ = std::fs::remove_dir_all(persistence.dir());

    fixture
        .start_ready(Duration::from_secs(5))
        .await
        .expect("single-node cluster should elect a leader");
    let _ = fixture
        .request_any_replica(
            1,
            PaxosCommand::PUT {
                key: "persist".to_string(),
                version: 1,
                value: 7,
            },
            Duration::from_secs(8),
        )
        .await
        .expect("single-node write should succeed");

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
    let mut fixture = PmmcTestCluster::new(1)
        .await
        .expect("single-node PMMC cluster should initialize");
    let persistence = Persistence::cluster(fixture.ip());
    let _ = std::fs::remove_dir_all(persistence.dir());

    fixture
        .start_ready(Duration::from_secs(5))
        .await
        .expect("single-node cluster should elect a leader");

    let _ = fixture
        .request_any_replica(
            1,
            PaxosCommand::PUT {
                key: "cleanup".to_string(),
                version: 1,
                value: 9,
            },
            Duration::from_secs(8),
        )
        .await
        .expect("cleanup write should succeed");

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

    fixture
        .cluster_mut()
        .cleanup()
        .await
        .expect("cleanup should succeed");
    assert!(
        !persistence.dir().exists(),
        "cleanup should remove the entire cluster persistence root"
    );
}

#[tokio::test]
async fn configuration_endpoint_submit_and_await_roundtrip() {
    let mut fixture = PmmcTestCluster::new(1)
        .await
        .expect("single-node PMMC cluster should initialize");
    fixture
        .start_ready(Duration::from_secs(5))
        .await
        .expect("single-node cluster should become ready");
    let cluster = fixture.cluster();

    let node = cluster.get_node_uuids()[0];

    let emit_op = cluster
        .process_manager
        .submit_configuration_op(node, ConfigurationCommand::Status)
        .await
        .expect("emit op should submit");
    let emit = cluster
        .process_manager
        .await_configuration_op(emit_op, Duration::from_secs(2))
        .await
        .expect("emit op should complete");
    assert_eq!(emit, ConfigurationReplyOutcome::Active);

    let stop_op = cluster
        .process_manager
        .submit_configuration_op(node, ConfigurationCommand::Stop)
        .await
        .expect("stop op should submit");
    let stop = cluster
        .process_manager
        .await_configuration_op(stop_op, Duration::from_secs(6))
        .await
        .expect("stop op should complete");
    assert_eq!(stop, ConfigurationReplyOutcome::Stopped);

    let status = cluster
        .process_manager
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
    let mut fixture = PmmcTestCluster::new(3)
        .await
        .expect("three-node PMMC cluster should initialize");
    fixture
        .start_ready(Duration::from_secs(8))
        .await
        .expect("cluster should become ready");
    let cluster = fixture.cluster();

    let initial_leader_idx = cluster
        .leader_index()
        .await
        .expect("leader should exist after readiness barrier");
    let initial_members = cluster.get_node_uuids();
    let removed_leader_uuid = initial_members[initial_leader_idx];

    let write_before = fixture
        .request_any_replica(
            1,
            PaxosCommand::PUT {
                key: "reconfig_restart_key".to_string(),
                version: 1,
                value: 11,
            },
            Duration::from_secs(20),
        )
        .await
        .expect("write before reconfiguration should succeed");
    assert!(
        matches!(write_before, ReplyOutcome::WriteOk { .. }),
        "expected write before reconfiguration to succeed, got: {:?}",
        write_before
    );

    let new_replica_uuid = PmmcCluster::node_uuid(fixture.ip(), 99);
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

    fixture
        .reconfigure_and_ready(patch, Duration::from_secs(30))
        .await
        .expect("reconfiguration should stop and restart cluster");
    let cluster = fixture.cluster();

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

    let read_after_restart = fixture
        .request_any_replica(
            2,
            PaxosCommand::GET {
                key: "reconfig_restart_key".to_string(),
            },
            Duration::from_secs(20),
        )
        .await
        .expect("read after restart should return a response");
    match read_after_restart {
        ReplyOutcome::GetOk { value, .. } => assert_eq!(value.0, 11),
        other => panic!("expected GetOk after restart, got: {:?}", other),
    }

    let write_after_restart = fixture
        .request_any_replica(
            3,
            PaxosCommand::PUT {
                key: "reconfig_restart_key".to_string(),
                version: 1,
                value: 42,
            },
            Duration::from_secs(20),
        )
        .await
        .expect("write after restart should return a response");
    assert!(
        matches!(write_after_restart, ReplyOutcome::WriteOk { .. }),
        "expected write after restart to succeed, got: {:?}",
        write_after_restart
    );

    let read_after_write = fixture
        .request_any_replica(
            4,
            PaxosCommand::GET {
                key: "reconfig_restart_key".to_string(),
            },
            Duration::from_secs(20),
        )
        .await
        .expect("read after write should return a response");
    match read_after_write {
        ReplyOutcome::GetOk { value, .. } => assert_eq!(value.0, 42),
        other => panic!("expected final GetOk with updated value, got: {:?}", other),
    }
}

#[tokio::test]
async fn update_configuration_lifecycle_finishes_within_30s_and_preserves_state() {
    let mut fixture = PmmcTestCluster::new(3)
        .await
        .expect("three-node PMMC cluster should initialize");
    fixture
        .start_ready(Duration::from_secs(8))
        .await
        .expect("cluster should become ready");

    let write_before = fixture
        .request_any_replica(
            1,
            PaxosCommand::PUT {
                key: "lifecycle_key".to_string(),
                version: 1,
                value: 17,
            },
            Duration::from_secs(20),
        )
        .await
        .expect("initial write should succeed");
    assert!(
        matches!(write_before, ReplyOutcome::WriteOk { .. }),
        "expected initial write to succeed, got: {:?}",
        write_before
    );

    let patch = ReconfigPatch::new().alpha_max_inflight(9);
    timeout(
        Duration::from_secs(30),
        fixture.reconfigure_and_ready(patch, Duration::from_secs(30)),
    )
    .await
    .expect("update_configuration timed out after 30 seconds")
    .expect("update_configuration should succeed");

    let cluster = fixture.cluster();

    let status_node = cluster.get_node_uuids()[0];
    let status = cluster
        .process_manager
        .configuration_operation(
            status_node,
            ConfigurationCommand::Status,
            Duration::from_secs(2),
        )
        .await
        .expect("status should be queryable after update_configuration");
    assert_eq!(status, ConfigurationReplyOutcome::Active);

    let read_after = fixture
        .request_any_replica(
            2,
            PaxosCommand::GET {
                key: "lifecycle_key".to_string(),
            },
            Duration::from_secs(20),
        )
        .await
        .expect("read after lifecycle update should succeed");
    match read_after {
        ReplyOutcome::GetOk { value, .. } => assert_eq!(value.0, 17),
        other => panic!(
            "expected persisted value after lifecycle restart, got: {:?}",
            other
        ),
    }
}
