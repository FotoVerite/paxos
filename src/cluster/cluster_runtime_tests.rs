use std::{collections::HashSet, net::IpAddr, path::Path, sync::Arc};

use tokio::time::{Duration, sleep, timeout};

use crate::{
    common::persistence::Persistence,
    monitor::NoOpObserver,
    node::config::{PmmcNodeConfig, Roles},
    paxos_command::PaxosCommand,
};

use super::ClusterRuntime;

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

    let cluster = ClusterRuntime::new_with_configs(ip, configs, Arc::new(NoOpObserver))
        .await
        .expect("role-split cluster should initialize");

    assert_eq!(cluster.num_nodes(), 7);
    assert_eq!(cluster.quorum_size(), 2, "3 acceptors => quorum 2");
    assert_eq!(cluster.get_node_uuids().len(), 7);
}

#[test]
fn node_uuid_is_stable_and_unique_per_index() {
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let u0a = ClusterRuntime::node_uuid(ip, 0);
    let u0b = ClusterRuntime::node_uuid(ip, 0);
    let u1 = ClusterRuntime::node_uuid(ip, 1);

    assert_eq!(u0a, u0b, "same ip/index must produce stable UUID");
    assert_ne!(u0a, u1, "different node indexes must map to different UUIDs");

    let mut set = HashSet::new();
    for i in 0..8 {
        set.insert(ClusterRuntime::node_uuid(ip, i));
    }
    assert_eq!(set.len(), 8, "UUID mapping should be unique across indexes");
}

#[tokio::test]
async fn single_node_cluster_persists_state_under_ip_node_directory() {
    let ip = IpAddr::V4([127, 0, 0, 42].into());
    let cluster = ClusterRuntime::new(ip, 1, Arc::new(NoOpObserver))
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

    cluster
        .propose_from(
            0,
            PaxosCommand::PUT {
                key: "persist".to_string(),
                version: 1,
                value: 7,
            }
            .with_client(uuid, 1),
        )
        .await;

    timeout(Duration::from_secs(2), async {
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
