use std::{collections::HashSet, net::IpAddr, sync::Arc};

use crate::{
    monitor::NoOpObserver,
    node::config::{PmmcNodeConfig, Roles},
};

use super::PmmcCluster;

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

    let cluster = PmmcCluster::new_with_configs(0, ip, configs, Arc::new(NoOpObserver))
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
    assert_ne!(u0a, u1, "different node indexes must map to different UUIDs");

    let mut set = HashSet::new();
    for i in 0..8 {
        set.insert(PmmcCluster::node_uuid(ip, i));
    }
    assert_eq!(set.len(), 8, "UUID mapping should be unique across indexes");
}
