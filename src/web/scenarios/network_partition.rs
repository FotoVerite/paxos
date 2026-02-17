use crate::cluster::cluster::Cluster;
use crate::cluster::network_simulator::NetworkFailure;
use crate::decree_generator::DecreeGenerator;
use crate::monitor::{Event, PaxosObserver, current_timestamp_millis};
use crate::paxos_command::PaxosCommand;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct NetworkPartitionScenario;

impl NetworkPartitionScenario {
    /// Execute one iteration of network partition scenario
    pub async fn execute_iteration(
        cluster: &Arc<Mutex<Cluster>>,
        proposal_count: usize,
        node_count: usize,
        decree_gen: &mut DecreeGenerator,
        observer: Arc<dyn PaxosObserver>,
    ) {
        // Create partition at proposal 0
        if proposal_count == 0 {
            Self::create_partition(cluster, node_count, observer.clone()).await;
        }

        // Heal partition at proposal 5
        if proposal_count == 5 {
            Self::heal_partition(cluster, node_count, observer).await;
        }

        // Regular proposals
        Self::propose(cluster, decree_gen).await;
    }

    async fn create_partition(
        cluster: &Arc<Mutex<Cluster>>,
        _node_count: usize,
        observer: Arc<dyn PaxosObserver>,
    ) {
        println!("Creating network partition at proposal 0");
        let cluster_lock = cluster.lock().await;

        // Partition A: nodes 0,1,2 (quorum)
        let partition_a_vec = vec![
            cluster_lock.uuid(0),
            cluster_lock.uuid(1),
            cluster_lock.uuid(2),
        ];
        // Partition B: nodes 3,4 (no quorum)
        let partition_b_vec = vec![cluster_lock.uuid(3), cluster_lock.uuid(4)];

        // For each node in partition A: block messages TO partition B
        // When A sends to B, block it by specifying B as unreachable
        for from_uuid in partition_a_vec.iter().copied() {
            for target_uuid in partition_b_vec.iter().copied() {
                if let Some(sim) = cluster_lock.get_simulator(from_uuid) {
                    let mut blocked = HashSet::new();
                    blocked.insert(target_uuid);
                    sim.set_failure(target_uuid, NetworkFailure::Partition { nodes: blocked })
                        .await;
                }
            }
        }

        // For each node in partition B: block messages TO partition A
        // When B sends to A, block it by specifying A as unreachable
        for from_uuid in partition_b_vec.iter().copied() {
            for target_uuid in partition_a_vec.iter().copied() {
                if let Some(sim) = cluster_lock.get_simulator(from_uuid) {
                    let mut blocked = HashSet::new();
                    blocked.insert(target_uuid);
                    sim.set_failure(target_uuid, NetworkFailure::Partition { nodes: blocked })
                        .await;
                }
            }
        }

        // Emit partition created event
        observer.on_event(Event::PartitionCreated {
            partition_a: partition_a_vec,
            partition_b: partition_b_vec,
            created_at: current_timestamp_millis(),
        });
    }

    async fn heal_partition(
        cluster: &Arc<Mutex<Cluster>>,
        node_count: usize,
        observer: Arc<dyn PaxosObserver>,
    ) {
        println!("Healing network partition at proposal 5");
        let cluster_lock = cluster.lock().await;
        for i in 0..node_count {
            if let Some(sim) = cluster_lock.get_simulator(cluster_lock.uuid(i)) {
                sim.clear_all_failures().await;
            }
        }

        // Partition A: nodes 0,1,2 (quorum)
        let partition_a_vec = vec![
            cluster_lock.uuid(0),
            cluster_lock.uuid(1),
            cluster_lock.uuid(2),
        ];
        // Partition B: nodes 3,4 (no quorum)
        let partition_b_vec = vec![cluster_lock.uuid(3), cluster_lock.uuid(4)];

        // Emit partition healed event with partition info
        observer.on_event(Event::PartitionHealed {
            partition_a: partition_a_vec,
            partition_b: partition_b_vec,
            created_at: current_timestamp_millis(),
        });
    }

    async fn propose(cluster: &Arc<Mutex<Cluster>>, decree_gen: &mut DecreeGenerator) {
        let mut cluster_lock = cluster.lock().await;
        let decree = decree_gen.next();
        let cmd = PaxosCommand::EnactDecree {
            author: "Proposer".to_string(),
            law: decree,
        };
        cluster_lock.propose(cmd).await;
    }
}
