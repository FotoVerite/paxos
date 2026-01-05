use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::cluster::cluster::Cluster;
use crate::paxos_command::PaxosCommand;
use crate::decree_generator::DecreeGenerator;
use crate::cluster::network_simulator::NetworkFailure;
use crate::monitor::{PaxosObserver, Event, current_timestamp_millis};

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
        node_count: usize,
        observer: Arc<dyn PaxosObserver>,
    ) {
        println!("Creating network partition at proposal 0");
        let cluster_lock = cluster.lock().await;

        // Partition A: nodes 0,1,2 (quorum)
        let partition_a: HashSet<usize> = [0, 1, 2].iter().copied().collect();
        let partition_a_vec = vec![0, 1, 2];
        // Partition B: nodes 3,4 (no quorum)
        let partition_b: HashSet<usize> = [3, 4].iter().copied().collect();
        let partition_b_vec = vec![3, 4];

        // For each node in partition A: block messages to partition B
        for i in [0, 1, 2] {
            for target in [3, 4] {
                if let Some(sim) = cluster_lock.get_simulator(i) {
                    sim.set_failure(
                        target,
                        NetworkFailure::Partition {
                            nodes: partition_a.clone(),
                        },
                    )
                    .await;
                }
            }
        }

        // For each node in partition B: block messages to partition A
        for i in [3, 4] {
            for target in [0, 1, 2] {
                if let Some(sim) = cluster_lock.get_simulator(i) {
                    sim.set_failure(
                        target,
                        NetworkFailure::Partition {
                            nodes: partition_b.clone(),
                        },
                    )
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
            if let Some(sim) = cluster_lock.get_simulator(i) {
                sim.clear_all_failures().await;
            }
        }

        // Emit partition healed event
        observer.on_event(Event::PartitionHealed {
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
