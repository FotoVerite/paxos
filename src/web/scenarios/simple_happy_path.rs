use crate::cluster::cluster::Cluster;
use crate::decree_generator::DecreeGenerator;
use crate::monitor::Event;
use crate::node::config::{ClassicNodeConfig, LearningStrategy, Roles};
use crate::paxos_command::PaxosCommand;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SimpleHappyPathScenario;

impl SimpleHappyPathScenario {
    /// Initialize a 5-node cluster with a designated leader
    /// leader_id: if Some, use that node as leader; if None, pick randomly
    pub async fn init_cluster(
        id: usize,
        ip: std::net::IpAddr,
        observer: Arc<dyn crate::monitor::PaxosObserver>,
        leader_node: Option<usize>,
        learning_strategy: LearningStrategy,
    ) -> anyhow::Result<Cluster> {
        // Create 5 full nodes with the specified learning strategy
        let configs = (0..5)
            .map(|_| ClassicNodeConfig {
                roles: Roles {
                    proposer: true,
                    acceptor: true,
                    learner: true,
                },
                learning_strategy: learning_strategy.clone(),
            })
            .collect();

        let cluster = Cluster::new_with_configs(id, ip, configs, observer.clone()).await?;

        // Emit leader elected event
        let elected_leader = leader_node.unwrap_or_else(|| {
            // Random selection from 0-4
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                % 5) as usize
        });

        let leader_uuid = cluster.nodes[elected_leader].uuid;
        observer.on_event(Event::LeaderElected {
            id: leader_uuid,
            created_at: crate::monitor::current_timestamp_millis(),
        });

        Ok(cluster)
    }

    /// Execute one iteration of the simple happy path scenario
    /// Leader proposes commands regularly
    pub async fn execute_iteration(
        cluster: &Arc<Mutex<Cluster>>,
        proposal_count: usize,
        decree_gen: &mut DecreeGenerator,
        leader_node: usize,
    ) {
        // Leader proposes every 25 iterations
        if proposal_count % 25 == 0 {
            let cluster_lock = cluster.lock().await;
            let decree = decree_gen.next();
            let cmd = PaxosCommand::EnactDecree {
                author: format!("Leader {}", leader_node),
                law: decree,
            };

            // Only node `leader_node` proposes
            cluster_lock.nodes[leader_node].propose(cmd, None).await;
        }
    }
}
