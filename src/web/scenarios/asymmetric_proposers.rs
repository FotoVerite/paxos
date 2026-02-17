use std::sync::Arc;

use crate::cluster::cluster::Cluster;
use crate::decree_generator::DecreeGenerator;
use crate::node::config::{ClassicNodeConfig, LearningStrategy, Roles};
use crate::paxos_command::PaxosCommand;

pub struct AsymmetricProposersScenario;

impl AsymmetricProposersScenario {
    /// Initialize a 6-node cluster with 2 proposer/learners and 4 acceptors
    pub async fn init_cluster(
        id: usize,
        ip: std::net::IpAddr,
        observer: Arc<dyn crate::monitor::PaxosObserver>,
        learning_strategy: LearningStrategy,
    ) -> anyhow::Result<Cluster> {
        let mut configs = Vec::new();

        // Nodes 0-1: Proposers + Learners
        for _ in 0..2 {
            configs.push(ClassicNodeConfig {
                roles: Roles {
                    proposer: true,
                    acceptor: false,
                    learner: true,
                },
                learning_strategy: learning_strategy.clone(),
            });
        }

        // Nodes 2-5: Acceptors only
        for _ in 0..4 {
            configs.push(ClassicNodeConfig {
                roles: Roles {
                    proposer: false,
                    acceptor: true,
                    learner: false,
                },
                learning_strategy: learning_strategy.clone(),
            });
        }

        Cluster::new_with_configs(id, ip, configs, observer).await
    }

    /// Execute one iteration of competing proposers scenario
    /// Nodes 0 and 1 propose simultaneously every iteration
    pub async fn execute_iteration(
        cluster: &Arc<tokio::sync::Mutex<Cluster>>,
        proposal_count: usize,
        decree_gen: &mut DecreeGenerator,
    ) {
        // Both proposers propose every iteration, simultaneously
        if proposal_count > 0 {
            let cluster0 = cluster.clone();
            let cluster1 = cluster.clone();

            // Get decrees for each proposer
            let decree0 = decree_gen.next();
            let decree1 = decree_gen.next();

            // Spawn both proposals concurrently to ensure they happen at the same time
            let p0 = tokio::spawn(async move {
                let mut cluster = cluster0.lock().await;
                let cmd = PaxosCommand::EnactDecree {
                    author: "Proposer 0".to_string(),
                    law: decree0,
                };
                let proposer_uuid = cluster.nodes[0].uuid;
                cluster.propose_from(proposer_uuid, cmd).await;
            });

            let p1 = tokio::spawn(async move {
                let mut cluster = cluster1.lock().await;
                let cmd = PaxosCommand::EnactDecree {
                    author: "Proposer 1".to_string(),
                    law: decree1,
                };
                let proposer_uuid = cluster.nodes[1].uuid;
                cluster.propose_from(proposer_uuid, cmd).await;
            });

            // Wait for both to complete
            let _ = tokio::join!(p0, p1);
        }
    }
}
