use std::sync::Arc;
use tokio::sync::Mutex;
use crate::cluster::cluster::Cluster;
use crate::paxos_command::PaxosCommand;
use crate::decree_generator::DecreeGenerator;
use crate::node::config::{NodeConfig, Roles, LearningStrategy};

pub struct PartialRolesScenario;

impl PartialRolesScenario {
    pub async fn init_cluster(id: usize, ip: std::net::IpAddr, observer: Arc<dyn crate::monitor::PaxosObserver>) -> anyhow::Result<Cluster> {
        let mut configs = Vec::new();
        
        // Node 0-1: Proposers only
         for _ in 0..2 {
             configs.push(NodeConfig {
                 roles: Roles { proposer: true, acceptor: false, learner: false },
                 learning_strategy: LearningStrategy::ProposerManaged,
             });
         }
         
         // Node 2-4: Acceptors only
         for _ in 0..3 {
             configs.push(NodeConfig {
                 roles: Roles { proposer: false, acceptor: true, learner: false },
                 learning_strategy: LearningStrategy::ProposerManaged,
             });
         }
         
         // Node 5-6: Learners only
         for _ in 0..2 {
             configs.push(NodeConfig {
                 roles: Roles { proposer: false, acceptor: false, learner: true },
                 learning_strategy: LearningStrategy::ProposerManaged,
             });
         }
         
         // Node 7-8: Full nodes (proposers + acceptors + learners)
         for _ in 0..2 {
             configs.push(NodeConfig {
                 roles: Roles { proposer: true, acceptor: true, learner: true },
                 learning_strategy: LearningStrategy::ProposerManaged,
             });
         }

        Cluster::new_with_configs(id, ip, configs, observer).await
    }

    pub async fn execute_iteration(
        cluster: &Arc<Mutex<Cluster>>,
        proposal_count: usize,
        decree_gen: &mut DecreeGenerator,
    ) {
        // Propose every 30 iterations if we have proposers
        if proposal_count % 30 == 0 {
            let cluster_lock = cluster.lock().await;
            let decree = decree_gen.next();
            let cmd = PaxosCommand::EnactDecree {
                author: format!("Partial Roles User {}", proposal_count / 30),
                law: decree,
            };
            
            // Randomly select a node that has the proposer role
            // In our config: 0, 1, 7, 8 are proposers
            let proposers = vec![0, 1, 7, 8];
            let proposer_idx = proposers[proposal_count % proposers.len()];
            
            cluster_lock.nodes[proposer_idx].propose(cmd, None).await;
        }
    }
}
