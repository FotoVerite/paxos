use std::sync::Arc;

use rand::Rng;
use crate::cluster::cluster::Cluster;
use crate::common::types::NodeId;
use crate::paxos_command::PaxosCommand;
use crate::decree_generator::DecreeGenerator;

pub struct CompetingProposersScenario;

impl CompetingProposersScenario {
    /// Execute one iteration of competing proposers scenario
    /// Random proposers propose simultaneously every 2 iterations
    pub async fn execute_iteration(
        cluster: &Arc<tokio::sync::Mutex<Cluster>>,
        proposal_count: usize,
        decree_gen: &mut DecreeGenerator,
    ) {
        if proposal_count % 2 == 0 {
            let cluster0 = cluster.clone();
            let cluster1 = cluster.clone();

            // Get decrees for each proposer
            let decree0 = decree_gen.next();
            let decree1 = decree_gen.next();

            // Spawn both proposals concurrently
            let p0 = tokio::spawn(async move {
                let mut cluster = cluster0.lock().await;
                let cmd = PaxosCommand::EnactDecree {
                    author: "Proposer 0".to_string(),
                    law: decree0,
                };
                let pick = [0, 2, 3, 4][rand::rng().random_range(0..4)];
                cluster.propose_from(NodeId(pick), cmd).await;
            });

            let p1 = tokio::spawn(async move {
                let mut cluster = cluster1.lock().await;
                let cmd = PaxosCommand::EnactDecree {
                    author: "Proposer 1".to_string(),
                    law: decree1,
                };
                cluster.propose_from(NodeId(1), cmd).await;
            });

            // Wait for both to complete
            let _ = tokio::join!(p0, p1);
        }
    }
}
