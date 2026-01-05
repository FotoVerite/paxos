use std::sync::Arc;
use tokio::sync::Mutex;
use crate::cluster::cluster::Cluster;
use crate::paxos_command::PaxosCommand;
use crate::decree_generator::DecreeGenerator;

pub struct HappyPathScenario;

impl HappyPathScenario {
    /// Execute one iteration of happy path scenario
    /// Proposals every 20 iterations (every 100 seconds = 2 seconds real time)
    pub async fn execute_iteration(
        cluster: &Arc<Mutex<Cluster>>,
        proposal_count: usize,
        decree_gen: &mut DecreeGenerator,
    ) {
        if proposal_count % 20 == 0 {
            let mut cluster_lock = cluster.lock().await;
            let decree = decree_gen.next();
            let cmd = PaxosCommand::EnactDecree {
                author: format!("Web User {}", proposal_count / 20),
                law: decree,
            };
            cluster_lock.propose(cmd).await;
        }
    }
}
