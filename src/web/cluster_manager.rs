use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::cluster::cluster::Cluster;
use crate::paxos_command::PaxosCommand;
use crate::web::websocket_observer::WebSocketObserver;

pub struct ClusterManager {
    cluster: Mutex<Option<Arc<Mutex<Cluster>>>>,
    observer: Arc<WebSocketObserver>,
}

impl ClusterManager {
    pub fn new(observer: Arc<WebSocketObserver>) -> Self {
        Self {
            cluster: Mutex::new(None),
            observer,
        }
    }

    pub async fn start_scenario(&self, node_count: usize, duration_secs: u64) -> anyhow::Result<()> {
        println!("Starting new scenario with {} nodes for {} seconds", node_count, duration_secs);
        
        // Create new cluster
        let mut cluster = Cluster::new(0, node_count, self.observer.clone()).await?;
        
        // Send cluster info to visualizer
        self.observer.set_cluster_info(node_count, cluster.quorum_size()).await;
        
        // Start all nodes
        for i in 0..node_count {
            cluster.nodes[i].start();
        }
        
        sleep(Duration::from_millis(100)).await;
        
        let cluster_arc = Arc::new(Mutex::new(cluster));
        
        // Store cluster
        {
            let mut current = self.cluster.lock().await;
            *current = Some(cluster_arc.clone());
        }
        
        // Spawn scenario runner
        let cluster_for_runner = cluster_arc.clone();
        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let mut proposal_count = 0;
            
            while start.elapsed().as_secs() < duration_secs {
                // Make a proposal every 2 seconds (every 20 iterations of 100ms)
                if proposal_count % 20 == 0 {
                    let mut cluster = cluster_for_runner.lock().await;
                    let cmd = PaxosCommand::EnactDecree {
                        author: format!("Web User {}", proposal_count / 20),
                        law: format!("Proposal #{}", proposal_count / 20),
                    };
                    cluster.propose(cmd).await;
                }
                
                proposal_count += 1;
                sleep(Duration::from_millis(100)).await;
            }
            
            println!("Scenario completed after {} seconds", duration_secs);
        });
        
        Ok(())
    }

    pub async fn propose(&self, decree: PaxosCommand) -> anyhow::Result<()> {
        let current = self.cluster.lock().await;
        if let Some(cluster_arc) = current.as_ref() {
            let mut cluster = cluster_arc.lock().await;
            cluster.propose(decree).await;
            Ok(())
        } else {
            Err(anyhow::anyhow!("No active cluster"))
        }
    }
}
