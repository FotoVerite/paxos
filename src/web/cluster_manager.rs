use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, sleep};

use crate::cluster::cluster::Cluster;
use crate::paxos_command::PaxosCommand;
use crate::web::websocket_observer::WebSocketObserver;

pub struct ClusterManager {
    cluster: Mutex<Option<Arc<Mutex<Cluster>>>>,
    observer: Arc<WebSocketObserver>,
    stop_tx: Mutex<Option<broadcast::Sender<()>>>,
}

impl ClusterManager {
    pub fn new() -> Self {
        Self {
            cluster: Mutex::new(None),
            observer: Arc::new(WebSocketObserver::new(500)),
            stop_tx: Mutex::new(None),
        }
    }
    pub fn get_observer(&self) -> Arc<WebSocketObserver> {
        return Arc::clone(&self.observer)
    }

    pub async fn start_scenario(
        &self,
        node_count: usize,
        duration_secs: u64,
        scenario_type: &str,
    ) -> anyhow::Result<()> {
        println!(
            "Starting new scenario '{}' with {} nodes for {} seconds",
            scenario_type, node_count, duration_secs
        );

        // Create cancellation channel
        let (stop_tx, _) = broadcast::channel(1);
        {
            let mut tx = self.stop_tx.lock().await;
            *tx = Some(stop_tx.clone());
        }

        // Create new cluster
        let mut cluster = Cluster::new(0, node_count, self.observer.clone()).await?;

        // Send cluster info to visualizer
        self.observer
            .set_cluster_info(node_count, cluster.quorum_size())
            .await;

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

        // Spawn scenario runner based on type
        let cluster_for_runner = cluster_arc.clone();
        let mut stop_rx = stop_tx.subscribe();
        let scenario_type = scenario_type.to_string();

        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let mut proposal_count = 0;

            loop {
                // Check if stop was signaled
                if stop_rx.try_recv().is_ok() {
                    println!("Scenario stopped early");
                    break;
                }

                if start.elapsed().as_secs() >= duration_secs {
                    println!("Scenario completed after {} seconds", duration_secs);
                    break;
                }

                match scenario_type.as_str() {
                    "competing_proposers" => {
                        // Competing proposers: node 0 and node 1 both propose simultaneously
                        // Every 2 iterations (every 1ms), spawn both proposals concurrently
                        if proposal_count % 2 == 0 {
                            let cluster0 = cluster_for_runner.clone();
                            let cluster1 = cluster_for_runner.clone();
                            let attempt = proposal_count / 2;
                            
                            // Spawn both proposals concurrently
                            let p0 = tokio::spawn(async move {
                                let mut cluster = cluster0.lock().await;
                                let cmd = PaxosCommand::EnactDecree {
                                    author: "Proposer 0".to_string(),
                                    law: format!("Value from proposer 0 (attempt #{})", attempt),
                                };
                                cluster.propose_from(0, cmd).await;
                            });
                            
                            let p1 = tokio::spawn(async move {
                                let mut cluster = cluster1.lock().await;
                                let cmd = PaxosCommand::EnactDecree {
                                    author: "Proposer 1".to_string(),
                                    law: format!("Value from proposer 1 (attempt #{})", attempt),
                                };
                                cluster.propose_from(1, cmd).await;
                            });
                            
                            // Wait for both to complete
                            let _ = tokio::join!(p0, p1);
                        }
                    }
                    _ => {
                        // Default "happy_path": proposals every 2 seconds
                        if proposal_count % 20 == 0 {
                            let mut cluster = cluster_for_runner.lock().await;
                            let cmd = PaxosCommand::EnactDecree {
                                author: format!("Web User {}", proposal_count / 20),
                                law: format!("Proposal #{}", proposal_count / 20),
                            };
                            cluster.propose(cmd).await;
                        }
                    }
                }

                proposal_count += 1;
                sleep(Duration::from_micros(100)).await;
            }
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

    pub async fn stop_scenario(&self) -> anyhow::Result<()> {
        // Signal the scenario task to stop
        {
            let mut tx = self.stop_tx.lock().await;
            if let Some(stop_tx) = tx.take() {
                let _ = stop_tx.send(());
            }
        }

        // Clear the cluster
        let mut current = self.cluster.lock().await;
        *current = None;
        println!("Scenario stopped");
        Ok(())
    }
}
