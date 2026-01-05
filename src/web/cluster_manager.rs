use rand::Rng;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, sleep};

use crate::cluster::cluster::Cluster;
use crate::paxos_command::PaxosCommand;
use crate::web::websocket_observer::WebSocketObserver;
use crate::decree_generator::DecreeGenerator;

pub struct ClusterManager {
    cluster: Mutex<Option<Arc<Mutex<Cluster>>>>,
    observer: Arc<WebSocketObserver>,
    stop_tx: Mutex<Option<broadcast::Sender<()>>>,
    decree_generator: Mutex<DecreeGenerator>,
}

impl ClusterManager {
    pub fn new() -> Self {
        Self {
            cluster: Mutex::new(None),
            observer: Arc::new(WebSocketObserver::new(500)),
            stop_tx: Mutex::new(None),
            decree_generator: Mutex::new(DecreeGenerator::new()),
        }
    }
    pub fn get_observer(&self) -> Arc<WebSocketObserver> {
        return Arc::clone(&self.observer);
    }

    pub async fn start_scenario(
        &self,
        ip: IpAddr,
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
        let mut cluster = Cluster::new(0, ip, node_count, self.observer.clone()).await?;

        // Send cluster info to visualizer
        self.observer
            .set_cluster_info(node_count, cluster.quorum_size())
            .await;

        // Enable network simulator for failure injection
        cluster.enable_failures().await;

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
        let observer_for_runner = self.observer.clone();
        let mut decree_gen = self.decree_generator.lock().await.clone();

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
                                cluster.propose_from(pick, cmd).await;
                            });

                            let p1 = tokio::spawn(async move {
                                let mut cluster = cluster1.lock().await;
                                let cmd = PaxosCommand::EnactDecree {
                                    author: "Proposer 1".to_string(),
                                    law: decree1,
                                };
                                cluster.propose_from(1, cmd).await;
                            });

                            // Wait for both to complete
                            let _ = tokio::join!(p0, p1);
                        }
                    }
                    "network_partition" => {
                        // Network partition scenario
                        // Proposal 2: Create partition (nodes 0,1,2 vs 3,4)
                        if proposal_count == 0 {
                            println!("Creating network partition at proposal {}", proposal_count);
                            let cluster = cluster_for_runner.lock().await;

                            // Partition A: nodes 0,1,2 (quorum)
                            let partition_a: std::collections::HashSet<usize> =
                                [0, 1, 2].iter().copied().collect();
                            let partition_a_vec = vec![0, 1, 2];
                            // Partition B: nodes 3,4 (no quorum)
                            let partition_b: std::collections::HashSet<usize> =
                                [3, 4].iter().copied().collect();
                            let partition_b_vec = vec![3, 4];

                            // For each node in partition A: block messages to partition B
                            for i in [0, 1, 2] {
                                for target in [3, 4] {
                                    if let Some(sim) = cluster.get_simulator(i) {
                                        sim.set_failure(target, crate::cluster::network_simulator::NetworkFailure::Partition { nodes: partition_a.clone() }).await;
                                    }
                                }
                            }

                            // For each node in partition B: block messages to partition A
                            for i in [3, 4] {
                                for target in [0, 1, 2] {
                                    if let Some(sim) = cluster.get_simulator(i) {
                                        sim.set_failure(target, crate::cluster::network_simulator::NetworkFailure::Partition { nodes: partition_b.clone() }).await;
                                    }
                                }
                            }

                            // Emit partition created event
                            let observer: &dyn crate::monitor::PaxosObserver =
                                &*observer_for_runner;
                            observer.on_event(crate::monitor::Event::PartitionCreated {
                                partition_a: partition_a_vec,
                                partition_b: partition_b_vec,
                                created_at: crate::monitor::current_timestamp_millis(),
                            });
                        }

                        // Proposal 5: Heal partition
                        if proposal_count == 5 {
                            println!("Healing network partition at proposal {}", proposal_count);
                            let cluster = cluster_for_runner.lock().await;
                            for i in 0..node_count {
                                if let Some(sim) = cluster.get_simulator(i) {
                                    sim.clear_all_failures().await;
                                }
                            }

                            // Emit partition healed event
                            let observer: &dyn crate::monitor::PaxosObserver =
                                &*observer_for_runner;
                            observer.on_event(crate::monitor::Event::PartitionHealed {
                                created_at: crate::monitor::current_timestamp_millis(),
                            });
                        }

                        // Proposals every 5 iterations
                        let mut cluster = cluster_for_runner.lock().await;
                        let decree = decree_gen.next();
                        let cmd = PaxosCommand::EnactDecree {
                            author: format!("Proposer {}", proposal_count % 3),
                            law: decree,
                        };
                        cluster.propose(cmd).await;
                    }
                    _ => {
                        // Default "happy_path": proposals every 2 seconds
                        if proposal_count % 20 == 0 {
                            let mut cluster = cluster_for_runner.lock().await;
                            let decree = decree_gen.next();
                            let cmd = PaxosCommand::EnactDecree {
                                author: format!("Web User {}", proposal_count / 20),
                                law: decree,
                            };
                            cluster.propose(cmd).await;
                        }
                    }
                }

                proposal_count += 1;
                sleep(Duration::from_millis(5000)).await;
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

    pub async fn reset(&self) -> anyhow::Result<()> {
        // Get node UUIDs BEFORE stopping scenario (which clears the cluster)
        let mut uuids = Vec::new();
        {
            let mut current = self.cluster.lock().await;
            if let Some(cluster_arc) = current.as_ref() {
                let cluster = cluster_arc.lock().await;
                uuids = cluster.get_node_uuids();
            }
            // Clear the cluster reference
            *current = None;
        }

        // Signal the scenario task to stop (without clearing cluster again)
        {
            let mut tx = self.stop_tx.lock().await;
            if let Some(stop_tx) = tx.take() {
                let _ = stop_tx.send(());
            }
        }

        // Delete .bin files for each node
        for uuid in uuids {
            let ledger_path = format!(".paxos/ledger_{}.bin", uuid);
            let acceptor_path = format!(".paxos/acceptor_{}.bin", uuid);
            
            let _ = std::fs::remove_file(&ledger_path);
            let _ = std::fs::remove_file(&acceptor_path);
            
            println!("Deleted state files for node {}", uuid);
        }

        // Clear the observer state
        self.observer.clear().await;
        
        println!("Reset complete - ready for new scenario selection");
        Ok(())
    }
}
