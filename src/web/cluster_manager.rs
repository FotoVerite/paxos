use rand::Rng;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, sleep};

use crate::cluster::cluster::Cluster;
use crate::paxos_command::PaxosCommand;
use crate::web::websocket_observer::WebSocketObserver;
use crate::decree_generator::DecreeGenerator;
use crate::web::scenarios::{CatchUpScenario, CompetingProposersScenario, NetworkPartitionScenario, HappyPathScenario};

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

        // Setup scenario-specific initial state
        if scenario_type == "catch_up" {
            CatchUpScenario::setup(ip, node_count).await?;
        }

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
                        CompetingProposersScenario::execute_iteration(&cluster_for_runner, proposal_count, &mut decree_gen).await;
                    }
                    "network_partition" => {
                        NetworkPartitionScenario::execute_iteration(
                            &cluster_for_runner,
                            proposal_count,
                            node_count,
                            &mut decree_gen,
                            observer_for_runner.clone(),
                        )
                        .await;
                    }
                    "catch_up" => {
                        let mut cluster = cluster_for_runner.lock().await;
                        if let Err(e) = CatchUpScenario::propose_gap_filler(&mut cluster, 0).await {
                            eprintln!("Error proposing gap filler: {}", e);
                        }
                    }
                    _ => {
                        // Default "happy_path"
                        HappyPathScenario::execute_iteration(&cluster_for_runner, proposal_count, &mut decree_gen).await;
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
