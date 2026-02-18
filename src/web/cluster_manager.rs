use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::cluster::{cluster::Cluster, pmmc_cluster::PmmcCluster};
use crate::decree_generator::DecreeGenerator;
use crate::node::config::{ClassicNodeConfig, LearningStrategy, Roles};
use crate::paxos_command::PaxosCommand;
use crate::web::scenarios::{
    AsymmetricProposersScenario, CatchUpScenario, CompetingProposersScenario, HappyPathScenario,
    NetworkPartitionScenario, PartialRolesScenario, SimpleHappyPathScenario,
};
use crate::web::websocket_observer::WebSocketObserver;

pub struct ClusterManager {
    cluster: Mutex<Option<ActiveCluster>>,
    observer: Arc<WebSocketObserver>,
    stop_tx: Mutex<Option<broadcast::Sender<()>>>,
    decree_generator: Mutex<DecreeGenerator>,
}

#[derive(Clone)]
enum ActiveCluster {
    Classic(Arc<Mutex<Cluster>>),
    Pmmc(Arc<Mutex<PmmcCluster>>),
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

    async fn create_cluster_with_strategy(
        id: usize,
        ip: IpAddr,
        node_count: usize,
        observer: Arc<dyn crate::monitor::PaxosObserver>,
        learning_strategy: LearningStrategy,
    ) -> anyhow::Result<Cluster> {
        let configs = (0..node_count)
            .map(|_| ClassicNodeConfig {
                roles: Roles {
                    proposer: true,
                    acceptor: true,
                    learner: true,
                },
                learning_strategy: learning_strategy.clone(),
            })
            .collect();

        Cluster::new_with_configs(id, ip, configs, observer).await
    }

    async fn create_pmmc_cluster(
        id: usize,
        ip: IpAddr,
        node_count: usize,
        observer: Arc<dyn crate::monitor::PaxosObserver>,
    ) -> anyhow::Result<PmmcCluster> {
        PmmcCluster::new(id, ip, node_count, observer).await
    }

    pub async fn start_scenario(
        &self,
        ip: IpAddr,
        node_count: usize,
        duration_secs: u64,
        scenario_type: &str,
        learning_strategy: &str,
        leader_node: Option<usize>,
    ) -> anyhow::Result<()> {
        println!(
            "Starting new scenario '{}' with {} nodes for {} seconds",
            scenario_type, node_count, duration_secs
        );

        // Clear any previous cluster state
        {
            let mut current = self.cluster.lock().await;
            *current = None;
        }

        // Clear the observer to reset visualizer state
        self.observer.clear().await;

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

        // Parse learning strategy
        let learning_strat = match learning_strategy {
            "Direct" => crate::node::config::LearningStrategy::Direct,
            _ => crate::node::config::LearningStrategy::ProposerManaged,
        };

        // Create new cluster
        let (active_for_runner, node_count) = if scenario_type == "pmmc_single_client" {
            let mut cluster =
                Self::create_pmmc_cluster(0, ip, node_count, self.observer.clone()).await?;
            let node_count = cluster.num_nodes();
            self.observer
                .set_cluster_info(node_count, cluster.quorum_size(), cluster.get_node_uuids())
                .await;
            cluster.enable_failures().await;
            for i in 0..node_count {
                cluster.nodes[i].start();
            }
            sleep(Duration::from_millis(100)).await;
            let cluster_arc = Arc::new(Mutex::new(cluster));
            let mut current = self.cluster.lock().await;
            *current = Some(ActiveCluster::Pmmc(cluster_arc.clone()));
            (ActiveCluster::Pmmc(cluster_arc), node_count)
        } else {
            let mut cluster = if scenario_type == "partial_roles" {
                PartialRolesScenario::init_cluster(0, ip, self.observer.clone(), learning_strat)
                    .await?
            } else if scenario_type == "simple_happy_path" {
                SimpleHappyPathScenario::init_cluster(
                    0,
                    ip,
                    self.observer.clone(),
                    leader_node,
                    learning_strat,
                )
                .await?
            } else if scenario_type == "asymmetric_proposers" {
                AsymmetricProposersScenario::init_cluster(
                    0,
                    ip,
                    self.observer.clone(),
                    learning_strat,
                )
                .await?
            } else {
                Self::create_cluster_with_strategy(
                    0,
                    ip,
                    node_count,
                    self.observer.clone(),
                    learning_strat,
                )
                .await?
            };

            let node_count = cluster.nodes.len();
            self.observer
                .set_cluster_info(node_count, cluster.quorum_size(), cluster.get_node_uuids())
                .await;
            cluster.enable_failures().await;
            for i in 0..node_count {
                cluster.nodes[i].start();
            }
            sleep(Duration::from_millis(100)).await;
            let cluster_arc = Arc::new(Mutex::new(cluster));
            let mut current = self.cluster.lock().await;
            *current = Some(ActiveCluster::Classic(cluster_arc.clone()));
            (ActiveCluster::Classic(cluster_arc), node_count)
        };

        // Spawn scenario runner based on type
        let mut stop_rx = stop_tx.subscribe();
        let scenario_type = scenario_type.to_string();
        let observer_for_runner = self.observer.clone();
        let mut decree_gen = self.decree_generator.lock().await.clone();
        let leader_for_runner = leader_node.unwrap_or(0); // Default to node 0 if not specified

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

                match &active_for_runner {
                    ActiveCluster::Classic(cluster_for_runner) => match scenario_type.as_str() {
                        "competing_proposers" => {
                            CompetingProposersScenario::execute_iteration(
                                cluster_for_runner,
                                proposal_count,
                                &mut decree_gen,
                            )
                            .await;
                        }
                        "asymmetric_proposers" => {
                            AsymmetricProposersScenario::execute_iteration(
                                cluster_for_runner,
                                proposal_count,
                                &mut decree_gen,
                            )
                            .await;
                        }
                        "network_partition" => {
                            NetworkPartitionScenario::execute_iteration(
                                cluster_for_runner,
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
                        "partial_roles" => {
                            PartialRolesScenario::execute_iteration(
                                cluster_for_runner,
                                proposal_count,
                                &mut decree_gen,
                            )
                            .await;
                        }
                        "simple_happy_path" => {
                            SimpleHappyPathScenario::execute_iteration(
                                cluster_for_runner,
                                proposal_count,
                                &mut decree_gen,
                                leader_for_runner,
                            )
                            .await;
                        }
                        _ => {
                            HappyPathScenario::execute_iteration(
                                cluster_for_runner,
                                proposal_count,
                                &mut decree_gen,
                            )
                            .await;
                        }
                    },
                    ActiveCluster::Pmmc(cluster_for_runner) => {
                        if scenario_type == "pmmc_single_client" && proposal_count % 2 == 0 {
                            let cluster = cluster_for_runner.lock().await;
                            let value = proposal_count + 1;
                            let client_id = Uuid::from_u128(0xC0);
                            let request_id = proposal_count as u64 + 1;
                            cluster
                                .propose_from(
                                    0,
                                    PaxosCommand::PUT {
                                        key: "pmmc-single-client".to_string(),
                                        version: 1,
                                        value,
                                    }
                                    .with_client(client_id, request_id),
                                )
                                .await;
                        }
                    }
                }

                proposal_count += 1;
                sleep(Duration::from_millis(1000)).await;
            }
        });

        Ok(())
    }

    pub async fn propose(&self, decree: PaxosCommand) -> anyhow::Result<()> {
        let current = self.cluster.lock().await;
        match current.as_ref() {
            Some(ActiveCluster::Classic(cluster_arc)) => {
                let mut cluster = cluster_arc.lock().await;
                cluster.propose(decree).await;
                Ok(())
            }
            Some(ActiveCluster::Pmmc(cluster_arc)) => {
                let cluster = cluster_arc.lock().await;
                cluster.propose_from(0, decree).await;
                Ok(())
            }
            None => Err(anyhow::anyhow!("No active cluster")),
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
            if let Some(active) = current.as_ref() {
                match active {
                    ActiveCluster::Classic(cluster_arc) => {
                        let cluster = cluster_arc.lock().await;
                        uuids = cluster.get_node_uuids();
                    }
                    ActiveCluster::Pmmc(cluster_arc) => {
                        let cluster = cluster_arc.lock().await;
                        uuids = cluster.get_node_uuids();
                    }
                }
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
            let leader_path = format!(".paxos/leader_{}.bin", uuid);
            let replica_path = format!(".paxos/replica_{}.bin", uuid);
            let decree_notes_path = format!(".paxos/decree_notes_{}.bin", uuid);
            let store_path = format!(".paxos/store_{}.bin", uuid);

            let _ = std::fs::remove_file(&ledger_path);
            let _ = std::fs::remove_file(&acceptor_path);
            let _ = std::fs::remove_file(&leader_path);
            let _ = std::fs::remove_file(&replica_path);
            let _ = std::fs::remove_file(&decree_notes_path);
            let _ = std::fs::remove_file(&store_path);

            println!("Deleted state files for node {}", uuid);
        }

        // Clear the observer state
        self.observer.clear().await;

        println!("Reset complete - ready for new scenario selection");
        Ok(())
    }
}
