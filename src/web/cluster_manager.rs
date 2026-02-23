use std::collections::HashSet;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio::fs;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::cluster::{cluster::Cluster, pmmc_cluster::PmmcCluster};
use crate::decree_generator::DecreeGenerator;
use crate::common::persistence::DATA_DIR;
use crate::message::{ClientMessage, Message};
use crate::monitor::{Event, PaxosObserver, current_timestamp_millis};
use crate::node::config::{ClassicNodeConfig, LearningStrategy, PmmcNodeConfig, Roles};
use crate::paxos_command::PaxosCommand;
use crate::console_observer::ConsoleObserver;
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

struct ObserverFanout {
    websocket: Arc<WebSocketObserver>,
    console: Arc<ConsoleObserver>,
}

impl PaxosObserver for ObserverFanout {
    fn on_event(&self, event: crate::monitor::Event) {
        self.websocket.on_event(event.clone());
        self.console.on_event(event);
    }

    fn on_message(&self, indexes: &[Uuid], message: Message) {
        self.websocket.on_message(indexes, message.clone());
        self.console.on_message(indexes, message);
    }
}

impl ClusterManager {
    const CLEANUP_NODE_SLOTS_PER_IP: usize = 256;

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

    async fn create_pmmc_role_split_cluster(
        id: usize,
        ip: IpAddr,
        observer: Arc<dyn crate::monitor::PaxosObserver>,
    ) -> anyhow::Result<PmmcCluster> {
        let mut configs = Vec::new();
        for _ in 0..2 {
            configs.push(PmmcNodeConfig {
                roles: Roles {
                    proposer: true,
                    acceptor: false,
                    learner: false,
                },
            });
        }
        for _ in 0..2 {
            configs.push(PmmcNodeConfig {
                roles: Roles {
                    proposer: false,
                    acceptor: false,
                    learner: true,
                },
            });
        }
        for _ in 0..3 {
            configs.push(PmmcNodeConfig {
                roles: Roles {
                    proposer: false,
                    acceptor: true,
                    learner: false,
                },
            });
        }
        PmmcCluster::new_with_configs(id, ip, configs, observer).await
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

        // Always start scenarios from a clean durable state to avoid stale
        // ballots/decisions poisoning liveness across runs.
        if let Ok(removed) = self.purge_persisted_state_for_ip(ip).await {
            if removed > 0 {
                println!(
                    "Purged {} persisted state files for {} before scenario start",
                    removed, ip
                );
            }
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

        let scenario_observer: Arc<dyn PaxosObserver> = Arc::new(ObserverFanout {
            websocket: self.observer.clone(),
            console: Arc::new(ConsoleObserver),
        });

        // Create new cluster
        let (active_for_runner, node_count) = if scenario_type == "pmmc_single_client"
            || scenario_type == "pmmc_role_split"
            || scenario_type == "pmmc_leader_crash"
            || scenario_type == "pmmc_replica_crash_failover"
            || scenario_type == "pmmc_leader_partition_heal"
            || scenario_type == "pmmc_acceptor_majority_loss_then_recover"
        {
            let mut cluster = if scenario_type == "pmmc_role_split"
                || scenario_type == "pmmc_leader_crash"
                || scenario_type == "pmmc_replica_crash_failover"
                || scenario_type == "pmmc_leader_partition_heal"
                || scenario_type == "pmmc_acceptor_majority_loss_then_recover"
            {
                Self::create_pmmc_role_split_cluster(0, ip, scenario_observer.clone()).await?
            } else {
                Self::create_pmmc_cluster(0, ip, node_count, scenario_observer.clone()).await?
            };
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
                PartialRolesScenario::init_cluster(0, ip, scenario_observer.clone(), learning_strat)
                    .await?
            } else if scenario_type == "simple_happy_path" {
                SimpleHappyPathScenario::init_cluster(
                    0,
                    ip,
                    scenario_observer.clone(),
                    leader_node,
                    learning_strat,
                )
                .await?
            } else if scenario_type == "asymmetric_proposers" {
                AsymmetricProposersScenario::init_cluster(
                    0,
                    ip,
                    scenario_observer.clone(),
                    learning_strat,
                )
                .await?
            } else {
                Self::create_cluster_with_strategy(
                    0,
                    ip,
                    node_count,
                    scenario_observer.clone(),
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
            let mut single_client_tx = None;
            let mut single_client_rx = None;
            let single_client_id = Uuid::from_u128(0xC0);
            let single_client_limit = 5usize;
            let role_split_limit = 5usize;
            let leader_crash_limit = 5usize;
            let replica_crash_failover_limit = 5usize;
            let leader_partition_heal_limit = 5usize;
            let acceptor_majority_loss_then_recover_limit = 7usize;
            let mut leader_crash_done = false;
            let mut replica_crash_done = false;
            let mut leader_partitioned_idx: Option<usize> = None;
            let mut leader_partitioned_uuid: Option<Uuid> = None;
            let mut leader_partition_healed = false;
            let mut leader_partition_started_at: Option<std::time::Instant> = None;
            let mut leader_partition_response_count = 0usize;
            let mut acceptor_majority_partitioned = false;
            let mut acceptor_majority_healed = false;
            let mut acceptor_majority_partition_b: Vec<Uuid> = Vec::new();
            let mut acceptor_majority_timeout_count = 0usize;
            let mut client_node_index = if scenario_type == "pmmc_leader_crash"
                || scenario_type == "pmmc_replica_crash_failover"
                || scenario_type == "pmmc_leader_partition_heal"
                || scenario_type == "pmmc_acceptor_majority_loss_then_recover"
            {
                2
            } else {
                0
            };

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
                        if scenario_type == "pmmc_single_client"
                            || scenario_type == "pmmc_leader_crash"
                            || scenario_type == "pmmc_replica_crash_failover"
                            || scenario_type == "pmmc_leader_partition_heal"
                            || scenario_type == "pmmc_acceptor_majority_loss_then_recover"
                        {
                            let is_leader_crash = scenario_type == "pmmc_leader_crash";
                            let is_replica_crash_failover =
                                scenario_type == "pmmc_replica_crash_failover";
                            let is_leader_partition_heal =
                                scenario_type == "pmmc_leader_partition_heal";
                            let is_acceptor_majority_loss_then_recover =
                                scenario_type == "pmmc_acceptor_majority_loss_then_recover";
                            let target_limit = if is_leader_crash {
                                leader_crash_limit
                            } else if is_replica_crash_failover {
                                replica_crash_failover_limit
                            } else if is_leader_partition_heal {
                                leader_partition_heal_limit
                            } else if is_acceptor_majority_loss_then_recover {
                                acceptor_majority_loss_then_recover_limit
                            } else {
                                single_client_limit
                            };
                            if proposal_count >= target_limit {
                                let scenario_label = if is_leader_crash {
                                    "leader-crash"
                                } else if is_replica_crash_failover {
                                    "replica-crash-failover"
                                } else if is_leader_partition_heal {
                                    "leader-partition-heal"
                                } else if is_acceptor_majority_loss_then_recover {
                                    "acceptor-majority-loss-then-recover"
                                } else {
                                    "single-client"
                                };
                                println!(
                                    "PMMC {} completed after {} successful requests",
                                    scenario_label,
                                    target_limit
                                );
                                break;
                            }

                            if is_leader_partition_heal
                                && !leader_partition_healed
                                && leader_partitioned_idx.is_some()
                                && (
                                    leader_partition_started_at
                                        .map(|t| t.elapsed() >= Duration::from_secs(2))
                                        .unwrap_or(false)
                                    || leader_partition_response_count >= 2
                                )
                            {
                                let idx = leader_partitioned_idx.unwrap();
                                let healed_uuid = {
                                    let cluster = cluster_for_runner.lock().await;
                                    cluster.heal_node(idx).await
                                };
                                if let Some(uuid) = healed_uuid {
                                    let healed_id = leader_partitioned_uuid.unwrap_or(uuid);
                                    observer_for_runner.on_event(Event::PartitionHealed {
                                        partition_a: vec![],
                                        partition_b: vec![healed_id],
                                        created_at: current_timestamp_millis(),
                                    });
                                    println!(
                                        "PMMC leader-partition-heal: healed isolated leader node {} ({})",
                                        idx, uuid
                                    );
                                    leader_partition_healed = true;
                                    leader_partition_started_at = None;
                                    leader_partition_response_count = 0;
                                } else {
                                    eprintln!(
                                        "PMMC leader-partition-heal: failed healing leader node {}",
                                        idx
                                    );
                                }
                            }

                            if proposal_count == 0 {
                                // Allow initial PMMC leader election churn to settle
                                // before issuing the first client request.
                                sleep(Duration::from_millis(500)).await;
                            }

                            if single_client_tx.is_none() || single_client_rx.is_none() {
                                let cluster = cluster_for_runner.lock().await;
                                match cluster
                                    .connect_client_to(client_node_index, single_client_id)
                                    .await
                                {
                                    Some((tx, rx)) => {
                                        single_client_tx = Some(tx);
                                        single_client_rx = Some(rx);
                                    }
                                    None => {
                                        eprintln!(
                                            "Failed to attach PMMC client to replica node {}",
                                            client_node_index
                                        );
                                        break;
                                    }
                                }
                            }

                            let request_id = proposal_count as u64 + 1;
                            let value = proposal_count + 1;
                            let cmd = PaxosCommand::PUT {
                                key: "pmmc-single-client".to_string(),
                                version: 1,
                                value,
                            }
                            .with_client(single_client_id, request_id);

                            if let Some(tx) = &single_client_tx {
                                if tx.send(ClientMessage::PROPOSE { cmd }).await.is_err() {
                                    eprintln!("PMMC single-client channel closed before send");
                                    break;
                                }
                            }

                            let recv_future = async {
                                if let Some(rx) = &mut single_client_rx {
                                    rx.recv().await
                                } else {
                                    None
                                }
                            };

                            let recv_result = tokio::select! {
                                _ = stop_rx.recv() => {
                                    println!("Scenario stopped early");
                                    break;
                                }
                                result = tokio::time::timeout(Duration::from_secs(3), recv_future) => result,
                            };

                            match recv_result {
                                Ok(Some(ClientMessage::RESPONSE { request_id: rid, .. })) => {
                                    if rid != request_id {
                                        eprintln!(
                                            "PMMC single-client out-of-order response id={}, expected={}",
                                            rid, request_id
                                        );
                                    }
                                    proposal_count += 1;

                                    if is_leader_crash && !leader_crash_done && proposal_count >= 1 {
                                        let (leader_idx_opt, cluster_size) = {
                                            let cluster = cluster_for_runner.lock().await;
                                            (cluster.leader_index().await, cluster.num_nodes())
                                        };

                                        let Some(crashed_idx) = leader_idx_opt else {
                                            eprintln!(
                                                "PMMC leader-crash: no active leader yet; delaying crash"
                                            );
                                            continue;
                                        };

                                        let crashed_uuid = {
                                            let cluster = cluster_for_runner.lock().await;
                                            cluster.crash_node(crashed_idx).await
                                        };

                                        match crashed_uuid {
                                            Some(uuid) => {
                                                println!(
                                                    "PMMC leader-crash: isolated leader node {} ({})",
                                                    crashed_idx, uuid
                                                );
                                                if crashed_idx == client_node_index {
                                                    if is_leader_crash {
                                                        // Role-split topology: replicas are nodes 2 and 3.
                                                        client_node_index = if client_node_index == 2 {
                                                            3
                                                        } else {
                                                            2
                                                        };
                                                    } else {
                                                        client_node_index =
                                                            (client_node_index + 1) % cluster_size;
                                                    }
                                                    single_client_tx = None;
                                                    single_client_rx = None;
                                                }
                                                leader_crash_done = true;
                                                sleep(Duration::from_millis(700)).await;
                                            }
                                            None => {
                                                eprintln!(
                                                    "PMMC leader-crash: failed to isolate leader node {}",
                                                    crashed_idx
                                                );
                                            }
                                        }
                                    }

                                    if is_replica_crash_failover
                                        && !replica_crash_done
                                        && proposal_count >= 1
                                    {
                                        let crashed_uuid = {
                                            let cluster = cluster_for_runner.lock().await;
                                            cluster.crash_node(2).await
                                        };
                                        match crashed_uuid {
                                            Some(uuid) => {
                                                println!(
                                                    "PMMC replica-crash-failover: crashed replica node 2 ({})",
                                                    uuid
                                                );
                                                if client_node_index == 2 {
                                                    client_node_index = 3;
                                                    single_client_tx = None;
                                                    single_client_rx = None;
                                                }
                                                replica_crash_done = true;
                                                sleep(Duration::from_millis(700)).await;
                                            }
                                            None => {
                                                eprintln!(
                                                    "PMMC replica-crash-failover: failed to crash replica node 2"
                                                );
                                            }
                                        }
                                    }

                                    if is_leader_partition_heal && leader_partitioned_idx.is_none() && proposal_count >= 1 {
                                        let maybe_partition = {
                                            let cluster = cluster_for_runner.lock().await;
                                            let leader_idx = cluster.leader_index().await;
                                            match leader_idx {
                                                Some(idx) => {
                                                    let isolated = cluster.isolate_node(idx).await;
                                                    let uuids = cluster.get_node_uuids();
                                                    Some((idx, isolated, uuids))
                                                }
                                                None => None,
                                            }
                                        };

                                        match maybe_partition {
                                            Some((idx, Some(uuid), uuids)) => {
                                                let partition_a: Vec<Uuid> =
                                                    uuids.into_iter().filter(|u| *u != uuid).collect();
                                                observer_for_runner.on_event(Event::PartitionCreated {
                                                    partition_a,
                                                    partition_b: vec![uuid],
                                                    created_at: current_timestamp_millis(),
                                                });
                                                println!(
                                                    "PMMC leader-partition-heal: isolated leader node {} ({})",
                                                    idx, uuid
                                                );
                                                leader_partitioned_idx = Some(idx);
                                                leader_partitioned_uuid = Some(uuid);
                                                leader_partition_started_at =
                                                    Some(std::time::Instant::now());
                                                leader_partition_response_count = 0;
                                                sleep(Duration::from_millis(700)).await;
                                            }
                                            Some((idx, None, _)) => {
                                                eprintln!(
                                                    "PMMC leader-partition-heal: failed to isolate leader node {}",
                                                    idx
                                                );
                                            }
                                            None => {
                                                eprintln!(
                                                    "PMMC leader-partition-heal: no active leader yet; delaying partition"
                                                );
                                            }
                                        }
                                    }

                                    if is_leader_partition_heal
                                        && leader_partitioned_idx.is_some()
                                        && !leader_partition_healed
                                    {
                                        leader_partition_response_count += 1;
                                    }

                                    if is_acceptor_majority_loss_then_recover
                                        && !acceptor_majority_partitioned
                                        && proposal_count >= 2
                                    {
                                        let maybe_partition = {
                                            let cluster = cluster_for_runner.lock().await;
                                            let uuids = cluster.get_node_uuids();
                                            if uuids.len() >= 7 {
                                                let mut isolated = Vec::new();
                                                for idx in [5usize, 6usize] {
                                                    if let Some(uuid) = cluster.isolate_node(idx).await {
                                                        isolated.push(uuid);
                                                    }
                                                }
                                                Some((uuids, isolated))
                                            } else {
                                                None
                                            }
                                        };

                                        match maybe_partition {
                                            Some((uuids, isolated)) if isolated.len() == 2 => {
                                                let partition_a: Vec<Uuid> = uuids
                                                    .into_iter()
                                                    .filter(|u| !isolated.contains(u))
                                                    .collect();
                                                observer_for_runner.on_event(Event::PartitionCreated {
                                                    partition_a,
                                                    partition_b: isolated.clone(),
                                                    created_at: current_timestamp_millis(),
                                                });
                                                println!(
                                                    "PMMC acceptor-majority-loss-then-recover: isolated acceptors 5 and 6"
                                                );
                                                acceptor_majority_partitioned = true;
                                                acceptor_majority_partition_b = isolated;
                                                sleep(Duration::from_millis(700)).await;
                                            }
                                            Some(_) => {
                                                eprintln!(
                                                    "PMMC acceptor-majority-loss-then-recover: failed to isolate acceptor majority"
                                                );
                                            }
                                            None => {
                                                eprintln!(
                                                    "PMMC acceptor-majority-loss-then-recover: expected role-split topology with 7 nodes"
                                                );
                                            }
                                        }
                                    }

                                    continue;
                                }
                                Ok(Some(ClientMessage::PROPOSE { .. })) => {
                                    eprintln!("PMMC single-client received unexpected PROPOSE on response channel");
                                    break;
                                }
                                Ok(None) => {
                                    eprintln!("PMMC single-client response channel closed");
                                    break;
                                }
                                Err(_) => {
                                    if is_acceptor_majority_loss_then_recover
                                        && acceptor_majority_partitioned
                                        && !acceptor_majority_healed
                                    {
                                        acceptor_majority_timeout_count += 1;
                                        let (healed, uuids) = {
                                            let cluster = cluster_for_runner.lock().await;
                                            let mut healed = Vec::new();
                                            for idx in [5usize, 6usize] {
                                                if let Some(uuid) = cluster.heal_node(idx).await {
                                                    healed.push(uuid);
                                                }
                                            }
                                            (healed, cluster.get_node_uuids())
                                        };

                                        if healed.len() == 2 {
                                            let partition_a: Vec<Uuid> = uuids
                                                .into_iter()
                                                .filter(|u| !acceptor_majority_partition_b.contains(u))
                                                .collect();
                                            observer_for_runner.on_event(Event::PartitionHealed {
                                                partition_a,
                                                partition_b: acceptor_majority_partition_b.clone(),
                                                created_at: current_timestamp_millis(),
                                            });
                                            println!(
                                                "PMMC acceptor-majority-loss-then-recover: healed acceptors 5 and 6 after {} timeout(s)",
                                                acceptor_majority_timeout_count
                                            );
                                            acceptor_majority_healed = true;
                                            sleep(Duration::from_millis(700)).await;
                                        } else {
                                            eprintln!(
                                                "PMMC acceptor-majority-loss-then-recover: failed to heal acceptor majority"
                                            );
                                        }
                                        continue;
                                    }

                                    if is_replica_crash_failover || is_leader_partition_heal {
                                        eprintln!(
                                            "PMMC {} timed out waiting for response to request {}; retrying",
                                            if is_replica_crash_failover {
                                                "replica-crash-failover"
                                            } else {
                                                "leader-partition-heal"
                                            },
                                            request_id
                                        );
                                        sleep(Duration::from_millis(250)).await;
                                        continue;
                                    }
                                    eprintln!(
                                        "PMMC single-client timed out waiting for response to request {}",
                                        request_id
                                    );
                                    break;
                                }
                            }
                        } else if scenario_type == "pmmc_role_split" {
                            if proposal_count >= role_split_limit {
                                println!(
                                    "PMMC role-split completed after {} proposals",
                                    role_split_limit
                                );
                                break;
                            }
                            if proposal_count == 0 {
                                // Allow initial PMMC leader election churn to settle
                                // before issuing the first client request.
                                sleep(Duration::from_millis(500)).await;
                            }
                            let cluster = cluster_for_runner.lock().await;
                            let value = proposal_count + 1;
                            let client_id = Uuid::from_u128(0xC0);
                            let request_id = proposal_count as u64 + 1;
                            let proposer_index = 2;
                            cluster
                                .propose_from(
                                    proposer_index,
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

    pub async fn reset(&self, ip: IpAddr) -> anyhow::Result<()> {
        // Clear the cluster reference
        {
            let mut current = self.cluster.lock().await;
            *current = None;
        }

        // Signal the scenario task to stop (without clearing cluster again)
        {
            let mut tx = self.stop_tx.lock().await;
            if let Some(stop_tx) = tx.take() {
                let _ = stop_tx.send(());
            }
        }

        // Remove all persisted node versions (including stale UUIDs/corrupt/tmp variants).
        let removed = self.purge_persisted_state_for_ip(ip).await?;
        println!("Deleted {} persisted state files for {}", removed, ip);

        // Clear the observer state
        self.observer.clear().await;

        println!("Reset complete - ready for new scenario selection");
        Ok(())
    }

    fn owned_uuid_set_for_ip(ip: IpAddr, slots: usize) -> HashSet<String> {
        let mut out = HashSet::with_capacity(slots * 2);
        for i in 0..slots {
            out.insert(crate::cluster::cluster::Cluster::node_uuid(ip, i).to_string());
            out.insert(crate::cluster::pmmc_cluster::PmmcCluster::node_uuid(ip, i).to_string());
        }
        out
    }

    fn extract_uuid_from_state_filename(name: &str) -> Option<&str> {
        const PREFIXES: [&str; 6] = [
            "ledger_",
            "acceptor_",
            "leader_",
            "replica_",
            "decree_notes_",
            "store_",
        ];

        let prefix = PREFIXES.iter().find(|p| name.starts_with(**p))?;
        let rest = &name[prefix.len()..];
        let idx = rest.find(".bin")?;
        let candidate = &rest[..idx];
        if Uuid::parse_str(candidate).is_ok() {
            Some(candidate)
        } else {
            None
        }
    }

    async fn purge_persisted_state_for_ip(&self, ip: IpAddr) -> anyhow::Result<usize> {
        if !Path::new(DATA_DIR).exists() {
            return Ok(0);
        }

        let owned = Self::owned_uuid_set_for_ip(ip, Self::CLEANUP_NODE_SLOTS_PER_IP);
        let mut removed = 0usize;
        let mut entries = fs::read_dir(DATA_DIR).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(uuid) = Self::extract_uuid_from_state_filename(name) else {
                continue;
            };
            if !owned.contains(uuid) {
                continue;
            }
            if fs::remove_file(entry.path()).await.is_ok() {
                removed += 1;
            }
        }

        Ok(removed)
    }
}
