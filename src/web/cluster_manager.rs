use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, sleep};
use tracing::{info, instrument, warn};
use uuid::Uuid;

use crate::cluster::{
    classic_cluster::ClassicCluster, cluster_configuration::ClusterConfiguration,
    pmmc_cluster::PmmcCluster,
};
use crate::common::persistence::Persistence;
use crate::console_observer::ConsoleObserver;
use crate::decree_generator::DecreeGenerator;
use crate::message::MessageTrace;
use crate::monitor::PaxosObserver;
use crate::node::config::{ClassicNodeConfig, LearningStrategy, Roles};
use crate::paxos_command::PaxosCommand;
use crate::web::scenarios::{
    CatchUpScenario, ClassicScenarioExecution, ClassicScenarioLoader, ScenarioType,
    pmmc::{PmmcScenarioExecution, PmmcScenarioLoader},
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
    Classic(Arc<Mutex<ClassicCluster>>),
    Pmmc(Arc<Mutex<PmmcCluster>>),
}

struct ScenarioObserver {
    websocket: Arc<WebSocketObserver>,
    console: Arc<ConsoleObserver>,
}

impl PaxosObserver for ScenarioObserver {
    fn on_event(&self, event: crate::monitor::Event) {
        self.websocket.on_event(event.clone());
        self.console.on_event(event);
    }

    fn on_message_trace(&self, trace: MessageTrace) {
        self.websocket.on_message_trace(trace.clone());
        self.console.on_message_trace(trace);
    }
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

    async fn create_classic_cluster(
        id: usize,
        ip: IpAddr,
        configuration: ClusterConfiguration,
        observer: Arc<dyn crate::monitor::PaxosObserver>,
    ) -> anyhow::Result<ClassicCluster> {
        ClassicCluster::new_with_configuration(id, ip, configuration, observer).await
    }

    fn pmmc_configuration_for_scenario(
        ip: IpAddr,
        scenario_type: ScenarioType,
        node_count: usize,
        pmmc_spec: Option<&crate::web::scenarios::pmmc::spec::PmmcScenarioSpec>,
    ) -> anyhow::Result<ClusterConfiguration> {
        let initial_strategy = pmmc_spec
            .and_then(|spec| spec.initial_configuration.strategy)
            .unwrap_or(
                crate::cluster::cluster_configuration::ConfigurationStrategy::JointConsensus,
            );
        let initial_alpha = pmmc_spec
            .and_then(|spec| spec.initial_configuration.alpha_max_inflight)
            .unwrap_or(5);

        if matches!(
            scenario_type,
            ScenarioType::PmmcReconfigJointConsensus
                | ScenarioType::PmmcReconfigStopSign
                | ScenarioType::PmmcReconfigDelayedStopSign
                | ScenarioType::PmmcReconfigPadding
                | ScenarioType::PmmcReconfigBrickWall
        ) {
            let mut configs = Vec::with_capacity(5);
            configs.push(crate::node::config::PmmcNodeConfig {
                roles: Roles {
                    proposer: true,
                    acceptor: false,
                    learner: false,
                },
            });
            for _ in 0..3 {
                configs.push(crate::node::config::PmmcNodeConfig {
                    roles: Roles {
                        proposer: false,
                        acceptor: true,
                        learner: false,
                    },
                });
            }
            configs.push(crate::node::config::PmmcNodeConfig {
                roles: Roles {
                    proposer: false,
                    acceptor: false,
                    learner: true,
                },
            });
            return ClusterConfiguration::bootstrap_pmmc_with_params(
                ip,
                configs,
                initial_strategy,
                Some(initial_alpha),
            )
            .map_err(Into::into);
        }

        if scenario_type.uses_role_split_topology() {
            let mut configs = Vec::with_capacity(7);
            for _ in 0..2 {
                configs.push(crate::node::config::PmmcNodeConfig {
                    roles: Roles {
                        proposer: true,
                        acceptor: false,
                        learner: false,
                    },
                });
            }
            for _ in 0..2 {
                configs.push(crate::node::config::PmmcNodeConfig {
                    roles: Roles {
                        proposer: false,
                        acceptor: false,
                        learner: true,
                    },
                });
            }
            for _ in 0..3 {
                configs.push(crate::node::config::PmmcNodeConfig {
                    roles: Roles {
                        proposer: false,
                        acceptor: true,
                        learner: false,
                    },
                });
            }
            ClusterConfiguration::bootstrap_pmmc_with_params(
                ip,
                configs,
                initial_strategy,
                Some(initial_alpha),
            )
            .map_err(Into::into)
        } else {
            ClusterConfiguration::bootstrap_pmmc_with_params(
                ip,
                vec![crate::node::config::PmmcNodeConfig::default(); node_count],
                initial_strategy,
                Some(initial_alpha),
            )
            .map_err(Into::into)
        }
    }

    async fn create_pmmc_runtime(
        ip: IpAddr,
        configuration: ClusterConfiguration,
        observer: Arc<dyn crate::monitor::PaxosObserver>,
    ) -> anyhow::Result<PmmcCluster> {
        PmmcCluster::new_with_configuration(ip, configuration, observer).await
    }

    fn classic_configuration_for_scenario(
        ip: IpAddr,
        scenario_type: ScenarioType,
        requested_node_count: usize,
        learning_strategy: LearningStrategy,
        classic_spec: Option<&crate::web::scenarios::classic::spec::ClassicScenarioSpec>,
    ) -> anyhow::Result<(ClusterConfiguration, usize)> {
        let actual_node_count = classic_spec
            .and_then(|spec| spec.node_count())
            .unwrap_or_else(|| scenario_type.classic_node_count(requested_node_count));
        let configs: Vec<ClassicNodeConfig> = classic_spec
            .and_then(|spec| spec.node_configs(learning_strategy.clone()))
            .unwrap_or_else(|| scenario_type.classic_configs(actual_node_count, learning_strategy));
        let configuration = ClusterConfiguration::bootstrap_classic(ip, configs)?;
        Ok((configuration, actual_node_count))
    }

    async fn prepare_scenario_start(
        &self,
        ip: IpAddr,
        node_count: usize,
        scenario_type: ScenarioType,
    ) -> anyhow::Result<broadcast::Sender<()>> {
        self.signal_stop().await;
        let cleaned = self.cleanup_active_cluster().await?;

        if !cleaned {
            if let Ok(removed) = self.purge_persisted_state_for_ip(ip).await {
                if removed > 0 {
                    info!(
                        removed_state_files = removed,
                        "Purged persisted state before scenario start"
                    );
                }
            }
        } else {
            info!("Cleaned active cluster persistence before scenario start");
        }

        self.observer.clear().await;

        if scenario_type == ScenarioType::CatchUp {
            CatchUpScenario::setup(ip, node_count).await?;
        }

        Ok(self.replace_stop_channel().await)
    }

    fn parse_learning_strategy(learning_strategy: &str) -> LearningStrategy {
        match learning_strategy {
            "Direct" => LearningStrategy::Direct,
            _ => LearningStrategy::ProposerManaged,
        }
    }

    async fn store_active_cluster(&self, cluster: ActiveCluster) {
        let mut current = self.cluster.lock().await;
        *current = Some(cluster);
    }

    async fn cleanup_active_cluster(&self) -> anyhow::Result<bool> {
        let active = {
            let mut current = self.cluster.lock().await;
            current.take()
        };

        match active {
            Some(ActiveCluster::Classic(cluster_arc)) => {
                let cluster = cluster_arc.lock().await;
                cluster.cleanup().await?;
                Ok(true)
            }
            Some(ActiveCluster::Pmmc(cluster_arc)) => {
                let mut cluster = cluster_arc.lock().await;
                cluster.cleanup().await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn replace_stop_channel(&self) -> broadcast::Sender<()> {
        let (stop_tx, _) = broadcast::channel(1);
        let mut tx = self.stop_tx.lock().await;
        *tx = Some(stop_tx.clone());
        stop_tx
    }

    async fn signal_stop(&self) {
        let mut tx = self.stop_tx.lock().await;
        if let Some(stop_tx) = tx.take() {
            let _ = stop_tx.send(());
        }
    }

    async fn build_active_cluster(
        &self,
        ip: IpAddr,
        requested_node_count: usize,
        scenario_type: ScenarioType,
        learning_strategy: LearningStrategy,
        leader_node: Option<usize>,
        scenario_observer: Arc<dyn PaxosObserver>,
        classic_spec: Option<&crate::web::scenarios::classic::spec::ClassicScenarioSpec>,
        pmmc_spec: Option<&crate::web::scenarios::pmmc::spec::PmmcScenarioSpec>,
    ) -> anyhow::Result<(ActiveCluster, usize)> {
        if scenario_type.is_pmmc() {
            let configuration = Self::pmmc_configuration_for_scenario(
                ip,
                scenario_type,
                requested_node_count,
                pmmc_spec,
            )?;
            let cluster = Self::create_pmmc_runtime(ip, configuration, scenario_observer).await?;
            let node_count = cluster.num_nodes();
            info!(
                runtime = "pmmc",
                actual_node_count = node_count,
                quorum_size = cluster.quorum_size(),
                topology = scenario_type.pmmc_topology_summary(node_count),
                "Initialized PMMC cluster"
            );
            self.observer
                .set_cluster_info(node_count, cluster.quorum_size(), cluster.get_node_uuids())
                .await;
            cluster.enable_failures().await;
            cluster
                .start_all_ready(Duration::from_secs(8))
                .await
                .map_err(|err| anyhow::anyhow!("PMMC cluster failed readiness barrier: {err}"))?;

            let cluster_arc = Arc::new(Mutex::new(cluster));
            self.store_active_cluster(ActiveCluster::Pmmc(cluster_arc.clone()))
                .await;
            Ok((ActiveCluster::Pmmc(cluster_arc), node_count))
        } else {
            let (configuration, _actual_node_count) = Self::classic_configuration_for_scenario(
                ip,
                scenario_type,
                requested_node_count,
                learning_strategy,
                classic_spec,
            )?;
            let mut cluster =
                Self::create_classic_cluster(0, ip, configuration, scenario_observer.clone())
                    .await?;
            if let Some(spec) = classic_spec {
                spec.emit_startup_events(scenario_observer, &cluster.get_node_uuids(), leader_node);
            } else {
                scenario_type.emit_classic_startup_events(
                    scenario_observer,
                    &cluster.get_node_uuids(),
                    leader_node,
                );
            }

            let node_count = cluster.nodes.len();
            info!(
                runtime = "classic",
                actual_node_count = node_count,
                quorum_size = cluster.quorum_size(),
                topology = scenario_type.classic_topology_summary(),
                "Initialized Classic cluster"
            );
            self.observer
                .set_cluster_info(node_count, cluster.quorum_size(), cluster.get_node_uuids())
                .await;
            cluster.enable_failures().await;
            for i in 0..node_count {
                cluster.nodes[i].start();
            }
            sleep(Duration::from_millis(100)).await;

            let cluster_arc = Arc::new(Mutex::new(cluster));
            self.store_active_cluster(ActiveCluster::Classic(cluster_arc.clone()))
                .await;
            Ok((ActiveCluster::Classic(cluster_arc), node_count))
        }
    }

    async fn run_classic_iteration(
        cluster_for_runner: &Arc<Mutex<ClassicCluster>>,
        scenario_type: ScenarioType,
        proposal_count: usize,
        decree_gen: &mut DecreeGenerator,
        leader_for_runner: usize,
        observer_for_runner: Arc<WebSocketObserver>,
        classic_spec: Option<&crate::web::scenarios::classic::spec::ClassicScenarioSpec>,
    ) {
        if let Some(spec) = classic_spec {
            ClassicScenarioExecution::execute_iteration(
                cluster_for_runner,
                proposal_count,
                decree_gen,
                spec,
                leader_for_runner,
                observer_for_runner,
            )
            .await;
            return;
        }

        if scenario_type == ScenarioType::CatchUp {
            let mut cluster = cluster_for_runner.lock().await;
            if let Err(e) = CatchUpScenario::propose_gap_filler(&mut cluster, 0).await {
                warn!(error = %e, "Error proposing gap filler");
            }
            return;
        }
    }

    async fn spawn_scenario_runner(
        &self,
        active_for_runner: ActiveCluster,
        scenario_type: ScenarioType,
        scenario_run_id: Uuid,
        duration_secs: u64,
        node_count: usize,
        stop_tx: broadcast::Sender<()>,
        leader_node: Option<usize>,
        classic_spec: Option<crate::web::scenarios::classic::spec::ClassicScenarioSpec>,
        pmmc_spec: Option<crate::web::scenarios::pmmc::spec::PmmcScenarioSpec>,
    ) -> anyhow::Result<()> {
        let mut stop_rx = stop_tx.subscribe();
        let observer_for_runner = self.observer.clone();
        let mut decree_gen = self.decree_generator.lock().await.clone();
        let leader_for_runner = leader_node.unwrap_or(0);

        info!(%scenario_run_id, actual_node_count = node_count, "Launching scenario runner task");

        tokio::spawn(async move {
            match active_for_runner {
                ActiveCluster::Classic(cluster_for_runner) => {
                    let start = std::time::Instant::now();
                    let mut proposal_count = 0usize;

                    loop {
                        if stop_rx.try_recv().is_ok() {
                            break;
                        }
                        if start.elapsed().as_secs() >= duration_secs {
                            break;
                        }

                        Self::run_classic_iteration(
                            &cluster_for_runner,
                            scenario_type,
                            proposal_count,
                            &mut decree_gen,
                            leader_for_runner,
                            observer_for_runner.clone(),
                            classic_spec.as_ref(),
                        )
                        .await;

                        proposal_count += 1;
                        sleep(Duration::from_millis(1000)).await;
                    }
                }
                ActiveCluster::Pmmc(cluster_for_runner) => {
                    let spec = match pmmc_spec {
                        Some(spec) => spec,
                        None => match PmmcScenarioLoader::load_for(scenario_type).await {
                            Ok(spec) => spec,
                            Err(err) => {
                                warn!(
                                    %scenario_run_id,
                                    scenario_type = scenario_type.as_str(),
                                    error = %err,
                                    "Failed to load PMMC scenario spec"
                                );
                                return;
                            }
                        },
                    };

                    PmmcScenarioExecution::new(
                        scenario_run_id,
                        duration_secs,
                        spec,
                        cluster_for_runner,
                        observer_for_runner,
                    )
                    .run(stop_rx)
                    .await;
                }
            }
        });

        Ok(())
    }

    #[instrument(
        name = "cluster_manager.start_scenario",
        skip(self),
        fields(
            client_ip = %ip,
            scenario_type = scenario_type.as_str(),
            node_count = node_count,
            duration_secs = duration_secs,
            learning_strategy = learning_strategy,
            leader_node = ?leader_node
        )
    )]
    pub async fn start_scenario(
        &self,
        ip: IpAddr,
        node_count: usize,
        duration_secs: u64,
        scenario_type: ScenarioType,
        learning_strategy: &str,
        leader_node: Option<usize>,
    ) -> anyhow::Result<()> {
        let scenario_run_id = Uuid::new_v4();
        info!("Starting new scenario");
        info!(%scenario_run_id, "Allocated scenario run id");

        let stop_tx = self
            .prepare_scenario_start(ip, node_count, scenario_type)
            .await?;

        let learning_strat = Self::parse_learning_strategy(learning_strategy);
        let pmmc_spec = if scenario_type.is_pmmc() {
            Some(PmmcScenarioLoader::load_for(scenario_type).await?)
        } else {
            None
        };
        let classic_spec = if scenario_type.is_pmmc() {
            None
        } else {
            ClassicScenarioLoader::load_for(scenario_type).await.ok()
        };

        let scenario_observer: Arc<dyn PaxosObserver> = Arc::new(ScenarioObserver {
            websocket: self.observer.clone(),
            console: Arc::new(ConsoleObserver),
        });

        let (active_for_runner, node_count) = self
            .build_active_cluster(
                ip,
                node_count,
                scenario_type,
                learning_strat,
                leader_node,
                scenario_observer,
                classic_spec.as_ref(),
                pmmc_spec.as_ref(),
            )
            .await?;

        self.spawn_scenario_runner(
            active_for_runner,
            scenario_type,
            scenario_run_id,
            duration_secs,
            node_count,
            stop_tx,
            leader_node,
            classic_spec,
            pmmc_spec,
        )
        .await?;

        Ok(())
    }

    #[instrument(name = "cluster_manager.propose", skip(self, decree))]
    pub async fn propose(&self, decree: PaxosCommand) -> anyhow::Result<()> {
        let current = self.cluster.lock().await;
        match current.as_ref() {
            Some(ActiveCluster::Classic(cluster_arc)) => {
                let mut cluster = cluster_arc.lock().await;
                cluster.propose(decree).await;
                Ok(())
            }
            Some(ActiveCluster::Pmmc(_)) => {
                warn!("Rejecting direct proposal: PMMC uses internal client request flow");
                Err(anyhow::anyhow!(
                    "Direct /api/propose is not supported for PMMC; use scenario client flow"
                ))
            }
            None => {
                warn!("Rejecting proposal because no active cluster exists");
                Err(anyhow::anyhow!("No active cluster"))
            }
        }
    }

    #[instrument(name = "cluster_manager.stop_scenario", skip(self))]
    pub async fn stop_scenario(&self) -> anyhow::Result<()> {
        self.signal_stop().await;
        let _ = self.cleanup_active_cluster().await?;
        info!("Scenario stopped");
        Ok(())
    }

    #[instrument(name = "cluster_manager.reset", skip(self), fields(client_ip = %ip))]
    pub async fn reset(&self, ip: IpAddr) -> anyhow::Result<()> {
        self.signal_stop().await;
        let cleaned = self.cleanup_active_cluster().await?;

        if !cleaned {
            // Remove all persisted node versions (including stale UUIDs/corrupt/tmp variants).
            let removed = self.purge_persisted_state_for_ip(ip).await?;
            info!(
                removed_state_files = removed,
                "Deleted persisted state files"
            );
        } else {
            info!("Deleted persisted state through active cluster cleanup");
        }

        // Clear the observer state
        self.observer.clear().await;

        info!("Reset complete");
        Ok(())
    }

    async fn purge_persisted_state_for_ip(&self, ip: IpAddr) -> anyhow::Result<usize> {
        let persistence = Persistence::cluster(ip);
        let removed = usize::from(persistence.dir().exists());
        persistence.purge_cluster_dir().await?;
        info!(client_ip = %ip, removed_state_roots = removed, "Purged cluster persistence root");
        Ok(removed)
    }
}
