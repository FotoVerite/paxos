use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;
use tokio::time::sleep;
use uuid::Uuid;

use crate::cluster::vertical::{
    activation::{PredecessorChain},
    configuration::VerticalClusterConfiguration,
    master::ActivationStatus,
    quorum::Quorum,
    system::VerticalSystem,
};
use crate::common::ballot::Ballot;
use crate::node::vertical_paxos::replica::VerticalClientReply;
use crate::scenario::{ScenarioStep, VerticalClientExpectation};

use super::spec::{VerticalConfigurationPlan, VerticalScenarioSpec};

pub struct VerticalScenarioRunner;

impl VerticalScenarioRunner {
    pub async fn run(
        system: Arc<VerticalSystem>,
        spec: &VerticalScenarioSpec,
        member_ids: &[Uuid],
        mut stop_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let configurations = Self::build_configurations(spec, member_ids)?;

        for phase in &spec.scenario.phases {
            for step in &phase.steps {
                let should_continue = Self::execute_step(
                    &system,
                    &configurations,
                    member_ids,
                    step,
                    &mut stop_rx,
                )
                .await?;
                if !should_continue {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn execute_step(
        system: &VerticalSystem,
        configurations: &HashMap<String, Arc<VerticalClusterConfiguration>>,
        member_ids: &[Uuid],
        step: &ScenarioStep,
        stop_rx: &mut broadcast::Receiver<()>,
    ) -> anyhow::Result<bool> {
        match step {
            ScenarioStep::Wait { duration } => Self::sleep_or_stop(stop_rx, *duration).await,
            ScenarioStep::VerticalInstallConfiguration { configuration_key } => {
                let configuration = Self::configuration(configurations, configuration_key)?;
                system.install_configuration(configuration).await?;
                Ok(true)
            }
            ScenarioStep::VerticalStartActivation {
                configuration_key,
                predecessors,
            } => {
                let predecessor_chain = Arc::new(PredecessorChain::new(
                    predecessors
                        .iter()
                        .map(|key| Self::configuration(configurations, key))
                        .collect::<anyhow::Result<Vec<_>>>()?,
                ));
                let configuration = Self::configuration(configurations, configuration_key)?;
                system
                    .start_activation(configuration.id(), predecessor_chain)
                    .await?;
                Ok(true)
            }
            ScenarioStep::VerticalWaitActivationReady {
                configuration_key,
                timeout,
            } => {
                let configuration = Self::configuration(configurations, configuration_key)?;
                Self::wait_for_activation_ready(system, configuration.id(), stop_rx, *timeout).await
            }
            ScenarioStep::VerticalSubmitClientRequest {
                replica_index,
                command,
                expect,
            } => {
                let replica_id = Self::member_id(member_ids, *replica_index)?;
                let reply = system.submit_client_request(replica_id, command.clone()).await?;
                if let Some(expectation) = expect {
                    Self::validate_reply(*expectation, &reply)?;
                }
                Ok(true)
            }
            ScenarioStep::Propose { .. }
            | ScenarioStep::Partition { .. }
            | ScenarioStep::HealPartition { .. }
            | ScenarioStep::AddDelay { .. }
            | ScenarioStep::AddPacketLoss { .. }
            | ScenarioStep::ClearFailures { .. }
            | ScenarioStep::EnableFailures
            | ScenarioStep::DisableFailures => {
                anyhow::bail!("vertical scenario runner received an unsupported classic-only step")
            }
        }
    }

    fn build_configurations(
        spec: &VerticalScenarioSpec,
        member_ids: &[Uuid],
    ) -> anyhow::Result<HashMap<String, Arc<VerticalClusterConfiguration>>> {
        let mut built = HashMap::new();

        for plan in &spec.configurations {
            let configuration = Arc::new(VerticalClusterConfiguration::new(
                Self::stable_configuration_id(spec, plan),
                Self::member_id(member_ids, plan.leader_index)?,
                plan.variant,
                Ballot::new(plan.ballot_number, Self::member_id(member_ids, plan.leader_index)?),
                plan.start_index,
                Self::resolve_members(member_ids, &plan.acceptors)?,
                Self::resolve_members(member_ids, &plan.replicas)?,
                Quorum::new(Self::resolve_members(member_ids, &plan.read_quorum)?)
                    .map_err(|err| anyhow::anyhow!("invalid read quorum for {}: {err:?}", plan.key))?,
                Quorum::new(Self::resolve_members(member_ids, &plan.write_quorum)?)
                    .map_err(|err| anyhow::anyhow!("invalid write quorum for {}: {err:?}", plan.key))?,
                plan.complete_bal_of
                    .as_ref()
                    .map(|key| {
                        built
                            .get(key)
                            .map(|configuration: &Arc<VerticalClusterConfiguration>| {
                                configuration.ballot()
                            })
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "configuration {} references missing complete_bal predecessor {}",
                                    plan.key,
                                    key
                                )
                            })
                    })
                    .transpose()?,
            )
            .map_err(|err| anyhow::anyhow!("invalid vertical configuration {}: {err:?}", plan.key))?);
            built.insert(plan.key.clone(), configuration);
        }

        Ok(built)
    }

    fn stable_configuration_id(spec: &VerticalScenarioSpec, plan: &VerticalConfigurationPlan) -> Uuid {
        Uuid::new_v5(
            &Uuid::NAMESPACE_DNS,
            format!("vertical-scenario:{}:{}", spec.key, plan.key).as_bytes(),
        )
    }

    fn configuration(
        configurations: &HashMap<String, Arc<VerticalClusterConfiguration>>,
        key: &str,
    ) -> anyhow::Result<Arc<VerticalClusterConfiguration>> {
        configurations
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing vertical configuration {key}"))
    }

    fn member_id(member_ids: &[Uuid], index: usize) -> anyhow::Result<Uuid> {
        member_ids
            .get(index)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("vertical scenario referenced missing member index {index}"))
    }

    fn resolve_members(member_ids: &[Uuid], indexes: &[usize]) -> anyhow::Result<Vec<Uuid>> {
        indexes
            .iter()
            .map(|index| Self::member_id(member_ids, *index))
            .collect()
    }

    fn validate_reply(
        expectation: VerticalClientExpectation,
        reply: &VerticalClientReply,
    ) -> anyhow::Result<()> {
        let matches = matches!(
            (expectation, reply),
            (VerticalClientExpectation::Accepted, VerticalClientReply::Accepted { .. })
                | (VerticalClientExpectation::Pending, VerticalClientReply::Pending { .. })
                | (VerticalClientExpectation::Redirect, VerticalClientReply::Redirect { .. })
        );

        if matches {
            return Ok(());
        }

        anyhow::bail!(
            "vertical scenario expected {:?} client reply but received {:?}",
            expectation,
            reply
        )
    }

    async fn sleep_or_stop(
        stop_rx: &mut broadcast::Receiver<()>,
        duration: Duration,
    ) -> anyhow::Result<bool> {
        tokio::select! {
            _ = sleep(duration) => Ok(true),
            result = stop_rx.recv() => {
                Ok(!matches!(result, Ok(_) | Err(broadcast::error::RecvError::Closed)))
            }
        }
    }

    async fn wait_for_activation_ready(
        system: &VerticalSystem,
        configuration_id: Uuid,
        stop_rx: &mut broadcast::Receiver<()>,
        timeout_duration: Duration,
    ) -> anyhow::Result<bool> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            if system
                .master()
                .activation(configuration_id)
                .await
                .is_some_and(|record| record.status() == &ActivationStatus::Ready)
            {
                return Ok(true);
            }

            let now = Instant::now();
            if now >= deadline {
                anyhow::bail!(
                    "timed out waiting for vertical activation {} to become ready",
                    configuration_id
                );
            }

            let sleep_for = (deadline - now).min(Duration::from_millis(25));
            tokio::select! {
                _ = sleep(sleep_for) => {}
                result = stop_rx.recv() => {
                    match result {
                        Ok(_) | Err(broadcast::error::RecvError::Closed) => return Ok(false),
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
            }
        }
    }
}
