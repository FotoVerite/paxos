use std::sync::Arc;

use rand::Rng;
use tokio::sync::Mutex;

use crate::cluster::classic::ClassicCluster;
use crate::decree_generator::DecreeGenerator;
use crate::monitor::{Event, PaxosObserver, current_timestamp_millis};
use crate::paxos_command::PaxosCommand;

use super::spec::{ClassicAction, ClassicProposerSelector, ClassicScenarioSpec, ClassicTrigger};

pub struct ClassicScenarioExecution;

impl ClassicScenarioExecution {
    pub async fn execute_iteration(
        cluster: &Arc<Mutex<ClassicCluster>>,
        proposal_count: usize,
        decree_gen: &mut DecreeGenerator,
        spec: &ClassicScenarioSpec,
        leader_for_runner: usize,
        observer: Arc<dyn PaxosObserver>,
    ) {
        Self::run_actions(cluster, proposal_count, spec, observer).await;
        if proposal_count < spec.start_at {
            return;
        }
        if proposal_count % spec.proposal_interval != 0 {
            return;
        }

        let plans = spec.proposals.clone();
        let proposals = {
            let cluster = cluster.lock().await;
            plans
                .into_iter()
                .filter_map(|plan| {
                    Self::resolve_proposer_uuid(&cluster, &plan.selector, leader_for_runner).map(
                        |proposer_uuid| {
                            let cmd = PaxosCommand::EnactDecree {
                                author: plan.author,
                                law: decree_gen.next(),
                            };
                            (proposer_uuid, cmd)
                        },
                    )
                })
                .collect::<Vec<_>>()
        };

        if proposals.is_empty() {
            return;
        }

        let mut tasks = Vec::with_capacity(proposals.len());
        for (proposer_uuid, cmd) in proposals {
            let cluster = Arc::clone(cluster);
            tasks.push(tokio::spawn(async move {
                let mut cluster = cluster.lock().await;
                cluster.propose_from(proposer_uuid, cmd).await;
            }));
        }

        for task in tasks {
            let _ = task.await;
        }
    }

    async fn run_actions(
        cluster: &Arc<Mutex<ClassicCluster>>,
        proposal_count: usize,
        spec: &ClassicScenarioSpec,
        observer: Arc<dyn PaxosObserver>,
    ) {
        for rule in &spec.actions {
            let matches = match rule.when {
                ClassicTrigger::AtIteration { count } => proposal_count == count,
            };
            if !matches {
                continue;
            }

            match &rule.action {
                ClassicAction::PartitionGroups {
                    partition_a,
                    partition_b,
                } => {
                    Self::partition_groups(cluster, partition_a, partition_b, observer.clone())
                        .await;
                }
                ClassicAction::HealGroups {
                    partition_a,
                    partition_b,
                } => {
                    Self::heal_groups(cluster, partition_a, partition_b, observer.clone()).await;
                }
            }
        }
    }

    async fn partition_groups(
        cluster: &Arc<Mutex<ClassicCluster>>,
        partition_a: &[usize],
        partition_b: &[usize],
        observer: Arc<dyn PaxosObserver>,
    ) {
        let cluster = cluster.lock().await;
        for from in partition_a {
            for to in partition_b {
                cluster.partition(*from, *to).await;
            }
        }

        observer.on_event(Event::PartitionCreated {
            partition_a: partition_a.iter().map(|i| cluster.uuid(*i)).collect(),
            partition_b: partition_b.iter().map(|i| cluster.uuid(*i)).collect(),
            created_at: current_timestamp_millis(),
        });
    }

    async fn heal_groups(
        cluster: &Arc<Mutex<ClassicCluster>>,
        partition_a: &[usize],
        partition_b: &[usize],
        observer: Arc<dyn PaxosObserver>,
    ) {
        let cluster = cluster.lock().await;
        for from in partition_a {
            for to in partition_b {
                cluster.heal_partition(*from, *to).await;
            }
        }

        observer.on_event(Event::PartitionHealed {
            partition_a: partition_a.iter().map(|i| cluster.uuid(*i)).collect(),
            partition_b: partition_b.iter().map(|i| cluster.uuid(*i)).collect(),
            created_at: current_timestamp_millis(),
        });
    }

    fn resolve_proposer_uuid(
        cluster: &ClassicCluster,
        selector: &ClassicProposerSelector,
        leader_for_runner: usize,
    ) -> Option<uuid::Uuid> {
        match selector {
            ClassicProposerSelector::RandomAny => {
                let mut rng = rand::rng();
                cluster
                    .nodes
                    .get(rng.random_range(0..cluster.nodes.len()))
                    .map(|node| node.uuid)
            }
            ClassicProposerSelector::FixedIndex { index } => {
                cluster.nodes.get(*index).map(|node| node.uuid)
            }
            ClassicProposerSelector::RandomFromIndices { indices } => {
                if indices.is_empty() {
                    return None;
                }
                let mut rng = rand::rng();
                let pick = indices[rng.random_range(0..indices.len())];
                cluster.nodes.get(pick).map(|node| node.uuid)
            }
            ClassicProposerSelector::Leader => {
                cluster.nodes.get(leader_for_runner).map(|node| node.uuid)
            }
        }
    }
}
