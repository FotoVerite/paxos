use serde::{Deserialize, Serialize};

use std::sync::Arc;

use crate::monitor::{Event, PaxosObserver, current_timestamp_millis};
use crate::node::config::{ClassicNodeConfig, LearningStrategy, Roles};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassicScenarioSpec {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub topology: Option<ClassicTopologySpec>,
    #[serde(default)]
    pub startup: ClassicStartupSpec,
    #[serde(default)]
    pub start_at: usize,
    pub proposal_interval: usize,
    #[serde(default)]
    pub actions: Vec<ClassicActionRule>,
    pub proposals: Vec<ClassicProposalPlan>,
}

impl ClassicScenarioSpec {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.proposal_interval == 0 {
            anyhow::bail!(
                "classic scenario '{}' must have a non-zero proposal interval",
                self.name
            );
        }
        if self.proposals.is_empty() {
            anyhow::bail!(
                "classic scenario '{}' must define at least one proposal plan",
                self.name
            );
        }
        if let Some(topology) = &self.topology {
            if topology.groups.is_empty() {
                anyhow::bail!(
                    "classic scenario '{}' topology must define at least one group",
                    self.name
                );
            }
            if topology.groups.iter().any(|group| group.count == 0) {
                anyhow::bail!(
                    "classic scenario '{}' topology groups must have positive counts",
                    self.name
                );
            }
        }
        Ok(())
    }

    pub fn node_count(&self) -> Option<usize> {
        self.topology
            .as_ref()
            .map(|topology| topology.groups.iter().map(|group| group.count).sum())
    }

    pub fn node_configs(&self, learning_strategy: LearningStrategy) -> Option<Vec<ClassicNodeConfig>> {
        self.topology.as_ref().map(|topology| {
            let mut configs = Vec::with_capacity(
                topology.groups.iter().map(|group| group.count).sum(),
            );
            for group in &topology.groups {
                for _ in 0..group.count {
                    configs.push(ClassicNodeConfig {
                        roles: Roles {
                            proposer: group.roles.proposer,
                            acceptor: group.roles.acceptor,
                            learner: group.roles.learner,
                        },
                        learning_strategy: learning_strategy.clone(),
                    });
                }
            }
            configs
        })
    }

    pub fn emit_startup_events(
        &self,
        observer: Arc<dyn PaxosObserver>,
        node_uuids: &[Uuid],
        leader_node: Option<usize>,
    ) {
        if !self.startup.emit_leader_elected || node_uuids.is_empty() {
            return;
        }

        let elected_leader = leader_node.unwrap_or_else(|| {
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                % node_uuids.len() as u128) as usize
        });

        observer.on_event(Event::LeaderElected {
            id: node_uuids[elected_leader],
            created_at: current_timestamp_millis(),
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassicActionRule {
    pub when: ClassicTrigger,
    pub action: ClassicAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClassicTrigger {
    AtIteration { count: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClassicAction {
    PartitionGroups {
        partition_a: Vec<usize>,
        partition_b: Vec<usize>,
    },
    HealGroups {
        partition_a: Vec<usize>,
        partition_b: Vec<usize>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassicStartupSpec {
    #[serde(default)]
    pub emit_leader_elected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassicTopologySpec {
    pub groups: Vec<ClassicNodeGroupSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassicNodeGroupSpec {
    pub count: usize,
    pub roles: ClassicRolesSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassicRolesSpec {
    pub proposer: bool,
    pub acceptor: bool,
    pub learner: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassicProposalPlan {
    pub author: String,
    pub selector: ClassicProposerSelector,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClassicProposerSelector {
    RandomAny,
    FixedIndex { index: usize },
    RandomFromIndices { indices: Vec<usize> },
    Leader,
}
