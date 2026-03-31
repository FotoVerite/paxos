use serde::{Deserialize, Serialize};

use crate::cluster::pmmc::reconfiguration::ConfigurationStrategy;

use super::reconfiguration::PmmcReconfigurationSpec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PmmcScenarioSpec {
    pub name: String,
    pub description: String,
    pub topology: PmmcTopologySpec,
    #[serde(default)]
    pub clients: Vec<PmmcClientSpec>,
    pub request_plan: PmmcRequestPlan,
    pub completion: PmmcCompletion,
    #[serde(default)]
    pub actions: Vec<PmmcActionRule>,
    #[serde(default)]
    pub reconfiguration: Option<PmmcReconfigurationSpec>,
    #[serde(default)]
    pub initial_configuration: PmmcInitialConfigurationSpec,
    #[serde(default)]
    pub timings: PmmcTimingSpec,
}

impl PmmcScenarioSpec {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.clients.is_empty() {
            anyhow::bail!(
                "PMMC scenario '{}' must define at least one client",
                self.name
            );
        }

        if self.completion.target_successes() == 0 {
            anyhow::bail!(
                "PMMC scenario '{}' must require at least one successful request",
                self.name
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PmmcInitialConfigurationSpec {
    pub strategy: Option<ConfigurationStrategy>,
    pub alpha_max_inflight: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PmmcTopologySpec {
    pub kind: PmmcTopologyKind,
    pub node_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PmmcTopologyKind {
    FullMesh,
    RoleSplit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PmmcClientSpec {
    pub id: String,
    pub start_replica_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PmmcRequestPlan {
    pub kind: PmmcRequestPlanKind,
    #[serde(default = "default_wait_for_reply")]
    pub wait_for_reply: bool,
    #[serde(default)]
    pub multi_key: bool,
}

const fn default_wait_for_reply() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PmmcRequestPlanKind {
    SequentialKvPuts {
        key: String,
        value_prefix: String,
        start_at: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PmmcActionRule {
    pub when: PmmcTrigger,
    pub action: PmmcAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PmmcTrigger {
    OnStart,
    AfterSuccesses { count: usize },
    AfterAttempts { count: usize },
    AfterTimeouts { count: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PmmcAction {
    CrashNode {
        node_index: usize,
    },
    CrashCurrentLeader,
    IsolateNode {
        node_index: usize,
    },
    IsolateCurrentLeader,
    HealNode {
        node_index: usize,
    },
    HealCurrentLeader,
    MoveClient {
        client_id: String,
        replica_index: usize,
    },
    SleepMs {
        millis: u64,
    },
    EmitNote {
        note: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PmmcCompletion {
    SuccessCount { count: usize },
}

impl PmmcCompletion {
    pub fn target_successes(&self) -> usize {
        match self {
            Self::SuccessCount { count } => *count,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PmmcTimingSpec {
    #[serde(default = "default_initial_settle_ms")]
    pub initial_settle_ms: u64,
    #[serde(default = "default_response_timeout_ms")]
    pub response_timeout_ms: u64,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_post_action_pause_ms")]
    pub post_action_pause_ms: u64,
    #[serde(default = "default_loop_interval_ms")]
    pub loop_interval_ms: u64,
}

impl Default for PmmcTimingSpec {
    fn default() -> Self {
        Self {
            initial_settle_ms: default_initial_settle_ms(),
            response_timeout_ms: default_response_timeout_ms(),
            retry_backoff_ms: default_retry_backoff_ms(),
            post_action_pause_ms: default_post_action_pause_ms(),
            loop_interval_ms: default_loop_interval_ms(),
        }
    }
}

const fn default_initial_settle_ms() -> u64 {
    500
}

const fn default_response_timeout_ms() -> u64 {
    2_000
}

const fn default_retry_backoff_ms() -> u64 {
    250
}

const fn default_post_action_pause_ms() -> u64 {
    700
}

const fn default_loop_interval_ms() -> u64 {
    1_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_non_empty_client_list() {
        let spec = PmmcScenarioSpec {
            name: "bad".to_string(),
            description: "missing client".to_string(),
            topology: PmmcTopologySpec {
                kind: PmmcTopologyKind::FullMesh,
                node_count: 5,
            },
            clients: Vec::new(),
            request_plan: PmmcRequestPlan {
                kind: PmmcRequestPlanKind::SequentialKvPuts {
                    key: "k".to_string(),
                    value_prefix: "v".to_string(),
                    start_at: 1,
                },
                wait_for_reply: true,
                multi_key: false,
            },
            completion: PmmcCompletion::SuccessCount { count: 5 },
            actions: Vec::new(),
            reconfiguration: None,
            initial_configuration: PmmcInitialConfigurationSpec::default(),
            timings: PmmcTimingSpec::default(),
        };

        assert!(spec.validate().is_err());
    }
}
