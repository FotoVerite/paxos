use std::collections::HashSet;

use uuid::Uuid;

use crate::cluster::cluster_configuration::{
    ConfigurationStrategy, reconfig_patch::ReconfigPatch,
};
use crate::node::config::Roles;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PmmcReconfigurationSpec {
    pub alpha_max_inflight: Option<usize>,
    pub strategy: Option<ConfigurationStrategy>,
    #[serde(default)]
    pub add: Vec<PmmcReconfigurationAddSpec>,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PmmcReconfigurationAddSpec {
    pub node_id: String,
    pub roles: Roles,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum PmmcReconfigurationSpecError {
    #[error("scenario_id cannot be empty")]
    EmptyScenarioId,
    #[error("node_id cannot be empty")]
    EmptyNodeId,
    #[error("duplicate add node_id: {node_id}")]
    DuplicateAddNodeId { node_id: String },
    #[error("duplicate remove node_id: {node_id}")]
    DuplicateRemoveNodeId { node_id: String },
    #[error("node_id resolver failed for '{node_id}': {reason}")]
    NodeIdResolutionFailed { node_id: String, reason: String },
}

// Stable namespace for PMMC scenario node IDs.
const PMMC_SCENARIO_NODE_NAMESPACE: Uuid = Uuid::from_u128(0xd14cf00dc0de5a1e91f00dd00dd00dd0);

impl PmmcReconfigurationSpec {
    pub fn to_runtime_patch(
        &self,
        scenario_id: &str,
    ) -> Result<ReconfigPatch, PmmcReconfigurationSpecError> {
        let scenario_id = scenario_id.trim();
        if scenario_id.is_empty() {
            return Err(PmmcReconfigurationSpecError::EmptyScenarioId);
        }

        self.to_runtime_patch_with_resolver(|node_id| {
            Ok(stable_node_uuid_for_scenario(scenario_id, node_id))
        })
    }

    pub fn to_runtime_patch_with_resolver<F>(
        &self,
        mut resolve: F,
    ) -> Result<ReconfigPatch, PmmcReconfigurationSpecError>
    where
        F: FnMut(&str) -> Result<Uuid, String>,
    {
        let mut patch = ReconfigPatch::new();
        patch.alpha_max_inflight = self.alpha_max_inflight;
        patch.strategy = self.strategy;

        let mut seen_add = HashSet::new();
        for add in &self.add {
            let node_id = normalize_node_id(&add.node_id)?;
            if !seen_add.insert(node_id.clone()) {
                return Err(PmmcReconfigurationSpecError::DuplicateAddNodeId { node_id });
            }

            let uuid = resolve(&node_id).map_err(|reason| {
                PmmcReconfigurationSpecError::NodeIdResolutionFailed {
                    node_id: node_id.clone(),
                    reason,
                }
            })?;
            patch.add.insert(uuid, add.roles.clone());
        }

        let mut seen_remove = HashSet::new();
        for remove in &self.remove {
            let node_id = normalize_node_id(remove)?;
            if !seen_remove.insert(node_id.clone()) {
                return Err(PmmcReconfigurationSpecError::DuplicateRemoveNodeId { node_id });
            }

            let uuid = resolve(&node_id).map_err(|reason| {
                PmmcReconfigurationSpecError::NodeIdResolutionFailed {
                    node_id: node_id.clone(),
                    reason,
                }
            })?;
            patch.remove.insert(uuid);
        }

        Ok(patch)
    }
}

pub fn stable_node_uuid_for_scenario(scenario_id: &str, node_id: &str) -> Uuid {
    let scenario_id = scenario_id.trim();
    let node_id = node_id.trim();
    let name = format!("{scenario_id}:{node_id}");
    Uuid::new_v5(&PMMC_SCENARIO_NODE_NAMESPACE, name.as_bytes())
}

fn normalize_node_id(node_id: &str) -> Result<String, PmmcReconfigurationSpecError> {
    let normalized = node_id.trim();
    if normalized.is_empty() {
        return Err(PmmcReconfigurationSpecError::EmptyNodeId);
    }
    Ok(normalized.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roles(proposer: bool, acceptor: bool, learner: bool) -> Roles {
        Roles {
            proposer,
            acceptor,
            learner,
        }
    }

    #[test]
    fn stable_uuid_is_deterministic() {
        let a = stable_node_uuid_for_scenario("scenario-a", "node-1");
        let b = stable_node_uuid_for_scenario("scenario-a", "node-1");
        let c = stable_node_uuid_for_scenario("scenario-a", "node-2");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn scenario_patch_converts_to_runtime_patch() {
        let scenario = PmmcReconfigurationSpec {
            alpha_max_inflight: Some(8),
            strategy: Some(ConfigurationStrategy::StopSign),
            add: vec![PmmcReconfigurationAddSpec {
                node_id: "replica-9".to_string(),
                roles: roles(false, false, true),
            }],
            remove: vec!["acceptor-0".to_string()],
        };

        let runtime = scenario
            .to_runtime_patch("pmmc-reconfig")
            .expect("conversion should succeed");

        assert_eq!(runtime.alpha_max_inflight, Some(8));
        assert_eq!(runtime.strategy, Some(ConfigurationStrategy::StopSign));
        assert_eq!(runtime.add.len(), 1);
        assert_eq!(runtime.remove.len(), 1);
    }

    #[test]
    fn duplicate_add_node_id_fails() {
        let scenario = PmmcReconfigurationSpec {
            add: vec![
                PmmcReconfigurationAddSpec {
                    node_id: "n1".to_string(),
                    roles: roles(true, false, false),
                },
                PmmcReconfigurationAddSpec {
                    node_id: "n1".to_string(),
                    roles: roles(false, true, false),
                },
            ],
            ..PmmcReconfigurationSpec::default()
        };

        let err = scenario
            .to_runtime_patch("s")
            .expect_err("duplicate node IDs must fail");
        assert!(matches!(
            err,
            PmmcReconfigurationSpecError::DuplicateAddNodeId { .. }
        ));
    }

    #[test]
    fn duplicate_remove_node_id_fails() {
        let scenario = PmmcReconfigurationSpec {
            remove: vec!["n1".to_string(), "n1".to_string()],
            ..PmmcReconfigurationSpec::default()
        };

        let err = scenario
            .to_runtime_patch("s")
            .expect_err("duplicate node IDs must fail");
        assert!(matches!(
            err,
            PmmcReconfigurationSpecError::DuplicateRemoveNodeId { .. }
        ));
    }
}
