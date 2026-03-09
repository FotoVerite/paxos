use std::path::{Path, PathBuf};

use crate::web::scenarios::ScenarioType;

use super::spec::PmmcScenarioSpec;

pub struct PmmcScenarioLoader;

impl PmmcScenarioLoader {
    pub async fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<PmmcScenarioSpec> {
        let contents = tokio::fs::read_to_string(path).await?;
        let spec: PmmcScenarioSpec = serde_json::from_str(&contents)?;
        spec.validate()?;
        Ok(spec)
    }

    pub async fn load_for(scenario_type: ScenarioType) -> anyhow::Result<PmmcScenarioSpec> {
        let path = Self::path_for(scenario_type).ok_or_else(|| {
            anyhow::anyhow!(
                "scenario '{}' is not a PMMC scenario",
                scenario_type.as_str()
            )
        })?;
        Self::load(path).await
    }

    pub fn path_for(scenario_type: ScenarioType) -> Option<PathBuf> {
        let file_name = match scenario_type {
            ScenarioType::PmmcSingleClient => "pmmc_single_client.json",
            ScenarioType::PmmcRoleSplit => "pmmc_role_split.json",
            ScenarioType::PmmcLeaderCrash => "pmmc_leader_crash.json",
            ScenarioType::PmmcReplicaCrashFailover => "pmmc_replica_crash_failover.json",
            ScenarioType::PmmcLeaderPartitionHeal => "pmmc_leader_partition_heal.json",
            ScenarioType::PmmcAcceptorMajorityLossThenRecover => {
                "pmmc_acceptor_majority_loss_then_recover.json"
            }
            ScenarioType::PmmcStaggeredLeaderJoin => "pmmc_staggered_leader_join.json",
            _ => return None,
        };

        Some(PathBuf::from("scenarios").join("pmmc").join(file_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn loads_single_client_spec() {
        let spec = PmmcScenarioLoader::load_for(ScenarioType::PmmcSingleClient)
            .await
            .unwrap();

        assert_eq!(spec.name, "pmmc_single_client");
        assert_eq!(spec.topology.node_count, 5);
        assert_eq!(spec.clients.len(), 1);
    }

    #[tokio::test]
    async fn loads_all_built_in_pmmc_specs() {
        let scenarios = [
            ScenarioType::PmmcSingleClient,
            ScenarioType::PmmcRoleSplit,
            ScenarioType::PmmcLeaderCrash,
            ScenarioType::PmmcReplicaCrashFailover,
            ScenarioType::PmmcLeaderPartitionHeal,
            ScenarioType::PmmcAcceptorMajorityLossThenRecover,
            ScenarioType::PmmcStaggeredLeaderJoin,
        ];

        for scenario_type in scenarios {
            let spec = PmmcScenarioLoader::load_for(scenario_type).await.unwrap();
            assert_eq!(spec.name, scenario_type.as_str());
            assert!(spec.completion.target_successes() > 0);
            assert!(!spec.clients.is_empty());
        }
    }
}
