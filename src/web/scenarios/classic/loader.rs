use std::path::{Path, PathBuf};

use crate::web::scenarios::ScenarioType;

use super::spec::ClassicScenarioSpec;

pub struct ClassicScenarioLoader;

impl ClassicScenarioLoader {
    pub async fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<ClassicScenarioSpec> {
        let contents = tokio::fs::read_to_string(path).await?;
        let spec: ClassicScenarioSpec = serde_json::from_str(&contents)?;
        spec.validate()?;
        Ok(spec)
    }

    pub async fn load_for(scenario_type: ScenarioType) -> anyhow::Result<ClassicScenarioSpec> {
        let path = Self::path_for(scenario_type).ok_or_else(|| {
            anyhow::anyhow!(
                "scenario '{}' is not a generic classic scenario",
                scenario_type.as_str()
            )
        })?;
        Self::load(path).await
    }

    pub fn path_for(scenario_type: ScenarioType) -> Option<PathBuf> {
        let file_name = match scenario_type {
            ScenarioType::HappyPath => "happy_path.json",
            ScenarioType::CompetingProposers => "competing_proposers.json",
            ScenarioType::AsymmetricProposers => "asymmetric_proposers.json",
            ScenarioType::HappyPathWithLeader => "happy_path_with_leader.json",
            ScenarioType::PartialRoles => "partial_roles.json",
            ScenarioType::NetworkPartition => "network_partition.json",
            _ => return None,
        };

        Some(PathBuf::from("scenarios").join("classic").join(file_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn loads_generic_classic_specs() {
        let scenarios = [
            ScenarioType::HappyPath,
            ScenarioType::CompetingProposers,
            ScenarioType::AsymmetricProposers,
            ScenarioType::HappyPathWithLeader,
            ScenarioType::PartialRoles,
            ScenarioType::NetworkPartition,
        ];

        for scenario_type in scenarios {
            let spec = ClassicScenarioLoader::load_for(scenario_type)
                .await
                .unwrap();
            assert_eq!(spec.name, scenario_type.as_str());
            assert!(!spec.proposals.is_empty());
            assert!(spec.proposal_interval > 0);
        }
    }
}
