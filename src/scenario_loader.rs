use crate::scenario::Scenario;
use std::path::Path;

pub struct ScenarioLoader;

impl ScenarioLoader {
    /// Load a scenario from a JSON file
    pub async fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Scenario> {
        let contents = tokio::fs::read_to_string(path).await?;
        let scenario = serde_json::from_str(&contents)?;
        Ok(scenario)
    }

    /// Load all scenarios from a directory
    pub async fn load_all<P: AsRef<Path>>(dir: P) -> anyhow::Result<Vec<(String, Scenario)>> {
        let mut scenarios = Vec::new();
        let mut entries = tokio::fs::read_dir(dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                match Self::load(&path).await {
                    Ok(scenario) => {
                        let name = path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("unknown")
                            .to_string();
                        scenarios.push((name, scenario));
                    }
                    Err(e) => {
                        eprintln!("Failed to load scenario from {:?}: {}", path, e);
                    }
                }
            }
        }

        Ok(scenarios)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_load_scenario() {
        // This test assumes scenarios/partition_recovery.json exists
        if let Ok(scenario) = ScenarioLoader::load("scenarios/partition_recovery.json").await {
            assert_eq!(scenario.name, "Partition Recovery Scenario");
            assert_eq!(scenario.node_count, 5);
        }
    }
}
