use crate::paxos_command::PaxosCommand;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ScenarioStep {
    #[serde(rename = "propose")]
    Propose {
        command: PaxosCommand,
    },
    #[serde(rename = "wait")]
    Wait {
        #[serde(with = "duration_serde")]
        duration: Duration,
    },
    #[serde(rename = "partition")]
    Partition {
        node1: usize,
        node2: usize,
    },
    #[serde(rename = "heal_partition")]
    HealPartition {
        node1: usize,
        node2: usize,
    },
    #[serde(rename = "add_delay")]
    AddDelay {
        from: usize,
        to: usize,
        #[serde(with = "duration_serde")]
        delay: Duration,
    },
    #[serde(rename = "add_packet_loss")]
    AddPacketLoss {
        from: usize,
        to: usize,
        drop_rate: f32,
    },
    #[serde(rename = "clear_failures")]
    ClearFailures {
        node: usize,
    },
    #[serde(rename = "enable_failures")]
    EnableFailures,
    #[serde(rename = "disable_failures")]
    DisableFailures,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioPhase {
    pub name: String,
    pub steps: Vec<ScenarioStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub description: String,
    pub node_count: usize,
    pub phases: Vec<ScenarioPhase>,
}

pub struct ScenarioBuilder {
    name: String,
    description: String,
    node_count: usize,
    phases: Vec<ScenarioPhase>,
    current_phase: Option<ScenarioPhase>,
}

impl ScenarioBuilder {
    pub fn new(name: &str, node_count: usize) -> Self {
        Self {
            name: name.to_string(),
            description: String::new(),
            node_count,
            phases: Vec::new(),
            current_phase: None,
        }
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    pub fn phase(mut self, name: &str) -> Self {
        if let Some(phase) = self.current_phase.take() {
            self.phases.push(phase);
        }
        self.current_phase = Some(ScenarioPhase {
            name: name.to_string(),
            steps: Vec::new(),
        });
        self
    }

    pub fn propose(mut self, command: PaxosCommand) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase.steps.push(ScenarioStep::Propose { command });
        self
    }

    pub fn wait(mut self, duration: Duration) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase.steps.push(ScenarioStep::Wait { duration });
        self
    }

    pub fn partition(mut self, node1: usize, node2: usize) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase.steps.push(ScenarioStep::Partition { node1, node2 });
        self
    }

    pub fn heal_partition(mut self, node1: usize, node2: usize) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase.steps.push(ScenarioStep::HealPartition { node1, node2 });
        self
    }

    pub fn add_delay(mut self, from: usize, to: usize, delay: Duration) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase.steps.push(ScenarioStep::AddDelay { from, to, delay });
        self
    }

    pub fn add_packet_loss(mut self, from: usize, to: usize, drop_rate: f32) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase
            .steps
            .push(ScenarioStep::AddPacketLoss { from, to, drop_rate });
        self
    }

    pub fn clear_failures(mut self, node: usize) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase.steps.push(ScenarioStep::ClearFailures { node });
        self
    }

    pub fn enable_failures(mut self) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase.steps.push(ScenarioStep::EnableFailures);
        self
    }

    pub fn disable_failures(mut self) -> Self {
        let phase = self
            .current_phase
            .get_or_insert_with(|| ScenarioPhase {
                name: "default".to_string(),
                steps: Vec::new(),
            });
        phase.steps.push(ScenarioStep::DisableFailures);
        self
    }

    pub fn build(mut self) -> Scenario {
        if let Some(phase) = self.current_phase.take() {
            self.phases.push(phase);
        }
        Scenario {
            name: self.name,
            description: self.description,
            node_count: self.node_count,
            phases: self.phases,
        }
    }
}

// Helper module for Duration serialization
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scenario_builder() {
        let scenario = ScenarioBuilder::new("test", 3)
            .description("A test scenario")
            .phase("normal")
            .propose(PaxosCommand::NOOP)
            .wait(Duration::from_millis(100))
            .enable_failures()
            .phase("with_partition")
            .partition(0, 1)
            .wait(Duration::from_millis(200))
            .heal_partition(0, 1)
            .build();

        assert_eq!(scenario.name, "test");
        assert_eq!(scenario.node_count, 3);
        assert_eq!(scenario.phases.len(), 2);
        assert_eq!(scenario.phases[0].name, "normal");
        assert_eq!(scenario.phases[1].name, "with_partition");
    }

    #[test]
    fn test_scenario_serialization() {
        let scenario = ScenarioBuilder::new("serialize_test", 5)
            .description("Test JSON serialization")
            .propose(PaxosCommand::NOOP)
            .wait(Duration::from_millis(100))
            .enable_failures()
            .partition(0, 1)
            .build();

        let json = serde_json::to_string_pretty(&scenario).unwrap();
        let deserialized: Scenario = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, scenario.name);
        assert_eq!(deserialized.node_count, scenario.node_count);
    }
}
