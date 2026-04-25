use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cluster::vertical::configuration::VerticalPaxosVariant;
use crate::paxos_command::PaxosCommand;
use crate::scenario::{Scenario, ScenarioBuilder, VerticalClientExpectation};

use super::scenario_key::VerticalScenarioKey;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerticalConfigurationPlan {
    pub key: String,
    pub leader_index: usize,
    pub variant: VerticalPaxosVariant,
    pub ballot_number: usize,
    pub start_index: usize,
    pub acceptors: Vec<usize>,
    pub replicas: Vec<usize>,
    pub read_quorum: Vec<usize>,
    pub write_quorum: Vec<usize>,
    #[serde(default)]
    pub complete_bal_of: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerticalScenarioSpec {
    pub key: String,
    pub name: String,
    pub description: String,
    pub node_count: usize,
    pub configurations: Vec<VerticalConfigurationPlan>,
    pub scenario: Scenario,
}

impl VerticalScenarioSpec {
    pub fn for_key(key: VerticalScenarioKey) -> Self {
        match key {
            VerticalScenarioKey::ActivationOnly => activation_only(),
            VerticalScenarioKey::ReconfigurationOnly => reconfiguration_only(),
            VerticalScenarioKey::RedirectAfterHandoff => redirect_after_handoff(),
            VerticalScenarioKey::Vp1RecoveryCost => vp1_recovery_cost(),
            VerticalScenarioKey::Vp2BoundedRecovery => vp2_bounded_recovery(),
        }
    }

    pub fn by_key(&self) -> BTreeMap<String, VerticalConfigurationPlan> {
        self.configurations
            .iter()
            .cloned()
            .map(|plan| (plan.key.clone(), plan))
            .collect()
    }
}

fn command(key: &str, value: usize, request_id: u64) -> PaxosCommand {
    PaxosCommand::PUT {
        key: key.to_string(),
        version: 0,
        value,
    }
    .with_client(uuid::Uuid::new_v4(), request_id)
}

fn config_plan(
    key: &str,
    leader_index: usize,
    variant: VerticalPaxosVariant,
    ballot_number: usize,
    start_index: usize,
    acceptors: Vec<usize>,
    replicas: Vec<usize>,
    read_quorum: Vec<usize>,
    write_quorum: Vec<usize>,
    complete_bal_of: Option<&str>,
) -> VerticalConfigurationPlan {
    VerticalConfigurationPlan {
        key: key.to_string(),
        leader_index,
        variant,
        ballot_number,
        start_index,
        acceptors,
        replicas,
        read_quorum,
        write_quorum,
        complete_bal_of: complete_bal_of.map(str::to_string),
    }
}

fn activation_only() -> VerticalScenarioSpec {
    let scenario = ScenarioBuilder::new("vertical_activation_only", 4)
        .description("Install a single configuration and wait for it to become active.")
        .phase("activation")
        .vertical_install_configuration("A")
        .wait(Duration::from_millis(800))
        .vertical_start_activation("A", std::iter::empty::<&str>())
        .vertical_wait_activation_ready("A", Duration::from_secs(3))
        .wait(Duration::from_millis(1_200))
        .build();

    VerticalScenarioSpec {
        key: VerticalScenarioKey::ActivationOnly.as_str().to_string(),
        name: "Activation Only".to_string(),
        description:
            "Shows that a configuration being installed is not the same thing as it being active."
                .to_string(),
        node_count: 4,
        configurations: vec![config_plan(
            "A",
            0,
            VerticalPaxosVariant::V1,
            1,
            0,
            vec![0, 1, 2],
            vec![0, 3],
            vec![0, 1],
            vec![0, 2],
            None,
        )],
        scenario,
    }
}

fn reconfiguration_only() -> VerticalScenarioSpec {
    let scenario = ScenarioBuilder::new("vertical_reconfiguration_only", 4)
        .description("Show one active configuration handing off cleanly to a successor.")
        .phase("configuration_a")
        .vertical_install_configuration("A")
        .wait(Duration::from_millis(700))
        .vertical_start_activation("A", std::iter::empty::<&str>())
        .vertical_wait_activation_ready("A", Duration::from_secs(3))
        .vertical_submit_client_request(
            3,
            command("olive", 1, 1),
            Some(VerticalClientExpectation::Accepted),
        )
        .wait(Duration::from_millis(1_100))
        .phase("configuration_b")
        .vertical_install_configuration("B")
        .wait(Duration::from_millis(800))
        .vertical_start_activation("B", ["A"])
        .vertical_wait_activation_ready("B", Duration::from_secs(3))
        .wait(Duration::from_millis(1_200))
        .build();

    VerticalScenarioSpec {
        key: VerticalScenarioKey::ReconfigurationOnly
            .as_str()
            .to_string(),
        name: "Reconfiguration Only".to_string(),
        description: "Shows the successor leader taking over after a clean activation handoff."
            .to_string(),
        node_count: 4,
        configurations: base_reconfiguration_plans(VerticalPaxosVariant::V2, 1, Some("A")),
        scenario,
    }
}

fn redirect_after_handoff() -> VerticalScenarioSpec {
    let scenario = ScenarioBuilder::new("vertical_redirect_after_handoff", 4)
        .description(
            "Show a stale replica rejecting new work and redirecting the client to the active set.",
        )
        .phase("bring_up_a")
        .vertical_install_configuration("A")
        .wait(Duration::from_millis(700))
        .vertical_start_activation("A", std::iter::empty::<&str>())
        .vertical_wait_activation_ready("A", Duration::from_secs(3))
        .vertical_submit_client_request(
            3,
            command("olive", 1, 1),
            Some(VerticalClientExpectation::Accepted),
        )
        .wait(Duration::from_millis(1_000))
        .phase("handoff_to_b")
        .vertical_install_configuration("B")
        .wait(Duration::from_millis(850))
        .vertical_start_activation("B", ["A"])
        .vertical_wait_activation_ready("B", Duration::from_secs(3))
        .wait(Duration::from_millis(700))
        .vertical_submit_client_request(
            0,
            command("citadel", 2, 2),
            Some(VerticalClientExpectation::Redirect),
        )
        .wait(Duration::from_millis(950))
        .vertical_submit_client_request(
            2,
            command("citadel", 2, 3),
            Some(VerticalClientExpectation::Accepted),
        )
        .wait(Duration::from_millis(1_200))
        .build();

    VerticalScenarioSpec {
        key: VerticalScenarioKey::RedirectAfterHandoff
            .as_str()
            .to_string(),
        name: "Redirect After Handoff".to_string(),
        description:
            "Shows the old configuration refusing new work after the successor becomes active."
                .to_string(),
        node_count: 4,
        configurations: base_reconfiguration_plans(VerticalPaxosVariant::V2, 1, Some("A")),
        scenario,
    }
}

fn vp1_recovery_cost() -> VerticalScenarioSpec {
    let scenario = ScenarioBuilder::new("vertical_vp1_recovery_cost", 4)
        .description(
            "Build some history, then show a VP1 successor recovering from the full prefix again.",
        )
        .phase("history_on_a")
        .vertical_install_configuration("A")
        .wait(Duration::from_millis(700))
        .vertical_start_activation("A", std::iter::empty::<&str>())
        .vertical_wait_activation_ready("A", Duration::from_secs(3))
        .vertical_submit_client_request(
            3,
            command("olive", 1, 1),
            Some(VerticalClientExpectation::Accepted),
        )
        .wait(Duration::from_millis(900))
        .vertical_submit_client_request(
            0,
            command("olive", 2, 2),
            Some(VerticalClientExpectation::Accepted),
        )
        .wait(Duration::from_millis(1_000))
        .phase("vp1_successor")
        .vertical_install_configuration("B")
        .wait(Duration::from_millis(900))
        .vertical_start_activation("B", ["A"])
        .vertical_wait_activation_ready("B", Duration::from_secs(4))
        .wait(Duration::from_millis(1_300))
        .build();

    VerticalScenarioSpec {
        key: VerticalScenarioKey::Vp1RecoveryCost.as_str().to_string(),
        name: "VP1 Recovery Cost".to_string(),
        description: "The successor configuration still has to reason from the full prefix, which is the part VP1 makes painful.".to_string(),
        node_count: 4,
        configurations: base_reconfiguration_plans(VerticalPaxosVariant::V1, 2, None),
        scenario,
    }
}

fn vp2_bounded_recovery() -> VerticalScenarioSpec {
    let scenario = ScenarioBuilder::new("vertical_vp2_bounded_recovery", 4)
        .description(
            "Build the same history, then show a VP2 successor with a bounded recovery window.",
        )
        .phase("history_on_a")
        .vertical_install_configuration("A")
        .wait(Duration::from_millis(700))
        .vertical_start_activation("A", std::iter::empty::<&str>())
        .vertical_wait_activation_ready("A", Duration::from_secs(3))
        .vertical_submit_client_request(
            3,
            command("olive", 1, 1),
            Some(VerticalClientExpectation::Accepted),
        )
        .wait(Duration::from_millis(900))
        .vertical_submit_client_request(
            0,
            command("olive", 2, 2),
            Some(VerticalClientExpectation::Accepted),
        )
        .wait(Duration::from_millis(1_000))
        .phase("vp2_successor")
        .vertical_install_configuration("B")
        .wait(Duration::from_millis(900))
        .vertical_start_activation("B", ["A"])
        .vertical_wait_activation_ready("B", Duration::from_secs(4))
        .wait(Duration::from_millis(1_300))
        .build();

    VerticalScenarioSpec {
        key: VerticalScenarioKey::Vp2BoundedRecovery.as_str().to_string(),
        name: "VP2 Bounded Recovery".to_string(),
        description: "The successor configuration can bound recovery work instead of reasoning from slot zero again.".to_string(),
        node_count: 4,
        configurations: base_reconfiguration_plans(VerticalPaxosVariant::V2, 2, Some("A")),
        scenario,
    }
}

fn base_reconfiguration_plans(
    successor_variant: VerticalPaxosVariant,
    successor_start_index: usize,
    successor_complete_bal_of: Option<&str>,
) -> Vec<VerticalConfigurationPlan> {
    vec![
        config_plan(
            "A",
            0,
            VerticalPaxosVariant::V1,
            1,
            0,
            vec![0, 1, 2],
            vec![0, 3],
            vec![0, 1],
            vec![0, 2],
            None,
        ),
        config_plan(
            "B",
            3,
            successor_variant,
            2,
            successor_start_index,
            vec![1, 2, 3],
            vec![2, 3],
            vec![1, 3],
            vec![2, 3],
            successor_complete_bal_of,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_all_vertical_scenarios() {
        for key in [
            VerticalScenarioKey::ActivationOnly,
            VerticalScenarioKey::ReconfigurationOnly,
            VerticalScenarioKey::RedirectAfterHandoff,
            VerticalScenarioKey::Vp1RecoveryCost,
            VerticalScenarioKey::Vp2BoundedRecovery,
        ] {
            let spec = VerticalScenarioSpec::for_key(key);
            assert!(!spec.scenario.phases.is_empty());
            assert!(!spec.configurations.is_empty());
        }
    }
}
