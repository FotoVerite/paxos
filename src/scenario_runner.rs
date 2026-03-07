use crate::cluster::classic_cluster::ClassicCluster;
use crate::scenario::{Scenario, ScenarioStep};
use tokio::time::sleep;

pub struct ScenarioRunner;

impl ScenarioRunner {
    /// Execute a scenario against a cluster.
    /// No assertions - just runs the steps and logs what happens.
    pub async fn run(cluster: &mut ClassicCluster, scenario: &Scenario) -> anyhow::Result<()> {
        println!("\n=== Running Scenario: {} ===", scenario.name);
        println!("Description: {}", scenario.description);
        println!("Nodes: {}\n", scenario.node_count);

        for phase in &scenario.phases {
            println!("--- Phase: {} ---", phase.name);
            for step in &phase.steps {
                Self::execute_step(cluster, step).await?;
            }
            println!();
        }

        println!("=== Scenario Complete: {} ===\n", scenario.name);
        Ok(())
    }

    async fn execute_step(cluster: &mut ClassicCluster, step: &ScenarioStep) -> anyhow::Result<()> {
        match step {
            ScenarioStep::Propose { command } => {
                println!("  [PROPOSE] {}", command);
                cluster.propose(command.clone()).await;
            }
            ScenarioStep::Wait { duration } => {
                println!("  [WAIT] {:?}", duration);
                sleep(*duration).await;
            }
            ScenarioStep::Partition { node1, node2 } => {
                println!("  [PARTITION] {} <-> {}", node1, node2);
                cluster
                    .partition(cluster.uuid(*node1), cluster.uuid(*node2))
                    .await;
            }
            ScenarioStep::HealPartition { node1, node2 } => {
                println!("  [HEAL] {} <-> {}", node1, node2);
                cluster
                    .heal_partition(cluster.uuid(*node1), cluster.uuid(*node2))
                    .await;
            }
            ScenarioStep::AddDelay { from, to, delay } => {
                println!("  [DELAY] {} -> {} ({:?})", from, to, delay);
                cluster
                    .add_delay(cluster.uuid(*from), cluster.uuid(*to), *delay)
                    .await;
            }
            ScenarioStep::AddPacketLoss {
                from,
                to,
                drop_rate,
            } => {
                println!(
                    "  [PACKET_LOSS] {} -> {} ({}%)",
                    from,
                    to,
                    drop_rate * 100.0
                );
                cluster
                    .add_packet_loss(cluster.uuid(*from), cluster.uuid(*to), *drop_rate)
                    .await;
            }
            ScenarioStep::ClearFailures { node } => {
                println!("  [CLEAR_FAILURES] node {}", node);
                // Would need cluster method for this
            }
            ScenarioStep::EnableFailures => {
                println!("  [ENABLE_FAILURES]");
                cluster.enable_failures().await;
            }
            ScenarioStep::DisableFailures => {
                println!("  [DISABLE_FAILURES]");
                cluster.disable_failures().await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::console_observer::ConsoleObserver;
    use crate::paxos_command::PaxosCommand;
    use crate::scenario::ScenarioBuilder;
    use std::net::IpAddr;
    use std::sync::Arc;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_scenario_runner_basic() {
        let observer = Arc::new(ConsoleObserver);
        let ip = IpAddr::V4([127, 0, 0, 1].into());
        let mut cluster = ClassicCluster::new(0, ip, 3, observer).await.unwrap();

        for i in 0..3 {
            cluster.nodes[i].start();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let scenario = ScenarioBuilder::new("test_runner", 3)
            .description("Test the scenario runner")
            .propose(PaxosCommand::NOOP)
            .wait(Duration::from_millis(100))
            .enable_failures()
            .partition(0, 1)
            .wait(Duration::from_millis(100))
            .heal_partition(0, 1)
            .build();

        ScenarioRunner::run(&mut cluster, &scenario).await.unwrap();
    }
}
