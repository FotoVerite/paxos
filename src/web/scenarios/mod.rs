pub mod catch_up;
pub mod classic;
pub mod pmmc;

use std::sync::Arc;

use crate::monitor::{Event, PaxosObserver, current_timestamp_millis};
use crate::node::config::{ClassicNodeConfig, LearningStrategy, Roles};
pub use catch_up::CatchUpScenario;
pub use classic::{ClassicScenarioExecution, ClassicScenarioLoader};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioType {
    HappyPath,
    CompetingProposers,
    AsymmetricProposers,
    NetworkPartition,
    CatchUp,
    PartialRoles,
    HappyPathWithLeader,
    PmmcSingleClient,
    PmmcRoleSplit,
    PmmcLeaderCrash,
    PmmcReplicaCrashFailover,
    PmmcLeaderPartitionHeal,
    PmmcAcceptorMajorityLossThenRecover,
    PmmcStaggeredLeaderJoin,
    PmmcReconfigJointConsensus,
    PmmcReconfigStopSign,
    PmmcReconfigDelayedStopSign,
    PmmcReconfigPadding,
    PmmcReconfigBrickWall,
}

impl ScenarioType {
    pub fn parse(value: &str) -> Self {
        match value {
            "" | "happy_path" => Self::HappyPath,
            "competing_proposers" => Self::CompetingProposers,
            "asymmetric_proposers" => Self::AsymmetricProposers,
            "network_partition" => Self::NetworkPartition,
            "catch_up" => Self::CatchUp,
            "partial_roles" => Self::PartialRoles,
            "happy_path_with_leader" => Self::HappyPathWithLeader,
            "pmmc_single_client" => Self::PmmcSingleClient,
            "pmmc_role_split" => Self::PmmcRoleSplit,
            "pmmc_leader_crash" => Self::PmmcLeaderCrash,
            "pmmc_replica_crash_failover" => Self::PmmcReplicaCrashFailover,
            "pmmc_leader_partition_heal" => Self::PmmcLeaderPartitionHeal,
            "pmmc_acceptor_majority_loss_then_recover" => Self::PmmcAcceptorMajorityLossThenRecover,
            "pmmc_staggered_leader_join" => Self::PmmcStaggeredLeaderJoin,
            "pmmc_reconfig_joint_consensus" => Self::PmmcReconfigJointConsensus,
            "pmmc_reconfig_stop_sign" => Self::PmmcReconfigStopSign,
            "pmmc_reconfig_delayed_stop_sign" => Self::PmmcReconfigDelayedStopSign,
            "pmmc_reconfig_padding" => Self::PmmcReconfigPadding,
            "pmmc_reconfig_brick_wall" => Self::PmmcReconfigBrickWall,
            _ => Self::HappyPath,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::HappyPath => "happy_path",
            Self::CompetingProposers => "competing_proposers",
            Self::AsymmetricProposers => "asymmetric_proposers",
            Self::NetworkPartition => "network_partition",
            Self::CatchUp => "catch_up",
            Self::PartialRoles => "partial_roles",
            Self::HappyPathWithLeader => "happy_path_with_leader",
            Self::PmmcSingleClient => "pmmc_single_client",
            Self::PmmcRoleSplit => "pmmc_role_split",
            Self::PmmcLeaderCrash => "pmmc_leader_crash",
            Self::PmmcReplicaCrashFailover => "pmmc_replica_crash_failover",
            Self::PmmcLeaderPartitionHeal => "pmmc_leader_partition_heal",
            Self::PmmcAcceptorMajorityLossThenRecover => "pmmc_acceptor_majority_loss_then_recover",
            Self::PmmcStaggeredLeaderJoin => "pmmc_staggered_leader_join",
            Self::PmmcReconfigJointConsensus => "pmmc_reconfig_joint_consensus",
            Self::PmmcReconfigStopSign => "pmmc_reconfig_stop_sign",
            Self::PmmcReconfigDelayedStopSign => "pmmc_reconfig_delayed_stop_sign",
            Self::PmmcReconfigPadding => "pmmc_reconfig_padding",
            Self::PmmcReconfigBrickWall => "pmmc_reconfig_brick_wall",
        }
    }

    pub fn is_pmmc(self) -> bool {
        matches!(
            self,
            Self::PmmcSingleClient
                | Self::PmmcRoleSplit
                | Self::PmmcLeaderCrash
                | Self::PmmcReplicaCrashFailover
                | Self::PmmcLeaderPartitionHeal
                | Self::PmmcAcceptorMajorityLossThenRecover
                | Self::PmmcStaggeredLeaderJoin
                | Self::PmmcReconfigJointConsensus
                | Self::PmmcReconfigStopSign
                | Self::PmmcReconfigDelayedStopSign
                | Self::PmmcReconfigPadding
                | Self::PmmcReconfigBrickWall
        )
    }

    pub fn uses_role_split_topology(self) -> bool {
        matches!(
            self,
            Self::PmmcRoleSplit
                | Self::PmmcLeaderCrash
                | Self::PmmcReplicaCrashFailover
                | Self::PmmcLeaderPartitionHeal
                | Self::PmmcAcceptorMajorityLossThenRecover
                | Self::PmmcStaggeredLeaderJoin
        )
    }

    pub fn pmmc_topology_summary(self, node_count: usize) -> &'static str {
        match self {
            Self::PmmcRoleSplit
            | Self::PmmcLeaderCrash
            | Self::PmmcReplicaCrashFailover
            | Self::PmmcLeaderPartitionHeal
            | Self::PmmcAcceptorMajorityLossThenRecover
            | Self::PmmcStaggeredLeaderJoin => "2 leaders, 2 replicas, 3 acceptors",
            Self::PmmcReconfigJointConsensus
            | Self::PmmcReconfigStopSign
            | Self::PmmcReconfigDelayedStopSign
            | Self::PmmcReconfigPadding
            | Self::PmmcReconfigBrickWall => "1 leader, 1 replica, 3 acceptors",
            _ if node_count > 0 => "all nodes have leader+acceptor+replica roles",
            _ => "unknown",
        }
    }

    pub fn initial_client_node_index(self) -> usize {
        match self {
            Self::PmmcLeaderCrash
            | Self::PmmcReplicaCrashFailover
            | Self::PmmcLeaderPartitionHeal
            | Self::PmmcAcceptorMajorityLossThenRecover
            | Self::PmmcStaggeredLeaderJoin => 2,
            Self::PmmcReconfigJointConsensus
            | Self::PmmcReconfigStopSign
            | Self::PmmcReconfigDelayedStopSign
            | Self::PmmcReconfigPadding
            | Self::PmmcReconfigBrickWall => 4,
            _ => 0,
        }
    }

    pub fn pmmc_target_limit(self) -> Option<usize> {
        match self {
            Self::PmmcSingleClient => Some(5),
            Self::PmmcRoleSplit => Some(5),
            Self::PmmcLeaderCrash => Some(5),
            Self::PmmcReplicaCrashFailover => Some(5),
            Self::PmmcLeaderPartitionHeal => Some(5),
            Self::PmmcAcceptorMajorityLossThenRecover => Some(7),
            Self::PmmcStaggeredLeaderJoin => Some(5),
            Self::PmmcReconfigJointConsensus => Some(5),
            Self::PmmcReconfigStopSign => Some(5),
            Self::PmmcReconfigDelayedStopSign => Some(5),
            Self::PmmcReconfigPadding => Some(5),
            Self::PmmcReconfigBrickWall => Some(5),
            _ => None,
        }
    }

    pub fn classic_node_count(self, requested_node_count: usize) -> usize {
        match self {
            Self::AsymmetricProposers => 6,
            Self::PartialRoles => 9,
            Self::HappyPathWithLeader => 5,
            _ => requested_node_count,
        }
    }

    pub fn classic_topology_summary(self) -> &'static str {
        match self {
            Self::AsymmetricProposers => "2 proposer+learner nodes, 4 acceptor-only nodes",
            Self::PartialRoles => "2 proposers, 3 acceptors, 2 learners, 2 full nodes",
            Self::HappyPathWithLeader => "5 full-role nodes with designated leader event",
            _ => "classic Paxos roles derived from node config",
        }
    }

    pub fn classic_configs(
        self,
        requested_node_count: usize,
        learning_strategy: LearningStrategy,
    ) -> Vec<ClassicNodeConfig> {
        let full_node = || ClassicNodeConfig {
            roles: Roles::default(),
            learning_strategy: learning_strategy.clone(),
        };
        let make_node = |roles: Roles| ClassicNodeConfig {
            roles,
            learning_strategy: learning_strategy.clone(),
        };

        match self {
            Self::AsymmetricProposers => {
                let mut configs = Vec::with_capacity(6);
                for _ in 0..2 {
                    configs.push(make_node(Roles {
                        proposer: true,
                        acceptor: false,
                        learner: true,
                    }));
                }
                for _ in 0..4 {
                    configs.push(make_node(Roles {
                        proposer: false,
                        acceptor: true,
                        learner: false,
                    }));
                }
                configs
            }
            Self::PartialRoles => {
                let mut configs = Vec::with_capacity(9);
                for _ in 0..2 {
                    configs.push(make_node(Roles {
                        proposer: true,
                        acceptor: false,
                        learner: false,
                    }));
                }
                for _ in 0..3 {
                    configs.push(make_node(Roles {
                        proposer: false,
                        acceptor: true,
                        learner: false,
                    }));
                }
                for _ in 0..2 {
                    configs.push(make_node(Roles {
                        proposer: false,
                        acceptor: false,
                        learner: true,
                    }));
                }
                for _ in 0..2 {
                    configs.push(full_node());
                }
                configs
            }
            Self::HappyPathWithLeader => (0..5).map(|_| full_node()).collect(),
            _ => (0..requested_node_count).map(|_| full_node()).collect(),
        }
    }

    pub fn emit_classic_startup_events(
        self,
        observer: Arc<dyn PaxosObserver>,
        node_uuids: &[Uuid],
        leader_node: Option<usize>,
    ) {
        if self != Self::HappyPathWithLeader || node_uuids.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::ScenarioType;

    #[test]
    fn parses_reconfiguration_scenario_types() {
        let cases = [
            (
                "pmmc_reconfig_joint_consensus",
                ScenarioType::PmmcReconfigJointConsensus,
            ),
            ("pmmc_reconfig_stop_sign", ScenarioType::PmmcReconfigStopSign),
            (
                "pmmc_reconfig_delayed_stop_sign",
                ScenarioType::PmmcReconfigDelayedStopSign,
            ),
            ("pmmc_reconfig_padding", ScenarioType::PmmcReconfigPadding),
            (
                "pmmc_reconfig_brick_wall",
                ScenarioType::PmmcReconfigBrickWall,
            ),
        ];

        for (raw, expected) in cases {
            let parsed = ScenarioType::parse(raw);
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_str(), raw);
            assert!(parsed.is_pmmc());
        }
    }
}
