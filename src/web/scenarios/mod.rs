pub mod asymmetric_proposers;
pub mod catch_up;
pub mod competing_proposers;
pub mod happy_path;
pub mod network_partition;
pub mod partial_roles;
pub mod pmmc;
pub mod simple_happy_path;

pub use asymmetric_proposers::AsymmetricProposersScenario;
pub use catch_up::CatchUpScenario;
pub use competing_proposers::CompetingProposersScenario;
pub use happy_path::HappyPathScenario;
pub use network_partition::NetworkPartitionScenario;
pub use partial_roles::PartialRolesScenario;
pub use simple_happy_path::SimpleHappyPathScenario;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioType {
    HappyPath,
    CompetingProposers,
    AsymmetricProposers,
    NetworkPartition,
    CatchUp,
    PartialRoles,
    SimpleHappyPath,
    PmmcSingleClient,
    PmmcRoleSplit,
    PmmcLeaderCrash,
    PmmcReplicaCrashFailover,
    PmmcLeaderPartitionHeal,
    PmmcAcceptorMajorityLossThenRecover,
    PmmcStaggeredLeaderJoin,
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
            "simple_happy_path" => Self::SimpleHappyPath,
            "pmmc_single_client" => Self::PmmcSingleClient,
            "pmmc_role_split" => Self::PmmcRoleSplit,
            "pmmc_leader_crash" => Self::PmmcLeaderCrash,
            "pmmc_replica_crash_failover" => Self::PmmcReplicaCrashFailover,
            "pmmc_leader_partition_heal" => Self::PmmcLeaderPartitionHeal,
            "pmmc_acceptor_majority_loss_then_recover" => Self::PmmcAcceptorMajorityLossThenRecover,
            "pmmc_staggered_leader_join" => Self::PmmcStaggeredLeaderJoin,
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
            Self::SimpleHappyPath => "simple_happy_path",
            Self::PmmcSingleClient => "pmmc_single_client",
            Self::PmmcRoleSplit => "pmmc_role_split",
            Self::PmmcLeaderCrash => "pmmc_leader_crash",
            Self::PmmcReplicaCrashFailover => "pmmc_replica_crash_failover",
            Self::PmmcLeaderPartitionHeal => "pmmc_leader_partition_heal",
            Self::PmmcAcceptorMajorityLossThenRecover => {
                "pmmc_acceptor_majority_loss_then_recover"
            }
            Self::PmmcStaggeredLeaderJoin => "pmmc_staggered_leader_join",
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

    pub fn initial_client_node_index(self) -> usize {
        match self {
            Self::PmmcLeaderCrash
            | Self::PmmcReplicaCrashFailover
            | Self::PmmcLeaderPartitionHeal
            | Self::PmmcAcceptorMajorityLossThenRecover
            | Self::PmmcStaggeredLeaderJoin => 2,
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
            _ => None,
        }
    }
}
