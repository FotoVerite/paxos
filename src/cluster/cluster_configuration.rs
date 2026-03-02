mod conversions;
mod reconfig_errors;
mod reconfig_patch;

use std::net::IpAddr;

use uuid::Uuid;

use crate::node::config::{ClassicNodeConfig, PmmcNodeConfig, Roles};
use crate::cluster::cluster_configuration::reconfig_errors::ReconfigError;

#[derive(Debug, Clone, Copy)]
pub enum ConfigurationStatus {
    PENDING,
    ACTIVE,
    RETIRED,
    FAILED,
}

#[derive(Debug, Clone, Copy)]
pub enum ConfigurationStrategy {
    JointConsensus,
    StopSign,
    DelayedStopSign,
    Padding,
    BrickWall,
}

#[derive(Debug, Clone)]
pub struct ClusterConfiguration {
    id: u64,
    status: ConfigurationStatus,
    nodes: Vec<(Uuid, Roles)>,
    strategy: ConfigurationStrategy,
}

impl ClusterConfiguration {
    pub fn bootstrap_classic(
        ip: IpAddr,
        configs: Vec<ClassicNodeConfig>,
    ) -> Result<Self, ReconfigError> {
        let nodes = configs
            .into_iter()
            .enumerate()
            .map(|(index, config)| (Self::classic_node_uuid(ip, index), config.roles))
            .collect();

        let config = Self {
            id: 0,
            status: ConfigurationStatus::ACTIVE,
            nodes,
            strategy: ConfigurationStrategy::JointConsensus,
        };

        if !config.nodes.iter().any(|(_, roles)| roles.proposer) {
            return Err(ReconfigError::NoLeaders);
        }
        if !config.nodes.iter().any(|(_, roles)| roles.acceptor) {
            return Err(ReconfigError::NoAcceptors);
        }
        if !config.nodes.iter().any(|(_, roles)| roles.learner) {
            return Err(ReconfigError::NoLearners);
        }

        Ok(config)
    }

    pub fn bootstrap_pmmc(ip: IpAddr, configs: Vec<PmmcNodeConfig>) -> Result<Self, ReconfigError> {
        let nodes = configs
            .into_iter()
            .enumerate()
            .map(|(index, config)| (Self::pmmc_node_uuid(ip, index), config.roles))
            .collect();

        let config = Self {
            id: 0,
            status: ConfigurationStatus::ACTIVE,
            nodes,
            strategy: ConfigurationStrategy::JointConsensus,
        };

        if !config.nodes.iter().any(|(_, roles)| roles.proposer) {
            return Err(ReconfigError::NoLeaders);
        }
        if !config.nodes.iter().any(|(_, roles)| roles.acceptor) {
            return Err(ReconfigError::NoAcceptors);
        }
        if !config.nodes.iter().any(|(_, roles)| roles.learner) {
            return Err(ReconfigError::NoLearners);
        }

        Ok(config)
    }

    fn next_id(&self) -> u64 {
        self.id.clone() + 1
    }

    fn pmmc_node_uuid(ip: IpAddr, node_id: usize) -> Uuid {
        let namespace = Uuid::NAMESPACE_DNS;
        let name = format!("{}:pmmc:{}", ip, node_id);
        Uuid::new_v5(&namespace, name.as_bytes())
    }

    fn classic_node_uuid(ip: IpAddr, node_id: usize) -> Uuid {
        let namespace = Uuid::NAMESPACE_DNS;
        let name = format!("{}:{}", ip, node_id);
        Uuid::new_v5(&namespace, name.as_bytes())
    }

    fn nodes_with(&self, pred: impl Fn(&Roles) -> bool) -> Vec<Uuid> {
        self.nodes
            .iter()
            .filter_map(|(uuid, roles)| pred(roles).then_some(*uuid))
            .collect()
    }
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn members(&self) -> Vec<(Uuid, Roles)> {
        self.nodes.clone()
    }

    pub fn member(&self, index: usize) -> Option<Uuid> {
        self.nodes.get(index).map(|(uuid, _)| uuid.clone())
    }

    pub fn member_uuids(&self) -> Vec<Uuid> {
        self.members().into_iter().map(|(uuid, _)| uuid).collect()
    }

    pub fn node_configs(&self) -> Vec<(Uuid, PmmcNodeConfig)> {
        self.members()
            .into_iter()
            .map(|(uuid, roles)| (uuid, PmmcNodeConfig { roles }))
            .collect()
    }
    pub fn leaders(&self) -> Vec<Uuid> {
        self.nodes_with(|role| role.proposer)
    }

    pub fn acceptors(&self) -> Vec<Uuid> {
        self.nodes_with(|role| role.acceptor)
    }

    pub fn replicas(&self) -> Vec<Uuid> {
        self.nodes_with(|role| role.learner)
    }

    pub fn quorum(&self) -> usize {
        let mut quorum = 0usize;
        for (_, roles) in &self.nodes {
            if roles.acceptor {
                quorum += 1
            }
        }
        (quorum / 2) + 1
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn roles(proposer: bool, acceptor: bool, learner: bool) -> Roles {
        Roles {
            proposer,
            acceptor,
            learner,
        }
    }

    fn config() -> ClusterConfiguration {
        let leader = Uuid::from_u128(1);
        let acceptor = Uuid::from_u128(2);
        let replica = Uuid::from_u128(3);
        let full = Uuid::from_u128(4);
        let nodes = vec![
            (leader, roles(true, false, false)),
            (acceptor, roles(false, true, false)),
            (replica, roles(false, false, true)),
            (full, roles(true, true, true)),
        ];

        ClusterConfiguration {
            id: 0,
            status: ConfigurationStatus::ACTIVE,
            nodes,
            strategy: ConfigurationStrategy::JointConsensus,
        }
    }

    #[test]
    fn leaders_returns_proposer_nodes() {
        let cfg = config();
        let leaders = cfg.leaders();

        assert_eq!(leaders.len(), 2);
        assert!(leaders.contains(&Uuid::from_u128(1)));
        assert!(leaders.contains(&Uuid::from_u128(4)));
    }

    #[test]
    fn acceptors_returns_acceptor_nodes() {
        let cfg = config();
        let acceptors = cfg.acceptors();

        assert_eq!(acceptors.len(), 2);
        assert!(acceptors.contains(&Uuid::from_u128(2)));
        assert!(acceptors.contains(&Uuid::from_u128(4)));
    }

    #[test]
    fn replicas_returns_learner_nodes() {
        let cfg = config();
        let replicas = cfg.replicas();

        assert_eq!(replicas.len(), 2);
        assert!(replicas.contains(&Uuid::from_u128(3)));
        assert!(replicas.contains(&Uuid::from_u128(4)));
    }

    #[test]
    fn bootstrap_pmmc_generates_stable_ordered_members() {
        let ip = IpAddr::V4([127, 0, 0, 1].into());
        let cfg = ClusterConfiguration::bootstrap_pmmc(
            ip,
            vec![
                PmmcNodeConfig {
                    roles: roles(true, false, false),
                },
                PmmcNodeConfig {
                    roles: roles(false, true, false),
                },
                PmmcNodeConfig {
                    roles: roles(false, false, true),
                },
            ],
        )
        .expect("bootstrap config should succeed");

        let members = cfg.members();
        assert_eq!(members.len(), 3);
        assert_eq!(members[0].0, ClusterConfiguration::pmmc_node_uuid(ip, 0));
        assert_eq!(members[1].0, ClusterConfiguration::pmmc_node_uuid(ip, 1));
        assert_eq!(members[2].0, ClusterConfiguration::pmmc_node_uuid(ip, 2));
        assert_eq!(cfg.quorum(), 1);
    }

    #[test]
    fn bootstrap_classic_generates_stable_ordered_members() {
        let ip = IpAddr::V4([127, 0, 0, 2].into());
        let cfg = ClusterConfiguration::bootstrap_classic(
            ip,
            vec![
                ClassicNodeConfig {
                    roles: roles(true, true, false),
                    learning_strategy: crate::node::config::LearningStrategy::default(),
                },
                ClassicNodeConfig {
                    roles: roles(false, true, true),
                    learning_strategy: crate::node::config::LearningStrategy::default(),
                },
                ClassicNodeConfig {
                    roles: roles(true, false, true),
                    learning_strategy: crate::node::config::LearningStrategy::default(),
                },
            ],
        )
        .expect("classic bootstrap config should succeed");

        let members = cfg.members();
        assert_eq!(members.len(), 3);
        assert_eq!(members[0].0, ClusterConfiguration::classic_node_uuid(ip, 0));
        assert_eq!(members[1].0, ClusterConfiguration::classic_node_uuid(ip, 1));
        assert_eq!(members[2].0, ClusterConfiguration::classic_node_uuid(ip, 2));
        assert_eq!(cfg.quorum(), 2);
    }
}
