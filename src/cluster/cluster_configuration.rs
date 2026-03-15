mod conversions;
pub mod reconciler;
pub mod reconfig_patch;
mod types;

use std::collections::HashMap;
use std::net::IpAddr;

use uuid::Uuid;

use crate::cluster::cluster_configuration::types::ReconfigError;
use crate::node::config::{ClassicNodeConfig, PmmcNodeConfig, Roles};
use crate::rsm::checkpoint::RsmCheckpoint;
use crate::rsm::entry::KVEntry;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConfigurationStatus {
    PENDING,
    ACTIVE,
    RETIRED,
    FAILED,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConfigurationStrategy {
    JointConsensus,
    StopSign,
    DelayedStopSign,
    Padding,
    BrickWall,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterConfiguration {
    id: u64,
    status: ConfigurationStatus,
    nodes: Vec<(Uuid, Roles)>,
    strategy: ConfigurationStrategy,
    epoch: usize,
    alpha_max_inflight: usize,
    checkpoint: Option<RsmCheckpoint>,
}

impl ClusterConfiguration {
    fn validate_roles(nodes: &[(Uuid, Roles)]) -> Result<(), ReconfigError> {
        if !nodes.iter().any(|(_, roles)| roles.proposer) {
            return Err(ReconfigError::NoLeaders);
        }
        if !nodes.iter().any(|(_, roles)| roles.acceptor) {
            return Err(ReconfigError::NoAcceptors);
        }
        if !nodes.iter().any(|(_, roles)| roles.learner) {
            return Err(ReconfigError::NoLearners);
        }

        Ok(())
    }

    fn bootstrap_with_roles(
        nodes: Vec<(Uuid, Roles)>,
        strategy: ConfigurationStrategy,
        alpha_max_inflight: Option<usize>,
    ) -> Result<Self, ReconfigError> {
        Self::validate_roles(&nodes)?;
        let alpha_max_inflight = match alpha_max_inflight {
            Some(val) => val,
            None => 0,
        };
        Ok(Self {
            id: 0,
            epoch: 1,
            status: ConfigurationStatus::ACTIVE,
            nodes,
            strategy,
            alpha_max_inflight,
            checkpoint: None,
        })
    }

    pub fn bootstrap_classic(
        ip: IpAddr,
        configs: Vec<ClassicNodeConfig>,
    ) -> Result<Self, ReconfigError> {
        let nodes = configs
            .into_iter()
            .enumerate()
            .map(|(index, config)| (Self::classic_node_uuid(ip, index), config.roles))
            .collect();
        Self::bootstrap_with_roles(nodes, ConfigurationStrategy::JointConsensus, Some(5))
    }

    pub fn bootstrap_pmmc(ip: IpAddr, configs: Vec<PmmcNodeConfig>) -> Result<Self, ReconfigError> {
        Self::bootstrap_pmmc_with_params(
            ip,
            configs,
            ConfigurationStrategy::JointConsensus,
            Some(5),
        )
    }

    pub fn bootstrap_pmmc_with_params(
        ip: IpAddr,
        configs: Vec<PmmcNodeConfig>,
        strategy: ConfigurationStrategy,
        alpha_max_inflight: Option<usize>,
    ) -> Result<Self, ReconfigError> {
        let nodes = configs
            .into_iter()
            .enumerate()
            .map(|(index, config)| (Self::pmmc_node_uuid(ip, index), config.roles))
            .collect();
        Self::bootstrap_with_roles(nodes, strategy, alpha_max_inflight)
    }

    fn next_id(&self) -> u64 {
        self.id.clone() + 1
    }

    fn next_epoch(&self) -> usize {
        self.epoch.clone() + 1
    }
    pub fn strategy(&self) -> ConfigurationStrategy {
        self.strategy
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn epoch(&self) -> usize {
        self.epoch
    }

    pub fn status(&self) -> ConfigurationStatus {
        self.status
    }

    pub fn starting_slot(&self) -> usize {
        match &self.checkpoint {
            None => 0,
            Some(checkpoint) => checkpoint
                .manifest()
                .last_applied_slot()
                .map_or(0, |slot| slot + 1),
        }
    }
    pub fn kv_store(&self) -> Option<HashMap<String, KVEntry>> {
        match &self.checkpoint {
            None => None,
            Some(checkpoint) => Some(checkpoint.state().kv_store().clone()),
        }
    }

    pub fn add_checkpoint(&mut self, checkpoint: RsmCheckpoint) {
        if self.status != ConfigurationStatus::PENDING {
            return;
        }
        self.checkpoint = Some(checkpoint)
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

    pub fn alpha_max_inflight(&self) -> usize {
        self.alpha_max_inflight
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
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::*;
    use crate::rsm::checkpoint::{
        CheckpointManifest, CheckpointState, RsmCheckpoint, RsmCheckpointState,
    };

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
            epoch: 1,
            status: ConfigurationStatus::ACTIVE,
            nodes,
            strategy: ConfigurationStrategy::JointConsensus,
            alpha_max_inflight: 0,
            checkpoint: None,
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

    #[test]
    fn starting_slot_is_zero_when_checkpoint_has_no_applied_slots() {
        let mut cfg = config();
        let checkpoint = RsmCheckpoint::new(
            CheckpointManifest::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                1,
                1,
                None,
                1_742_123_456_789,
            ),
            CheckpointState::new(RsmCheckpointState::new(HashMap::new()), HashMap::new()),
        );
        cfg.checkpoint = Some(checkpoint);

        assert_eq!(cfg.starting_slot(), 0);
    }

    #[test]
    fn starting_slot_advances_from_last_applied_slot() {
        let mut cfg = config();
        let checkpoint = RsmCheckpoint::new(
            CheckpointManifest::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                1,
                1,
                Some(17),
                1_742_123_456_789,
            ),
            CheckpointState::new(RsmCheckpointState::new(HashMap::new()), HashMap::new()),
        );
        cfg.checkpoint = Some(checkpoint);

        assert_eq!(cfg.starting_slot(), 18);
    }
}
