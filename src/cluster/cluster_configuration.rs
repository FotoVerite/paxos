mod conversions;
mod reconfig_errors;
mod reconfig_patch;

use std::collections::HashMap;

use uuid::Uuid;

use crate::node::config::{PmmcNodeConfig, Roles};

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
    nodes: HashMap<Uuid, Roles>,
    strategy: ConfigurationStrategy,
}

impl ClusterConfiguration {
    fn next_id(&self) -> u64 {
        self.id.clone() + 1
    }

    fn nodes_with(&self, pred: impl Fn(&Roles) -> bool) -> Vec<Uuid> {
        self.nodes
            .iter()
            .filter_map(|(uuid, role)| pred(role).then_some(*uuid))
            .collect()
    }
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn members(&self) -> Vec<(Uuid, Roles)> {
        let mut members: Vec<(Uuid, Roles)> = self
            .nodes
            .iter()
            .map(|(uuid, roles)| (*uuid, roles.clone()))
            .collect();
        members.sort_by_key(|(uuid, _)| uuid.as_u128());
        members
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
        let mut nodes = HashMap::new();
        nodes.insert(leader, roles(true, false, false));
        nodes.insert(acceptor, roles(false, true, false));
        nodes.insert(replica, roles(false, false, true));
        nodes.insert(full, roles(true, true, true));

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
}
