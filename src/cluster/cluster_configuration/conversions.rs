use crate::cluster::cluster_configuration::{
    ClusterConfiguration, ConfigurationStatus, reconfig_errors::ReconfigError,
    reconfig_patch::ReconfigPatch,
};


impl TryFrom<(&ClusterConfiguration, ReconfigPatch)> for ClusterConfiguration {
    type Error = ReconfigError;

    fn try_from(
        (prev, patch): (&ClusterConfiguration, ReconfigPatch),
    ) -> Result<Self, Self::Error> {
        let mut new_config = Self {
            id: prev.next_id(),
            strategy: prev.strategy,
            status: ConfigurationStatus::PENDING,
            nodes: prev.nodes.clone(),
        };
        for (uuid, roles) in patch.add {
            if new_config.nodes.contains_key(&uuid) {
                return Err(ReconfigError::InvalidMembership(
                    "Node ID already in configuration",
                ));
            }
            new_config.nodes.insert(uuid, roles);
        }

        for uuid in patch.remove {
            if !new_config.nodes.contains_key(&uuid) {
                return Err(ReconfigError::InvalidMembership(
                    "Node not in configuration",
                ));
            }
            new_config.nodes.remove(&uuid);
        }
        if let Some(strategy) = patch.strategy {
            new_config.strategy = strategy
        }
        if !new_config.nodes.values().any(|roles| roles.proposer) {
            return Err(ReconfigError::NoLeaders);
        }
        if !new_config.nodes.values().any(|roles| roles.acceptor) {
            return Err(ReconfigError::NoAcceptors);
        }
        if !new_config.nodes.values().any(|roles| roles.learner) {
            return Err(ReconfigError::NoLearners);
        }
        Ok(new_config)
    }
}
#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use uuid::Uuid;

    use super::*;
    use crate::cluster::cluster_configuration::ConfigurationStrategy;
    use crate::node::config::Roles;

    fn roles(proposer: bool, acceptor: bool, learner: bool) -> Roles {
        Roles {
            proposer,
            acceptor,
            learner,
        }
    }

    fn base_config() -> ClusterConfiguration {
        let a0 = Uuid::from_u128(1);
        let l0 = Uuid::from_u128(11);
        let r0 = Uuid::from_u128(2);
        let r1 = Uuid::from_u128(22);
        let mut nodes = HashMap::new();
        nodes.insert(a0, roles(false, true, false));
        nodes.insert(l0, roles(true, false, false));
        nodes.insert(r0, roles(false, false, true));
        nodes.insert(r1, roles(false, false, true));

        ClusterConfiguration {
            id: 7,
            status: ConfigurationStatus::ACTIVE,
            nodes,
            strategy: ConfigurationStrategy::JointConsensus,
        }
    }

    #[test]
    fn add_node_extends_previous_configuration() {
        let prev = base_config();
        let new_uuid = Uuid::from_u128(3);
        let mut add = HashMap::new();
        add.insert(new_uuid, roles(true, false, false));

        let next = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch {
                strategy: None,
                add,
                remove: HashSet::new(),
            },
        ))
        .expect("add should succeed");

        assert_eq!(next.id, prev.id + 1);
        assert!(next.nodes.contains_key(&new_uuid));
        assert_eq!(next.nodes.len(), prev.nodes.len() + 1);
    }

    #[test]
    fn remove_existing_node_from_previous_configuration() {
        let prev = base_config();
        let remove_uuid = Uuid::from_u128(22);
        let mut remove = HashSet::new();
        remove.insert(remove_uuid);

        let next = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch {
                strategy: None,
                add: HashMap::new(),
                remove,
            },
        ))
        .expect("remove should succeed");

        assert!(!next.nodes.contains_key(&remove_uuid));
        assert_eq!(next.nodes.len(), prev.nodes.len() - 1);
    }

    #[test]
    fn patch_can_override_strategy() {
        let prev = base_config();

        let next = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch {
                strategy: Some(ConfigurationStrategy::BrickWall),
                add: HashMap::new(),
                remove: HashSet::new(),
            },
        ))
        .expect("strategy-only reconfig should succeed");

        assert!(matches!(next.strategy, ConfigurationStrategy::BrickWall));
    }

    #[test]
    fn duplicate_add_is_rejected() {
        let prev = base_config();
        let existing_uuid = Uuid::from_u128(1);
        let mut add = HashMap::new();
        add.insert(existing_uuid, roles(true, true, false));

        let result = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch {
                strategy: None,
                add,
                remove: HashSet::new(),
            },
        ));

        assert!(matches!(result, Err(ReconfigError::InvalidMembership(_))));
    }

    #[test]
    fn removing_last_acceptor_is_rejected() {
        let prev = base_config();
        let mut remove = HashSet::new();
        remove.insert(Uuid::from_u128(1));

        let result = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch {
                strategy: None,
                add: HashMap::new(),
                remove,
            },
        ));

        assert!(matches!(result, Err(ReconfigError::NoAcceptors)));
    }

    #[test]
    fn removing_last_leader_is_rejected() {
        let prev = base_config();
        let mut remove = HashSet::new();
        remove.insert(Uuid::from_u128(11));

        let result = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch {
                strategy: None,
                add: HashMap::new(),
                remove,
            },
        ));

        assert!(matches!(result, Err(ReconfigError::NoLeaders)));
    }

    #[test]
    fn removing_last_learner_is_rejected() {
        let prev = base_config();
        let mut remove = HashSet::new();
        remove.insert(Uuid::from_u128(2));
        remove.insert(Uuid::from_u128(22));

        let result = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch {
                strategy: None,
                add: HashMap::new(),
                remove,
            },
        ));

        assert!(matches!(result, Err(ReconfigError::NoLearners)));
    }
}
