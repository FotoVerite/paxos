use std::collections::HashSet;

use uuid::Uuid;

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
            epoch: prev.epoch,
            strategy: prev.strategy,
            status: ConfigurationStatus::PENDING,
            nodes: prev.nodes.clone(),
            alpha_max_inflight: prev.alpha_max_inflight,
            starting_slot: prev.starting_slot,
            kv_store: None,
        };
        for (uuid, roles) in patch.add {
            if new_config
                .nodes
                .iter()
                .any(|(existing_uuid, _)| *existing_uuid == uuid)
            {
                return Err(ReconfigError::InvalidMembership(
                    "Node ID already in configuration",
                ));
            }
            new_config.nodes.push((uuid, roles));
        }

        for uuid in patch.remove {
            if !new_config
                .nodes
                .iter()
                .any(|(existing_uuid, _)| *existing_uuid == uuid)
            {
                return Err(ReconfigError::InvalidMembership(
                    "Node not in configuration",
                ));
            }
            new_config
                .nodes
                .retain(|(existing_uuid, _)| *existing_uuid != uuid);
        }
        if let Some(strategy) = patch.strategy {
            new_config.strategy = strategy
        }
        if let Some(alpha) = patch.alpha_max_inflight {
            new_config.alpha_max_inflight = alpha
        }
        if !new_config.nodes.iter().any(|(_, roles)| roles.proposer) {
            return Err(ReconfigError::NoLeaders);
        }
        if !new_config.nodes.iter().any(|(_, roles)| roles.acceptor) {
            return Err(ReconfigError::NoAcceptors);
        }
        if !new_config.nodes.iter().any(|(_, roles)| roles.learner) {
            return Err(ReconfigError::NoLearners);
        }

        let old: HashSet<Uuid> = prev.acceptors().iter().copied().collect();
        let new = new_config.acceptors().iter().copied().collect();

        if old != new {
            new_config.epoch += 1;
        }
        Ok(new_config)
    }
}
#[cfg(test)]
mod tests {
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
        let nodes = vec![
            (a0, roles(false, true, false)),
            (l0, roles(true, false, false)),
            (r0, roles(false, false, true)),
            (r1, roles(false, false, true)),
        ];

        ClusterConfiguration {
            id: 7,
            epoch: 1,
            status: ConfigurationStatus::ACTIVE,
            nodes,
            strategy: ConfigurationStrategy::JointConsensus,
            alpha_max_inflight: 5,
            starting_slot: 0,
            kv_store: None,
        }
    }

    #[test]
    fn add_node_extends_previous_configuration() {
        let prev = base_config();
        let new_uuid = Uuid::from_u128(3);

        let next = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch::new().add_node(new_uuid, roles(true, false, false)),
        ))
        .expect("add should succeed");

        assert_eq!(next.id, prev.id + 1);
        assert!(next.nodes.iter().any(|(uuid, _)| *uuid == new_uuid));
        assert_eq!(next.nodes.len(), prev.nodes.len() + 1);
    }

    #[test]
    fn remove_existing_node_from_previous_configuration() {
        let prev = base_config();
        let remove_uuid = Uuid::from_u128(22);

        let next =
            ClusterConfiguration::try_from((&prev, ReconfigPatch::new().remove_node(remove_uuid)))
                .expect("remove should succeed");

        assert!(!next.nodes.iter().any(|(uuid, _)| *uuid == remove_uuid));
        assert_eq!(next.nodes.len(), prev.nodes.len() - 1);
    }

    #[test]
    fn patch_can_override_strategy() {
        let prev = base_config();

        let next = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch::new().strategy(ConfigurationStrategy::BrickWall),
        ))
        .expect("strategy-only reconfig should succeed");

        assert!(matches!(next.strategy, ConfigurationStrategy::BrickWall));
    }

    #[test]
    fn duplicate_add_is_rejected() {
        let prev = base_config();
        let existing_uuid = Uuid::from_u128(1);

        let result = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch::new().add_node(existing_uuid, roles(true, true, false)),
        ));

        assert!(matches!(result, Err(ReconfigError::InvalidMembership(_))));
    }

    #[test]
    fn removing_last_acceptor_is_rejected() {
        let prev = base_config();

        let result = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch::new().remove_node(Uuid::from_u128(1)),
        ));

        assert!(matches!(result, Err(ReconfigError::NoAcceptors)));
    }

    #[test]
    fn removing_last_leader_is_rejected() {
        let prev = base_config();

        let result = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch::new().remove_node(Uuid::from_u128(11)),
        ));

        assert!(matches!(result, Err(ReconfigError::NoLeaders)));
    }

    #[test]
    fn removing_last_learner_is_rejected() {
        let prev = base_config();

        let result = ClusterConfiguration::try_from((
            &prev,
            ReconfigPatch::new()
                .remove_node(Uuid::from_u128(2))
                .remove_node(Uuid::from_u128(22)),
        ));

        assert!(matches!(result, Err(ReconfigError::NoLearners)));
    }
}
