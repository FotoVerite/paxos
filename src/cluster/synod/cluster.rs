use std::{collections::HashMap, sync::Arc};

use uuid::Uuid;

use crate::{
    cluster::{
        synod_vertical::SynodVerticalSystem, vertical::configuration::VerticalClusterConfiguration,
    },
    common::persistence::ClusterPersistence,
    monitor::PaxosObserver,
};

use super::{ClientAssignment, ClientId, SynodMembership};

pub struct SynodCluster {
    assignments: HashMap<ClientId, ClientAssignment>,
    assignment_order: Vec<ClientId>,
    system: SynodVerticalSystem,
}

impl SynodCluster {
    pub async fn new(
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let system = SynodVerticalSystem::new(Vec::new(), persistence, observer).await?;
        system.start().await;

        Ok(Self {
            assignments: HashMap::new(),
            assignment_order: Vec::new(),
            system,
        })
    }

    pub async fn assign_client(
        &mut self,
        client_id: Option<ClientId>,
    ) -> anyhow::Result<ClientAssignment> {
        if let Some(client_id) = client_id {
            if let Some(assignment) = self.assignments.get(&client_id) {
                return Ok(assignment.clone());
            }
        }

        let assignment = ClientAssignment {
            client_id: ClientId::new(),
            node_id: Uuid::new_v4(),
        };

        self.system.runtime().spawn_node(assignment.node_id).await?;
        self.assignment_order.push(assignment.client_id.clone());
        self.assignments
            .insert(assignment.client_id.clone(), assignment.clone());
        self.reconfigure_current_membership().await?;

        Ok(assignment)
    }

    pub fn assignment_for(&self, client_id: &ClientId) -> Option<&ClientAssignment> {
        self.assignments.get(client_id)
    }

    pub fn assignment_count(&self) -> usize {
        self.assignments.len()
    }

    pub async fn server_node_ids(&self) -> Vec<Uuid> {
        self.system.runtime().member_ids().await
    }

    pub async fn active_configuration_id(&self) -> Option<Uuid> {
        self.system.master().active_configuration_id().await
    }

    pub async fn active_configuration(&self) -> Option<Arc<VerticalClusterConfiguration>> {
        self.system.active_configuration().await
    }

    pub fn membership(&self) -> SynodMembership {
        let assignments = self
            .assignment_order
            .iter()
            .filter_map(|client_id| self.assignments.get(client_id))
            .cloned()
            .collect();

        SynodMembership {
            assignments,
            bootstrap_node: self.bootstrap_node(),
        }
    }

    pub fn bootstrap_node(&self) -> Option<Uuid> {
        self.assignment_order
            .first()
            .and_then(|client_id| self.assignments.get(client_id))
            .map(|assignment| assignment.node_id)
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        self.system.cleanup().await
    }

    async fn reconfigure_current_membership(&mut self) -> anyhow::Result<()> {
        let member_ids = self.server_node_ids().await;
        if member_ids.is_empty() {
            return Ok(());
        }

        self.system
            .reconfigure_members(
                member_ids,
                self.bootstrap_node()
                    .expect("non-empty membership should have bootstrap node"),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        common::persistence::ClusterPersistence,
        monitor::{NoOpObserver, PaxosObserver},
    };

    use super::*;

    async fn test_cluster(name: &str) -> SynodCluster {
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        SynodCluster::new(Arc::new(ClusterPersistence::for_test(name)), observer)
            .await
            .expect("synod cluster should build")
    }

    #[tokio::test]
    async fn assigns_new_client_to_server_owned_node() {
        let mut cluster = test_cluster("synod_assign_new_client").await;

        let assignment = cluster
            .assign_client(None)
            .await
            .expect("assignment should work");

        assert!(assignment.client_id.as_str().starts_with("c_"));
        assert_eq!(cluster.assignment_count(), 1);
        assert_eq!(
            cluster.assignment_for(&assignment.client_id),
            Some(&assignment)
        );
        assert_eq!(cluster.server_node_ids().await, vec![assignment.node_id]);
        assert!(cluster.active_configuration_id().await.is_some());
        cluster.cleanup().await.expect("cleanup should work");
    }

    #[tokio::test]
    async fn reuses_known_client_assignment() {
        let mut cluster = test_cluster("synod_reuse_known_client").await;
        let first = cluster
            .assign_client(None)
            .await
            .expect("assignment should work");

        let second = cluster
            .assign_client(Some(first.client_id.clone()))
            .await
            .expect("rejoin should work");

        assert_eq!(second, first);
        assert_eq!(cluster.assignment_count(), 1);
        assert_eq!(cluster.server_node_ids().await, vec![first.node_id]);
        assert_eq!(
            cluster
                .active_configuration()
                .await
                .expect("config should exist")
                .replicas(),
            &[first.node_id]
        );
        cluster.cleanup().await.expect("cleanup should work");
    }

    #[tokio::test]
    async fn unknown_client_gets_fresh_assignment() {
        let mut cluster = test_cluster("synod_unknown_client").await;

        let assignment = cluster
            .assign_client(Some(ClientId::from_existing("c_missing")))
            .await
            .expect("assignment should work");

        assert_ne!(assignment.client_id.as_str(), "c_missing");
        assert_eq!(cluster.assignment_count(), 1);
        assert_eq!(cluster.server_node_ids().await, vec![assignment.node_id]);
        assert!(cluster.active_configuration_id().await.is_some());
        cluster.cleanup().await.expect("cleanup should work");
    }

    #[tokio::test]
    async fn first_assignment_is_bootstrap_node() {
        let mut cluster = test_cluster("synod_bootstrap_node").await;
        let first = cluster
            .assign_client(None)
            .await
            .expect("assignment should work");
        let _second = cluster
            .assign_client(None)
            .await
            .expect("assignment should work");

        assert_eq!(cluster.bootstrap_node(), Some(first.node_id));
        cluster.cleanup().await.expect("cleanup should work");
    }

    #[tokio::test]
    async fn membership_reports_client_node_assignments() {
        let mut cluster = test_cluster("synod_membership").await;
        let first = cluster
            .assign_client(None)
            .await
            .expect("assignment should work");
        let second = cluster
            .assign_client(None)
            .await
            .expect("assignment should work");

        let membership = cluster.membership();

        assert_eq!(membership.assignments, vec![first.clone(), second]);
        assert_eq!(membership.bootstrap_node, Some(first.node_id));
        assert_eq!(membership.node_count(), 2);
        cluster.cleanup().await.expect("cleanup should work");
    }

    #[tokio::test]
    async fn new_client_join_reconfigures_active_membership() {
        let mut cluster = test_cluster("synod_reconfigure_membership").await;
        let first = cluster
            .assign_client(None)
            .await
            .expect("first assignment should work");
        let first_config = cluster
            .active_configuration()
            .await
            .expect("first config should exist");

        let second = cluster
            .assign_client(None)
            .await
            .expect("second assignment should work");
        let second_config = cluster
            .active_configuration()
            .await
            .expect("second config should exist");

        assert_ne!(first_config.id(), second_config.id());
        assert_eq!(second_config.leader(), first.node_id);
        assert_eq!(second_config.acceptors(), &[first.node_id, second.node_id]);
        assert_eq!(second_config.replicas(), &[first.node_id, second.node_id]);
        assert_eq!(
            second_config.read_quorum().members(),
            &[first.node_id, second.node_id]
        );
        assert_eq!(
            second_config.write_quorum().members(),
            &[first.node_id, second.node_id]
        );
        assert_eq!(second_config.complete_bal(), Some(first_config.ballot()));
        assert_eq!(
            cluster.active_configuration_id().await,
            Some(second_config.id())
        );
        cluster.cleanup().await.expect("cleanup should work");
    }
}
