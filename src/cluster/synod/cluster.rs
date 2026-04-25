use std::{collections::HashMap, sync::Arc};

use uuid::Uuid;

use crate::{
    cluster::{
        synod_vertical::SynodVerticalSystem, vertical::configuration::VerticalClusterConfiguration,
    },
    common::persistence::ClusterPersistence,
    monitor::PaxosObserver,
    node::vertical_paxos::replica::VerticalClientReply,
    paxos_command::PaxosCommand,
};

use super::{
    ClientAssignment, ClientId, SynodMembership, SynodProposalError, SynodProposalReceipt,
    SynodReadModel, SynodReadModelObserver, SynodRequestStatus, SynodRoomState,
    emoji_increment_command, is_valid_rust_emoji,
};

pub struct SynodCluster {
    assignments: HashMap<ClientId, ClientAssignment>,
    assignment_order: Vec<ClientId>,
    system: SynodVerticalSystem,
    read_model: SynodReadModel,
}

struct PreparedEmojiProposal {
    client_id: ClientId,
    request_id: u64,
    emoji: String,
    assignment: ClientAssignment,
    command: PaxosCommand,
}

impl SynodCluster {
    pub async fn new(
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let read_model = SynodReadModel::default();
        let observer = Arc::new(SynodReadModelObserver::new(read_model.clone(), observer));
        let system = SynodVerticalSystem::new(Vec::new(), persistence, observer).await?;
        system.start().await;

        Ok(Self {
            assignments: HashMap::new(),
            assignment_order: Vec::new(),
            system,
            read_model,
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

    pub async fn propose_emoji(
        &self,
        client_id: ClientId,
        request_id: u64,
        emoji: String,
    ) -> Result<SynodProposalReceipt, SynodProposalError> {
        let proposal = self.prepare_emoji_proposal(client_id, request_id, emoji)?;
        self.record_proposal_started(&proposal);

        let reply = self.submit_rsm_increment(&proposal).await.map_err(|err| {
            self.record_proposal_failed(&proposal, err.to_string());
            SynodProposalError::Submit(err)
        })?;
        self.record_proposal_reply(&proposal, &reply);

        Ok(self.receipt_for(proposal, reply))
    }

    fn prepare_emoji_proposal(
        &self,
        client_id: ClientId,
        request_id: u64,
        emoji: String,
    ) -> Result<PreparedEmojiProposal, SynodProposalError> {
        if !is_valid_rust_emoji(&emoji) {
            self.read_model.record_failed(
                &client_id,
                request_id,
                &emoji,
                None,
                "emoji is not in the synod room pool",
            );
            return Err(SynodProposalError::InvalidEmoji(emoji));
        }

        let assignment = self.assignment_for(&client_id).cloned().ok_or_else(|| {
            self.read_model.record_failed(
                &client_id,
                request_id,
                &emoji,
                None,
                "client has not joined the synod room",
            );
            SynodProposalError::UnknownClient(client_id.clone())
        })?;

        Ok(PreparedEmojiProposal {
            command: emoji_increment_command(&emoji)
                .with_client(client_id.stable_uuid(), request_id),
            client_id,
            request_id,
            emoji,
            assignment,
        })
    }

    fn record_proposal_started(&self, proposal: &PreparedEmojiProposal) {
        self.read_model.record_proposed(
            &proposal.client_id,
            proposal.request_id,
            &proposal.emoji,
            proposal.assignment.node_id,
        );
    }

    async fn submit_rsm_increment(
        &self,
        proposal: &PreparedEmojiProposal,
    ) -> anyhow::Result<VerticalClientReply> {
        self.system
            .submit_client_request(proposal.assignment.node_id, proposal.command.clone())
            .await
    }

    fn record_proposal_reply(&self, proposal: &PreparedEmojiProposal, reply: &VerticalClientReply) {
        self.read_model
            .record_reply(&proposal.client_id, proposal.request_id, reply);
    }

    fn record_proposal_failed(&self, proposal: &PreparedEmojiProposal, error: String) {
        self.read_model.record_failed(
            &proposal.client_id,
            proposal.request_id,
            &proposal.emoji,
            Some(proposal.assignment.node_id),
            error,
        );
    }

    fn receipt_for(
        &self,
        proposal: PreparedEmojiProposal,
        reply: VerticalClientReply,
    ) -> SynodProposalReceipt {
        let status = self
            .request_status(&proposal.client_id, proposal.request_id)
            .expect("submitted request should have read-model status");

        SynodProposalReceipt {
            client_id: proposal.client_id,
            request_id: proposal.request_id,
            emoji: proposal.emoji,
            assigned_node: proposal.assignment.node_id,
            reply,
            status,
        }
    }

    pub fn request_status(
        &self,
        client_id: &ClientId,
        request_id: u64,
    ) -> Option<SynodRequestStatus> {
        self.read_model.request_status(client_id, request_id)
    }

    pub fn room_state(&self) -> SynodRoomState {
        self.read_model.room_state()
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
    async fn joined_client_can_propose_rust_emoji() {
        let mut cluster = test_cluster("synod_propose_emoji").await;
        let assignment = cluster
            .assign_client(None)
            .await
            .expect("assignment should work");

        let receipt = cluster
            .propose_emoji(assignment.client_id.clone(), 1, "🦀".to_string())
            .await
            .expect("proposal should submit");

        assert_eq!(receipt.client_id, assignment.client_id);
        assert_eq!(receipt.request_id, 1);
        assert_eq!(receipt.emoji, "🦀");
        assert_eq!(receipt.assigned_node, assignment.node_id);
        assert!(matches!(
            receipt.reply,
            crate::node::vertical_paxos::replica::VerticalClientReply::Accepted { slot: 0, .. }
        ));
        cluster.cleanup().await.expect("cleanup should work");
    }

    #[tokio::test]
    async fn proposal_rejects_unknown_client() {
        let cluster = test_cluster("synod_propose_unknown").await;

        let err = cluster
            .propose_emoji(ClientId::from_existing("c_missing"), 1, "🦀".to_string())
            .await
            .expect_err("unknown client should reject");

        assert!(matches!(err, SynodProposalError::UnknownClient(_)));
        cluster.cleanup().await.expect("cleanup should work");
    }

    #[tokio::test]
    async fn proposal_rejects_non_pool_emoji() {
        let mut cluster = test_cluster("synod_propose_invalid").await;
        let assignment = cluster
            .assign_client(None)
            .await
            .expect("assignment should work");

        let err = cluster
            .propose_emoji(assignment.client_id, 1, "🍒".to_string())
            .await
            .expect_err("invalid emoji should reject");

        assert!(matches!(err, SynodProposalError::InvalidEmoji(_)));
        cluster.cleanup().await.expect("cleanup should work");
    }

    #[test]
    fn emoji_proposal_uses_rsm_increment_command() {
        let cmd = emoji_increment_command("🦀");

        assert_eq!(
            cmd,
            crate::paxos_command::PaxosCommand::INCREMENT {
                key: "synod:emoji:🦀".to_string(),
                value: 1,
            }
        );
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
