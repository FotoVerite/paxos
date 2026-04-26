use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use uuid::Uuid;

use crate::{
    cluster::{
        synod_vertical::SynodVerticalSystem, vertical::configuration::VerticalClusterConfiguration,
    },
    common::persistence::ClusterPersistence,
    monitor::PaxosObserver,
    node::vertical_paxos::replica::VerticalClientReply,
    paxos_command::PaxosCommand,
    rsm::checkpoint::RsmCheckpoint,
};

use super::{
    ClientAssignment, ClientId, SynodMembership, SynodProposalError, SynodProposalReceipt,
    SynodReadModel, SynodReadModelObserver, SynodRequestStatus, SynodRoomState,
    emoji_increment_command, is_valid_rust_emoji,
};

pub const SYNOD_CLIENT_TTL: Duration = Duration::from_secs(60);

pub struct SynodCluster {
    assignments: HashMap<ClientId, ClientAssignment>,
    last_seen: HashMap<ClientId, Instant>,
    assignment_order: Vec<ClientId>,
    system: SynodVerticalSystem,
    read_model: SynodReadModel,
    persistence: Arc<ClusterPersistence>,
    observer: Arc<dyn PaxosObserver>,
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
        let system = Self::build_system(
            Arc::clone(&persistence),
            Arc::clone(&observer),
            read_model.clone(),
        )
        .await?;
        system.start().await;

        Ok(Self {
            assignments: HashMap::new(),
            last_seen: HashMap::new(),
            assignment_order: Vec::new(),
            system,
            read_model,
            persistence,
            observer,
        })
    }

    async fn build_system(
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
        read_model: SynodReadModel,
    ) -> anyhow::Result<SynodVerticalSystem> {
        let observer = Arc::new(SynodReadModelObserver::new(read_model, observer));
        SynodVerticalSystem::new(Vec::new(), persistence, observer).await
    }

    pub async fn assign_client(
        &mut self,
        client_id: Option<ClientId>,
    ) -> anyhow::Result<ClientAssignment> {
        if let Some(client_id) = client_id {
            if let Some(assignment) = self.assignments.get(&client_id) {
                self.last_seen.insert(client_id, Instant::now());
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
        self.last_seen
            .insert(assignment.client_id.clone(), Instant::now());
        self.reconfigure_current_membership().await?;

        Ok(assignment)
    }

    pub fn record_client_name(&self, client_id: &ClientId, client_name: impl Into<String>) {
        self.read_model.record_client_name(client_id, client_name);
    }

    pub fn client_name(&self, client_id: &ClientId) -> Option<String> {
        self.read_model.client_name(client_id)
    }

    pub fn heartbeat_client(&mut self, client_id: &ClientId) -> Option<ClientAssignment> {
        let assignment = self.assignments.get(client_id)?.clone();
        self.last_seen.insert(client_id.clone(), Instant::now());
        Some(assignment)
    }

    pub async fn decommission_idle_clients(
        &mut self,
        idle_for: Duration,
    ) -> anyhow::Result<Vec<ClientAssignment>> {
        let cutoff = Instant::now()
            .checked_sub(idle_for)
            .unwrap_or_else(Instant::now);
        self.decommission_clients_last_seen_before(cutoff).await
    }

    pub fn assignment_for(&self, client_id: &ClientId) -> Option<&ClientAssignment> {
        self.assignments.get(client_id)
    }

    async fn current_replica_checkpoint(&self) -> anyhow::Result<Option<RsmCheckpoint>> {
        let Some(source_node_id) = self.bootstrap_node() else {
            return Ok(None);
        };
        self.system
            .replica_checkpoint(source_node_id)
            .await
            .map(Some)
    }

    pub async fn propose_emoji(
        &self,
        client_id: ClientId,
        request_id: u64,
        emoji: String,
    ) -> Result<SynodProposalReceipt, SynodProposalError> {
        // Validate the proposal early (client must exist, emoji must be valid).
        // This allows early rejection without waiting for cluster readiness.
        let proposal = self.prepare_emoji_proposal(client_id.clone(), request_id, emoji)?;

        // Wait until the cluster has an active configuration before allowing proposals.
        // This ensures all nodes have been activated and are ready to accept client requests.
        self.wait_for_active_configuration().await?;
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
        let target_node = if let Some(config) = self.system.active_configuration().await {
            config.leader()
        } else {
            proposal.assignment.node_id
        };
        self.system
            .submit_client_request(target_node, proposal.command.clone())
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

    async fn wait_for_active_configuration(&self) -> Result<(), SynodProposalError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                // Wait for the master to have an active_configuration_id set.
                // This indicates that activate_replica_set() has been called and replicas
                // should have their active_configuration set.
                if self
                    .system
                    .master()
                    .active_configuration_id()
                    .await
                    .is_some()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            SynodProposalError::Submit(anyhow::anyhow!("timed out waiting for cluster to be ready"))
        })
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

    pub fn assignment_order(&self) -> &[ClientId] {
        &self.assignment_order
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        self.system.cleanup().await
    }

    async fn reconfigure_current_membership(&mut self) -> anyhow::Result<()> {
        let member_ids = self
            .assignment_order
            .iter()
            .filter_map(|client_id| self.assignments.get(client_id))
            .map(|assignment| assignment.node_id)
            .collect::<Vec<_>>();
        let checkpoint = self.current_replica_checkpoint().await?;
        self.reconfigure_member_ids(member_ids, checkpoint).await
    }

    async fn reconfigure_member_ids(
        &mut self,
        member_ids: Vec<Uuid>,
        checkpoint: Option<RsmCheckpoint>,
    ) -> anyhow::Result<()> {
        if member_ids.is_empty() {
            return Ok(());
        }

        self.system
            .reconfigure_members_with_checkpoint(
                member_ids,
                self.bootstrap_node()
                    .expect("non-empty membership should have bootstrap node"),
                checkpoint,
            )
            .await?;
        Ok(())
    }

    pub async fn decommission_clients_last_seen_before(
        &mut self,
        cutoff: Instant,
    ) -> anyhow::Result<Vec<ClientAssignment>> {
        let expired_client_ids = self
            .assignment_order
            .iter()
            .filter(|client_id| {
                self.last_seen
                    .get(*client_id)
                    .is_some_and(|last_seen| *last_seen < cutoff)
            })
            .cloned()
            .collect::<Vec<_>>();

        if expired_client_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut expired = Vec::new();
        for client_id in &expired_client_ids {
            self.last_seen.remove(client_id);
            if let Some(assignment) = self.assignments.remove(client_id) {
                expired.push(assignment);
            }
        }
        self.assignment_order
            .retain(|client_id| !expired_client_ids.contains(client_id));

        let remaining_node_ids = self
            .assignment_order
            .iter()
            .filter_map(|client_id| self.assignments.get(client_id))
            .map(|assignment| assignment.node_id)
            .collect::<Vec<_>>();

        if remaining_node_ids.is_empty() {
            self.system.cleanup().await?;
            self.system = Self::build_system(
                Arc::clone(&self.persistence),
                Arc::clone(&self.observer),
                self.read_model.clone(),
            )
            .await?;
            self.system.start().await;
        } else {
            let checkpoint = self.current_replica_checkpoint().await?;
            self.reconfigure_member_ids(remaining_node_ids, checkpoint)
                .await?;
            for assignment in &expired {
                self.system
                    .runtime()
                    .stop_and_remove_node(assignment.node_id)
                    .await;
            }
        }

        Ok(expired)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
