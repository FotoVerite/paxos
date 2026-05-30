use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::broadcast;
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
    ClientAssignment, ClientId, SynodActivityEvent, SynodActivityKind, SynodMembership,
    SynodProposalError, SynodProposalReceipt, SynodReadModel, SynodReadModelObserver,
    SynodRequestStatus, SynodRoomState, SynodRoomUpdate, emoji_increment_command,
    is_valid_rust_emoji,
};

pub const SYNOD_CLIENT_TTL: Duration = Duration::from_secs(60);

pub struct SynodCluster {
    assignments: HashMap<ClientId, ClientAssignment>,
    last_seen: HashMap<ClientId, Instant>,
    assignment_order: Vec<ClientId>,
    system: Arc<SynodVerticalSystem>,
    read_model: SynodReadModel,
    updates: broadcast::Sender<SynodRoomUpdate>,
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

pub struct SynodProposalPlan {
    proposal: PreparedEmojiProposal,
    system: Arc<SynodVerticalSystem>,
    read_model: SynodReadModel,
    updates: broadcast::Sender<SynodRoomUpdate>,
}

impl SynodProposalPlan {
    pub async fn submit(self) -> Result<SynodProposalReceipt, SynodProposalError> {
        self.wait_for_active_configuration().await?;
        self.record_proposal_started();

        let reply = self.submit_rsm_increment().await.map_err(|err| {
            self.record_proposal_failed(err.to_string());
            SynodProposalError::Submit(err)
        })?;
        self.record_proposal_reply(&reply);

        Ok(self.receipt_for(reply))
    }

    async fn wait_for_active_configuration(&self) -> Result<(), SynodProposalError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
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

    fn emit_update(&self, update: SynodRoomUpdate) {
        let _ = self.updates.send(update);
    }

    fn record_proposal_started(&self) {
        let status = self.read_model.record_proposed(
            &self.proposal.client_id,
            self.proposal.request_id,
            &self.proposal.emoji,
            self.proposal.assignment.node_id,
        );
        self.emit_update(SynodRoomUpdate::RequestState(status));
    }

    async fn submit_rsm_increment(&self) -> anyhow::Result<VerticalClientReply> {
        self.system
            .submit_client_request(
                self.proposal.assignment.node_id,
                self.proposal.command.clone(),
            )
            .await
    }

    fn record_proposal_reply(&self, reply: &VerticalClientReply) {
        if let Some(status) =
            self.read_model
                .record_reply(&self.proposal.client_id, self.proposal.request_id, reply)
        {
            self.emit_update(SynodRoomUpdate::RequestState(status));
        }
    }

    fn record_proposal_failed(&self, error: String) {
        let status = self.read_model.record_failed(
            &self.proposal.client_id,
            self.proposal.request_id,
            &self.proposal.emoji,
            Some(self.proposal.assignment.node_id),
            error,
        );
        self.emit_update(SynodRoomUpdate::RequestState(status));
    }

    fn receipt_for(self, reply: VerticalClientReply) -> SynodProposalReceipt {
        let status = self
            .read_model
            .request_status(&self.proposal.client_id, self.proposal.request_id)
            .expect("submitted request should have read-model status");

        SynodProposalReceipt {
            client_id: self.proposal.client_id,
            request_id: self.proposal.request_id,
            emoji: self.proposal.emoji,
            assigned_node: self.proposal.assignment.node_id,
            reply,
            status,
        }
    }
}

impl SynodCluster {
    pub async fn new(
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let read_model = SynodReadModel::default();
        let (updates, _) = broadcast::channel(256);
        let system = Arc::new(
            Self::build_system(
                Arc::clone(&persistence),
                Arc::clone(&observer),
                read_model.clone(),
                updates.clone(),
            )
            .await?,
        );
        system.start().await;

        Ok(Self {
            assignments: HashMap::new(),
            last_seen: HashMap::new(),
            assignment_order: Vec::new(),
            system,
            read_model,
            updates,
            persistence,
            observer,
        })
    }

    async fn build_system(
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
        read_model: SynodReadModel,
        updates: broadcast::Sender<SynodRoomUpdate>,
    ) -> anyhow::Result<SynodVerticalSystem> {
        let observer = Arc::new(SynodReadModelObserver::new(read_model, updates, observer));
        SynodVerticalSystem::new(Vec::new(), persistence, observer).await
    }

    pub fn subscribe_updates(&self) -> broadcast::Receiver<SynodRoomUpdate> {
        self.updates.subscribe()
    }

    fn emit_update(&self, update: SynodRoomUpdate) {
        let _ = self.updates.send(update);
    }

    pub fn emit_client_joined(&self, assignment: &ClientAssignment) {
        self.emit_update(SynodRoomUpdate::Activity(SynodActivityEvent {
            kind: SynodActivityKind::ClientJoined,
            message: format!(
                "{} joined on node {}",
                self.client_name(&assignment.client_id)
                    .unwrap_or_else(|| assignment.client_id.to_string()),
                assignment.node_id
            ),
            client_id: Some(assignment.client_id.to_string()),
            client_name: self.client_name(&assignment.client_id),
            node_id: Some(assignment.node_id),
            previous_leader: None,
            next_leader: self.bootstrap_node(),
            configuration_id: None,
            checkpoint_slot: None,
        }));
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
        self.emit_update(SynodRoomUpdate::RoomChanged);

        Ok(assignment)
    }

    pub fn record_client_name(&self, client_id: &ClientId, client_name: impl Into<String>) {
        self.read_model.record_client_name(client_id, client_name);
        self.emit_update(SynodRoomUpdate::RoomChanged);
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

    pub fn has_assignment(&self, client_id: &ClientId) -> bool {
        self.assignments.contains_key(client_id)
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
        self.plan_emoji_proposal(client_id, request_id, emoji)?
            .submit()
            .await
    }

    pub fn plan_emoji_proposal(
        &self,
        client_id: ClientId,
        request_id: u64,
        emoji: String,
    ) -> Result<SynodProposalPlan, SynodProposalError> {
        if !is_valid_rust_emoji(&emoji) {
            let status = self.read_model.record_failed(
                &client_id,
                request_id,
                &emoji,
                None,
                "emoji is not in the synod room pool",
            );
            self.emit_update(SynodRoomUpdate::RequestState(status));
            return Err(SynodProposalError::InvalidEmoji(emoji));
        }

        let assignment = self.assignment_for(&client_id).cloned().ok_or_else(|| {
            let status = self.read_model.record_failed(
                &client_id,
                request_id,
                &emoji,
                None,
                "client has not joined the synod room",
            );
            self.emit_update(SynodRoomUpdate::RequestState(status));
            SynodProposalError::UnknownClient(client_id.clone())
        })?;

        let proposal = PreparedEmojiProposal {
            command: emoji_increment_command(&emoji)
                .with_client(client_id.stable_uuid(), request_id),
            client_id,
            request_id,
            emoji,
            assignment,
        };

        Ok(SynodProposalPlan {
            proposal,
            system: Arc::clone(&self.system),
            read_model: self.read_model.clone(),
            updates: self.updates.clone(),
        })
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
        let previous_leader = self
            .system
            .active_configuration()
            .await
            .map(|configuration| configuration.leader());
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

        tracing::warn!(
            target: "synod::despawn",
            expired_clients = expired_client_ids.len(),
            total_clients = self.assignment_order.len(),
            bootstrap_node = ?self.bootstrap_node(),
            "decommissioning idle synod clients"
        );

        let mut expired = Vec::new();
        for client_id in &expired_client_ids {
            self.last_seen.remove(client_id);
            if let Some(assignment) = self.assignments.remove(client_id) {
                tracing::warn!(
                    target: "synod::despawn",
                    client_id = %assignment.client_id,
                    node_id = %assignment.node_id,
                    "removing idle synod client assignment"
                );
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
        let expired_node_ids = expired
            .iter()
            .map(|assignment| assignment.node_id)
            .collect::<Vec<_>>();

        for assignment in &expired {
            self.emit_update(SynodRoomUpdate::Activity(SynodActivityEvent {
                kind: SynodActivityKind::HeartbeatExpired,
                message: format!(
                    "{} missed heartbeat; node {} removed",
                    self.client_name(&assignment.client_id)
                        .unwrap_or_else(|| assignment.client_id.to_string()),
                    assignment.node_id
                ),
                client_id: Some(assignment.client_id.to_string()),
                client_name: self.client_name(&assignment.client_id),
                node_id: Some(assignment.node_id),
                previous_leader,
                next_leader: self.bootstrap_node(),
                configuration_id: None,
                checkpoint_slot: None,
            }));
        }

        if let Some(leader) = previous_leader {
            if expired_node_ids.contains(&leader) {
                self.emit_update(SynodRoomUpdate::Activity(SynodActivityEvent {
                    kind: SynodActivityKind::LeaderRemoved,
                    message: format!(
                        "leader {} expired; next leader {}",
                        leader,
                        self.bootstrap_node()
                            .map(|node_id| node_id.to_string())
                            .unwrap_or_else(|| "none".to_string())
                    ),
                    client_id: None,
                    client_name: None,
                    node_id: Some(leader),
                    previous_leader: Some(leader),
                    next_leader: self.bootstrap_node(),
                    configuration_id: None,
                    checkpoint_slot: None,
                }));
            }
        }

        if remaining_node_ids.is_empty() {
            tracing::warn!(
                target: "synod::despawn",
                expired_nodes = expired.len(),
                "all synod clients expired; resetting room system"
            );
            self.system.cleanup().await?;
            self.read_model.reset();
            self.system = Arc::new(
                Self::build_system(
                    Arc::clone(&self.persistence),
                    Arc::clone(&self.observer),
                    self.read_model.clone(),
                    self.updates.clone(),
                )
                .await?,
            );
            self.system.start().await;
            self.emit_update(SynodRoomUpdate::Activity(SynodActivityEvent {
                kind: SynodActivityKind::RoomIdle,
                message: "all clients expired; room reset and waiting for a join".to_string(),
                client_id: None,
                client_name: None,
                node_id: None,
                previous_leader,
                next_leader: None,
                configuration_id: None,
                checkpoint_slot: None,
            }));
        } else {
            let checkpoint = self.current_replica_checkpoint().await?;
            let checkpoint_slot = checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.manifest().last_applied_slot());
            tracing::warn!(
                target: "synod::despawn",
                remaining_nodes = ?remaining_node_ids,
                expired_nodes = ?expired_node_ids,
                checkpoint_last_applied = ?checkpoint_slot,
                new_leader = ?self.bootstrap_node(),
                "reconfiguring synod room after idle decommission"
            );
            self.reconfigure_member_ids(remaining_node_ids, checkpoint)
                .await?;
            let active_configuration = self.system.active_configuration().await;
            self.emit_update(SynodRoomUpdate::Activity(SynodActivityEvent {
                kind: SynodActivityKind::ConfigurationChanged,
                message: format!(
                    "membership reconfigured; {} nodes remain",
                    self.assignment_order.len()
                ),
                client_id: None,
                client_name: None,
                node_id: None,
                previous_leader,
                next_leader: self.bootstrap_node(),
                configuration_id: active_configuration
                    .as_ref()
                    .map(|configuration| configuration.id()),
                checkpoint_slot,
            }));
            for assignment in &expired {
                tracing::warn!(
                    target: "synod::despawn",
                    node_id = %assignment.node_id,
                    "stopping and removing expired synod node"
                );
                self.system
                    .runtime()
                    .stop_and_remove_node(assignment.node_id)
                    .await;
            }
        }

        self.emit_update(SynodRoomUpdate::RoomChanged);
        Ok(expired)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
