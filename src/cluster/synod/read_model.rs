use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    monitor::{Event, MessageTrace, PaxosObserver},
    node::vertical_paxos::replica::VerticalClientReply,
    paxos_command::PaxosCommand,
};

use super::{ClientId, RUST_EMOJI_POOL};

const RECENT_LIMIT: usize = 48;
const EMOJI_KEY_PREFIX: &str = "synod:emoji:";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SynodRequestStage {
    Proposed,
    Accepted,
    Pending,
    Applied,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct SynodRequestStatus {
    pub client_id: String,
    pub client_name: Option<String>,
    pub request_id: u64,
    pub emoji: String,
    pub stage: SynodRequestStage,
    pub assigned_node: Uuid,
    pub slot: Option<usize>,
    pub proposed_slot: Option<usize>,
    pub applied_slot: Option<usize>,
    pub configuration_id: Option<Uuid>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SynodAppliedEmoji {
    pub sequence: u64,
    pub slot: usize,
    pub emoji: String,
    pub count: usize,
    pub client_id: Option<String>,
    pub client_name: Option<String>,
    pub request_id: Option<u64>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SynodHeatItem {
    pub emoji: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct SynodRoomState {
    pub cluster_slot: usize,
    pub last_applied: usize,
    pub heat: Vec<SynodHeatItem>,
    pub recent: Vec<SynodAppliedEmoji>,
    pub requests: Vec<SynodRequestStatus>,
}

#[derive(Debug, Clone)]
pub enum SynodRoomUpdate {
    RequestState(SynodRequestStatus),
    Applied(SynodAppliedEmoji),
    RoomChanged,
}

#[derive(Default)]
struct ReadModelState {
    applied_slots: HashSet<usize>,
    counts: HashMap<String, usize>,
    recent: VecDeque<SynodAppliedEmoji>,
    requests: HashMap<(Uuid, u64), SynodRequestStatus>,
    client_ids: HashMap<Uuid, String>,
    client_names: HashMap<Uuid, String>,
    last_applied: usize,
    sequence: u64,
}

#[derive(Clone, Default)]
pub struct SynodReadModel {
    state: Arc<Mutex<ReadModelState>>,
}

impl SynodReadModel {
    pub fn reset(&self) {
        let mut state = self.state.lock().expect("synod read model mutex poisoned");
        *state = ReadModelState::default();
    }

    pub fn record_client_name(&self, client_id: &ClientId, client_name: impl Into<String>) {
        let client_name = client_name.into();
        if client_name.trim().is_empty() {
            return;
        }

        let mut state = self.state.lock().expect("synod read model mutex poisoned");
        state
            .client_ids
            .insert(client_id.stable_uuid(), client_id.to_string());
        state
            .client_names
            .insert(client_id.stable_uuid(), client_name.trim().to_string());
    }

    pub fn client_name(&self, client_id: &ClientId) -> Option<String> {
        let state = self.state.lock().expect("synod read model mutex poisoned");
        state.client_names.get(&client_id.stable_uuid()).cloned()
    }

    fn client_name_for(state: &ReadModelState, client_id: &ClientId) -> Option<String> {
        state.client_names.get(&client_id.stable_uuid()).cloned()
    }

    pub fn record_proposed(
        &self,
        client_id: &ClientId,
        request_id: u64,
        emoji: &str,
        assigned_node: Uuid,
    ) -> SynodRequestStatus {
        let client_uuid = client_id.stable_uuid();
        let mut state = self.state.lock().expect("synod read model mutex poisoned");
        state.client_ids.insert(client_uuid, client_id.to_string());
        let client_name = Self::client_name_for(&state, client_id);
        let status = SynodRequestStatus {
            client_id: client_id.to_string(),
            client_name,
            request_id,
            emoji: emoji.to_string(),
            stage: SynodRequestStage::Proposed,
            assigned_node,
            slot: None,
            proposed_slot: None,
            applied_slot: None,
            configuration_id: None,
            error: None,
        };
        state
            .requests
            .insert((client_uuid, request_id), status.clone());
        status
    }

    pub fn record_reply(
        &self,
        client_id: &ClientId,
        request_id: u64,
        reply: &VerticalClientReply,
    ) -> Option<SynodRequestStatus> {
        let client_uuid = client_id.stable_uuid();
        let mut state = self.state.lock().expect("synod read model mutex poisoned");
        if let Some(status) = state.requests.get_mut(&(client_uuid, request_id)) {
            match reply {
                VerticalClientReply::Accepted {
                    configuration_id,
                    slot,
                } => {
                    status.stage = SynodRequestStage::Accepted;
                    status.configuration_id = Some(*configuration_id);
                    status.slot = Some(*slot);
                    status.proposed_slot = Some(*slot);
                }
                VerticalClientReply::Pending { configuration_id } => {
                    status.stage = SynodRequestStage::Pending;
                    status.configuration_id = *configuration_id;
                }
                VerticalClientReply::Redirect {
                    active_configuration_id,
                    ..
                } => {
                    status.stage = SynodRequestStage::Pending;
                    status.configuration_id = Some(*active_configuration_id);
                }
            }
            return Some(status.clone());
        }
        None
    }

    pub fn record_failed(
        &self,
        client_id: &ClientId,
        request_id: u64,
        emoji: &str,
        assigned_node: Option<Uuid>,
        error: impl Into<String>,
    ) -> SynodRequestStatus {
        let client_uuid = client_id.stable_uuid();
        let mut state = self.state.lock().expect("synod read model mutex poisoned");
        state.client_ids.insert(client_uuid, client_id.to_string());
        let client_name = Self::client_name_for(&state, client_id);
        let status = SynodRequestStatus {
            client_id: client_id.to_string(),
            client_name,
            request_id,
            emoji: emoji.to_string(),
            stage: SynodRequestStage::Failed,
            assigned_node: assigned_node.unwrap_or_else(Uuid::nil),
            slot: None,
            proposed_slot: None,
            applied_slot: None,
            configuration_id: None,
            error: Some(error.into()),
        };
        state
            .requests
            .insert((client_uuid, request_id), status.clone());
        status
    }

    pub fn request_status(
        &self,
        client_id: &ClientId,
        request_id: u64,
    ) -> Option<SynodRequestStatus> {
        let state = self.state.lock().expect("synod read model mutex poisoned");
        state
            .requests
            .get(&(client_id.stable_uuid(), request_id))
            .cloned()
    }

    pub fn room_state(&self) -> SynodRoomState {
        let state = self.state.lock().expect("synod read model mutex poisoned");
        let mut heat = RUST_EMOJI_POOL
            .iter()
            .map(|emoji| SynodHeatItem {
                emoji: (*emoji).to_string(),
                count: *state.counts.get(*emoji).unwrap_or(&0),
            })
            .collect::<Vec<_>>();
        heat.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.emoji.cmp(&b.emoji)));

        SynodRoomState {
            cluster_slot: state.applied_slots.len(),
            last_applied: state.last_applied,
            heat,
            recent: state.recent.iter().cloned().collect(),
            requests: {
                let mut requests = state.requests.values().cloned().collect::<Vec<_>>();
                requests.sort_by(|a, b| {
                    b.request_id
                        .cmp(&a.request_id)
                        .then_with(|| a.client_id.cmp(&b.client_id))
                });
                requests
            },
        }
    }

    fn record_applied(
        &self,
        id: Uuid,
        slot: usize,
        value: PaxosCommand,
        created_at: u64,
    ) -> Option<(Option<SynodRequestStatus>, SynodAppliedEmoji)> {
        let Some(emoji) = emoji_from_command(value.operation()) else {
            return None;
        };
        let client_identity = value.client_identity();

        let mut state = self.state.lock().expect("synod read model mutex poisoned");
        let request_status = if let Some((client_uuid, request_id)) = client_identity {
            let client_id = state
                .client_ids
                .get(&client_uuid)
                .cloned()
                .unwrap_or_else(|| client_uuid.to_string());
            let client_name = state.client_names.get(&client_uuid).cloned();
            let status = state
                .requests
                .entry((client_uuid, request_id))
                .or_insert_with(|| SynodRequestStatus {
                    client_id,
                    client_name,
                    request_id,
                    emoji: emoji.clone(),
                    stage: SynodRequestStage::Pending,
                    assigned_node: id,
                    slot: None,
                    proposed_slot: None,
                    applied_slot: None,
                    configuration_id: None,
                    error: None,
                });
            status.stage = SynodRequestStage::Applied;
            status.slot = Some(slot);
            status.applied_slot = Some(slot);
            Some(status.clone())
        } else {
            None
        };

        if !state.applied_slots.insert(slot) {
            return None;
        }

        let count = {
            let count = state.counts.entry(emoji.clone()).or_insert(0);
            *count += 1;
            *count
        };
        state.last_applied = state.last_applied.max(slot);
        state.sequence += 1;

        let (client_id, client_name, request_id) =
            client_identity.map_or((None, None, None), |(client_uuid, request_id)| {
                (
                    state.client_ids.get(&client_uuid).cloned(),
                    state.client_names.get(&client_uuid).cloned(),
                    Some(request_id),
                )
            });
        let applied = SynodAppliedEmoji {
            sequence: state.sequence,
            slot,
            emoji,
            count,
            client_id,
            client_name,
            request_id,
            created_at,
        };
        state.recent.push_back(applied);
        while state.recent.len() > RECENT_LIMIT {
            state.recent.pop_front();
        }
        Some((
            request_status,
            state
                .recent
                .back()
                .cloned()
                .expect("applied event just recorded"),
        ))
    }
}

pub struct SynodReadModelObserver {
    read_model: SynodReadModel,
    updates: broadcast::Sender<SynodRoomUpdate>,
    delegate: Arc<dyn PaxosObserver>,
}

impl SynodReadModelObserver {
    pub fn new(
        read_model: SynodReadModel,
        updates: broadcast::Sender<SynodRoomUpdate>,
        delegate: Arc<dyn PaxosObserver>,
    ) -> Self {
        Self {
            read_model,
            updates,
            delegate,
        }
    }
}

impl PaxosObserver for SynodReadModelObserver {
    fn on_event(&self, event: Event) {
        if let Event::VerticalReplicaApplied {
            id,
            slot,
            value,
            created_at,
            ..
        } = event.clone()
        {
            if let Some((request_status, applied)) =
                self.read_model.record_applied(id, slot, value, created_at)
            {
                if let Some(status) = request_status {
                    let _ = self.updates.send(SynodRoomUpdate::RequestState(status));
                }
                let _ = self.updates.send(SynodRoomUpdate::Applied(applied));
                let _ = self.updates.send(SynodRoomUpdate::RoomChanged);
            }
        }
        self.delegate.on_event(event);
    }

    fn on_message_trace(&self, trace: MessageTrace) {
        self.delegate.on_message_trace(trace);
    }
}

fn emoji_from_command(command: &PaxosCommand) -> Option<String> {
    match command {
        PaxosCommand::INCREMENT { key, .. } => key
            .strip_prefix(EMOJI_KEY_PREFIX)
            .map(std::string::ToString::to_string),
        _ => None,
    }
}
