use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    cluster::vertical::configuration::VerticalClusterConfiguration,
    common::persistence::NodePersistence,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        pvalue::PValue,
        vertical_paxos::{message::VerticalPaxosMessage, transport::VerticalPaxosHandle},
    },
    paxos_command::{ClientId, PaxosCommand, RequestId},
    rsm::{
        checkpoint::{CheckpointManifest, CheckpointState, RsmCheckpoint},
        kv_store::{KVStore, ReplyOutcome},
    },
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum VerticalClientReply {
    Accepted {
        configuration_id: Uuid,
        slot: usize,
    },
    Pending {
        configuration_id: Option<Uuid>,
    },
    Redirect {
        from_replica: Uuid,
        active_configuration_id: Uuid,
        leader: Uuid,
        replicas: Vec<Uuid>,
    },
}

#[derive(Debug, Clone)]
struct PendingClientRequest {
    cmd: PaxosCommand,
}

#[derive(Default)]
struct ReplicaData {
    installed_configuration: Option<Arc<VerticalClusterConfiguration>>,
    active_configuration: Option<Arc<VerticalClusterConfiguration>>,
    redirected_configuration: Option<Arc<VerticalClusterConfiguration>>,
    next_slot: usize,
    next_execution_slot: usize,
    proposals: BTreeMap<usize, PaxosCommand>,
    decisions: BTreeMap<usize, PaxosCommand>,
    applied_cache: HashMap<(ClientId, RequestId), ReplyOutcome>,
}

pub struct Replica {
    id: Uuid,
    peers: Arc<VerticalPaxosHandle>,
    store: Arc<KVStore>,
    observer: Arc<dyn PaxosObserver>,
    data: Arc<Mutex<ReplicaData>>,
}

impl Replica {
    pub async fn new(
        id: Uuid,
        persistence: NodePersistence,
        peers: Arc<VerticalPaxosHandle>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            id,
            peers,
            store: Arc::new(KVStore::init(id, persistence, None).await?),
            observer,
            data: Arc::new(Mutex::new(ReplicaData::default())),
        })
    }

    pub async fn install_configuration(&self, configuration: Arc<VerticalClusterConfiguration>) {
        if let Some(checkpoint) = configuration.checkpoint() {
            let _ = self.install_checkpoint(checkpoint).await;
        }
        let mut data = self.data.lock().await;
        data.next_slot = data.next_slot.max(configuration.start_index());
        data.next_execution_slot = data.next_execution_slot.max(configuration.start_index());
        data.installed_configuration = Some(configuration);
    }

    pub async fn activate_configuration(&self, configuration: Arc<VerticalClusterConfiguration>) {
        let mut data = self.data.lock().await;
        data.next_slot = data.next_slot.max(configuration.start_index());
        data.next_execution_slot = data.next_execution_slot.max(configuration.start_index());
        data.active_configuration = Some(configuration);
        data.redirected_configuration = None;
    }

    pub async fn decommission_to(&self, active_configuration: Arc<VerticalClusterConfiguration>) {
        let mut data = self.data.lock().await;
        data.active_configuration = None;
        data.redirected_configuration = Some(active_configuration);
    }

    pub async fn clear_routing_state(&self) {
        let mut data = self.data.lock().await;
        data.active_configuration = None;
        data.redirected_configuration = None;
    }

    pub async fn checkpoint(&self) -> RsmCheckpoint {
        let state = self.store.emit_checkpoint_state().await;
        let data = self.data.lock().await;
        let configuration = data
            .active_configuration
            .as_ref()
            .or(data.installed_configuration.as_ref());
        let ballot = configuration.map(|configuration| configuration.ballot());
        let client_dedup_watermark: HashMap<ClientId, RequestId> = data.applied_cache.keys().fold(
            HashMap::new(),
            |mut watermark, (client_id, request_id)| {
                watermark
                    .entry(*client_id)
                    .and_modify(|seen| *seen = (*seen).max(*request_id))
                    .or_insert(*request_id);
                watermark
            },
        );

        RsmCheckpoint::new(
            CheckpointManifest::new(
                Uuid::new_v4(),
                self.id,
                ballot.map_or(0, |ballot| ballot.number as u64),
                ballot.map_or(0, |ballot| ballot.epoch as u64),
                data.next_execution_slot.checked_sub(1),
                current_timestamp_millis(),
            ),
            CheckpointState::new(state, client_dedup_watermark),
        )
    }

    async fn install_checkpoint(&self, checkpoint: &RsmCheckpoint) -> anyhow::Result<()> {
        let next_slot = checkpoint
            .manifest()
            .last_applied_slot()
            .map_or(0, |slot| slot + 1);
        self.store
            .restore(checkpoint.state().kv_store().clone())
            .await?;
        let mut data = self.data.lock().await;
        data.applied_cache = checkpoint
            .state()
            .client_dedup_watermark()
            .iter()
            .map(|(client_id, request_id)| ((*client_id, *request_id), ReplyOutcome::Ok))
            .collect();
        data.next_slot = data.next_slot.max(next_slot);
        data.next_execution_slot = data.next_execution_slot.max(next_slot);
        Ok(())
    }

    pub async fn submit_client_request(&self, cmd: PaxosCommand) -> VerticalClientReply {
        let reply = {
            let mut data = self.data.lock().await;
            let configuration_id = data.installed_configuration.as_ref().map(|cfg| cfg.id());
            let Some(active_configuration) = data.active_configuration.clone() else {
                if let Some(active_configuration) = data.redirected_configuration.clone() {
                    let reply = VerticalClientReply::Redirect {
                        from_replica: self.id,
                        active_configuration_id: active_configuration.id(),
                        leader: active_configuration.leader(),
                        replicas: active_configuration.replicas().to_vec(),
                    };
                    self.observer.on_event(Event::VerticalReplicaRedirected {
                        from_replica: self.id,
                        active_configuration_id: active_configuration.id(),
                        leader: active_configuration.leader(),
                        replicas: active_configuration.replicas().to_vec(),
                        created_at: current_timestamp_millis(),
                    });
                    return reply;
                }
                return VerticalClientReply::Pending { configuration_id };
            };

            if let Some((client_id, request_id)) = cmd.client_identity() {
                if data.applied_cache.contains_key(&(client_id, request_id)) {
                    return VerticalClientReply::Accepted {
                        configuration_id: active_configuration.id(),
                        slot: data.next_slot.saturating_sub(1),
                    };
                }
            }

            let slot = data.next_slot;
            data.next_slot += 1;
            data.proposals.insert(slot, cmd.clone());

            let proposal = PendingClientRequest { cmd };
            (active_configuration, slot, proposal)
        };

        let (active_configuration, slot, proposal) = reply;
        self.peers
            .send(
                active_configuration.leader(),
                VerticalPaxosMessage::PROPOSE {
                    from: self.id,
                    slot,
                    cmd: proposal.cmd,
                },
            )
            .await;

        VerticalClientReply::Accepted {
            configuration_id: active_configuration.id(),
            slot,
        }
    }

    pub async fn handle_decision(&self, pvalue: PValue) -> Option<VerticalPaxosMessage> {
        let mut repropose = None;
        let slot = pvalue.slot();
        let cmd = pvalue.cmd();

        {
            let mut data = self.data.lock().await;
            if data.decisions.contains_key(&slot) {
                return Some(VerticalPaxosMessage::ACK {
                    from: self.id,
                    to: pvalue.ballot().node_id,
                    slot,
                });
            }

            data.decisions.insert(slot, cmd.clone());
            if let Some(pending) = data.proposals.remove(&slot) {
                if pending != cmd {
                    let new_slot = data.next_slot;
                    data.next_slot += 1;
                    data.proposals.insert(new_slot, pending.clone());
                    repropose = Some((new_slot, pending));
                }
            }
        }

        if let Some((new_slot, pending)) = repropose {
            let active_configuration = {
                let data = self.data.lock().await;
                data.active_configuration.clone()
            };
            if let Some(active_configuration) = active_configuration {
                self.peers
                    .send(
                        active_configuration.leader(),
                        VerticalPaxosMessage::PROPOSE {
                            from: self.id,
                            slot: new_slot,
                            cmd: pending,
                        },
                    )
                    .await;
            }
        }

        self.apply_ready_decisions().await;

        Some(VerticalPaxosMessage::ACK {
            from: self.id,
            to: pvalue.ballot().node_id,
            slot,
        })
    }

    async fn apply_ready_decisions(&self) {
        loop {
            let next = {
                let mut data = self.data.lock().await;
                let next_slot = data.next_execution_slot;
                data.decisions.remove(&next_slot).map(|cmd| {
                    data.next_execution_slot += 1;
                    (
                        next_slot,
                        cmd,
                        data.installed_configuration
                            .as_ref()
                            .map(|cfg| cfg.id())
                            .unwrap_or(Uuid::nil()),
                    )
                })
            };

            let Some((slot, cmd, configuration_id)) = next else {
                break;
            };

            let response = self.store.apply(cmd.clone()).await.ok();
            if let (Some((client_id, request_id)), Some(response)) =
                (cmd.client_identity(), response.clone())
            {
                self.data
                    .lock()
                    .await
                    .applied_cache
                    .insert((client_id, request_id), response);
            }

            self.observer.on_event(Event::VerticalReplicaApplied {
                id: self.id,
                configuration_id: if configuration_id == Uuid::nil() {
                    None
                } else {
                    Some(configuration_id)
                },
                slot,
                value: cmd,
                created_at: current_timestamp_millis(),
            });
        }
    }
}
