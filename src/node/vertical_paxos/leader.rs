use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    cluster::vertical::activation::ActivationSnapshot,
    common::ballot::Ballot,
    common::persistence::NodePersistence,
    monitor::PaxosObserver,
    node::vertical_paxos::{message::VerticalPaxosMessage, transport::VerticalPaxosHandle},
    paxos_command::PaxosCommand,
};

use self::{state::LeaderState, sync::LeaderSynchronizer};

pub mod commander;
pub mod state;
pub mod sync;

#[async_trait]
pub trait LeaderCallbacks: Send + Sync {
    async fn on_sync_started(&self, snapshot: Arc<ActivationSnapshot>);
    async fn on_sync_ready(&self, snapshot: Arc<ActivationSnapshot>);
    async fn on_sync_superseded(&self, snapshot: Arc<ActivationSnapshot>, superseded_by: Ballot);
}

pub struct Leader {
    uuid: Uuid,
    peers: Arc<VerticalPaxosHandle>,
    state: Arc<LeaderState>,
    observer: Arc<dyn PaxosObserver>,
    callbacks: Arc<dyn LeaderCallbacks>,
}

impl Leader {
    pub async fn new(
        uuid: Uuid,
        _persistence: NodePersistence,
        peers: Arc<VerticalPaxosHandle>,
        observer: Arc<dyn PaxosObserver>,
        callbacks: Arc<dyn LeaderCallbacks>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            uuid,
            peers,
            state: Arc::new(LeaderState::new()),
            observer,
            callbacks,
        })
    }

    pub fn id(&self) -> Uuid {
        self.uuid
    }

    pub async fn install_activation(&self, snapshot: Arc<ActivationSnapshot>) {
        self.state
            .install_activation(LeaderSynchronizer::new(
                self.uuid,
                Arc::clone(&self.peers),
                snapshot,
            ))
            .await;
    }

    pub async fn start_synchronization(&self) {
        if let Some(snapshot) = self.state.start_synchronization().await {
            self.callbacks.on_sync_started(snapshot).await;
        }
    }

    pub async fn propose_handler(&self, slot: usize, cmd: PaxosCommand) {
        self.state.record_proposal(slot, cmd).await;
    }

    async fn handle_sync_progress(&self, progress: LeaderProgress) {
        match progress {
            LeaderProgress::None => {}
            LeaderProgress::Ready { snapshot } => {
                self.callbacks.on_sync_ready(snapshot).await;
            }
            LeaderProgress::Superseded {
                snapshot,
                superseded_by,
            } => {
                self.callbacks
                    .on_sync_superseded(snapshot, superseded_by)
                    .await;
            }
        }
    }

    pub async fn reset(&self) {
        self.state.shutdown().await;
    }

    pub async fn is_leader(&self) -> bool {
        self.state.is_ready().await
    }

    pub async fn shutdown(&self) {
        self.state.shutdown().await;
    }

    pub async fn handle_message(&self, msg: VerticalPaxosMessage) -> Option<VerticalPaxosMessage> {
        match msg {
            VerticalPaxosMessage::P1B { .. } => {
                let progress = self.state.handle_p1b(msg).await;
                self.handle_sync_progress(progress).await;
                None
            }
            VerticalPaxosMessage::PROPOSE { from: _, slot, cmd } => {
                self.propose_handler(slot, cmd).await;
                None
            }
            VerticalPaxosMessage::ACK { from, slot, .. } => {
                self.state.handle_ack(from, slot).await;
                None
            }
            _ => self.state.handle_message(msg).await,
        }
    }

    pub fn peers(&self) -> &Arc<VerticalPaxosHandle> {
        &self.peers
    }

    pub fn observer(&self) -> &Arc<dyn PaxosObserver> {
        &self.observer
    }
}

pub enum LeaderProgress {
    None,
    Ready {
        snapshot: Arc<ActivationSnapshot>,
    },
    Superseded {
        snapshot: Arc<ActivationSnapshot>,
        superseded_by: Ballot,
    },
}

impl LeaderProgress {
    pub fn from_synchronizer(synchronizer: &LeaderSynchronizer) -> Self {
        if let Some(superseded_by) = synchronizer.superseded_by() {
            return Self::Superseded {
                snapshot: Arc::clone(synchronizer.snapshot()),
                superseded_by,
            };
        }

        if synchronizer.is_ready() {
            return Self::Ready {
                snapshot: Arc::clone(synchronizer.snapshot()),
            };
        }

        Self::None
    }
}
