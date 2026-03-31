use std::sync::Arc;

use crate::{
    cluster::vertical::activation::ActivationSnapshot,
    common::ballot::Ballot,
    node::vertical_paxos::{message::VerticalPaxosMessage, transport::VerticalPaxosHandle},
};

use self::{recovery::RecoverySynchronizer, write_quorum::WriteQuorumSynchronizer};

pub mod recovery;
pub mod scout_round;
pub mod write_quorum;

#[cfg(test)]
mod tests;

pub struct LeaderSynchronizer {
    snapshot: Arc<ActivationSnapshot>,
    peers: Arc<VerticalPaxosHandle>,
    recovery: RecoverySynchronizer,
    write_quorum: WriteQuorumSynchronizer,
    in_progress: bool,
    superseded_by: Option<Ballot>,
}

impl LeaderSynchronizer {
    pub fn new(
        leader_id: uuid::Uuid,
        peers: Arc<VerticalPaxosHandle>,
        snapshot: Arc<ActivationSnapshot>,
    ) -> Self {
        Self {
            snapshot: Arc::clone(&snapshot),
            peers: Arc::clone(&peers),
            recovery: RecoverySynchronizer::new(
                leader_id,
                Arc::clone(&peers),
                Arc::clone(&snapshot),
            ),
            write_quorum: WriteQuorumSynchronizer::new(leader_id, peers, Arc::clone(&snapshot)),
            in_progress: false,
            superseded_by: None,
        }
    }

    pub fn snapshot(&self) -> &Arc<ActivationSnapshot> {
        &self.snapshot
    }

    pub fn peers(&self) -> Arc<VerticalPaxosHandle> {
        Arc::clone(&self.peers)
    }

    pub async fn start(&mut self) {
        self.in_progress = true;
        self.superseded_by = None;
        self.recovery.start().await;
        self.write_quorum.start().await;
    }

    pub fn is_in_progress(&self) -> bool {
        self.in_progress
    }

    pub fn is_ready(&self) -> bool {
        self.superseded_by.is_none() && self.recovery.is_complete() && self.write_quorum.is_ready()
    }

    pub fn superseded_by(&self) -> Option<Ballot> {
        self.superseded_by
    }

    pub async fn handle_message(&mut self, msg: VerticalPaxosMessage) {
        if !self.in_progress {
            return;
        }

        self.recovery.handle_message(&msg).await;
        self.write_quorum.handle_message(&msg).await;

        let superseded_by = max_ballot(
            self.recovery.superseded_by(),
            self.write_quorum.superseded_by(),
        );

        if let Some(ballot) = superseded_by {
            self.superseded_by = Some(ballot);
            self.in_progress = false;
            self.recovery.shutdown().await;
            self.write_quorum.shutdown().await;
            return;
        }

        if self.is_ready() {
            self.in_progress = false;
            self.recovery.shutdown().await;
            self.write_quorum.shutdown().await;
        }
    }

    pub async fn shutdown(&mut self) {
        self.in_progress = false;
        self.superseded_by = None;
        self.recovery.shutdown().await;
        self.write_quorum.shutdown().await;
    }
}

fn max_ballot(left: Option<Ballot>, right: Option<Ballot>) -> Option<Ballot> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(ballot), None) | (None, Some(ballot)) => Some(ballot),
        (None, None) => None,
    }
}
