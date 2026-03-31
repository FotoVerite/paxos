use std::{collections::HashSet, sync::Arc};

use uuid::Uuid;

use crate::{
    cluster::vertical::activation::ActivationSnapshot,
    common::ballot::Ballot,
    node::vertical_paxos::{message::VerticalPaxosMessage, transport::VerticalPaxosHandle},
};

use super::scout_round::ScoutRound;

pub struct WriteQuorumSynchronizer {
    leader_id: Uuid,
    peers: Arc<VerticalPaxosHandle>,
    snapshot: Arc<ActivationSnapshot>,
    ballot: Ballot,
    required: HashSet<Uuid>,
    round: Option<ScoutRound>,
    ready: bool,
    superseded_by: Option<Ballot>,
}

impl WriteQuorumSynchronizer {
    pub fn new(
        leader_id: Uuid,
        peers: Arc<VerticalPaxosHandle>,
        snapshot: Arc<ActivationSnapshot>,
    ) -> Self {
        Self {
            leader_id,
            peers,
            ballot: snapshot.configuration().ballot(),
            required: snapshot
                .configuration()
                .write_quorum()
                .members()
                .iter()
                .copied()
                .collect(),
            snapshot,
            round: None,
            ready: false,
            superseded_by: None,
        }
    }

    pub async fn start(&mut self) {
        self.shutdown().await;
        self.superseded_by = None;
        self.ready = false;
        self.round = Some(ScoutRound::new(
            self.leader_id,
            Arc::clone(&self.peers),
            self.ballot,
            self.snapshot.configuration().start_index(),
            self.required.clone(),
        ));
    }

    pub async fn handle_message(&mut self, msg: &VerticalPaxosMessage) {
        let Some(round) = self.round.as_ref() else {
            return;
        };

        round.handle_message(msg).await;
        self.superseded_by = round.superseded_by().await;
        self.ready = self.superseded_by.is_none() && round.is_complete().await;
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn superseded_by(&self) -> Option<Ballot> {
        self.superseded_by
    }
    pub async fn shutdown(&mut self) {
        if let Some(round) = self.round.take() {
            round.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests;
