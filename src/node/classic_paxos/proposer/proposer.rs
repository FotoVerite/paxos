use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::common::ballot::Ballot;
use crate::common::persistence::NodePersistence;
use crate::common::types::DecreeId;
use crate::monitor::PaxosObserver;
use crate::node::classic_paxos::decree_notes::{DecreeNote, DecreeNotes};
use crate::node::classic_paxos::message::ClassicMessage;
use crate::node::classic_paxos::proposer::proposed_decree::ProposedDecree;
use crate::paxos_command::PaxosCommand;

pub struct Proposer {
    uuid: Uuid,
    quorum_size: usize,
    persistence: NodePersistence,
    decree_notes: Arc<Mutex<DecreeNotes>>,
    state: Mutex<HashMap<DecreeId, ProposedDecree>>,
    observer: Arc<dyn PaxosObserver>,
}

impl Proposer {
    pub fn new(
        uuid: Uuid,
        quorum_size: usize,
        persistence: NodePersistence,
        decree_notes: Arc<Mutex<DecreeNotes>>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        Self {
            uuid,
            quorum_size,
            persistence,
            decree_notes,
            state: Mutex::new(HashMap::new()),
            observer,
        }
    }

    pub async fn propose(&self, decree_num: DecreeId, cmd: PaxosCommand) -> ClassicMessage {
        // Increment the ballot number for every new proposal attempt.
        let mut state = self.state.lock().await;

        let entry = state.entry(decree_num).or_insert(ProposedDecree::init(
            self.uuid,
            decree_num,
            self.quorum_size,
            &cmd,
            Arc::clone(&self.observer),
        ));
        let highest_accepted = entry.highest_accepted().number;

        let mut decree_notes = self.decree_notes.lock().await;
        let notes = decree_notes
            .state
            .entry(decree_num)
            .or_insert(DecreeNote::new(self.uuid));
        let next_ballot = notes.next_ballot(highest_accepted);

        // Persist the updated ballot number
        #[cfg(feature = "persistence")]
        {
            if let Err(e) = decree_notes.save(&self.persistence).await {
                tracing::error!("[Node {}] Failed to persist decree notes: {}", self.uuid, e);
            }
        }

        if entry.update_ballot(next_ballot, &cmd) {
            let msg = entry.create_prepare_msg();

            tracing::info!(
                "[Node {}] Proposing decree {} with ballot ({}, {}) value: {:?}",
                self.uuid,
                decree_num,
                next_ballot.number,
                next_ballot.node_id,
                entry.proposed_value()
            );

            return msg;
        }
        tracing::warn!(
            "[Node {}] Proposal ballot did not advance for decree {}; reusing current ballot",
            self.uuid,
            decree_num
        );
        entry.create_prepare_msg()
    }

    pub async fn promise(
        &self,
        decree_num: DecreeId,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
        from_node: Uuid,
    ) -> Option<ClassicMessage> {
        let mut state = self.state.lock().await;
        let Some(entry) = state.get_mut(&decree_num) else {
            return None;
        };
        let mut decree_notes = self.decree_notes.lock().await;
        let notes = decree_notes
            .state
            .entry(decree_num)
            .or_insert(DecreeNote::new(self.uuid));
        match ballot.cmp(&notes.last_tried) {
            Ordering::Less => {
                // stale, ignore
                return None;
            }
            Ordering::Greater => {
                // preempted — future work: backoff / leader detection
                return None;
            }
            Ordering::Equal => {
                return self
                    .prepare(from_node, entry, accepted_ballot, accepted_value)
                    .await;
            }
        }
    }

    async fn prepare(
        &self,
        from_node: Uuid,
        proposed_decree: &mut ProposedDecree,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
    ) -> Option<ClassicMessage> {
        proposed_decree.update_quorum(from_node, accepted_ballot, &accepted_value);

        if proposed_decree.has_met_quorum() {
            return Some(proposed_decree.create_accept_msg());
        }
        None
    }

    pub async fn handle_message(
        &self,
        msg: impl Into<Option<ClassicMessage>>,
    ) -> Option<ClassicMessage> {
        let Some(msg) = msg.into() else {
            return None;
        };
        match msg {
            ClassicMessage::Promise {
                from,
                decree_num,
                ballot,
                accepted_ballot,
                accepted_value,
            } => {
                self.promise(decree_num, ballot, accepted_ballot, accepted_value, from)
                    .await
            }
            _ => None,
        }
    }
}
