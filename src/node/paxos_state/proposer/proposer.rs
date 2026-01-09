use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::common::types::{DecreeId, NodeId};
use crate::message::Message;
use crate::monitor::PaxosObserver;
use crate::node::paxos_state::ballot::Ballot;
use crate::node::paxos_state::decree_notes::{DecreeNote, DecreeNotes};
use crate::node::paxos_state::proposer::proposed_decree::ProposedDecree;
use crate::paxos_command::PaxosCommand;

pub struct Proposer {
    id: NodeId,
    uuid: Uuid,
    quorum_size: usize,
    decree_notes: Arc<Mutex<DecreeNotes>>,
    state: Mutex<HashMap<DecreeId, ProposedDecree>>,
    observer: Arc<dyn PaxosObserver>,
}

impl Proposer {
    pub fn new(
        id: NodeId,
        uuid: Uuid,
        quorum_size: usize,
        decree_notes: Arc<Mutex<DecreeNotes>>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        Self {
            id,
            uuid,
            quorum_size,
            decree_notes,
            state: Mutex::new(HashMap::new()),
            observer,
        }
    }

    pub async fn propose(&self, decree_num: DecreeId, cmd: PaxosCommand) -> Message {
        // Increment the ballot number for every new proposal attempt.
        let mut state = self.state.lock().await;

        let entry = state.entry(decree_num).or_insert(ProposedDecree::init(
            self.id,
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
            .or_insert(DecreeNote::new(self.id));
        let next_ballot = notes.next_ballot(highest_accepted);

        // Persist the updated ballot number
        #[cfg(feature = "persistence")]
        {
            if let Err(e) = decree_notes.save(self.uuid).await {
                tracing::error!("[Node {}] Failed to persist decree notes: {}", self.id, e);
            }
        }

        if entry.update_ballot(next_ballot, &cmd) {
            let msg = entry.create_prepare_msg();

            tracing::info!(
                "[Node {}] Proposing decree {} with ballot ({}, {}) value: {:?}",
                self.id,
                decree_num,
                next_ballot.number,
                next_ballot.node_id,
                entry.proposed_value()
            );

            return msg;
        }
        return Message::NACK;
    }

    pub async fn promise(
        &self,
        decree_num: DecreeId,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
        from_node: NodeId,
    ) -> Message {
        let mut state = self.state.lock().await;
        let Some(entry) = state.get_mut(&decree_num) else {
            return Message::NACK;
        };
        let mut decree_notes = self.decree_notes.lock().await;
        let notes = decree_notes
            .state
            .entry(decree_num)
            .or_insert(DecreeNote::new(self.id));
        match ballot.cmp(&notes.last_tried) {
            Ordering::Less => {
                // stale, ignore
                return Message::NACK;
            }
            Ordering::Greater => {
                // preempted — future work: backoff / leader detection
                return Message::NACK;
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
        from_node: NodeId,
        proposed_decree: &mut ProposedDecree,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
    ) -> Message {
        proposed_decree.update_quorum(from_node, accepted_ballot, &accepted_value);

        if proposed_decree.has_met_quorum() {
            return proposed_decree.create_accept_msg();
        }
        return Message::NACK;
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::Promise {
                from,
                decree_num,
                ballot,
                accepted_ballot,
                accepted_value,
            } => {
                self.promise(decree_num, ballot, accepted_ballot, accepted_value, from)
                    .await
            }
            _ => Message::NACK,
        }
    }
}
