use crate::{
    common::types::DecreeId,
    message::Message,
    monitor::{Event, PaxosObserver},
    node::classic_paxos::{ballot::Ballot, proposer::quorum::Quorum},
    paxos_command::PaxosCommand,
};
use std::{collections::HashSet, sync::Arc};
use uuid::Uuid;

pub struct ProposedDecree {
    node_uuid: Uuid,
    decree_num: DecreeId,
    current_ballot: Ballot,
    proposed_value: PaxosCommand,
    quorum: Quorum,
    observer: Arc<dyn PaxosObserver>,
}

impl ProposedDecree {
    pub fn init(
        node_uuid: Uuid,
        decree_num: DecreeId,
        quorum_size: usize,
        proposed_value: &PaxosCommand,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        return Self {
            node_uuid,
            decree_num,
            proposed_value: proposed_value.clone(),
            quorum: Quorum::init(quorum_size),
            current_ballot: Ballot::default(),
            observer,
        };
    }
    pub fn update_ballot(&mut self, ballot: Ballot, value: &PaxosCommand) -> bool {
        if ballot <= self.current_ballot {
            return false;
        }
        self.current_ballot = ballot;
        self.proposed_value = value.clone();
        self.quorum.reset();
        true
    }

    pub fn update_quorum(&mut self, node_id: Uuid, ballot: Ballot, value: &PaxosCommand) {
        if self.quorum.update(node_id, ballot) {
            self.proposed_value = value.clone();
        }
    }

    pub fn has_met_quorum(&self) -> bool {
        return self.quorum.has_met_quorum();
    }

    pub fn highest_accepted(&self) -> Ballot {
        return self.quorum.highest_accepted();
    }

    pub fn create_prepare_msg(&mut self) -> Message {
        let msg = Message::Prepare {
            from: self.node_uuid,
            decree_num: self.decree_num,
            ballot: self.current_ballot,
        };

        self.observer.on_event(Event::Proposal {
            decree_num: self.decree_num,
            id: self.node_uuid,
            value: self.proposed_value(),
            created_at: crate::monitor::current_timestamp_millis(),
        });

        msg
    }

    pub fn create_accept_msg(&mut self) -> Message {
        let msg = Message::Accept {
            from: self.node_uuid,
            decree_num: self.decree_num,
            ballot: self.current_ballot,
            value: self.proposed_value(),
            quorum: self.quorum_set(),
        };

        self.observer.on_event(Event::Accept {
            id: self.node_uuid,
            decree_num: self.decree_num,
            ballot: self.current_ballot.number,
            quorum: self.quorum_set(),
            value: self.proposed_value(),
            created_at: crate::monitor::current_timestamp_millis(),
        });

        msg
    }

    pub fn proposed_value(&self) -> PaxosCommand {
        self.proposed_value.clone()
    }

    pub fn quorum_set(&self) -> HashSet<Uuid> {
        self.quorum.quorum_set()
    }
}
