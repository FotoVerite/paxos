use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::Ok;
use tokio_util::sync::CancellationToken;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    message::Message,
    monitor::{Event, PaxosObserver},
    node::paxos_state::{ballot::Ballot, proposer::quorum::Quorum},
    paxos_command::PaxosCommand,
};

pub struct ProposedDecree {
    node_id: usize,
    decree_num: usize,
    current_ballot: Ballot,
    proposed_value: PaxosCommand,
    quorum: Quorum,
    observer: Arc<dyn PaxosObserver>,
}

impl ProposedDecree {
    pub fn init(
        node_id: usize,
        decree_num: usize,
        quorum_size: usize,
        proposed_value: &PaxosCommand,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        return Self {
            node_id,
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

    pub fn update_quorum(&mut self, node_id: usize, ballot: Ballot, value: &PaxosCommand) {
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
            from: self.node_id,
            decree_num: self.decree_num,
            ballot: self.current_ballot,
        };

        self.observer.on_event(Event::Proposal {
            decree_num: self.decree_num,
            id: self.node_id,
            value: self.proposed_value(),
            created_at: crate::monitor::current_timestamp_millis(),
        });

        msg
    }

    pub fn create_accept_msg(&mut self) -> Message {
        let msg = Message::Accept {
            from: self.node_id,
            decree_num: self.decree_num,
            ballot: self.current_ballot,
            value: self.proposed_value(),
            quorum: self.quorum_set(),
        };

        self.observer.on_event(Event::Accept {
            id: self.node_id,
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

    pub fn quorum_set(&self) -> HashSet<usize> {
        self.quorum.quorum_set()
    }
}

async fn retry(
    token: CancellationToken,
    peers: Arc<NetworkSimulator>,
    msg: Message,
) -> anyhow::Result<()> {
    let mut delay_ms: u64 = 100;

    loop {
        tokio::select! {

            _ = token.cancelled() => {
                tracing::debug!("task shutting down");
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {
            // retry the current ballot

                delay_ms *= 2; // cap backoff at 5s
                if delay_ms <= 800 {
                     tokio::select! {
                        _ = token.cancelled() => {
                            return Ok(());
                        }
                        _ = peers.broadcast(msg.clone()) => {}
                    }
                 }
            }
        }
    }
}
