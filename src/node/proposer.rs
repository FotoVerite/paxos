use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::ballot::Ballot;
use crate::paxos_command::PaxosCommand;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct Proposer {
    id: usize,
    quorum: usize,
    state: HashMap<usize, ProposedDecree>,
    observer: Arc<dyn PaxosObserver>,
}

struct ProposedDecree {
    ballot: Ballot,
    highest_seen_ballot: Ballot,
    proposed_value: PaxosCommand,
    chosen: bool,
    votes: HashMap<usize, HashSet<usize>>,
}

impl Proposer {
    pub fn new(id: usize, quorum: usize, observer: Arc<dyn PaxosObserver>) -> Self {
        Self {
            id,
            quorum,
            state: HashMap::new(),
            observer,
        }
    }

    pub fn promise(
        &mut self,
        decree_num: usize,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
    ) -> Message {
        if let Some(proposed_decree) = self.state.get_mut(&decree_num) {
            if ballot != proposed_decree.ballot {
                // Out of sync, we've already sent a Promise
                return Message::NACK;
            }
            if proposed_decree.chosen == true {
                return Message::Accept {
                    from: self.id,
                    decree_num,
                    ballot,
                    value: proposed_decree.proposed_value.clone(),
                };
            }

            if accepted_ballot > proposed_decree.highest_seen_ballot {
                proposed_decree.highest_seen_ballot = accepted_ballot;
                proposed_decree.proposed_value = accepted_value.clone();
            }
            let votes = proposed_decree.votes.entry(ballot.number).or_default();
            votes.insert(ballot.node_id);

            if votes.len() >= self.quorum {
                proposed_decree.chosen = true;
                return Message::Accept {
                    from: self.id,
                    decree_num,
                    ballot,
                    value: proposed_decree.proposed_value.clone(),
                };
            }
            return Message::NACK;
        }
        // We are in a bad state
        return Message::NACK;
    }

    pub fn propose(&mut self, decree_num: usize, cmd: PaxosCommand) -> Message {
        if self.state.contains_key(&decree_num) {
            // We're in a bad state
            return Message::NACK;
        }
        let ballot = Ballot {
            number: 1,
            node_id: self.id,
        };
        let proposed_decree = ProposedDecree {
            ballot,
            chosen: false,
            highest_seen_ballot: ballot,
            proposed_value: cmd.clone(),
            votes: HashMap::new(),
        };
        self.state.insert(decree_num, proposed_decree);

        self.observer.on_event(Event::Proposal {
            decree_num,
            id: self.id,
            value: cmd.clone(),
        });

        Message::Prepare {
            from: self.id,
            decree_num,
            ballot: ballot,
        }
    }

    pub async fn handle_message(&mut self, msg: Message) -> Message {
        match msg {
            Message::Promise {
                decree_num,
                ballot,
                accepted_ballot,
                accepted_value,
                ..
            } => self.promise(decree_num, ballot, accepted_ballot, accepted_value),
            _ => Message::NACK,
        }
    }
}
