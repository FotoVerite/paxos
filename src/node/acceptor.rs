use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::ballot::{Ballot};
use crate::paxos_command::PaxosCommand;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Acceptor {
    id: usize,
    state: HashMap<usize, AcceptedDecree>,
    observer: Arc<dyn PaxosObserver>,
}

struct AcceptedDecree {
    min_ballot: Ballot,
    accepted_ballot: Ballot,
    accepted_value: PaxosCommand,
}

impl Default for AcceptedDecree {
    fn default() -> AcceptedDecree {
        AcceptedDecree {
            min_ballot: Ballot {
                number: 0,
                node_id: 0,
            },
            accepted_ballot: Ballot {
                number: 0,
                node_id: 0,
            },
            accepted_value: PaxosCommand::NOOP,
        }
    }
}

impl Acceptor {
    pub fn new(id: usize, observer: Arc<dyn PaxosObserver>) -> Self {
        Self {
            id,
            state: HashMap::new(),
            observer,
        }
    }

    fn prepare(&mut self, decree_num: usize, ballot: Ballot) -> Message {
        let decree = self.state.entry(decree_num).or_default();
        if ballot > decree.min_ballot {
            decree.min_ballot = ballot;

            self.observer.on_event(Event::Promise {
                decree_num,
                id: self.id,
                ballot: ballot.number,
            });

            return Message::Promise {
                decree_num,
                ballot,
                accepted_ballot: decree.accepted_ballot,
                accepted_value: decree.accepted_value.clone(),
            };
        }
        return Message::NACK;
    }

    fn accept(&mut self, decree_num: usize, ballot: Ballot, cmd: PaxosCommand) -> Message {
        let decree = self.state.entry(decree_num).or_default();

        if ballot >= decree.min_ballot {
            decree.min_ballot = ballot;
            decree.accepted_ballot = ballot;
            decree.accepted_value = cmd.clone();

            self.observer.on_event(Event::Accept {
                decree_num,
                id: self.id,
                ballot: ballot.number,
                value: cmd.clone(),
            });

            return Message::Accepted {
                decree_num,
                ballot,
                value: cmd,
            };
        }
        return Message::NACK;
    }

    pub async fn handle_message(&mut self, msg: Message) -> Message {
        match msg {
            Message::Prepare { decree_num, ballot } => return self.prepare(decree_num, ballot),
            Message::Accept {
                decree_num,
                ballot,
                value,
            } => return self.accept(decree_num, ballot, value),
            _ => Message::NACK,
        }
    }
}
