use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::Arc,
};

use uuid::Uuid;

use crate::{
    message::Message,
    monitor::PaxosObserver,
    node::{paxos_state::ballot::Ballot, pvalue::PValue},
};
pub struct Scout {
    uuid: Uuid,
    quorum: usize,
    ballot: Ballot,
    observer: Arc<dyn PaxosObserver>,
    adopted: HashSet<Uuid>,
    pvalues: HashMap<usize, PValue>,
}

impl Scout {
    pub fn new(
        uuid: Uuid,
        quorum: usize,
        ballot: Ballot,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        Self {
            uuid,
            observer,
            quorum,
            ballot,
            adopted: HashSet::new(),
            pvalues: HashMap::new(),
        }
    }

    fn p1b(&mut self, acceptor: Uuid, ballot: Ballot, pvalues: Vec<PValue>) -> Message {
        if self.ballot < ballot {
            return Message::NACK;
        }
        if self.ballot == ballot {
            self.pmax(pvalues);
            self.adopted.insert(acceptor);
            return self.is_adopted();
        }
        return Message::PREEMPT {
            from: self.uuid,
            to: self.uuid,
            ballot,
        };
    }

    fn is_adopted(&self) -> Message {
        if self.adopted.len() >= self.quorum {
            let v = self.pvalues.iter().map(|(_, v)| v.clone()).collect();
            return Message::ADOPTED {
                from: self.uuid,
                to: self.uuid,
                ballot: self.ballot,
                pvalues: v,
            };
        }
        Message::NACK
    }

    fn pmax(&mut self, pvalues: Vec<PValue>) {
        for pvalue in pvalues.into_iter() {
            match self.pvalues.entry(pvalue.slot()) {
                Entry::Vacant(v) => {
                    v.insert(pvalue);
                }
                Entry::Occupied(mut o) => {
                    if o.get().ballot() < pvalue.ballot() {
                        o.insert(pvalue);
                    }
                }
            }
        }
    }

    pub async fn handle_message(&mut self, msg: Message) -> Message {
        match msg {
            Message::P1B {
                from,
                ballot,
                pvalues,
                ..
            } => self.p1b(from, ballot, pvalues),
            _ => Message::NACK,
        }
    }
}
