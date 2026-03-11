use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::Arc,
};

use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    node::{pmmc::message::PmmcMessage, pvalue::PValue},
};
pub struct Scout {
    uuid: Uuid,
    quorum: usize,
    ballot: Ballot,
    adopted: HashSet<Uuid>,
    pvalues: HashMap<usize, PValue>,
}

impl Scout {
    pub fn new(
        uuid: Uuid,
        quorum: usize,
        ballot: Ballot,
        _observer: Arc<dyn crate::monitor::PaxosObserver>,
    ) -> Self {
        Self {
            uuid,
            quorum,
            ballot,
            adopted: HashSet::new(),
            pvalues: HashMap::new(),
        }
    }

    pub fn pvalues(&self) -> Vec<PValue> {
        self.pvalues.iter().map(|(_, v)| v.clone()).collect()
    }

    fn p1b(&mut self, acceptor: Uuid, ballot: Ballot, pvalues: Vec<PValue>) -> Option<PmmcMessage> {
        if ballot > self.ballot {
            return Some(PmmcMessage::PREEMPT {
                from: self.uuid,
                to: self.uuid,
                ballot,
            });
        }

        if ballot == self.ballot {
            self.pmax(pvalues);
            self.adopted.insert(acceptor);
            return self.is_adopted();
        }

        None
    }

    fn is_adopted(&self) -> Option<PmmcMessage> {
        if self.adopted.len() >= self.quorum {
            let v = self.pvalues.iter().map(|(_, v)| v.clone()).collect();
            return Some(PmmcMessage::ADOPTED {
                from: self.uuid,
                to: self.uuid,
                ballot: self.ballot,
                pvalues: v,
            });
        }
        None
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

    pub async fn handle_message(&mut self, msg: PmmcMessage) -> Option<PmmcMessage> {
        match msg {
            PmmcMessage::P1B {
                from,
                ballot,
                pvalues,
                ..
            } => self.p1b(from, ballot, pvalues),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "scout_tests.rs"]
mod tests;
