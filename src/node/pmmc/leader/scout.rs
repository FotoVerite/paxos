use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::Arc,
};

use uuid::Uuid;

use crate::{
    message::Message,
    monitor::PaxosObserver,
    node::{classic_paxos::ballot::Ballot, pvalue::PValue},
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

    pub fn pvalues(&self) -> Vec<PValue> {
        self.pvalues.iter().map(|(_, v)| v.clone()).collect()
    }

    fn p1b(&mut self, acceptor: Uuid, ballot: Ballot, pvalues: Vec<PValue>) -> Message {
        if ballot > self.ballot {
            return Message::PREEMPT {
                from: self.uuid,
                to: self.uuid,
                ballot,
            };
        }

        if ballot == self.ballot {
            self.pmax(pvalues);
            self.adopted.insert(acceptor);
            return self.is_adopted();
        }

        Message::NACK
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use crate::{
        message::Message,
        monitor::NoOpObserver,
        node::{classic_paxos::ballot::Ballot, pvalue::PValue},
        paxos_command::PaxosCommand,
    };

    use super::Scout;

    fn cmd(value: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value,
        }
    }

    #[tokio::test]
    async fn preempts_when_acceptor_reports_higher_ballot() {
        let leader = Uuid::new_v4();
        let acceptor = Uuid::new_v4();
        let mut scout = Scout::new(leader, 2, Ballot::new(5, leader), Arc::new(NoOpObserver));

        let reply = scout
            .handle_message(Message::P1B {
                from: acceptor,
                to: leader,
                ballot: Ballot::new(6, acceptor),
                pvalues: vec![],
            })
            .await;

        assert!(
            matches!(reply, Message::PREEMPT { ballot, .. } if ballot == Ballot::new(6, acceptor)),
            "PMMC scout must preempt when any p1b carries a higher ballot"
        );
    }

    #[tokio::test]
    async fn ignores_lower_ballot_responses() {
        let leader = Uuid::new_v4();
        let acceptor = Uuid::new_v4();
        let mut scout = Scout::new(leader, 2, Ballot::new(5, leader), Arc::new(NoOpObserver));

        let reply = scout
            .handle_message(Message::P1B {
                from: acceptor,
                to: leader,
                ballot: Ballot::new(4, acceptor),
                pvalues: vec![],
            })
            .await;

        assert!(
            matches!(reply, Message::NACK),
            "PMMC scout should ignore stale lower-ballot p1b responses"
        );
    }

    #[tokio::test]
    async fn adopts_after_quorum_and_returns_pmax_per_slot() {
        let leader = Uuid::new_v4();
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();
        let leader_ballot = Ballot::new(10, leader);
        let mut scout = Scout::new(leader, 2, leader_ballot, Arc::new(NoOpObserver));
        let lower = PValue::new(7, Ballot::new(8, a1), cmd(100));
        let higher = PValue::new(7, Ballot::new(9, a2), cmd(200));
        let other_slot = PValue::new(9, Ballot::new(7, a1), cmd(300));

        let r1 = scout
            .handle_message(Message::P1B {
                from: a1,
                to: leader,
                ballot: leader_ballot,
                pvalues: vec![lower.clone(), other_slot.clone()],
            })
            .await;
        assert!(matches!(r1, Message::NACK));

        let r2 = scout
            .handle_message(Message::P1B {
                from: a2,
                to: leader,
                ballot: leader_ballot,
                pvalues: vec![higher.clone()],
            })
            .await;

        if let Message::ADOPTED {
            ballot, pvalues, ..
        } = r2
        {
            assert_eq!(ballot, leader_ballot);
            assert_eq!(pvalues.len(), 2);
            assert!(pvalues.contains(&higher));
            assert!(pvalues.contains(&other_slot));
            assert!(!pvalues.contains(&lower));
        } else {
            panic!("expected ADOPTED after quorum");
        }
    }

    #[tokio::test]
    async fn same_acceptor_cannot_satisfy_quorum_twice() {
        let leader = Uuid::new_v4();
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();
        let ballot = Ballot::new(3, leader);
        let mut scout = Scout::new(leader, 2, ballot, Arc::new(NoOpObserver));

        let first = scout
            .handle_message(Message::P1B {
                from: a1,
                to: leader,
                ballot,
                pvalues: vec![],
            })
            .await;
        assert!(matches!(first, Message::NACK));

        let dup = scout
            .handle_message(Message::P1B {
                from: a1,
                to: leader,
                ballot,
                pvalues: vec![],
            })
            .await;
        assert!(matches!(dup, Message::NACK));

        let quorum = scout
            .handle_message(Message::P1B {
                from: a2,
                to: leader,
                ballot,
                pvalues: vec![],
            })
            .await;

        assert!(matches!(quorum, Message::ADOPTED { .. }));
    }
}
