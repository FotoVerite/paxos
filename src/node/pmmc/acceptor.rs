use std::sync::Arc;

use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    common::persistence::NodePersistence,
    monitor::{Event, PaxosObserver},
    node::{
        pmmc::{
            acceptor::accepted_state::{AcceptedData, AcceptorState},
            message::PmmcMessage,
        },
        pvalue::PValue,
    },
};

pub mod accepted_state;

pub struct Acceptor {
    uuid: Uuid,
    persistence: NodePersistence,
    state: AcceptorState,
    observer: Arc<dyn PaxosObserver>,
}

impl Acceptor {
    pub async fn new(
        uuid: Uuid,
        persistence: NodePersistence,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let state: AcceptedData = persistence.load("acceptor.bin").await?;

        #[cfg(not(feature = "persistence"))]
        let state = AcceptedData::default();

        Ok(Self {
            uuid,
            persistence,
            observer,
            state: AcceptorState::init(state),
        })
    }

    async fn p1a(&self, ballot: Ballot, start_index: usize) -> PmmcMessage {
        let (pvalues, a_ballot, updated) = self.state.p1a(ballot, start_index).await;

        if updated {
            let _ = self.save().await;
            self.observer.on_event(Event::BallotAdopted {
                id: self.uuid,
                ballot: a_ballot,
            });
        }
        PmmcMessage::P1B {
            from: self.uuid,
            to: ballot.node_id,
            ballot: a_ballot,
            pvalues: pvalues,
        }
    }

    async fn p2a(&self, pvalue: PValue) -> PmmcMessage {
        let accepted = self.state.p2a(pvalue.clone()).await;

        if accepted {
            let _ = self.save().await;
            self.observer.on_event(Event::ProposalAccepted {
                id: self.uuid,
                pvalue: pvalue.clone(),
            });
        }
        PmmcMessage::P2B {
            from: self.uuid,
            to: pvalue.ballot().node_id,
            ballot: self.state.ballot_num().await,
            pvalue,
        }
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let state = self.state.dump().await;
        self.persistence.save("acceptor.bin", &state).await?;
        Ok(())
    }

    pub async fn handle_message(&self, msg: PmmcMessage) -> Option<PmmcMessage> {
        match msg {
            PmmcMessage::P1A {
                ballot,
                start_index,
                ..
            } => Some(self.p1a(ballot, start_index).await),
            PmmcMessage::P2A { pvalue, .. } => Some(self.p2a(pvalue).await),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use crate::{
        common::ballot::Ballot,
        monitor::NoOpObserver,
        node::{pmmc::message::PmmcMessage, pvalue::PValue},
        paxos_command::PaxosCommand,
    };

    use super::Acceptor;

    type Message = PmmcMessage;

    async fn new_acceptor() -> Acceptor {
        let uuid = Uuid::new_v4();
        Acceptor::new(
            uuid,
            crate::common::persistence::ClusterPersistence::for_test("pmmc_acceptor").node(uuid),
            Arc::new(NoOpObserver),
        )
        .await
        .expect("acceptor should initialize")
    }

    fn cmd(value: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value,
        }
    }

    #[tokio::test]
    async fn p1a_adopts_only_strictly_higher_ballots() {
        let acceptor = new_acceptor().await;
        let leader = Uuid::new_v4();
        let acceptor_id = Uuid::new_v4();
        let b5 = Ballot::new(5, leader);
        let b3 = Ballot::new(3, acceptor_id);

        let first = acceptor
            .handle_message(PmmcMessage::P1A {
                from: leader,
                ballot: b5,
                start_index: 0,
            })
            .await
            .expect("p1a should return p1b");
        assert!(matches!(
            first,
            PmmcMessage::P1B {
                to,
                ballot,
                pvalues,
                ..
            } if to == leader && ballot == b5 && pvalues.is_empty()
        ));

        let second = acceptor
            .handle_message(PmmcMessage::P1A {
                from: acceptor_id,
                ballot: b3,
                start_index: 0,
            })
            .await
            .expect("p1a should return p1b");
        assert!(matches!(
            second,
            PmmcMessage::P1B { to, ballot, .. } if to == acceptor_id && ballot == b5
        ));
    }

    #[tokio::test]
    async fn p2a_accepts_only_when_ballot_equals_current_ballot() {
        let acceptor = new_acceptor().await;
        let leader = Uuid::new_v4();
        let b7 = Ballot::new(7, leader);
        let b6 = Ballot::new(6, leader);
        let b8 = Ballot::new(8, leader);
        let v7 = PValue::new(1, b7, cmd(7));
        let v6 = PValue::new(2, b6, cmd(6));
        let v8 = PValue::new(3, b8, cmd(8));

        let _ = acceptor
            .handle_message(PmmcMessage::P1A {
                from: leader,
                ballot: b7,
                start_index: 0,
            })
            .await;

        let _ = acceptor
            .handle_message(PmmcMessage::P2A {
                from: leader,
                pvalue: v7.clone(),
            })
            .await;
        let _ = acceptor
            .handle_message(PmmcMessage::P2A {
                from: leader,
                pvalue: v6,
            })
            .await;
        let high = acceptor
            .handle_message(PmmcMessage::P2A {
                from: leader,
                pvalue: v8.clone(),
            })
            .await
            .expect("p2a should return p2b");

        assert!(
            matches!(high, PmmcMessage::P2B { ballot, .. } if ballot == b7),
            "paper rule: p2a with b > ballot_num should not advance ballot_num"
        );

        let check = acceptor
            .handle_message(PmmcMessage::P1A {
                from: leader,
                ballot: b7,
                start_index: 0,
            })
            .await
            .expect("p1a should return p1b");

        if let PmmcMessage::P1B {
            ballot, pvalues, ..
        } = check
        {
            assert_eq!(ballot, b7);
            assert!(
                pvalues.contains(&v7),
                "value at current ballot should be accepted"
            );
            assert!(
                !pvalues.contains(&v8),
                "paper rule: higher ballot p2a must not be accepted before adoption"
            );
        } else {
            panic!("expected P1B");
        }
    }

    #[tokio::test]
    async fn p1a_returns_previously_accepted_pvalues() {
        let acceptor = new_acceptor().await;
        let leader = Uuid::new_v4();
        let b4 = Ballot::new(4, leader);
        let v1 = PValue::new(1, b4, cmd(10));
        let v2 = PValue::new(2, b4, cmd(20));

        let _ = acceptor
            .handle_message(Message::P1A {
                from: leader,
                ballot: b4,
                start_index: 0,
            })
            .await;
        let _ = acceptor
            .handle_message(Message::P2A {
                from: leader,
                pvalue: v1.clone(),
            })
            .await;
        let _ = acceptor
            .handle_message(Message::P2A {
                from: leader,
                pvalue: v2.clone(),
            })
            .await;

        let reply = acceptor
            .handle_message(Message::P1A {
                from: leader,
                ballot: b4,
                start_index: 0,
            })
            .await
            .expect("p1a should return p1b");

        if let PmmcMessage::P1B {
            pvalues, ballot, ..
        } = reply
        {
            assert_eq!(ballot, b4);
            assert_eq!(pvalues.len(), 2);
            assert!(pvalues.contains(&v1));
            assert!(pvalues.contains(&v2));
        } else {
            panic!("expected P1B");
        }
    }

    #[tokio::test]
    async fn p1a_respects_start_index_filter() {
        let acceptor = new_acceptor().await;
        let leader = Uuid::new_v4();
        let b4 = Ballot::new(4, leader);
        let v1 = PValue::new(1, b4, cmd(10));
        let v2 = PValue::new(2, b4, cmd(20));
        let v4 = PValue::new(4, b4, cmd(40));

        let _ = acceptor
            .handle_message(Message::P1A {
                from: leader,
                ballot: b4,
                start_index: 0,
            })
            .await;
        let _ = acceptor
            .handle_message(Message::P2A {
                from: leader,
                pvalue: v1.clone(),
            })
            .await;
        let _ = acceptor
            .handle_message(Message::P2A {
                from: leader,
                pvalue: v2.clone(),
            })
            .await;
        let _ = acceptor
            .handle_message(Message::P2A {
                from: leader,
                pvalue: v4.clone(),
            })
            .await;

        let reply = acceptor
            .handle_message(Message::P1A {
                from: leader,
                ballot: b4,
                start_index: 2,
            })
            .await
            .expect("p1a should return p1b");

        if let PmmcMessage::P1B { pvalues, .. } = reply {
            assert_eq!(pvalues.len(), 2);
            assert!(pvalues.contains(&v2));
            assert!(pvalues.contains(&v4));
            assert!(!pvalues.contains(&v1));
        } else {
            panic!("expected P1B");
        }
    }

    #[tokio::test]
    async fn duplicate_p2a_is_idempotent_for_same_pvalue() {
        let acceptor = new_acceptor().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(11, leader);
        let value = PValue::new(7, ballot, cmd(77));

        let _ = acceptor
            .handle_message(Message::P1A {
                from: leader,
                ballot,
                start_index: 0,
            })
            .await;
        let _ = acceptor
            .handle_message(Message::P2A {
                from: leader,
                pvalue: value.clone(),
            })
            .await;
        let _ = acceptor
            .handle_message(Message::P2A {
                from: leader,
                pvalue: value.clone(),
            })
            .await;

        let reply = acceptor
            .handle_message(Message::P1A {
                from: leader,
                ballot,
                start_index: 0,
            })
            .await
            .expect("p1a should return p1b");

        if let PmmcMessage::P1B { pvalues, .. } = reply {
            assert_eq!(pvalues.len(), 1);
            assert_eq!(pvalues[0], value);
        } else {
            panic!("expected P1B");
        }
    }

    #[tokio::test]
    async fn unhandled_message_returns_none() {
        let acceptor = new_acceptor().await;
        let resp = acceptor
            .handle_message(Message::PROPOSE {
                from: Uuid::new_v4(),
                slot: 0,
                cmd: PaxosCommand::NOOP,
            })
            .await;
        assert!(resp.is_none());
    }
}
