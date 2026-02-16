use std::sync::Arc;

use uuid::Uuid;

use crate::{
    common::persistence::Persistence,
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{
        paxos_state::ballot::Ballot,
        pmmc::acceptor::accepted_state::{AcceptedData, AcceptorState},
        pvalue::PValue,
    },
};

pub mod accepted_state;

pub struct Acceptor {
    uuid: Uuid,
    state: AcceptorState,
    observer: Arc<dyn PaxosObserver>,
}

impl Acceptor {
    pub async fn new(uuid: Uuid, observer: Arc<dyn PaxosObserver>) -> anyhow::Result<Self> {
        let state: AcceptedData = Persistence::load(&format!("acceptor_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state = AcceptedData::default();

        Ok(Self {
            uuid,
            observer,
            state: AcceptorState::init(state),
        })
    }

    async fn p1a(&self, ballot: Ballot, start_index: usize) -> Message {
        let (pvalues, a_ballot, updated) = self.state.p1a(ballot, start_index).await;

        if updated {
            self.save().await;
            self.observer.on_event(Event::BallotAdopted {
                id: self.uuid,
                ballot: a_ballot,
            });
        }
        Message::P1B {
            from: self.uuid,
            to: ballot.node_id,
            ballot: a_ballot,
            pvalues: pvalues,
        }
    }

    async fn p2a(&self, pvalue: PValue) -> Message {
        let accepted = self.state.p2a(pvalue).await;

        if accepted {
            self.save().await;
            self.observer.on_event(Event::ProposalAccepted {
                id: self.uuid,
                pvalue,
            });
        }
        Message::P2B {
            from: self.uuid,
            to: pvalue.ballot().node_id,
            ballot: self.state.ballot_num().await,
            pvalue,
        }
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let state = self.state.dump().await;
        Persistence::save(&format!("acceptor_{}.bin", self.uuid), &state).await?;
        Ok(())
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::P1A {
                ballot,
                start_index,
                ..
            } => self.p1a(ballot, start_index).await,
            Message::P2A { pvalue, .. } => self.p2a(pvalue).await,
            _ => Message::NACK,
        }
    }
}
