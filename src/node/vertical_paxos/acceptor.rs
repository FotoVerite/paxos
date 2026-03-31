use std::sync::Arc;

use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    common::persistence::NodePersistence,
    monitor::{Event, PaxosObserver},
    node::{
        pvalue::PValue,
        vertical_paxos::{
            acceptor::record::{AcceptorRecord, AcceptorState},
            message::VerticalPaxosMessage,
        },
    },
};

pub mod record;

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
        #[cfg(feature = "persistence")]
        let state: AcceptorRecord = persistence.load("acceptor.bin").await?;

        #[cfg(not(feature = "persistence"))]
        let state = AcceptorRecord::default();

        Ok(Self {
            uuid,
            persistence,
            observer,
            state: AcceptorState::init(state),
        })
    }

    async fn p1a(&self, ballot: Ballot, start_index: usize) -> VerticalPaxosMessage {
        let (pvalues, a_ballot, updated) = self.state.p1a(ballot, start_index).await;

        if updated {
            let _ = self.save().await;
            self.observer.on_event(Event::BallotAdopted {
                id: self.uuid,
                ballot: a_ballot,
            });
        }
        VerticalPaxosMessage::P1B {
            from: self.uuid,
            to: ballot.node_id,
            ballot: a_ballot,
            pvalues: pvalues,
        }
    }

    async fn p2a(&self, pvalue: PValue) -> VerticalPaxosMessage {
        let accepted = self.state.p2a(pvalue.clone()).await;

        if accepted {
            let _ = self.save().await;
            self.observer.on_event(Event::ProposalAccepted {
                id: self.uuid,
                pvalue: pvalue.clone(),
            });
        }
        VerticalPaxosMessage::P2B {
            from: self.uuid,
            to: pvalue.ballot().node_id,
            ballot: self.state.promised_ballot().await,
            pvalue,
        }
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let state = self.state.dump().await;
        self.persistence.save("acceptor.bin", &state).await?;
        Ok(())
    }

    pub async fn handle_message(&self, msg: VerticalPaxosMessage) -> Option<VerticalPaxosMessage> {
        match msg {
            VerticalPaxosMessage::P1A {
                ballot,
                start_index,
                ..
            } => Some(self.p1a(ballot, start_index).await),
            VerticalPaxosMessage::P2A { pvalue, .. } => Some(self.p2a(pvalue).await),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
