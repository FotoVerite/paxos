use std::{sync::Arc};
use tokio::{sync::Mutex};
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    common::types::{NodeId, DecreeId},
    message::Message,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        inflight_proposals::{InflightProposal, InflightProposals},
        paxos_state::{
            acceptor::Acceptor, decree_notes::DecreeNotes, learner::Learner, ledger::Ledger,
            proposer::proposer::Proposer,
        },
    },
    paxos_command::PaxosCommand,
};

pub struct PaxosState {
    id: NodeId,
    proposer: Proposer,
    acceptor: Acceptor,
    learner: Learner,
    ledger: Ledger,
    peers: Arc<NetworkSimulator>,
    inflight_proposals: Arc<InflightProposals>,
}

impl PaxosState {
    pub async fn init(
        id: NodeId,
        uuid: Uuid,
        quorum: usize,
        peers: Arc<NetworkSimulator>,
        inflight_proposals: Arc<InflightProposals>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let ledger = Ledger::init(id, uuid).await?;
        let decree_notes = Arc::new(Mutex::new(DecreeNotes::load_or_init(uuid).await?));
        let state = Self {
            id,
            proposer: Proposer::new(
                id,
                uuid,
                quorum,
                Arc::clone(&decree_notes),
                Arc::clone(&observer),
            ),
            acceptor: Acceptor::new(id, uuid, Arc::clone(&observer)).await?,
            learner: Learner::new(id, quorum, Arc::clone(&decree_notes), Arc::clone(&observer)),
            ledger,
            peers,
            inflight_proposals,
        };
        Ok(state)
    }

    pub async fn propose(&self, cmd: PaxosCommand, decree_num: Option<DecreeId>) -> InflightProposal {
        let num = match decree_num {
            Some(num) => num,
            None => self.next().await,
        };
        let msg = self.proposer.propose(num, cmd.clone()).await;
        self.peers.broadcast(msg).await;
        self.inflight_proposals.insert(num, cmd.clone()).await
    }

    pub async fn retry_proposal(&self, inflight: InflightProposal)  {
        let msg = self.proposer.propose(inflight.decree_num, inflight.cmd.clone()).await;
        self.peers.broadcast(msg).await;
    }

    pub async fn next(&self) -> DecreeId {
        self.ledger.next().await
    }

    pub async fn get_next_gap(&self) -> Option<DecreeId> {
        self.ledger.next_gap().await
    }

    pub async fn handle_message(&self, msg: Message) {
        match msg {
            Message::Promise { .. } => {
                tracing::debug!("[Node {}] Handling Promise message", self.id);
                let reply = self.proposer.handle_message(msg).await;
                self.peers.broadcast(reply).await;
            }
            Message::Prepare { from, .. } | Message::Accept { from, .. } => {
                tracing::debug!(
                    "[Node {}] Handling Prepare/Accept from node {}",
                    self.id,
                    from
                );
                let reply = self.acceptor.handle_message(msg).await;
                if let Message::Accepted { .. } = &reply {
                    tracing::debug!(
                        "[Node {}] Acceptor replied with Accepted, sending back to {}",
                        self.id,
                        from
                    );
                } else {
                    tracing::debug!("[Node {}] Acceptor replied with NACK", self.id);
                }
                self.peers.send(from, reply).await;
            }
            Message::Accepted { .. } => {
                tracing::debug!("[Node {}] Handling Accepted message", self.id);
                let reply = self.learner.handle_message(msg, &self.ledger).await;
                if let Message::Success { .. } = &reply {
                    tracing::debug!(
                        "[Node {}] Learner reached quorum, broadcasting Success",
                        self.id
                    );
                    self.peers.broadcast(reply).await;
                } else {
                    tracing::debug!("[Node {}] Learner did not reach quorum yet", self.id);
                }
            }
            Message::Success { decree_num, .. } => {
                tracing::debug!("[Node {}] Handling Success message", self.id);
                self.learner.learn_decree(msg, &self.ledger).await;
                self.inflight_proposals.cancel(decree_num).await
            }
            _ => {
                tracing::debug!("[Node {}] Ignoring unhandled message type", self.id);
            }
        }
    }

    pub async fn emit_ledger_state(&self, id: NodeId, observer: Arc<dyn PaxosObserver>) {
        let initial_decrees = self.ledger.get_initial_decrees().await;
        for (decree_num, value) in initial_decrees {
            observer.on_event(Event::InitialDecree {
                id,
                decree_num,
                value,
                created_at: current_timestamp_millis(),
            });
        }
    }
}
