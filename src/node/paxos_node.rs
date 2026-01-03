use std::sync::Arc;

use tokio::sync::{Mutex, mpsc::Receiver};

use crate::{
    cluster::network_simulator::NetworkSimulator,
    message::Message,
    monitor::PaxosObserver,
    node::{
        acceptor::Acceptor, decree_notes::DecreeNotes, learner::Learner, ledger::Ledger,
        proposer::Proposer,
    },
    paxos_command::PaxosCommand,
};

pub struct PaxosNode {
    #[allow(dead_code)]
    id: usize,
    rx: Option<Receiver<Message>>,
    state: Arc<PaxosState>,
}

pub struct PaxosState {
    id: usize,
    peers: Arc<NetworkSimulator>, // Track the highest accepted_ballot from Promises
    proposer: Proposer,
    acceptor: Acceptor,
    learner: Learner,
    ledger: Ledger,
}

impl PaxosState {
    pub async fn handle_message(&self, msg: Message) {
        match msg {
            Message::Promise { .. } => {
                tracing::debug!("[Node {}] Handling Promise message", self.id);
                let reply = self.proposer.handle_message(msg).await;
                if let Message::Accept { quorum, .. } = &reply {
                    tracing::debug!("[Node {}] Sending Accept to quorum: {:?}", self.id, quorum);
                    self.peers.broadcast_to(&reply, quorum).await;
                }
            }
            Message::Prepare { from, .. } | Message::Accept { from, .. } => {
                tracing::debug!("[Node {}] Handling Prepare/Accept from node {}", self.id, from);
                let reply = self.acceptor.handle_message(msg).await;
                if let Message::Accepted { .. } = &reply {
                    tracing::debug!("[Node {}] Acceptor replied with Accepted, sending back to {}", self.id, from);
                } else {
                    tracing::debug!("[Node {}] Acceptor replied with NACK", self.id);
                }
                self.peers.send(from, reply).await;               
            }
            Message::Accepted { .. } => {
                tracing::debug!("[Node {}] Handling Accepted message", self.id);
                let reply = self.learner.handle_message(msg, &self.ledger).await;
                if let Message::Success { .. } = &reply {
                    tracing::debug!("[Node {}] Learner reached quorum, broadcasting Success", self.id);
                    self.peers.broadcast(reply).await;
                } else {
                    tracing::debug!("[Node {}] Learner did not reach quorum yet", self.id);
                }
            }
            Message::Success { .. } => {
                tracing::debug!("[Node {}] Handling Success message", self.id);
                self.learner.learn_decree(msg, &self.ledger).await;
            }

            _ => {
                tracing::debug!("[Node {}] Ignoring unhandled message type", self.id);
            }
        }
    }
}

impl PaxosNode {
    pub async fn new(
        id: usize,
        rx: Receiver<Message>,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<NetworkSimulator>,
        quorum: usize,
    ) -> anyhow::Result<Self> {
        let ledger = Ledger::init(id).await?;
        let decree_notes = Arc::new(Mutex::new(DecreeNotes::new()));
        let acceptor = Acceptor::new(id, Arc::clone(&observer)).await?;
        let state = Arc::new(PaxosState {
            id,
            peers,
            proposer: Proposer::new(id, quorum, Arc::clone(&decree_notes), Arc::clone(&observer))
                .await?,
            acceptor,
            learner: Learner::new(id, quorum, Arc::clone(&decree_notes), Arc::clone(&observer)),
            ledger,
        });
        Ok(Self {
            id,
            rx: Some(rx),
            state,
        })
    }

    pub async fn propose(&mut self, cmd: PaxosCommand) {
        let state = self.state.clone();
        let decree_num = state.ledger.next().await;
        let msg = state.proposer.propose(decree_num, cmd).await;
        state.peers.broadcast(msg).await;
    }

    pub fn start(&mut self) {
        let mut rx = self.rx.take().expect("worker already started");
        let state = Arc::clone(&self.state);

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                state.handle_message(msg).await;
            }
        });
    }
}
