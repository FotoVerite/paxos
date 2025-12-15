use std::sync::Arc;

use tokio::sync::{
    Mutex,
    mpsc::{Receiver, Sender},
};

use crate::{
    cluster::peer_sender::PeerSender,
    message::Message,
    monitor::PaxosObserver,
    node::{acceptor::Acceptor, learner::Learner, ledger::Ledger, proposer::Proposer},
};

pub struct PaxosNode {
    id: usize,
    rx: Option<Receiver<Message>>,
    state: Arc<Mutex<PaxosState>>,
}

pub struct PaxosState {
    peers: PeerSender, // Track the highest accepted_ballot from Promises
    proposer: Proposer,
    acceptor: Acceptor,
    learner: Learner,
    ledger: Ledger,
}

impl PaxosState {
    pub async fn propose(&mut self, value: String) {
        let msg = self.proposer.propose(value);

        self.peers.broadcast(msg).await
    }

    pub async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::Promise { ballot, .. } => {
                let reply = self.proposer.handle_message(msg).await;
                self.peers.send(ballot.node_id, reply);
            }
            Message::Prepare { ballot, .. } | Message::Accept { ballot, .. } => {
                let reply = self.acceptor.handle_message(msg).await;
                self.peers.send(ballot.node_id, reply);
            }
            Message::Accepted { .. } => {
                self.learner.handle_message(msg, &mut self.ledger).await;
            }

            _ => {}
        }
    }
}

impl PaxosNode {
    pub fn new(
        id: usize,
        rx: Receiver<Message>,
        observer: Arc<dyn PaxosObserver>,
        peers: PeerSender,
    ) -> Self {
        let state = Arc::new(Mutex::new(PaxosState {
            peers,
            proposer: Proposer::new(id, Arc::clone(&observer)),
            acceptor: Acceptor::new(id, Arc::clone(&observer)),
            learner: Learner::new(id, Arc::clone(&observer)),
        }));
        return Self {
            id,
            rx: Some(rx),
            state,
        };
    }

    pub async fn propose(&mut self, value: String) {
        let mut state = self.state.lock().await;
        state.propose(value).await;
    }

    pub fn start(&mut self) {
        let mut rx = self.rx.take().expect("worker already started");
        let state = Arc::clone(&self.state);

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let mut state = state.lock().await;
                state.handle_message(msg);
            }
        });
    }
}
