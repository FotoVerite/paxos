use std::sync::Arc;

use tokio::sync::{
    Mutex,
    mpsc::{Receiver},
};

use crate::{
    cluster::peer_sender::PeerSender,
    message::Message,
    monitor::PaxosObserver,
    node::{acceptor::Acceptor, learner::Learner, ledger::Ledger, proposer::Proposer},
    paxos_command::PaxosCommand,
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
    pub async fn propose(&mut self, value: PaxosCommand) {
        let decree_num = self.ledger.next().await;
        let msg = self.proposer.propose(decree_num, value);

        self.peers.broadcast(msg).await
    }

    pub async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::Promise { .. } => {
                let reply = self.proposer.handle_message(msg).await;
                self.peers.broadcast(reply).await;
            }
            Message::Prepare { from, .. } | Message::Accept { from, .. } => {
                let reply = self.acceptor.handle_message(msg).await;
                self.peers.send(from, reply).await;
            }
            Message::Accepted { .. } => {
                self.learner.handle_message(msg, &mut self.ledger).await;
            }

            _ => {}
        }
    }
}

impl PaxosNode {
    pub async fn new(
        id: usize,
        rx: Receiver<Message>,
        observer: Arc<dyn PaxosObserver>,
        peers: PeerSender,
        quorum: usize,
    ) -> anyhow::Result<Self> {
        let ledger = Ledger::init(id, quorum).await?;
        let acceptor = Acceptor::new(id, Arc::clone(&observer)).await?;
        let state = Arc::new(Mutex::new(PaxosState {
            peers,
            proposer: Proposer::new(id, quorum, Arc::clone(&observer)).await?,
            acceptor,
            learner: Learner::new(id, Arc::clone(&observer)),
            ledger,
        }));
        Ok(Self {
            id,
            rx: Some(rx),
            state,
        })
    }

    pub async fn propose(&mut self, cmd: PaxosCommand) {
        let mut state = self.state.lock().await;
        state.propose(cmd).await;
    }

    pub fn start(&mut self) {
        let mut rx = self.rx.take().expect("worker already started");
        let state = Arc::clone(&self.state);

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let mut state = state.lock().await;
                state.handle_message(msg).await;
            }
        });
    }
}
