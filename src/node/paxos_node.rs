use std::sync::Arc;
use uuid::Uuid;

use tokio::sync::{Mutex, mpsc::Receiver};

use crate::{
    cluster::network_simulator::NetworkSimulator,
    message::Message,
    monitor::{PaxosObserver, Event, current_timestamp_millis},
    node::{
        acceptor::Acceptor, decree_notes::DecreeNotes, learner::Learner, ledger::Ledger,
    },
    paxos_command::PaxosCommand,
};

pub struct PaxosNode {
     _id: usize,
     pub uuid: Uuid,
     rx: Option<Receiver<Message>>,
     state: Arc<PaxosState>,
 }

 pub struct PaxosState {
     id: usize,
     _uuid: Uuid,
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
        uuid: Uuid,
        rx: Receiver<Message>,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<NetworkSimulator>,
        quorum: usize,
    ) -> anyhow::Result<Self> {
        let ledger = Ledger::init(id, uuid).await?;
        
        // Emit InitialDecree events for any pre-populated decrees
        let initial_decrees = ledger.get_initial_decrees().await;
        for (decree_num, value) in initial_decrees {
            observer.on_event(Event::InitialDecree {
                id,
                decree_num,
                value,
                created_at: current_timestamp_millis(),
            });
        }
        
        let decree_notes = Arc::new(Mutex::new(DecreeNotes::load_or_init(uuid).await?));
        let acceptor = Acceptor::new(id, uuid, Arc::clone(&observer)).await?;
        let state = Arc::new(PaxosState {
            id,
            _uuid: uuid,
            peers,
            proposer: Proposer::new(id, uuid, quorum, Arc::clone(&decree_notes), Arc::clone(&observer))
                .await?,
            acceptor,
            learner: Learner::new(id, quorum, Arc::clone(&decree_notes), Arc::clone(&observer)),
            ledger,
        });
        Ok(Self {
            _id: id,
            uuid,
            rx: Some(rx),
            state,
        })
    }

    pub async fn propose(&mut self, cmd: PaxosCommand) {
        self.propose_with_decree_num(None, cmd).await;
    }

    pub async fn propose_with_decree_num(&mut self, decree_num: Option<usize>, cmd: PaxosCommand) {
        let state = self.state.clone();
        let num = match decree_num {
            Some(num) => num,
            None => state.ledger.next().await,
        };
        let msg = state.proposer.propose(num, cmd).await;
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

    pub async fn get_next_gap(&self) -> Option<usize> {
        self.state.ledger.next_gap().await
    }
    }
