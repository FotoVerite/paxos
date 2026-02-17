use std::sync::Arc;

use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    common::persistence::Persistence,
    message::Message,
    monitor::PaxosObserver,
    node::{
        classic_paxos::ballot::Ballot,
        pmmc::leader::leader_state::{LeaderData, LeaderState, durable::LeaderDurable},
    },
};

mod commander;
mod leader_state;
mod scout;

pub struct Leader {
    uuid: Uuid,
    quorum: usize,
    peers: Arc<NetworkSimulator>,
    state: Arc<LeaderState>,
    observer: Arc<dyn PaxosObserver>,
}

impl Leader {
    pub async fn new(
        uuid: Uuid,
        quorum: usize,
        peers: Arc<NetworkSimulator>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let state: LeaderDurable = Persistence::load(&format!("leader_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state = LeaderData::default();

        let state = LeaderState::init(uuid, state);
        let mut leader = Self {
            uuid,
            peers,
            quorum,
            observer,
            state: Arc::new(state),
        };
        Ok(leader)
    }

    pub async fn start_election(&self) {
        let ballot = self
            .state
            .start_scout(self.uuid, self.quorum, Arc::clone(&self.observer))
            .await;
        self.peers
            .broadcast(Message::P1A {
                from: self.uuid,
                ballot,
                start_index: 0,
            })
            .await;
    }

    async fn become_active(&self) {
        self.state.set_as_active().await;
    }

    pub async fn send_heartbeat(&self) {
        if !self.state.is_active().await {
            return;
        }
        let ballot = self.state.ballot().await;
        self.peers
            .broadcast(Message::HEARTBEAT {
                from: self.uuid,
                ballot: ballot.clone(),
            })
            .await;
    }

    async fn heartbeat_handler(&self, ballot: Ballot) -> Message {
        self.state.heartbeat_handler(ballot).await;
        return Message::NACK;
    }

    async fn preempt(&self, ballot: Ballot) {
        self.state.preempt(ballot).await;
    }

    async fn save(&self) -> anyhow::Result<()> {
        let state = self.state.dump().await;
        Persistence::save(&format!("Leader_{}.bin", self.uuid), &state).await?;
        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        self.state.is_active().await
    }

    pub async fn election_deadline(&self) -> Instant {
        self.state.election_deadline().await
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::ADOPTED { .. } => {
                self.become_active().await;
                return Message::NACK;
            }
            Message::P1B { .. } => self.state.handle_p1b(msg.clone()).await,

            Message::P2B { .. } => self.state.handle_p2b(msg.clone()).await,
            Message::PREEMPT { ballot, .. } => {
                self.preempt(ballot).await;
                return Message::NACK;
            }
            _ => Message::NACK,
        }
    }


}
