mod accepted_decree;
mod accepted_decrees;
mod prev_vote;
use crate::common::persistence::Persistence;
use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::paxos_state::acceptor::accepted_decrees::AcceptedDecrees;
use crate::node::paxos_state::ballot::Ballot;
use crate::paxos_command::PaxosCommand;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Acceptor {
    id: usize,
    uuid: Uuid,
    state: Mutex<AcceptedDecrees>,
    observer: Arc<dyn PaxosObserver>,
}
impl Acceptor {
    pub async fn new(id: usize, uuid: Uuid, observer: Arc<dyn PaxosObserver>) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let state = Persistence::load(&format!("acceptor_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state = AcceptedDecrees::init();

        Ok(Self {
            id,
            uuid,
            state: Mutex::new(state),
            observer,
        })
    }

    async fn save(&self) -> Result<()> {
        let state = self.state.lock().await;
        Persistence::save(&format!("acceptor_{}.bin", self.uuid), &*state).await
    }

    async fn prepare(&self, decree_num: usize, ballot: Ballot, from: usize) -> Message {
        let mut state = self.state.lock().await;
        let msg = state.promise(
            self.id,
            from,
            decree_num,
            ballot,
            Arc::clone(&self.observer),
        );

        drop(state);

        #[cfg(feature = "persistence")]
        {
            if let Err(e) = self.save().await {
                tracing::error!("[Node {}] Failed to persist acceptor state: {}", self.id, e);
            }
        }
        msg
    }

    async fn accept(
        &self,
        decree_num: usize,
        ballot: Ballot,
        cmd: PaxosCommand,
        from: usize,
    ) -> Message {
        let mut state = self.state.lock().await;
        let msg = state.accept(
            self.id,
            from,
            decree_num,
            ballot,
            cmd,
            Arc::clone(&self.observer),
        );

        drop(state);

        #[cfg(feature = "persistence")]
        {
            if let Err(e) = self.save().await {
                tracing::error!("[Node {}] Failed to persist acceptor state: {}", self.id, e);
            }
        }
        msg
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::Prepare {
                decree_num,
                ballot,
                from,
            } => self.prepare(decree_num, ballot, from).await,
            Message::Accept {
                decree_num,
                ballot,
                value,
                from,
                ..
            } => self.accept(decree_num, ballot, value, from).await,
            _ => Message::NACK,
        }
    }

    pub async fn prepopulate(
        uuid: Uuid,
        initial_decrees: Vec<(usize, PaxosCommand)>,
    ) -> anyhow::Result<()> {
        AcceptedDecrees::prepopulate(uuid, initial_decrees).await
    }
}
