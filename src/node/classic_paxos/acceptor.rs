mod accepted_decree;
mod accepted_decrees;
mod prev_vote;
use crate::common::persistence::NodePersistence;
use crate::common::types::DecreeId;
use crate::message::Message;
use crate::monitor::PaxosObserver;
use crate::node::classic_paxos::acceptor::accepted_decrees::AcceptedDecrees;
use crate::node::classic_paxos::ballot::Ballot;
use crate::paxos_command::PaxosCommand;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Acceptor {
    uuid: Uuid,
    persistence: NodePersistence,
    state: Mutex<AcceptedDecrees>,
    observer: Arc<dyn PaxosObserver>,
}
impl Acceptor {
    pub async fn new(
        uuid: Uuid,
        persistence: NodePersistence,
        observer: Arc<dyn PaxosObserver>,
    ) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let state = persistence.load("acceptor.bin").await?;

        #[cfg(not(feature = "persistence"))]
        let state = AcceptedDecrees::init();

        Ok(Self {
            uuid,
            persistence,
            state: Mutex::new(state),
            observer,
        })
    }

    async fn save(&self) -> Result<()> {
        let state = self.state.lock().await;
        self.persistence.save("acceptor.bin", &*state).await
    }

    async fn prepare(&self, decree_num: DecreeId, ballot: Ballot, from: Uuid) -> Message {
        let mut state = self.state.lock().await;
        let msg = state.promise(
            self.uuid,
            from,
            decree_num,
            ballot,
            Arc::clone(&self.observer),
        );

        drop(state);

        #[cfg(feature = "persistence")]
        {
            if let Err(e) = self.save().await {
                tracing::error!(
                    "[Node {}] Failed to persist acceptor state: {}",
                    self.uuid,
                    e
                );
            }
        }
        msg
    }

    async fn accept(
        &self,
        decree_num: DecreeId,
        ballot: Ballot,
        cmd: PaxosCommand,
        from: Uuid,
    ) -> Message {
        let mut state = self.state.lock().await;
        let msg = state.accept(
            self.uuid,
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
                tracing::error!(
                    "[Node {}] Failed to persist acceptor state: {}",
                    self.uuid,
                    e
                );
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
        persistence: &NodePersistence,
        initial_decrees: Vec<(DecreeId, PaxosCommand)>,
    ) -> anyhow::Result<()> {
        AcceptedDecrees::prepopulate(persistence, initial_decrees).await
    }
}
