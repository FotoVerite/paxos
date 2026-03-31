use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use tokio::{sync::mpsc::Receiver, time::sleep};

use crate::{
    common::persistence::NodePersistence,
    common::types::DecreeId,
    monitor::PaxosObserver,
    node::{
        classic_paxos::message::ClassicMessage,
        classic_paxos::paxos_state::PaxosState,
        classic_paxos::transport::ClassicHandle,
        config::ClassicNodeConfig,
        inflight_proposals::{InflightProposal, InflightProposals},
    },
    paxos_command::PaxosCommand,
};

pub struct PaxosNode {
    pub uuid: Uuid,
    rx: Option<Receiver<ClassicMessage>>,
    _inflight_proposals: Arc<InflightProposals>,
    state: Arc<PaxosState>,
}

impl PaxosNode {
    pub async fn new(
        uuid: Uuid,
        rx: Receiver<ClassicMessage>,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<ClassicHandle>,
        persistence: NodePersistence,
        quorum: usize,
        config: ClassicNodeConfig,
        topology: crate::node::peer_topology::PeerTopology,
    ) -> anyhow::Result<Self> {
        let inflight_proposals = Arc::new(InflightProposals::new());
        Ok(Self {
            uuid,
            rx: Some(rx),
            _inflight_proposals: Arc::clone(&inflight_proposals),
            state: Arc::new(
                PaxosState::init(
                    uuid,
                    quorum,
                    peers,
                    persistence,
                    Arc::clone(&inflight_proposals),
                    observer,
                    config,
                    topology,
                )
                .await?,
            ),
        })
    }

    pub async fn propose(&self, cmd: PaxosCommand, decree_num: Option<DecreeId>) {
        let inflight = self.state.propose(cmd, decree_num).await;
        self.spawn_retry(Duration::from_millis(200), inflight)
    }

    pub async fn get_next_gap(&self) -> Option<DecreeId> {
        self.state.get_next_gap().await
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

    pub fn spawn_retry(&self, mut dur: Duration, inflight: InflightProposal) {
        let state = Arc::clone(&self.state);
        let token = inflight.token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sleep(dur) => {
                        state.retry_proposal(inflight.clone()).await;
                        dur *= 2;
                        if dur > Duration::from_millis(8000) {
                            token.cancel();
                            break;
                        }
                    }
                    _ = token.cancelled() => break,
                }
            }
        });
    }
}
