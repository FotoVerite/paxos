use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use tokio::{sync::mpsc::Receiver, time::{self, sleep}};

use crate::{
    cluster::network_simulator::NetworkSimulator,
    common::types::{DecreeId, NodeId},
    message::Message,
    monitor::PaxosObserver,
    node::{
        config::NodeConfig,
        inflight_proposals::{InflightProposal, InflightProposals},
        paxos_state::paxos_state::PaxosState,
    },
    paxos_command::PaxosCommand,
};

pub struct PaxosNode {
    pub uuid: Uuid,
    rx: Option<Receiver<Message>>,
    _inflight_proposals: Arc<InflightProposals>,
    state: Arc<PaxosState>,
}

impl PaxosNode {
    pub async fn new(
        id: NodeId,
        uuid: Uuid,
        rx: Receiver<Message>,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<NetworkSimulator>,
        quorum: usize,
        config: NodeConfig,
        topology: crate::node::peer_topology::PeerTopology,
    ) -> anyhow::Result<Self> {
        let inflight_proposals = Arc::new(InflightProposals::new());
        Ok(Self {
            uuid,
            rx: Some(rx),
            _inflight_proposals: Arc::clone(&inflight_proposals),
            state: Arc::new(
                PaxosState::init(
                    id,
                    uuid,
                    quorum,
                    peers,
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
            loop {
                tokio::select! {
                     Some(msg) = rx.recv() => {
                         state.handle_message(msg).await;
                    }
                    _ = time::sleep_until(self.election_deadline), if !state.is_leader => {
                     
                    }

                 }
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
