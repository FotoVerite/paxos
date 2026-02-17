use std::{sync::Arc, time::Duration};

use tokio::{
    select,
    sync::mpsc::Receiver,
    time::{self, Instant, Interval, MissedTickBehavior, sleep_until},
};
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    message::Message,
    monitor::PaxosObserver,
    node::{
        classic_paxos::paxos_state::PaxosState, config::PmmcNodeConfig, pmmc::node_state::NodeState,
    },
};

pub struct PmmcNode {
    pub uuid: Uuid,
    rx: Option<Receiver<Message>>,
    state: Arc<NodeState>,
}

impl PmmcNode {
    pub async fn new(
        uuid: Uuid,
        rx: Receiver<Message>,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<NetworkSimulator>,
        quorum: usize,
        config: PmmcNodeConfig,
        topology: crate::node::peer_topology::PeerTopology,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            uuid,
            rx: Some(rx),
            state: Arc::new(
                NodeState::init(uuid, quorum, peers, observer, config, topology).await?,
            ),
        })
    }

    pub fn start(&mut self) {
        let mut rx = self.rx.take().expect("worker already started");
        let state = Arc::clone(&self.state);
        let mut hb = time::interval_at(
            Instant::now() + Duration::from_millis(150),
            Duration::from_millis(150),
        );
        hb.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::spawn(async move {
            loop {
                select! {
                    Some(msg) = rx.recv() => {
                        state.handle_message(msg).await;
                    }

                    _ = async {
                        if let Some(deadline) = state.election_deadline().await {
                            sleep_until(deadline).await;
                        }
                    } => {
                        state.start_election().await;
                    }

                    _ = hb.tick() => {
                            state.send_heartbeat();
                    }

                    else => break,
                }
            }
        });
    }
}
