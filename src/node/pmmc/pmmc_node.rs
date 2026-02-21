use std::{future::pending, sync::Arc, time::Duration};

use tokio::{
    select,
    sync::mpsc::{self, Receiver},
    time::{self, Instant, MissedTickBehavior, sleep_until},
};
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    message::{ClientMessage, Message},
    monitor::PaxosObserver,
    node::{config::PmmcNodeConfig, pmmc::node_state::NodeState},
    paxos_command::PaxosCommand,
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

    pub async fn propose(&self, cmd: PaxosCommand) {
        self.state.propose(cmd).await;
    }

    pub async fn connect_client(
        &self,
        client_id: Uuid,
    ) -> Option<(mpsc::Sender<ClientMessage>, mpsc::Receiver<ClientMessage>)> {
        self.state.connect_client(client_id).await
    }

    pub async fn is_leader(&self) -> bool {
        self.state.is_leader().await
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
                        match state.election_deadline().await {
                                Some(deadline) => sleep_until(deadline).await,
                                None => pending::<()>().await,
                            }
                        } => {
                            state.start_election().await;
                    }

                    _ = hb.tick() => {
                            state.send_heartbeat().await;
                    }

                    else => break,
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use tokio::{
        sync::mpsc,
        time::{sleep, timeout},
    };
    use uuid::Uuid;

    use crate::{
        cluster::network_simulator::NetworkSimulator,
        message::Message,
        monitor::{NoOpObserver, PaxosObserver},
        node::{
            classic_paxos::ballot::Ballot, config::PmmcNodeConfig, peer_topology::PeerTopology,
        },
    };

    use super::PmmcNode;

    async fn new_node_with_peer() -> (
        PmmcNode,
        mpsc::Sender<Message>,
        mpsc::Receiver<Message>,
        Uuid,
        Uuid,
    ) {
        let node_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let (node_tx, node_rx) = mpsc::channel(64);
        let (peer_tx, peer_rx) = mpsc::channel(64);

        let mut peers_map = HashMap::new();
        peers_map.insert(node_id, node_tx.clone());
        peers_map.insert(peer_id, peer_tx);
        let simulator = Arc::new(NetworkSimulator::new(
            node_id,
            peers_map,
            Arc::clone(&observer),
        ));
        let topology = PeerTopology::new(vec![peer_id], vec![], vec![node_id]);

        let node = PmmcNode::new(
            node_id,
            node_rx,
            Arc::clone(&observer),
            simulator,
            2,
            PmmcNodeConfig::default(),
            topology,
        )
        .await
        .expect("node init should work");

        (node, node_tx, peer_rx, node_id, peer_id)
    }

    #[tokio::test]
    async fn does_not_start_election_while_leader_active() {
        let (mut node, node_tx, mut peer_rx, node_id, _peer_id) = new_node_with_peer().await;
        node.start();

        let adopted_ballot = timeout(Duration::from_millis(500), async {
            loop {
                match peer_rx.recv().await {
                    Some(Message::P1A { ballot, .. }) => return Some(ballot),
                    Some(_) => {}
                    None => return None,
                }
            }
        })
        .await
        .expect("node should start an election and emit p1a")
        .expect("peer channel should stay open");

        node_tx
            .send(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: node_id,
                ballot: adopted_ballot,
                pvalues: vec![],
            })
            .await
            .expect("send should work");

        sleep(Duration::from_millis(60)).await;
        while peer_rx.try_recv().is_ok() {}

        let got_p1a = timeout(Duration::from_millis(500), async {
            loop {
                match peer_rx.recv().await {
                    Some(Message::P1A { .. }) => return true,
                    Some(_) => {}
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);

        assert!(!got_p1a, "active leader should not keep starting elections");
    }

    #[tokio::test]
    async fn heartbeat_tick_sends_when_active() {
        let (mut node, node_tx, mut peer_rx, node_id, _peer_id) = new_node_with_peer().await;
        node.start();

        node_tx
            .send(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: node_id,
                ballot: Ballot::new(0, node_id),
                pvalues: vec![],
            })
            .await
            .expect("send should work");

        let got_heartbeat = timeout(Duration::from_millis(700), async {
            loop {
                match peer_rx.recv().await {
                    Some(Message::HEARTBEAT { .. }) => return true,
                    Some(_) => {}
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);

        assert!(
            got_heartbeat,
            "active leader should emit periodic heartbeats"
        );
    }
}
