use std::{future::pending, sync::Arc, time::Duration};

use tokio::{
    select,
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
    time::{self, Instant, MissedTickBehavior, sleep_until},
};
use uuid::Uuid;

use crate::{
    cluster::{network_fabric::NetworkFabric, network_simulator::NetworkSimulator},
    common::persistence::NodePersistence,
    message::{ClientMessage, Message},
    monitor::PaxosObserver,
    node::{config::Roles, peer_topology::PeerTopology, pmmc::node_state::NodeState},
    paxos_command::PaxosCommand,
};

pub struct PmmcNode {
    pub uuid: Uuid,
    state: Arc<NodeState>,
}

impl PmmcNode {
    pub async fn new(
        uuid: Uuid,
        observer: Arc<dyn PaxosObserver>,
        fabric: Arc<NetworkFabric>,
        handle: Arc<NetworkSimulator>,
        persistence: NodePersistence,
        quorum: usize,
        roles: Roles,
        topology: PeerTopology,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            uuid,
            state: Arc::new(
                NodeState::init(uuid, quorum, fabric, handle, persistence, observer, roles, topology)
                    .await?,
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

    pub fn start(&self, mut rx: Receiver<Message>) -> JoinHandle<()> {
        let state = Arc::clone(&self.state);
        let mut hb = time::interval_at(
            Instant::now() + Duration::from_millis(150),
            Duration::from_millis(150),
        );
        hb.set_missed_tick_behavior(MissedTickBehavior::Skip);
        return tokio::spawn(async move {
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
    use std::{sync::Arc, time::Duration};

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
            classic_paxos::ballot::Ballot, peer_topology::PeerTopology,
        },
    };

    use super::PmmcNode;

    async fn new_node_with_peer() -> (
        PmmcNode,
        mpsc::Sender<Message>,
        mpsc::Receiver<Message>,
        mpsc::Receiver<Message>,
        Uuid,
        Uuid,
    ) {
        let node_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let (inbox_tx, inbox_rx) = mpsc::channel(64);
        let (peer_tx, peer_rx) = mpsc::channel(64);

        let fabric = Arc::new(crate::cluster::network_fabric::NetworkFabric::new(
            Arc::clone(&observer),
        ));
        fabric.register(peer_id, peer_tx).await;
        let handle = Arc::new(NetworkSimulator::from_fabric(node_id, Arc::clone(&fabric)));
        let topology = PeerTopology::new(vec![peer_id], vec![], vec![node_id]);

        let node = PmmcNode::new(
            node_id,
            Arc::clone(&observer),
            fabric,
            handle,
            crate::common::persistence::ClusterPersistence::for_test("pmmc_node").node(node_id),
            2,
            crate::node::config::Roles::default(),
            topology,
        )
        .await
        .expect("node init should work");

        (node, inbox_tx, inbox_rx, peer_rx, node_id, peer_id)
    }

    #[tokio::test]
    async fn does_not_start_election_while_leader_active() {
        let (node, inbox_tx, inbox_rx, mut peer_rx, node_id, _peer_id) = new_node_with_peer().await;
        node.start(inbox_rx);

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

        inbox_tx
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
        let (node, inbox_tx, inbox_rx, mut peer_rx, node_id, _peer_id) = new_node_with_peer().await;
        node.start(inbox_rx);

        inbox_tx
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
