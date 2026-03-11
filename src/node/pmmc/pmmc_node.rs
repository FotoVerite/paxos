use std::{future::pending, sync::Arc, time::Duration};

use tokio::{
    select,
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
    time::{self, Instant, MissedTickBehavior, sleep_until},
};
use uuid::Uuid;

use crate::{
    cluster::{
        cluster_configuration::ClusterConfiguration,
        configuration_handler::types::ConfigurationHandlerMessage,
    },
    common::persistence::NodePersistence,
    message::ClientMessage,
    monitor::PaxosObserver,
    node::{
        config::Roles,
        pmmc::{
            admin::NodeAdmin,
            message::PmmcMessage,
            node_state::NodeState,
            transport::{PmmcFabric, PmmcHandle},
        },
    },
};

pub struct PmmcNode {
    pub uuid: Uuid,
    state: Arc<NodeState>,
    admin: Arc<NodeAdmin>,
}

impl PmmcNode {
    pub async fn new(
        uuid: Uuid,
        observer: Arc<dyn PaxosObserver>,
        fabric: Arc<PmmcFabric>,
        handle: Arc<PmmcHandle>,
        persistence: NodePersistence,
        roles: Roles,
        configuration: Arc<ClusterConfiguration>,
    ) -> anyhow::Result<Self> {
        let state = Arc::new(
            NodeState::init(
                uuid,
                fabric,
                handle,
                persistence,
                observer,
                roles,
                configuration,
            )
            .await?,
        );
        Ok(Self {
            uuid,
            admin: Arc::new(NodeAdmin::new(uuid, Arc::clone(&state))),
            state,
        })
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

    pub async fn is_stopped(&self) -> bool {
        self.state.is_stopped().await
    }

    pub async fn attach_admin_transport(
        &self,
        endpoint_rx: mpsc::Receiver<ConfigurationHandlerMessage>,
        endpoint_tx: mpsc::Sender<ConfigurationHandlerMessage>,
    ) {
        self.admin.attach_transport(endpoint_rx, endpoint_tx).await;
    }

    pub async fn start_admin(&self) {
        Arc::clone(&self.admin).start().await;
    }

    pub async fn stop_admin(&self) {
        self.admin.stop().await;
    }

    pub fn start(&self, mut rx: Receiver<PmmcMessage>) -> JoinHandle<()> {
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
        })
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
        cluster::cluster_configuration::ClusterConfiguration,
        common::ballot::Ballot,
        monitor::{NoOpObserver, PaxosObserver},
        node::{
            config::PmmcNodeConfig,
            pmmc::{
                message::PmmcMessage,
                transport::{PmmcHandle, new_pmmc_fabric},
            },
        },
    };

    use super::PmmcNode;

    async fn new_node_with_peer() -> (
        PmmcNode,
        mpsc::Sender<PmmcMessage>,
        mpsc::Receiver<PmmcMessage>,
        mpsc::Receiver<PmmcMessage>,
        Uuid,
        Uuid,
    ) {
        let ip = std::net::IpAddr::V4([127, 0, 0, 1].into());
        let configuration = Arc::new(
            ClusterConfiguration::bootstrap_pmmc(
                ip,
                vec![PmmcNodeConfig::default(), PmmcNodeConfig::default()],
            )
            .expect("test config should bootstrap"),
        );
        let node_id = configuration.member(0).expect("node 0 should exist");
        let peer_id = configuration.member(1).expect("node 1 should exist");
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let (inbox_tx, inbox_rx) = mpsc::channel(64);
        let (peer_tx, peer_rx) = mpsc::channel(64);

        let fabric = Arc::new(new_pmmc_fabric(Arc::clone(&observer)));
        fabric.register(peer_id, peer_tx).await;
        let handle = Arc::new(PmmcHandle::from_fabric(node_id, Arc::clone(&fabric)));

        let node = PmmcNode::new(
            node_id,
            Arc::clone(&observer),
            fabric,
            handle,
            crate::common::persistence::ClusterPersistence::for_test("pmmc_node").node(node_id),
            crate::node::config::Roles::default(),
            configuration,
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
                    Some(PmmcMessage::P1A { ballot, .. }) => return Some(ballot),
                    Some(_) => {}
                    None => return None,
                }
            }
        })
        .await
        .expect("node should start an election and emit p1a")
        .expect("peer channel should stay open");

        inbox_tx
            .send(PmmcMessage::ADOPTED {
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
                    Some(PmmcMessage::P1A { .. }) => return true,
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
            .send(PmmcMessage::ADOPTED {
                from: Uuid::new_v4(),
                to: node_id,
                ballot: Ballot::with_epoch(0, node_id, 1),
                pvalues: vec![],
            })
            .await
            .expect("send should work");

        let got_heartbeat = timeout(Duration::from_millis(700), async {
            loop {
                match peer_rx.recv().await {
                    Some(PmmcMessage::HEARTBEAT { .. }) => return true,
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
