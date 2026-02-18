use std::sync::Arc;

use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{
        config::{LearningStrategy, PmmcNodeConfig},
        message_router::{MessageRouter, RoutingDecision},
        pmmc::{acceptor::Acceptor, leader::Leader},
    },
    paxos_command::PaxosCommand,
};

pub struct NodeState {
    uuid: Uuid,
    acceptor: Option<Acceptor>,
    peers: Arc<NetworkSimulator>,
    leader: Option<Leader>,
    router: MessageRouter,
    observer: Arc<dyn PaxosObserver>,
}

impl NodeState {
    pub async fn init(
        uuid: Uuid,
        quorum: usize,
        peers: Arc<NetworkSimulator>,
        observer: Arc<dyn PaxosObserver>,
        config: PmmcNodeConfig,
        topology: crate::node::peer_topology::PeerTopology,
    ) -> anyhow::Result<Self> {
        let leader = if config.roles.proposer {
            Some(Leader::new(uuid, quorum, Arc::clone(&peers), Arc::clone(&observer)).await?)
        } else {
            None
        };

        let acceptor = if config.roles.acceptor {
            Some(Acceptor::new(uuid, Arc::clone(&observer)).await?)
        } else {
            None
        };

        // let learner = if config.roles.learner {
        //     Some(Learner::new(
        //         id,
        //         uuid,
        //         quorum,
        //         learner_decree_notes,
        //         Arc::clone(&observer),
        //     ))
        // } else {
        //     None
        // };

        // Emit capabilities event
        let mut roles_str = Vec::new();
        if config.roles.proposer {
            roles_str.push("Leader".to_string());
        }
        if config.roles.acceptor {
            roles_str.push("Acceptor".to_string());
        }
        if config.roles.learner {
            roles_str.push("Learner".to_string());
        }

        observer.on_event(Event::NodeCapabilities {
            id: uuid,
            roles: roles_str,
            learning_strategy: "NONE".to_string(),
        });

        // Create message router
        use crate::node::message_router::MessageRouter;
        let router = MessageRouter::new(LearningStrategy::default(), topology);

        let state = Self {
            uuid,
            acceptor,
            leader,
            peers,
            router,
            observer,
        };
        Ok(state)
    }

    pub async fn is_leader(&self) -> bool {
        if let Some(leader) = &self.leader {
            leader.is_leader().await
        } else {
            false
        }
    }

    pub async fn election_deadline(&self) -> Option<Instant> {
        if let Some(leader) = &self.leader {
            Some(leader.election_deadline().await)
        } else {
            None
        }
    }

    pub async fn send_heartbeat(&self) {
        if let Some(leader) = &self.leader {
            leader.send_heartbeat().await;
        }
    }

    pub async fn start_election(&self) {
        if let Some(leader) = &self.leader {
            if !leader.is_leader().await {
                leader.start_election().await;
            }
        }
    }

    pub async fn propose(&self, cmd: PaxosCommand) {}

    pub async fn handle_message(&self, msg: Message) {
        match msg {
            Message::ACK { from, .. }
            | Message::ADOPTED { from, .. }
            | Message::HEARTBEAT { from, .. }
            | Message::P1B { from, .. }
            | Message::P2B { from, .. }
            | Message::PROPOSE { from, .. }
            | Message::PREEMPT { from, .. } => {
                if let Some(leader) = &self.leader {
                    tracing::debug!(
                        "[Node {}] Routing message from node {} to Leader component",
                        self.uuid,
                        from
                    );
                    let reply = leader.handle_message(msg).await;
                    self.dispatch(&reply, from).await;
                }
            }
            Message::P1A { from, .. } | Message::P2A { from, .. } => {
                if let Some(acceptor) = &self.acceptor {
                    tracing::debug!(
                        "[Node {}] Routing message from node {} to Acceptor component",
                        self.uuid,
                        from
                    );
                    let reply = acceptor.handle_message(msg).await;
                    self.dispatch(&reply, from).await;
                }
            }
            _ => {
                tracing::debug!("[Node {}] Ignoring unhandled message type", self.uuid);
            }
        }
    }

    async fn dispatch(&self, msg: &Message, from: Uuid) {
        if let Message::NACK = msg {
            return;
        }

        let decision = self.router.pmmc_route_response(msg, from);
        match decision {
            RoutingDecision::Broadcast => {
                self.peers.broadcast(msg.clone()).await;
            }
            RoutingDecision::SendTo(to) => {
                self.peers.send(to, msg.clone()).await;
            }
            RoutingDecision::SendToMany(nodes) => {
                for node in nodes {
                    self.peers.send(node, msg.clone()).await;
                }
            }
            RoutingDecision::Drop => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use tokio::{sync::mpsc, time::timeout};
    use uuid::Uuid;

    use crate::{
        cluster::network_simulator::NetworkSimulator,
        message::Message,
        monitor::{NoOpObserver, PaxosObserver},
        node::{
            classic_paxos::ballot::Ballot, config::PmmcNodeConfig, peer_topology::PeerTopology,
            pvalue::PValue,
        },
        paxos_command::PaxosCommand,
    };

    use super::NodeState;

    fn cmd(value: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value,
        }
    }

    #[tokio::test]
    async fn accepted_is_routed_to_replica_and_replies_ack() {
        let node_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let (node_tx, _node_rx) = mpsc::channel(32);
        let (peer_tx, mut peer_rx) = mpsc::channel(32);

        let mut peers_map = HashMap::new();
        peers_map.insert(node_id, node_tx);
        peers_map.insert(peer_id, peer_tx);
        let peers = Arc::new(NetworkSimulator::new(node_id, peers_map, Arc::clone(&observer)));
        let topology = PeerTopology::new(vec![peer_id], vec![node_id], vec![node_id]);

        let state = NodeState::init(
            node_id,
            2,
            peers,
            observer,
            PmmcNodeConfig::default(),
            topology,
        )
        .await
        .expect("node state init should work");

        state
            .handle_message(Message::ACCEPTED {
                from: peer_id,
                pvalue: PValue::new(0, Ballot::new(1, peer_id), cmd(7)),
            })
            .await;

        let msg = timeout(Duration::from_millis(120), peer_rx.recv())
            .await
            .expect("accepted should trigger replica ack path")
            .expect("peer should receive ack");
        assert!(
            matches!(msg, Message::ACK { .. }),
            "ACCEPTED should be handled by replica path and return ACK"
        );
    }
}
