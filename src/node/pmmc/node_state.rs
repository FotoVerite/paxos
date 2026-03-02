use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    cluster::{network_fabric::NetworkFabric, network_simulator::NetworkSimulator},
    common::persistence::NodePersistence,
    message::{ClientMessage, Message},
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        config::{LearningStrategy, Roles},
        message_router::{MessageRouter, RoutingDecision},
        peer_topology::PeerTopology,
        pmmc::{acceptor::Acceptor, leader::Leader, replica::Replica},
    },
    paxos_command::PaxosCommand,
};

pub struct NodeState {
    uuid: Uuid,
    acceptor: Option<Acceptor>,
    fabric: Arc<NetworkFabric>,
    leader: Option<Leader>,
    replica: Option<Replica>,
    router: MessageRouter,
    observer: Arc<dyn PaxosObserver>,
}

impl NodeState {
    pub async fn init(
        uuid: Uuid,
        quorum: usize,
        fabric: Arc<NetworkFabric>,
        handle: Arc<NetworkSimulator>,
        persistence: NodePersistence,
        observer: Arc<dyn PaxosObserver>,
        roles: Roles,
        topology: PeerTopology,
    ) -> anyhow::Result<Self> {
        let mut roles_str = Vec::new();

        let leader = if roles.proposer {
            roles_str.push("Leader".to_string());
            Some(
                Leader::new(
                    uuid,
                    persistence.clone(),
                    quorum,
                    topology.learners.clone(),
                    Arc::clone(&handle),
                    Arc::clone(&observer),
                )
                .await?,
            )
        } else {
            None
        };

        let acceptor = if roles.acceptor {
            roles_str.push("Acceptor".to_string());
            Some(Acceptor::new(uuid, persistence.clone(), Arc::clone(&observer)).await?)
        } else {
            None
        };

        let replica = if roles.learner {
            roles_str.push("Replica".to_string());

            Some(
                Replica::new(
                    uuid,
                    persistence.clone(),
                    Arc::clone(&observer),
                    Arc::clone(&fabric),
                )
                .await?,
            )
        } else {
            None
        };

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
            replica,
            fabric,
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
            if leader.is_leader().await {
                return;
            }
            leader.start_election().await;
        }
    }

    pub async fn propose(&self, cmd: PaxosCommand) {
        if let Some(replica) = &self.replica {
            let slot = replica.state.add_proposal(cmd.clone()).await;
            self.observer.on_event(Event::PmmcPropose {
                id: self.uuid,
                slot,
                cmd: cmd.clone(),
                created_at: current_timestamp_millis(),
            });
            self.fabric
                .broadcast(self.uuid, Message::PROPOSE {
                    from: self.uuid,
                    slot,
                    cmd,
                })
                .await;
        }
    }

    pub async fn connect_client(
        &self,
        client_id: Uuid,
    ) -> Option<(mpsc::Sender<ClientMessage>, mpsc::Receiver<ClientMessage>)> {
        let replica = self.replica.as_ref()?;
        let (client_tx, client_rx) = mpsc::channel(64);
        let (resp_tx, resp_rx) = mpsc::channel(64);
        replica
            .spawn_client_handler(client_id, client_rx, resp_tx)
            .await;
        Some((client_tx, resp_rx))
    }

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
            Message::ACCEPTED { from, .. } => {
                if let Some(replica) = &self.replica {
                    tracing::debug!(
                        "[Node {}] Routing message from node {} to Replica component",
                        self.uuid,
                        from
                    );
                    let reply = replica.handle_message(msg).await;
                    self.dispatch(&reply, from).await;
                }
            }
            _ => {
                tracing::debug!("[Node {}] Ignoring unhandled message type", self.uuid);
            }
        }
    }

    async fn dispatch(&self, msg: &Message, _from: Uuid) {
        if let Message::NACK = msg {
            return;
        }

        let decision = self.router.pmmc_route_response(msg);
        match decision {
            RoutingDecision::Broadcast => {
                self.fabric.broadcast(self.uuid, msg.clone()).await;
            }
            RoutingDecision::SendTo(to) => {
                self.emit_pmmc_message_event(msg, to);
                self.fabric.send(self.uuid, to, msg.clone()).await;
            }
            RoutingDecision::SendToMany(nodes) => {
                for node in nodes {
                    self.emit_pmmc_message_event(msg, node);
                    self.fabric.send(self.uuid, node, msg.clone()).await;
                }
            }
            RoutingDecision::Drop => {}
        }
    }

    fn emit_pmmc_message_event(&self, msg: &Message, to: Uuid) {
        let created_at = current_timestamp_millis();
        let evt = match msg {
            Message::P1A { from, ballot, .. } => Some(Event::PmmcP1A {
                from: *from,
                ballot: *ballot,
                created_at,
            }),
            Message::P1B { from, ballot, .. } => Some(Event::PmmcP1B {
                from: *from,
                to,
                ballot: *ballot,
                created_at,
            }),
            Message::P2A { from, pvalue } => Some(Event::PmmcP2A {
                from: *from,
                pvalue: pvalue.clone(),
                created_at,
            }),
            Message::P2B {
                from,
                ballot,
                pvalue,
                ..
            } => Some(Event::PmmcP2B {
                from: *from,
                to,
                ballot: *ballot,
                pvalue: pvalue.clone(),
                created_at,
            }),
            Message::ADOPTED { from, ballot, .. } => Some(Event::PmmcAdopted {
                from: *from,
                to,
                ballot: *ballot,
                created_at,
            }),
            Message::PREEMPT { from, ballot, .. } => Some(Event::PmmcPreempted {
                from: *from,
                to,
                ballot: *ballot,
                created_at,
            }),
            Message::HEARTBEAT { from, ballot } => Some(Event::PmmcHeartbeat {
                from: *from,
                ballot: *ballot,
                created_at,
            }),
            Message::ACK { from, slot, .. } => Some(Event::PmmcAck {
                from: *from,
                to,
                slot: *slot,
                created_at,
            }),
            _ => None,
        };
        if let Some(event) = evt {
            self.observer.on_event(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::{sync::mpsc, time::timeout};
    use uuid::Uuid;

    use crate::{
        cluster::{network_fabric::NetworkFabric, network_simulator::NetworkSimulator},
        message::Message,
        monitor::{NoOpObserver, PaxosObserver},
        node::{
            classic_paxos::ballot::Ballot, config::Roles, peer_topology::PeerTopology,
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
        let (peer_tx, mut peer_rx) = mpsc::channel(32);
        let fabric = Arc::new(NetworkFabric::new(Arc::clone(&observer)));
        fabric.register(peer_id, peer_tx).await;
        let handle = Arc::new(NetworkSimulator::from_fabric(node_id, Arc::clone(&fabric)));
        let topology = PeerTopology::new(vec![peer_id], vec![node_id], vec![node_id]);

        let state = NodeState::init(
            node_id,
            2,
            fabric,
            handle,
            crate::common::persistence::ClusterPersistence::for_test("node_state").node(node_id),
            observer,
            Roles::default(),
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
