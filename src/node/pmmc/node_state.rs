use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    cluster::pmmc::reconfiguration::ClusterConfiguration,
    common::persistence::NodePersistence,
    message::ClientMessage,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        config::Roles,
        peer_topology::PeerTopology,
        pmmc::{
            acceptor::Acceptor,
            leader::Leader,
            message::PmmcMessage,
            replica::{ClientReplySink, Replica},
            router::{PmmcComponent, PmmcRouter},
            transport::{PmmcFabric, PmmcHandle},
        },
        routing::{LocalRoutingDecision, ProtocolInboxRouter, ProtocolRouter, RoutingDecision},
    },
};

#[derive(Default)]
pub(super) struct NodeClientSink {
    clients: Mutex<HashMap<Uuid, mpsc::Sender<ClientMessage>>>,
}

impl NodeClientSink {
    async fn add_client(&self, client_id: Uuid, tx: mpsc::Sender<ClientMessage>) {
        self.clients.lock().await.insert(client_id, tx);
    }

    async fn remove_client(&self, client_id: Uuid) {
        self.clients.lock().await.remove(&client_id);
    }
}

#[async_trait]
impl ClientReplySink for NodeClientSink {
    async fn send(&self, client_id: Uuid, message: ClientMessage) {
        let tx = self.clients.lock().await.get(&client_id).cloned();
        if let Some(tx) = tx {
            let _ = tx.send(message).await;
        }
    }
}

pub struct PmmcNodeState {
    uuid: Uuid,
    acceptor: Option<Acceptor>,
    fabric: Arc<PmmcFabric>,
    leader: Option<Leader>,
    replica: Option<Arc<Replica>>,
    client_sink: Arc<NodeClientSink>,
    router: PmmcRouter,
    observer: Arc<dyn PaxosObserver>,
}

impl PmmcNodeState {
    pub async fn init(
        uuid: Uuid,
        fabric: Arc<PmmcFabric>,
        handle: Arc<PmmcHandle>,
        persistence: NodePersistence,
        observer: Arc<dyn PaxosObserver>,
        roles: Roles,
        configuration: Arc<ClusterConfiguration>,
    ) -> anyhow::Result<Self> {
        let client_sink = Arc::new(NodeClientSink::default());
        let mut roles_str = Vec::new();
        let topology = PeerTopology::from(&*configuration);
        let leader = if roles.proposer {
            roles_str.push("Leader".to_string());
            Some(
                Leader::new(
                    uuid,
                    persistence.clone(),
                    Arc::clone(&configuration),
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
            let reply_sink: Arc<dyn ClientReplySink> = client_sink.clone();
            Some(Arc::new(
                Replica::new(
                    uuid,
                    persistence.clone(),
                    Arc::clone(&observer),
                    Arc::clone(&fabric),
                    Arc::clone(&configuration),
                    reply_sink,
                )
                .await?,
            ))
        } else {
            None
        };

        observer.on_event(Event::NodeCapabilities {
            id: uuid,
            roles: roles_str,
            learning_strategy: "NONE".to_string(),
        });

        let router = PmmcRouter::new(topology);
        Ok(Self {
            uuid,
            acceptor,
            leader,
            replica,
            client_sink,
            fabric,
            router,
            observer,
        })
    }

    pub async fn is_leader(&self) -> bool {
        if let Some(leader) = &self.leader {
            leader.is_leader().await
        } else {
            false
        }
    }

    pub async fn is_stopped(&self) -> bool {
        match &self.replica {
            // Non-replica nodes (leader-only / acceptor-only) are part of an
            // active runtime member and should not report stopped.
            None => false,
            Some(replica) => replica.is_stopped().await,
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

    pub async fn shutdown(&self) {
        if let Some(leader) = &self.leader {
            leader.shutdown().await;
        }
    }

    pub(super) fn replica_handle(&self) -> Option<Arc<Replica>> {
        self.replica.as_ref().map(Arc::clone)
    }

    pub(super) async fn register_internal_client(
        &self,
        client_id: Uuid,
        capacity: usize,
    ) -> mpsc::Receiver<ClientMessage> {
        let (resp_tx, resp_rx) = mpsc::channel(capacity);
        self.client_sink.add_client(client_id, resp_tx).await;
        resp_rx
    }

    pub(super) async fn unregister_internal_client(&self, client_id: Uuid) {
        self.client_sink.remove_client(client_id).await;
    }

    pub async fn connect_client(
        &self,
        client_id: Uuid,
    ) -> Option<(mpsc::Sender<ClientMessage>, mpsc::Receiver<ClientMessage>)> {
        let replica = Arc::clone(self.replica.as_ref()?);
        let (client_tx, client_rx) = mpsc::channel(64);
        let (resp_tx, resp_rx) = mpsc::channel(64);
        self.client_sink.add_client(client_id, resp_tx).await;
        let sink = Arc::clone(&self.client_sink);
        tokio::spawn(async move {
            let mut rx = client_rx;
            while let Some(msg) = rx.recv().await {
                if let ClientMessage::PROPOSE { cmd } = msg {
                    replica.propose_from_client(cmd).await;
                }
            }
            sink.remove_client(client_id).await;
        });
        Some((client_tx, resp_rx))
    }

    pub async fn handle_message(&self, pmsg: PmmcMessage) {
        let from = match &pmsg {
            PmmcMessage::ACK { from, .. }
            | PmmcMessage::ACCEPTED { from, .. }
            | PmmcMessage::ADOPTED { from, .. }
            | PmmcMessage::HEARTBEAT { from, .. }
            | PmmcMessage::PROPOSE { from, .. }
            | PmmcMessage::PREEMPT { from, .. }
            | PmmcMessage::P1A { from, .. }
            | PmmcMessage::P1B { from, .. }
            | PmmcMessage::P2A { from, .. }
            | PmmcMessage::P2B { from, .. } => *from,
        };

        match self.router.classify(&pmsg, &()) {
            LocalRoutingDecision::DeliverTo(PmmcComponent::Leader) => {
                if let Some(leader) = &self.leader {
                    tracing::debug!(
                        "[Node {}] Routing message from node {} to Leader component",
                        self.uuid,
                        from
                    );
                    if let Some(reply) = leader.handle_message(pmsg).await {
                        self.route_and_send(&reply).await;
                    }
                }
            }
            LocalRoutingDecision::DeliverTo(PmmcComponent::Acceptor) => {
                if let Some(acceptor) = &self.acceptor {
                    tracing::debug!(
                        "[Node {}] Routing message from node {} to Acceptor component",
                        self.uuid,
                        from
                    );
                    if let Some(reply) = acceptor.handle_message(pmsg).await {
                        self.route_and_send(&reply).await;
                    }
                }
            }
            LocalRoutingDecision::DeliverTo(PmmcComponent::Replica) => {
                if let Some(replica) = &self.replica {
                    tracing::debug!(
                        "[Node {}] Routing message from node {} to Replica component",
                        self.uuid,
                        from
                    );
                    if let Some(reply) = replica.handle_message(pmsg).await {
                        self.route_and_send(&reply).await;
                    }
                }
            }
            LocalRoutingDecision::Drop => {}
        }
    }

    async fn route_and_send(&self, pmsg: &PmmcMessage) {
        let decision = self.router.route(pmsg, &());
        match decision {
            RoutingDecision::Broadcast => {
                self.fabric.broadcast(self.uuid, pmsg.clone()).await;
            }
            RoutingDecision::SendTo(to) => {
                self.emit_pmmc_message_event(pmsg, to);
                self.fabric.send(self.uuid, to, pmsg.clone()).await;
            }
            RoutingDecision::SendToMany(nodes) => {
                for node in nodes {
                    self.emit_pmmc_message_event(pmsg, node);
                    self.fabric.send(self.uuid, node, pmsg.clone()).await;
                }
            }
            RoutingDecision::Drop => {}
        }
    }

    fn emit_pmmc_message_event(&self, pmsg: &PmmcMessage, to: Uuid) {
        let created_at = current_timestamp_millis();
        let evt = match pmsg {
            PmmcMessage::P1A { from, ballot, .. } => Some(Event::PmmcP1A {
                from: *from,
                ballot: *ballot,
                created_at,
            }),
            PmmcMessage::P1B { from, ballot, .. } => Some(Event::PmmcP1B {
                from: *from,
                to,
                ballot: *ballot,
                created_at,
            }),
            PmmcMessage::P2A { from, pvalue } => Some(Event::PmmcP2A {
                from: *from,
                pvalue: pvalue.clone(),
                created_at,
            }),
            PmmcMessage::P2B {
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
            PmmcMessage::ADOPTED { from, ballot, .. } => Some(Event::PmmcAdopted {
                from: *from,
                to,
                ballot: *ballot,
                created_at,
            }),
            PmmcMessage::PREEMPT { from, ballot, .. } => Some(Event::PmmcPreempted {
                from: *from,
                to,
                ballot: *ballot,
                created_at,
            }),
            PmmcMessage::HEARTBEAT { from, ballot } => Some(Event::PmmcHeartbeat {
                from: *from,
                ballot: *ballot,
                created_at,
            }),
            PmmcMessage::ACK { from, slot, .. } => Some(Event::PmmcAck {
                from: *from,
                to,
                slot: *slot,
                created_at,
            }),
            PmmcMessage::ACCEPTED { .. } | PmmcMessage::PROPOSE { .. } => None,
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

    use crate::{
        cluster::pmmc::reconfiguration::ClusterConfiguration,
        common::ballot::Ballot,
        monitor::{NoOpObserver, PaxosObserver},
        node::{
            config::{PmmcNodeConfig, Roles},
            pmmc::{message::PmmcMessage, transport::new_pmmc_fabric},
            pvalue::PValue,
        },
        paxos_command::PaxosCommand,
    };

    use super::PmmcNodeState;

    fn cmd(value: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value,
        }
    }

    #[tokio::test]
    async fn accepted_is_routed_to_replica_and_replies_ack() {
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
        let (peer_tx, mut peer_rx) = mpsc::channel(32);
        let fabric = Arc::new(new_pmmc_fabric(Arc::clone(&observer)));
        fabric.register(peer_id, peer_tx).await;
        let handle = Arc::new(crate::node::pmmc::transport::PmmcHandle::from_fabric(
            node_id,
            Arc::clone(&fabric),
        ));

        let state = PmmcNodeState::init(
            node_id,
            fabric,
            handle,
            crate::common::persistence::ClusterPersistence::for_test("node_state").node(node_id),
            observer,
            Roles::default(),
            configuration,
        )
        .await
        .expect("node state init should work");

        state
            .handle_message(PmmcMessage::ACCEPTED {
                from: peer_id,
                pvalue: PValue::new(0, Ballot::new(1, peer_id), cmd(7)),
            })
            .await;

        let out = timeout(Duration::from_millis(500), peer_rx.recv())
            .await
            .expect("ack should arrive")
            .expect("peer channel should stay open");

        assert!(matches!(
            out,
            PmmcMessage::ACK { from, to, slot } if from == node_id && to == peer_id && slot == 0
        ));
    }

    #[tokio::test]
    async fn non_replica_node_does_not_report_stopped() {
        let ip = std::net::IpAddr::V4([127, 0, 0, 1].into());
        let configuration = Arc::new(
            ClusterConfiguration::bootstrap_pmmc(
                ip,
                vec![
                    PmmcNodeConfig {
                        roles: Roles {
                            proposer: true,
                            acceptor: false,
                            learner: false,
                        },
                    },
                    PmmcNodeConfig {
                        roles: Roles {
                            proposer: false,
                            acceptor: true,
                            learner: false,
                        },
                    },
                    PmmcNodeConfig {
                        roles: Roles {
                            proposer: false,
                            acceptor: true,
                            learner: false,
                        },
                    },
                    PmmcNodeConfig {
                        roles: Roles {
                            proposer: false,
                            acceptor: true,
                            learner: false,
                        },
                    },
                    PmmcNodeConfig {
                        roles: Roles {
                            proposer: false,
                            acceptor: false,
                            learner: true,
                        },
                    },
                ],
            )
            .expect("test config should bootstrap"),
        );

        let leader_only = configuration.member(0).expect("node 0 should exist");
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let fabric = Arc::new(new_pmmc_fabric(Arc::clone(&observer)));
        let handle = Arc::new(crate::node::pmmc::transport::PmmcHandle::from_fabric(
            leader_only,
            Arc::clone(&fabric),
        ));

        let state = PmmcNodeState::init(
            leader_only,
            fabric,
            handle,
            crate::common::persistence::ClusterPersistence::for_test("node_state")
                .node(leader_only),
            observer,
            Roles {
                proposer: true,
                acceptor: false,
                learner: false,
            },
            configuration,
        )
        .await
        .expect("node state init should work");

        assert!(
            !state.is_stopped().await,
            "leader-only node should report active runtime status"
        );
    }
}
