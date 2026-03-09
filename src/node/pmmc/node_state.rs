use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Instant;
use uuid::Uuid;

mod admin;

use crate::{
    cluster::{
        cluster_configuration::ClusterConfiguration,
        configuration_handler::types::{
            ConfigurationCommand, ConfigurationHandlerError, ConfigurationReplyOutcome,
        },
        network_fabric::NetworkFabric,
        network_handle::NetworkHandle,
    },
    common::persistence::NodePersistence,
    message::{ClientMessage, Message},
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        config::{LearningStrategy, Roles},
        message_router::{MessageRouter, RoutingDecision},
        peer_topology::PeerTopology,
        pmmc::{
            acceptor::Acceptor,
            leader::Leader,
            replica::{ClientReplySink, Replica},
        },
    },
};
use admin::NodeAdmin;

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

pub struct NodeState {
    uuid: Uuid,
    admin: NodeAdmin,
    acceptor: Option<Acceptor>,
    fabric: Arc<NetworkFabric>,
    leader: Option<Leader>,
    replica: Option<Arc<Replica>>,
    client_sink: Arc<NodeClientSink>,
    router: MessageRouter,
    observer: Arc<dyn PaxosObserver>,
}

impl NodeState {
    pub async fn init(
        uuid: Uuid,
        fabric: Arc<NetworkFabric>,
        handle: Arc<NetworkHandle>,
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

        let router = MessageRouter::new(LearningStrategy::default(), topology);
        let admin = NodeAdmin::new(uuid, replica.clone(), Arc::clone(&client_sink));
        Ok(Self {
            uuid,
            admin,
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
            None => true,
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

    pub async fn handle_configuration_command(
        &self,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        match cmd {
            ConfigurationCommand::Add { .. } | ConfigurationCommand::Remove { .. } => {
                self.handle_membership_command(cmd).await
            }
            _ => self.admin.handle_configuration_command(cmd).await,
        }
    }

    async fn handle_membership_command(
        &self,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        match cmd {
            ConfigurationCommand::Add { .. } | ConfigurationCommand::Remove { .. } => {
                Err(ConfigurationHandlerError::Rejected {
                    reason:
                        "membership updates not yet wired; route through node state reconciler path"
                            .to_string(),
                })
            }
            _ => Err(ConfigurationHandlerError::InvalidRequest {
                reason: "expected membership command".to_string(),
            }),
        }
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

    use crate::{
        cluster::{
            cluster_configuration::ClusterConfiguration, network_fabric::NetworkFabric,
            network_handle::NetworkHandle,
        },
        message::Message,
        monitor::{NoOpObserver, PaxosObserver},
        node::{
            classic_paxos::ballot::Ballot,
            config::{PmmcNodeConfig, Roles},
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
        let fabric = Arc::new(NetworkFabric::new(Arc::clone(&observer)));
        fabric.register(peer_id, peer_tx).await;
        let handle = Arc::new(NetworkHandle::from_fabric(node_id, Arc::clone(&fabric)));

        let state = NodeState::init(
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
            .handle_message(Message::ACCEPTED {
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
            Message::ACK { from, to, slot } if from == node_id && to == peer_id && slot == 0
        ));
    }
}
