use std::sync::Arc;

use async_trait::async_trait;
use tokio::{
    sync::{
        Mutex, RwLock,
        mpsc::{Receiver, Sender},
    },
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    cluster::{
        runtime::RuntimeNode,
        vertical::{
            activation::ActivationSnapshot, configuration::VerticalClusterConfiguration,
            master::VerticalMasterEvent,
        },
    },
    common::{ballot::Ballot, persistence::NodePersistence},
    monitor::PaxosObserver,
    node::routing::{LocalRoutingDecision, ProtocolInboxRouter, ProtocolRouter, RoutingDecision},
    node::vertical_paxos::{
        acceptor::Acceptor,
        leader::{Leader, LeaderCallbacks},
        message::VerticalPaxosMessage,
        node_state::{
            InstalledRoles, LeaderLifecycle, ReplicaLifecycle, VerticalPaxosNodeStateSketch,
        },
        replica::{Replica, VerticalClientReply},
        router::{VerticalInboxContext, VerticalPaxosComponent, VerticalPaxosRouter},
        transport::{VerticalPaxosFabric, VerticalPaxosHandle},
    },
    paxos_command::PaxosCommand,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerticalNodeSnapshot {
    pub installed_roles: InstalledRoles,
    pub acceptor_online: bool,
    pub leader: LeaderLifecycle,
    pub replica: ReplicaLifecycle,
}

pub struct VerticalNode {
    id: Uuid,
    acceptor: Arc<Acceptor>,
    leader: Arc<Leader>,
    replica: Arc<Replica>,
    handle: Arc<VerticalPaxosHandle>,
    router: Arc<VerticalPaxosRouter>,
    state: Arc<Mutex<VerticalPaxosNodeStateSketch>>,
    running: Arc<RwLock<bool>>,
}

struct NodeLeaderCallbacks {
    node_id: Uuid,
    state: Arc<Mutex<VerticalPaxosNodeStateSketch>>,
    master_inbox: Sender<VerticalMasterEvent>,
}

#[async_trait]
impl LeaderCallbacks for NodeLeaderCallbacks {
    async fn on_sync_started(&self, snapshot: Arc<ActivationSnapshot>) {
        let _ = self
            .master_inbox
            .send(VerticalMasterEvent::SyncStarted {
                leader_id: self.node_id,
                configuration_id: snapshot.configuration().id(),
                ballot: snapshot.configuration().ballot(),
            })
            .await;
    }

    async fn on_sync_ready(&self, snapshot: Arc<ActivationSnapshot>) {
        {
            let mut state = self.state.lock().await;
            state.mark_leader_ready(Arc::clone(&snapshot));
        }
        let _ = self
            .master_inbox
            .send(VerticalMasterEvent::SyncReady {
                leader_id: self.node_id,
                configuration_id: snapshot.configuration().id(),
                ballot: snapshot.configuration().ballot(),
            })
            .await;
    }

    async fn on_sync_superseded(&self, snapshot: Arc<ActivationSnapshot>, superseded_by: Ballot) {
        {
            let mut state = self.state.lock().await;
            state.supersede(superseded_by);
        }
        let _ = self
            .master_inbox
            .send(VerticalMasterEvent::SyncSuperseded {
                leader_id: self.node_id,
                configuration_id: snapshot.configuration().id(),
                ballot: snapshot.configuration().ballot(),
                superseded_by,
            })
            .await;
    }
}

impl VerticalNode {
    pub async fn new(
        id: Uuid,
        persistence: NodePersistence,
        fabric: Arc<VerticalPaxosFabric>,
        observer: Arc<dyn PaxosObserver>,
        master_inbox: Sender<VerticalMasterEvent>,
    ) -> anyhow::Result<Self> {
        let state = Arc::new(Mutex::new(VerticalPaxosNodeStateSketch::new(id)));
        let callbacks: Arc<dyn LeaderCallbacks> = Arc::new(NodeLeaderCallbacks {
            node_id: id,
            state: Arc::clone(&state),
            master_inbox,
        });
        let acceptor =
            Arc::new(Acceptor::new(id, persistence.clone(), Arc::clone(&observer)).await?);
        let handle = Arc::new(VerticalPaxosHandle::from_fabric(id, fabric));
        let replica = Arc::new(
            Replica::new(
                id,
                persistence.clone(),
                Arc::clone(&handle),
                Arc::clone(&observer),
            )
            .await?,
        );
        let leader =
            Arc::new(Leader::new(id, persistence, Arc::clone(&handle), observer, callbacks).await?);
        Ok(Self {
            id,
            acceptor,
            leader,
            replica,
            handle,
            router: Arc::new(VerticalPaxosRouter),
            state,
            running: Arc::new(RwLock::new(false)),
        })
    }

    pub async fn install_configuration(&self, configuration: Arc<VerticalClusterConfiguration>) {
        self.leader.reset().await;
        self.replica
            .install_configuration(Arc::clone(&configuration))
            .await;
        let mut state = self.state.lock().await;
        state.install_configuration(configuration);
    }

    pub async fn start_activation(&self, snapshot: Arc<ActivationSnapshot>) {
        {
            let mut state = self.state.lock().await;
            state.start_activation(Arc::clone(&snapshot));
        }
        self.leader.install_activation(snapshot).await;
        self.leader.start_synchronization().await;
    }

    pub async fn preempt_leader(&self, preempted_by: Ballot) {
        self.leader.reset().await;
        let mut state = self.state.lock().await;
        state.supersede(preempted_by);
    }

    pub async fn activate_replica_set(&self, configuration: Arc<VerticalClusterConfiguration>) {
        self.replica
            .activate_configuration(Arc::clone(&configuration))
            .await;
        let mut state = self.state.lock().await;
        state.activate_replica_set(configuration);
    }

    pub async fn decommission_replica_set(
        &self,
        active_configuration: Arc<VerticalClusterConfiguration>,
    ) {
        self.replica
            .decommission_to(Arc::clone(&active_configuration))
            .await;
        let mut state = self.state.lock().await;
        state.decommission_to(active_configuration);
    }

    pub async fn submit_client_request(&self, cmd: PaxosCommand) -> VerticalClientReply {
        self.replica.submit_client_request(cmd).await
    }

    pub async fn snapshot(&self) -> VerticalNodeSnapshot {
        let state = self.state.lock().await;
        VerticalNodeSnapshot {
            installed_roles: state.installed_roles().clone(),
            acceptor_online: state.acceptor_runtime().online(),
            leader: state.leader().clone(),
            replica: state.replica().clone(),
        }
    }

    async fn send_reply(&self, msg: VerticalPaxosMessage) {
        match self.router.route(&msg, &()) {
            RoutingDecision::SendTo(to) => self.handle.send(to, msg).await,
            RoutingDecision::Broadcast | RoutingDecision::SendToMany(_) | RoutingDecision::Drop => {
            }
        }
    }

    async fn should_route_to_acceptor(&self) -> bool {
        let state = self.state.lock().await;
        state.installed_roles().member_of_acceptor_set() && state.acceptor_runtime().online()
    }

    async fn handle_message(&self, msg: VerticalPaxosMessage) {
        let inbox_context = VerticalInboxContext {
            acceptor_online: self.should_route_to_acceptor().await,
        };

        match self.router.classify(&msg, &inbox_context) {
            LocalRoutingDecision::DeliverTo(VerticalPaxosComponent::Acceptor) => {
                if let Some(reply) = self.acceptor.handle_message(msg).await {
                    self.send_reply(reply).await;
                }
            }
            LocalRoutingDecision::DeliverTo(VerticalPaxosComponent::Leader) => {
                if let Some(reply) = self.leader.handle_message(msg).await {
                    self.send_reply(reply).await;
                }
            }
            LocalRoutingDecision::DeliverTo(VerticalPaxosComponent::Replica) => {
                if let VerticalPaxosMessage::ACCEPTED { pvalue, .. } = msg {
                    if let Some(reply) = self.replica.handle_decision(pvalue).await {
                        self.send_reply(reply).await;
                    }
                }
            }
            LocalRoutingDecision::Drop => {}
        }
    }

    fn clone_for_runtime(&self) -> Self {
        Self {
            id: self.id,
            acceptor: Arc::clone(&self.acceptor),
            leader: Arc::clone(&self.leader),
            replica: Arc::clone(&self.replica),
            handle: Arc::clone(&self.handle),
            router: Arc::clone(&self.router),
            state: Arc::clone(&self.state),
            running: Arc::clone(&self.running),
        }
    }
}

#[async_trait]
impl RuntimeNode<VerticalPaxosMessage> for VerticalNode {
    fn id(&self) -> Uuid {
        self.id
    }

    async fn start(&self, mut inbox: Receiver<VerticalPaxosMessage>) -> JoinHandle<()> {
        {
            let mut running = self.running.write().await;
            *running = true;
        }
        {
            let mut state = self.state.lock().await;
            state.set_acceptor_online(true);
            state.set_replica_online(true);
        }

        let node = self.clone_for_runtime();
        tokio::spawn(async move {
            while let Some(msg) = inbox.recv().await {
                if !*node.running.read().await {
                    break;
                }
                node.handle_message(msg).await;
            }
        })
    }

    async fn shutdown(&self) {
        {
            let mut running = self.running.write().await;
            *running = false;
        }
        let mut state = self.state.lock().await;
        state.set_acceptor_online(false);
        state.set_replica_online(false);
    }
}

#[cfg(test)]
mod tests;
