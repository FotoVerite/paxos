use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{
    Mutex, RwLock,
    mpsc::{self, Receiver, Sender},
};
use uuid::Uuid;

use crate::cluster::{
    cluster_configuration::ClusterConfiguration,
    configuration_handler::{
        ConfigurationHandler,
        endpoint::ConfigurationEndpoint,
        types::{
            ConfigurationCommand, ConfigurationHandlerError, ConfigurationHandlerMessage,
            ConfigurationOperationId, ConfigurationOperationStatus, ConfigurationReplyOutcome,
        },
    },
    runtime_state::RuntimeState,
};
use crate::{
    cluster::{
        network_fabric::NetworkFabric, network_handle::NetworkFailure,
        runtime_member::RuntimeMember,
    },
    common::persistence::ClusterPersistence,
    message::{ClientMessage, Message},
    monitor::PaxosObserver,
    node::config::{PmmcNodeConfig, Roles},
};

pub struct RuntimeRegistry {
    members: Arc<RwLock<HashMap<Uuid, Arc<RuntimeMember>>>>,
    member_ids: Vec<Uuid>,
    inboxes: RwLock<HashMap<Uuid, Arc<Mutex<Option<Receiver<Message>>>>>>,
    fabric: Arc<NetworkFabric>,
    handler: Arc<ConfigurationHandler>,
}

const CONFIG_HANDLER_ID: Uuid = Uuid::from_u128(0xC5);

impl RuntimeRegistry {
    async fn bind_member_endpoint(&self, id: Uuid, member: Arc<RuntimeMember>) {
        let (endpoint_tx, endpoint_rx) = mpsc::channel(256);
        let response_tx = self.handler.register_endpoint(id, endpoint_tx).await;
        member
            .attach_configuration_transport(endpoint_rx, response_tx)
            .await;
    }

    async fn take_provisioned_inbox(&self, id: Uuid) -> anyhow::Result<Receiver<Message>> {
        let inbox = {
            let inboxes = self.inboxes.read().await;
            inboxes.get(&id).cloned()
        };
        let Some(inbox) = inbox else {
            anyhow::bail!("runtime endpoint not provisioned for node {id}");
        };

        let mut rx = inbox.lock().await;
        let Some(inbox_rx) = rx.take() else {
            anyhow::bail!("runtime inbox already consumed for node {id}");
        };

        Ok(inbox_rx)
    }

    async fn build_member(
        &self,
        id: Uuid,
        roles: Roles,
        configuration: Arc<ClusterConfiguration>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<RuntimeMember> {
        let rx = self.take_provisioned_inbox(id).await?;
        RuntimeMember::new(
            id,
            roles,
            Arc::clone(&configuration),
            Arc::clone(&self.fabric),
            persistence.node(id),
            rx,
            observer,
        )
        .await
    }

    pub async fn init(
        members: Vec<(Uuid, PmmcNodeConfig)>,
        fabric: Arc<NetworkFabric>,
        configuration: Arc<ClusterConfiguration>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let member_ids = members.iter().map(|(id, _)| *id).collect();
        let registry = Self {
            members: Arc::new(RwLock::new(HashMap::new())),
            member_ids,
            inboxes: RwLock::new(HashMap::new()),
            fabric,
            handler: ConfigurationHandler::new(CONFIG_HANDLER_ID),
        };

        for (uuid, node_config) in members {
            registry.provision_endpoint(uuid).await;
            let member = Arc::new(
                registry
                    .build_member(
                        uuid,
                        node_config.roles,
                        Arc::clone(&configuration),
                        Arc::clone(&persistence),
                        Arc::clone(&observer),
                    )
                    .await?,
            );
            registry
                .bind_member_endpoint(uuid, Arc::clone(&member))
                .await;
            let mut runtime_members = registry.members.write().await;
            runtime_members.insert(uuid, member);
        }

        Ok(registry)
    }

    pub async fn provision_endpoint(&self, id: Uuid) {
        let (tx, rx) = mpsc::channel(1024);
        self.fabric.register(id, tx).await;

        let inbox = {
            let mut inboxes = self.inboxes.write().await;
            inboxes
                .entry(id)
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let mut inbox_rx = inbox.lock().await;
        *inbox_rx = Some(rx);
    }

    pub async fn spawn_process(
        &self,
        id: Uuid,
        roles: Roles,
        configuration: Arc<ClusterConfiguration>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<()> {
        self.provision_endpoint(id).await;

        let member = self
            .build_member(
                id,
                roles,
                Arc::clone(&configuration),
                Arc::clone(&persistence),
                observer,
            )
            .await?;
        let member = Arc::new(member);

        {
            let mut members = self.members.write().await;
            members.insert(id, Arc::clone(&member));
        }
        self.bind_member_endpoint(id, Arc::clone(&member)).await;
        member.start().await;
        Ok(())
    }

    pub async fn stop_process(&self, id: Uuid) {
        if let Some(member) = self.get(id).await {
            member.stop().await;
        }
    }

    pub async fn retire_endpoint(&self, id: Uuid) {
        self.handler.unregister_endpoint(id).await;
    }

    pub async fn start(&self) {
        self.handler.clone().start().await;
        let members: Vec<_> = {
            let members = self.members.read().await;
            members.values().cloned().collect()
        };
        for member in members {
            member.start().await;
        }
    }

    pub async fn submit_configuration_op(
        &self,
        endpoint: Uuid,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationOperationId, ConfigurationHandlerError> {
        self.handler.submit(endpoint, cmd).await
    }

    pub async fn await_configuration_op(
        &self,
        operation_id: ConfigurationOperationId,
        timeout: Duration,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        self.handler.await_outcome(operation_id, timeout).await
    }

    pub async fn configuration_op_status(
        &self,
        operation_id: ConfigurationOperationId,
    ) -> Result<ConfigurationOperationStatus, ConfigurationHandlerError> {
        self.handler.status(operation_id).await
    }

    pub async fn spawn_member(
        &self,
        uuid: Uuid,
        roles: Roles,
        configuration: Arc<ClusterConfiguration>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<()> {
        self.spawn_process(uuid, roles, configuration, persistence, observer)
            .await
    }

    pub async fn activate_member(&self, uuid: Uuid) {
        if let Some(member) = self.get(uuid).await {
            member.start().await;
        }
    }

    pub async fn stop_member(&self, uuid: Uuid) {
        self.stop_process(uuid).await;
    }

    pub async fn update_roles(_uuid: Uuid, _role: Roles) {}

    pub async fn get(&self, uuid: Uuid) -> Option<Arc<RuntimeMember>> {
        let members = self.members.read().await;
        members.get(&uuid).map(Arc::clone)
    }

    pub async fn current_leader(&self) -> Option<Arc<RuntimeMember>> {
        let members: Vec<_> = {
            let members = self.members.read().await;
            members.values().cloned().collect()
        };

        let mut leader = None;
        for member in members {
            if member.node.is_leader().await {
                match leader {
                    Some(_) => return None,
                    None => leader = Some(member),
                }
            }
        }
        leader
    }

    pub async fn state(&self, uuid: Uuid) -> Option<crate::cluster::runtime_state::RuntimeState> {
        let member = self.get(uuid).await?;
        Some(member.state().await)
    }

    pub async fn transition_state(
        &self,
        uuid: Uuid,
        expected: RuntimeState,
        next: RuntimeState,
    ) -> bool {
        let Some(member) = self.get(uuid).await else {
            return false;
        };
        member.transition_state(expected, next).await
    }

    pub async fn isolate_node(&self, uuid: Uuid) -> bool {
        if !self
            .transition_state(uuid, RuntimeState::Active, RuntimeState::Partitioned)
            .await
        {
            return false;
        }

        for member in &self.member_ids {
            if *member == uuid {
                continue;
            }
            self.fabric
                .set_failure(uuid, *member, NetworkFailure::Partition)
                .await;
            self.fabric
                .set_failure(*member, uuid, NetworkFailure::Partition)
                .await;
        }
        true
    }

    pub async fn heal_node(&self, uuid: Uuid) -> bool {
        if !self
            .transition_state(uuid, RuntimeState::Partitioned, RuntimeState::Active)
            .await
        {
            return false;
        }

        for member in &self.member_ids {
            if *member == uuid {
                continue;
            }
            self.fabric.clear_failure(*member, uuid).await;
        }
        self.fabric.clear_failures_from(uuid).await;
        true
    }

    pub async fn crash_node(&self, uuid: Uuid) -> bool {
        if !self
            .transition_state(uuid, RuntimeState::Active, RuntimeState::Crashed)
            .await
        {
            return false;
        }

        for member in &self.member_ids {
            if *member == uuid {
                continue;
            }
            self.fabric
                .set_failure(uuid, *member, NetworkFailure::Partition)
                .await;
            self.fabric
                .set_failure(*member, uuid, NetworkFailure::Partition)
                .await;
        }
        true
    }

    pub async fn register_configuration_endpoint(
        &self,
        endpoint: Uuid,
        sender: Sender<ConfigurationHandlerMessage>,
    ) -> Sender<ConfigurationHandlerMessage> {
        self.handler.register_endpoint(endpoint, sender).await
    }

    pub async fn unregister_configuration_endpoint(&self, endpoint: Uuid) {
        self.handler.unregister_endpoint(endpoint).await;
    }

    pub async fn connect_client_to(
        &self,
        uuid: Uuid,
        client_id: Uuid,
    ) -> Option<(Sender<ClientMessage>, Receiver<ClientMessage>)> {
        if let Some(m) = self.get(uuid).await {
            return m.connect_client(client_id).await;
        }
        None
    }
}
