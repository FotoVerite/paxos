//! PMMC runtime process management.
//!
//! This owns node lifecycle, network fault controls, and the wiring between
//! PMMC nodes and the configuration control plane.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, mpsc::{Receiver, Sender}};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{
    cluster::{
        pmmc::{
            control::{
            endpoint::ConfigurationEndpoint,
            ConfigurationHandler,
            types::{
                ConfigurationCommand, ConfigurationHandlerError, ConfigurationOperationId,
                ConfigurationOperationStatus, ConfigurationReplyOutcome,
            },
        },
            reconfiguration::ClusterConfiguration,
        },
        runtime::{NodeRegistry, RuntimeState},
    },
    common::{network_fabric::NetworkFailure, persistence::ClusterPersistence},
    message::ClientMessage,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        config::PmmcNodeConfig,
        pmmc::{
            message::PmmcMessage,
            node_factory::PmmcNodeFactory,
            pmmc_node::PmmcNode,
            transport::PmmcFabric,
        },
    },
};

const CONFIG_HANDLER_ID: Uuid = Uuid::from_u128(0xC5);

pub struct PmmcProcessManager {
    member_ids: Vec<Uuid>,
    node_registry: NodeRegistry<PmmcMessage>,
    node_factory: Arc<PmmcNodeFactory>,
    observer: Arc<dyn PaxosObserver>,
    handler: Arc<ConfigurationHandler>,
}

impl PmmcProcessManager {
    async fn bind_member_endpoint(&self, id: Uuid) -> anyhow::Result<()> {
        let (endpoint_tx, endpoint_rx) = mpsc::channel(256);
        let response_tx = self.handler.register_endpoint(id, endpoint_tx).await;
        let node = self
            .node_factory
            .get(id)
            .await
            .ok_or_else(|| anyhow::anyhow!("PMMC node {id} was not built"))?;
        node.attach_admin_transport(endpoint_rx, response_tx).await;
        Ok(())
    }

    pub async fn init(
        members: Vec<(Uuid, PmmcNodeConfig)>,
        fabric: Arc<PmmcFabric>,
        configuration: Arc<ClusterConfiguration>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let member_ids = members.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let node_factory = Arc::new(PmmcNodeFactory::new(
            Arc::clone(&fabric),
            persistence,
            Arc::clone(&observer),
            configuration,
        ));
        let node_registry =
            NodeRegistry::init(member_ids.clone(), fabric, node_factory.clone()).await?;

        let process_manager = Self {
            member_ids,
            node_registry,
            node_factory,
            observer,
            handler: ConfigurationHandler::new(CONFIG_HANDLER_ID),
        };

        for (uuid, _) in members {
            process_manager.bind_member_endpoint(uuid).await?;
        }

        Ok(process_manager)
    }

    pub async fn start(&self) {
        self.handler.clone().start().await;
        self.node_registry.start().await;
    }

    pub async fn start_for_reconfiguration(&self) {
        self.handler.clone().start().await;
        self.node_registry.start().await;
        for id in self.member_ids.iter().copied() {
            self.observe_event(Event::ReconfigurationNodeRebooted {
                id,
                created_at: current_timestamp_millis(),
            });
        }
    }

    pub fn observe_event(&self, event: Event) {
        self.observer.on_event(event);
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

    pub async fn configuration_operation(
        &self,
        endpoint: Uuid,
        cmd: ConfigurationCommand,
        timeout: Duration,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        let operation_id = self.submit_configuration_op(endpoint, cmd).await?;
        self.await_configuration_op(operation_id, timeout).await
    }

    pub async fn configuration_operation_batch<I, F>(
        &self,
        endpoints: I,
        mut cmd_for: F,
        timeout: Duration,
    ) -> HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>
    where
        I: IntoIterator<Item = Uuid>,
        F: FnMut(Uuid) -> ConfigurationCommand,
    {
        let mut results: HashMap<
            Uuid,
            Result<ConfigurationReplyOutcome, ConfigurationHandlerError>,
        > = HashMap::new();
        let mut pending: Vec<(Uuid, ConfigurationOperationId)> = Vec::new();

        for endpoint in endpoints {
            let cmd = cmd_for(endpoint);
            match self.submit_configuration_op(endpoint, cmd).await {
                Ok(operation_id) => pending.push((endpoint, operation_id)),
                Err(err) => {
                    results.insert(endpoint, Err(err));
                }
            }
        }

        let mut set = JoinSet::new();
        for (endpoint, operation_id) in pending {
            results.insert(
                endpoint,
                Err(ConfigurationHandlerError::Internal {
                    reason: "configuration batch await task did not complete".to_string(),
                }),
            );
            let handler = Arc::clone(&self.handler);
            set.spawn(async move {
                let outcome = handler.await_outcome(operation_id, timeout).await;
                (endpoint, outcome)
            });
        }

        while let Some(joined) = set.join_next().await {
            if let Ok((endpoint, outcome)) = joined {
                results.insert(endpoint, outcome);
            }
        }

        results
    }

    pub async fn configuration_op_status(
        &self,
        operation_id: ConfigurationOperationId,
    ) -> Result<ConfigurationOperationStatus, ConfigurationHandlerError> {
        self.handler.status(operation_id).await
    }

    pub async fn state(&self, uuid: Uuid) -> Option<RuntimeState> {
        self.node_registry.state(uuid).await
    }

    pub async fn transition_state(
        &self,
        uuid: Uuid,
        expected: RuntimeState,
        next: RuntimeState,
    ) -> bool {
        self.node_registry.transition_state(uuid, expected, next).await
    }

    pub async fn isolate_node(&self, uuid: Uuid) -> bool {
        if !self
            .transition_state(uuid, RuntimeState::Active, RuntimeState::Partitioned)
            .await
        {
            return false;
        }

        let fabric = self.node_registry.fabric();
        for member in &self.member_ids {
            if *member == uuid {
                continue;
            }
            fabric
                .set_failure(uuid, *member, NetworkFailure::Partition)
                .await;
            fabric
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

        let fabric = self.node_registry.fabric();
        for member in &self.member_ids {
            if *member == uuid {
                continue;
            }
            fabric.clear_failure(*member, uuid).await;
        }
        fabric.clear_failures_from(uuid).await;
        true
    }

    pub async fn crash_node(&self, uuid: Uuid) -> bool {
        if !self
            .transition_state(uuid, RuntimeState::Active, RuntimeState::Crashed)
            .await
        {
            return false;
        }

        let fabric = self.node_registry.fabric();
        for member in &self.member_ids {
            if *member == uuid {
                continue;
            }
            fabric
                .set_failure(uuid, *member, NetworkFailure::Partition)
                .await;
            fabric
                .set_failure(*member, uuid, NetworkFailure::Partition)
                .await;
        }
        true
    }

    pub async fn stop_process(&self, id: Uuid) {
        self.node_registry.stop_process(id).await;
    }

    pub async fn stop_member(&self, uuid: Uuid) {
        self.stop_process(uuid).await;
    }

    pub async fn retire_endpoint(&self, id: Uuid) {
        self.handler.unregister_endpoint(id).await;
    }

    pub async fn reset(&self) {
        let fabric = self.node_registry.fabric();
        for member in self.member_ids.iter().copied() {
            self.stop_member(member).await;
            self.handler.unregister_endpoint(member).await;
            fabric.unregister(member).await;
        }
    }

    pub async fn reset_for_reconfiguration(&self) {
        let fabric = self.node_registry.fabric();
        for member in self.member_ids.iter().copied() {
            self.stop_member(member).await;
            self.observe_event(Event::ReconfigurationNodeRetired {
                id: member,
                created_at: current_timestamp_millis(),
            });
            self.handler.unregister_endpoint(member).await;
            fabric.unregister(member).await;
        }
    }

    pub async fn current_leader(&self) -> Option<Arc<PmmcNode>> {
        let mut leader = None;
        for member_id in &self.member_ids {
            let Some(node) = self.node_factory.get(*member_id).await else {
                continue;
            };
            if node.is_leader().await {
                match leader {
                    Some(_) => return None,
                    None => leader = Some(node),
                }
            }
        }
        leader
    }

    pub async fn connect_client_to(
        &self,
        uuid: Uuid,
        client_id: Uuid,
    ) -> Option<(Sender<ClientMessage>, Receiver<ClientMessage>)> {
        self.node_registry.connect_client_to(uuid, client_id).await
    }
}
