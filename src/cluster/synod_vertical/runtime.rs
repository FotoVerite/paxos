use std::sync::Arc;

use anyhow::Context;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    cluster::{
        runtime::NodeRegistry,
        vertical::{
            activation::ActivationSnapshot, configuration::VerticalClusterConfiguration,
            master::VerticalMasterEvent,
        },
    },
    common::{ballot::Ballot, persistence::ClusterPersistence},
    monitor::PaxosObserver,
    node::vertical_paxos::{
        message::VerticalPaxosMessage,
        node::{VerticalNode, VerticalNodeSnapshot},
        node_factory::VerticalNodeFactory,
        replica::VerticalClientReply,
        transport::{VerticalPaxosFabric, new_vertical_paxos_fabric},
    },
};

pub struct SynodVerticalRuntime {
    member_ids: Vec<Uuid>,
    node_factory: Arc<VerticalNodeFactory>,
    node_registry: NodeRegistry<VerticalPaxosMessage>,
    fabric: Arc<VerticalPaxosFabric>,
    persistence: Arc<ClusterPersistence>,
    observer: Arc<dyn PaxosObserver>,
}

impl SynodVerticalRuntime {
    pub async fn new(
        member_ids: Vec<Uuid>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
        master_inbox: mpsc::Sender<VerticalMasterEvent>,
    ) -> anyhow::Result<Self> {
        let fabric = Arc::new(new_vertical_paxos_fabric(Arc::clone(&observer)));
        let node_factory = Arc::new(VerticalNodeFactory::new(
            Arc::clone(&fabric),
            Arc::clone(&persistence),
            Arc::clone(&observer),
            master_inbox,
        ));
        let node_registry = NodeRegistry::init(
            member_ids.clone(),
            Arc::clone(&fabric),
            node_factory.clone(),
        )
        .await?;

        Ok(Self {
            member_ids,
            node_factory,
            node_registry,
            fabric,
            persistence,
            observer,
        })
    }

    pub fn member_ids(&self) -> &[Uuid] {
        &self.member_ids
    }

    pub fn fabric(&self) -> Arc<VerticalPaxosFabric> {
        Arc::clone(&self.fabric)
    }

    pub fn persistence(&self) -> Arc<ClusterPersistence> {
        Arc::clone(&self.persistence)
    }

    pub fn observer(&self) -> Arc<dyn PaxosObserver> {
        Arc::clone(&self.observer)
    }

    pub async fn start(&self) {
        self.node_registry.start().await;
    }

    pub async fn stop(&self) {
        for id in self.member_ids.iter().copied() {
            self.node_registry.stop_process(id).await;
        }
    }

    pub async fn stop_node(&self, id: Uuid) {
        self.node_registry.stop_process(id).await;
    }

    pub async fn spawn_node(&self, id: Uuid) -> anyhow::Result<()> {
        self.node_registry.spawn_process(id).await
    }

    pub async fn node(&self, id: Uuid) -> Option<Arc<VerticalNode>> {
        self.node_factory.get(id).await
    }

    pub async fn snapshot(&self, id: Uuid) -> Option<VerticalNodeSnapshot> {
        Some(self.node(id).await?.snapshot().await)
    }

    pub async fn install_configuration_on_nodes(
        &self,
        configuration: Arc<VerticalClusterConfiguration>,
    ) -> anyhow::Result<()> {
        for node_id in self.member_ids.iter().copied() {
            let node = self
                .node(node_id)
                .await
                .with_context(|| format!("vertical node {node_id} was not built"))?;
            node.install_configuration(Arc::clone(&configuration)).await;
        }
        Ok(())
    }

    pub async fn activate_designated_leader(
        &self,
        snapshot: Arc<ActivationSnapshot>,
    ) -> anyhow::Result<()> {
        let leader_id = snapshot.configuration().leader();
        let leader = self
            .node(leader_id)
            .await
            .with_context(|| format!("activation leader {leader_id} was not built"))?;
        leader.start_activation(snapshot).await;
        Ok(())
    }

    pub async fn activate_replica_set(
        &self,
        configuration: Arc<VerticalClusterConfiguration>,
    ) -> anyhow::Result<()> {
        for node_id in configuration.replicas().iter().copied() {
            let node = self
                .node(node_id)
                .await
                .with_context(|| format!("vertical replica node {node_id} was not built"))?;
            node.activate_replica_set(Arc::clone(&configuration)).await;
        }
        Ok(())
    }

    pub async fn decommission_replica_set(
        &self,
        configuration: Arc<VerticalClusterConfiguration>,
        active_configuration: Arc<VerticalClusterConfiguration>,
    ) -> anyhow::Result<()> {
        for node_id in configuration.replicas().iter().copied() {
            let node = self
                .node(node_id)
                .await
                .with_context(|| format!("vertical replica node {node_id} was not built"))?;
            node.decommission_replica_set(Arc::clone(&active_configuration))
                .await;
        }
        Ok(())
    }

    pub async fn preempt_leader_node(
        &self,
        leader_id: Uuid,
        preempted_by: Ballot,
    ) -> anyhow::Result<()> {
        let leader = self
            .node(leader_id)
            .await
            .with_context(|| format!("preempted leader {leader_id} was not built"))?;
        leader.preempt_leader(preempted_by).await;
        Ok(())
    }

    pub async fn submit_client_request(
        &self,
        replica_id: Uuid,
        cmd: crate::paxos_command::PaxosCommand,
    ) -> anyhow::Result<VerticalClientReply> {
        let replica = self
            .node(replica_id)
            .await
            .with_context(|| format!("replica ingress node {replica_id} was not built"))?;
        Ok(replica.submit_client_request(cmd).await)
    }
}
