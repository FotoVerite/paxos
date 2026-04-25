use std::sync::Arc;

use anyhow::Context;
use uuid::Uuid;

use crate::{
    cluster::{
        runtime::{ControlPlaneRegistry, RuntimeNode},
        vertical::{
            activation::{ActivationSnapshot, PredecessorChain},
            configuration::VerticalClusterConfiguration,
            master::VerticalMasterEvent,
        },
    },
    common::persistence::ClusterPersistence,
    monitor::PaxosObserver,
    node::vertical_paxos::replica::VerticalClientReply,
    paxos_command::PaxosCommand,
};

use super::{master::SynodVerticalMaster, runtime::SynodVerticalRuntime};

pub struct SynodVerticalSystem {
    runtime: Arc<SynodVerticalRuntime>,
    master: Arc<SynodVerticalMaster>,
    control_plane: Arc<ControlPlaneRegistry<VerticalMasterEvent>>,
}

impl SynodVerticalSystem {
    pub async fn new(
        member_ids: Vec<Uuid>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let control_plane = Arc::new(ControlPlaneRegistry::new());
        let master_id = Uuid::nil();
        let master_tx = control_plane.provision_endpoint(master_id).await;
        let runtime = Arc::new(
            SynodVerticalRuntime::new(member_ids, persistence, observer, master_tx).await?,
        );
        let master = Arc::new(SynodVerticalMaster::new(Arc::clone(&runtime)));
        control_plane
            .spawn_actor(
                master_id,
                master.clone() as Arc<dyn RuntimeNode<VerticalMasterEvent>>,
            )
            .await?;

        Ok(Self {
            runtime,
            master,
            control_plane,
        })
    }

    pub fn runtime(&self) -> Arc<SynodVerticalRuntime> {
        Arc::clone(&self.runtime)
    }

    pub fn master(&self) -> Arc<SynodVerticalMaster> {
        Arc::clone(&self.master)
    }

    pub async fn start(&self) {
        self.control_plane.start().await;
        self.runtime.start().await;
    }

    pub async fn install_configuration(
        &self,
        configuration: Arc<VerticalClusterConfiguration>,
    ) -> anyhow::Result<()> {
        self.master.install_configuration(configuration).await
    }

    pub async fn start_activation(
        &self,
        configuration_id: Uuid,
        predecessor_chain: Arc<PredecessorChain>,
    ) -> anyhow::Result<Arc<ActivationSnapshot>> {
        self.master
            .start_activation(configuration_id, predecessor_chain)
            .await
    }

    pub async fn submit_client_request(
        &self,
        replica_id: Uuid,
        cmd: PaxosCommand,
    ) -> anyhow::Result<VerticalClientReply> {
        self.runtime.submit_client_request(replica_id, cmd).await
    }

    pub async fn snapshot(
        &self,
        node_id: Uuid,
    ) -> anyhow::Result<crate::node::vertical_paxos::node::VerticalNodeSnapshot> {
        self.runtime
            .snapshot(node_id)
            .await
            .with_context(|| format!("vertical node {node_id} was not built"))
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        self.control_plane.stop_process(Uuid::nil()).await;
        self.runtime.stop().await;
        self.runtime.persistence().purge_cluster_dir().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
