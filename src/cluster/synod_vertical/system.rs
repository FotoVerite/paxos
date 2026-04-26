use std::{sync::Arc, time::Duration};

use anyhow::Context;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    cluster::{
        runtime::{ControlPlaneRegistry, RuntimeNode},
        vertical::{
            activation::{ActivationSnapshot, PredecessorChain},
            configuration::{VerticalClusterConfiguration, VerticalPaxosVariant},
            master::VerticalMasterEvent,
            quorum::Quorum,
        },
    },
    common::{ballot::Ballot, persistence::ClusterPersistence},
    monitor::PaxosObserver,
    node::vertical_paxos::replica::VerticalClientReply,
    paxos_command::PaxosCommand,
    rsm::checkpoint::RsmCheckpoint,
};

use super::{
    master::{ActivationStatus, SynodVerticalMaster},
    runtime::SynodVerticalRuntime,
};

pub struct SynodVerticalSystem {
    runtime: Arc<SynodVerticalRuntime>,
    master: Arc<SynodVerticalMaster>,
    control_plane: Arc<ControlPlaneRegistry<VerticalMasterEvent>>,
    configuration_sequence: Mutex<ConfigurationSequence>,
}

struct ConfigurationSequence {
    next_ballot_number: usize,
    next_start_index: usize,
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
            configuration_sequence: Mutex::new(ConfigurationSequence {
                next_ballot_number: 1,
                next_start_index: 0,
            }),
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

    pub async fn active_configuration(&self) -> Option<Arc<VerticalClusterConfiguration>> {
        let active_id = self.master.active_configuration_id().await?;
        self.master.configuration(active_id).await
    }

    pub async fn reconfigure_members(
        &self,
        member_ids: Vec<Uuid>,
        leader: Uuid,
    ) -> anyhow::Result<Arc<VerticalClusterConfiguration>> {
        self.reconfigure_members_with_checkpoint(member_ids, leader, None)
            .await
    }

    pub async fn reconfigure_members_with_checkpoint(
        &self,
        member_ids: Vec<Uuid>,
        leader: Uuid,
        checkpoint: Option<RsmCheckpoint>,
    ) -> anyhow::Result<Arc<VerticalClusterConfiguration>> {
        if member_ids.is_empty() {
            anyhow::bail!("cannot configure empty synod vertical membership");
        }
        if !member_ids.contains(&leader) {
            anyhow::bail!("leader {leader} is not in synod vertical membership");
        }

        let mut sequence = self.configuration_sequence.lock().await;
        let previous_configuration = self.active_configuration().await;
        let quorum = majority_quorum(&member_ids);
        let mut configuration = VerticalClusterConfiguration::new(
            Uuid::new_v4(),
            leader,
            VerticalPaxosVariant::V1,
            Ballot::new(sequence.next_ballot_number, leader),
            sequence.next_start_index,
            member_ids.clone(),
            member_ids,
            Quorum::new(quorum.clone())
                .map_err(|err| anyhow::anyhow!("read quorum should build: {err:?}"))?,
            Quorum::new(quorum)
                .map_err(|err| anyhow::anyhow!("write quorum should build: {err:?}"))?,
            previous_configuration
                .as_ref()
                .map(|configuration| configuration.ballot()),
        )
        .map_err(|err| anyhow::anyhow!("vertical configuration should build: {err:?}"))?;
        if let Some(checkpoint) = checkpoint {
            configuration = configuration.with_checkpoint(checkpoint);
        }
        let configuration = Arc::new(configuration);
        let configuration_id = configuration.id();
        let predecessor_chain = previous_configuration.into_iter().collect::<Vec<_>>();

        self.install_configuration(Arc::clone(&configuration))
            .await?;
        self.start_activation(
            configuration_id,
            Arc::new(PredecessorChain::new(predecessor_chain)),
        )
        .await?;
        self.wait_for_activation_ready(configuration_id).await?;

        sequence.next_ballot_number += 1;
        sequence.next_start_index = sequence
            .next_start_index
            .max(configuration.start_index() + 1);
        Ok(configuration)
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

    pub async fn replica_checkpoint(&self, node_id: Uuid) -> anyhow::Result<RsmCheckpoint> {
        self.runtime
            .replica_checkpoint(node_id)
            .await
            .with_context(|| format!("vertical node {node_id} was not built"))
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        self.control_plane.stop_process(Uuid::nil()).await;
        self.runtime.stop().await;
        self.runtime.persistence().purge_cluster_dir().await?;
        Ok(())
    }

    async fn wait_for_activation_ready(&self, configuration_id: Uuid) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self
                    .master()
                    .activation(configuration_id)
                    .await
                    .is_some_and(|record| record.status() == &ActivationStatus::Ready)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("timed out activating synod config {configuration_id}"))
    }
}

fn majority_quorum(member_ids: &[Uuid]) -> Vec<Uuid> {
    let majority = (member_ids.len() / 2) + 1;
    member_ids.iter().copied().take(majority).collect()
}

#[cfg(test)]
mod tests;
