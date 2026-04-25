use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use tokio::{sync::RwLock, sync::mpsc::Receiver, task::JoinHandle};
use uuid::Uuid;

use crate::{
    cluster::{
        runtime::RuntimeNode,
        vertical::{
            activation::{ActivationSnapshot, PredecessorChain},
            configuration::VerticalClusterConfiguration,
            master::VerticalMasterEvent,
        },
    },
    common::ballot::Ballot,
};

use super::runtime::SynodVerticalRuntime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationStatus {
    Synchronizing,
    Ready,
    Preempted { by_ballot: Ballot },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationRecord {
    snapshot: Arc<ActivationSnapshot>,
    status: ActivationStatus,
}

impl ActivationRecord {
    pub fn started(snapshot: Arc<ActivationSnapshot>) -> Self {
        Self {
            snapshot,
            status: ActivationStatus::Synchronizing,
        }
    }

    pub fn mark_ready(&mut self) {
        self.status = ActivationStatus::Ready;
    }

    pub fn preempt(&mut self, by_ballot: Ballot) {
        self.status = ActivationStatus::Preempted { by_ballot };
    }

    pub fn snapshot(&self) -> &Arc<ActivationSnapshot> {
        &self.snapshot
    }

    pub fn status(&self) -> &ActivationStatus {
        &self.status
    }
}

pub struct SynodVerticalMaster {
    runtime: Arc<SynodVerticalRuntime>,
    configurations: Arc<RwLock<HashMap<Uuid, Arc<VerticalClusterConfiguration>>>>,
    activations: Arc<RwLock<HashMap<Uuid, ActivationRecord>>>,
    active_configuration_id: Arc<RwLock<Option<Uuid>>>,
}

impl SynodVerticalMaster {
    pub fn new(runtime: Arc<SynodVerticalRuntime>) -> Self {
        Self {
            runtime,
            configurations: Arc::new(RwLock::new(HashMap::new())),
            activations: Arc::new(RwLock::new(HashMap::new())),
            active_configuration_id: Arc::new(RwLock::new(None)),
        }
    }

    pub fn runtime(&self) -> Arc<SynodVerticalRuntime> {
        Arc::clone(&self.runtime)
    }

    pub async fn configuration(&self, id: Uuid) -> Option<Arc<VerticalClusterConfiguration>> {
        self.configurations.read().await.get(&id).cloned()
    }

    pub async fn activation(&self, configuration_id: Uuid) -> Option<ActivationRecord> {
        self.activations
            .read()
            .await
            .get(&configuration_id)
            .cloned()
    }

    pub async fn active_configuration_id(&self) -> Option<Uuid> {
        *self.active_configuration_id.read().await
    }

    pub async fn install_configuration(
        &self,
        configuration: Arc<VerticalClusterConfiguration>,
    ) -> anyhow::Result<()> {
        self.runtime
            .install_configuration_on_nodes(Arc::clone(&configuration))
            .await?;
        self.configurations
            .write()
            .await
            .insert(configuration.id(), Arc::clone(&configuration));
        for node_id in self.runtime.member_ids().iter().copied() {
            let mut roles = Vec::new();
            if configuration.leader() == node_id {
                roles.push("Leader".to_string());
            }
            if configuration.acceptors().contains(&node_id) {
                roles.push("Acceptor".to_string());
            }
            if configuration.replicas().contains(&node_id) {
                roles.push("Replica".to_string());
            }
            self.runtime
                .observer()
                .on_event(crate::monitor::Event::NodeCapabilities {
                    id: node_id,
                    roles,
                    learning_strategy: "Vertical".to_string(),
                });
        }
        self.runtime
            .observer()
            .on_event(crate::monitor::Event::VerticalConfigurationInstalled {
                configuration_id: configuration.id(),
                leader: configuration.leader(),
                replicas: configuration.replicas().to_vec(),
                acceptors: configuration.acceptors().to_vec(),
                created_at: crate::monitor::current_timestamp_millis(),
            });
        Ok(())
    }

    pub async fn start_activation(
        &self,
        configuration_id: Uuid,
        predecessor_chain: Arc<PredecessorChain>,
    ) -> anyhow::Result<Arc<ActivationSnapshot>> {
        let configuration = self
            .configuration(configuration_id)
            .await
            .with_context(|| format!("unknown vertical configuration {configuration_id}"))?;
        let snapshot = Arc::new(ActivationSnapshot::new(
            Arc::clone(&configuration),
            predecessor_chain,
        ));
        self.activations.write().await.insert(
            configuration_id,
            ActivationRecord::started(Arc::clone(&snapshot)),
        );
        self.runtime
            .activate_designated_leader(Arc::clone(&snapshot))
            .await?;
        self.runtime
            .observer()
            .on_event(crate::monitor::Event::VerticalActivationStarted {
                configuration_id,
                leader: configuration.leader(),
                ballot: configuration.ballot(),
                created_at: crate::monitor::current_timestamp_millis(),
            });
        Ok(snapshot)
    }

    pub async fn preempt_leader(
        &self,
        configuration_id: Uuid,
        preempted_by: Ballot,
    ) -> anyhow::Result<()> {
        let configuration = self
            .configuration(configuration_id)
            .await
            .with_context(|| format!("unknown vertical configuration {configuration_id}"))?;

        self.runtime
            .preempt_leader_node(configuration.leader(), preempted_by)
            .await?;

        let mut activations = self.activations.write().await;
        let record = activations
            .get_mut(&configuration_id)
            .with_context(|| format!("configuration {configuration_id} has no activation"))?;
        record.preempt(preempted_by);
        self.runtime
            .observer()
            .on_event(crate::monitor::Event::VerticalActivationSuperseded {
                configuration_id,
                leader: configuration.leader(),
                ballot: configuration.ballot(),
                superseded_by: preempted_by,
                created_at: crate::monitor::current_timestamp_millis(),
            });
        Ok(())
    }

    async fn handle_event(&self, event: VerticalMasterEvent) {
        match event {
            VerticalMasterEvent::SyncStarted {
                configuration_id, ..
            } => {
                if let Some(record) = self.activations.write().await.get_mut(&configuration_id) {
                    record.status = ActivationStatus::Synchronizing;
                }
            }
            VerticalMasterEvent::SyncReady {
                configuration_id,
                leader_id,
                ballot,
            } => {
                if let Some(record) = self.activations.write().await.get_mut(&configuration_id) {
                    record.mark_ready();
                }
                if let Some(configuration) = self.configuration(configuration_id).await {
                    let previous = *self.active_configuration_id.read().await;
                    if self
                        .runtime
                        .activate_replica_set(Arc::clone(&configuration))
                        .await
                        .is_ok()
                    {
                        self.runtime.observer().on_event(
                            crate::monitor::Event::VerticalActivationReady {
                                configuration_id,
                                leader: leader_id,
                                ballot,
                                created_at: crate::monitor::current_timestamp_millis(),
                            },
                        );
                        self.runtime.observer().on_event(
                            crate::monitor::Event::VerticalReplicaSetActivated {
                                configuration_id,
                                replicas: configuration.replicas().to_vec(),
                                created_at: crate::monitor::current_timestamp_millis(),
                            },
                        );
                        *self.active_configuration_id.write().await = Some(configuration_id);

                        if let Some(previous_id) = previous.filter(|id| *id != configuration_id) {
                            if let Some(previous_configuration) =
                                self.configuration(previous_id).await
                            {
                                if self
                                    .runtime
                                    .decommission_replica_set(
                                        Arc::clone(&previous_configuration),
                                        Arc::clone(&configuration),
                                    )
                                    .await
                                    .is_ok()
                                {
                                    self.runtime.observer().on_event(
                                        crate::monitor::Event::VerticalReplicaSetDecommissioned {
                                            configuration_id: previous_id,
                                            replicas: previous_configuration.replicas().to_vec(),
                                            created_at: crate::monitor::current_timestamp_millis(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            }
            VerticalMasterEvent::SyncSuperseded {
                configuration_id,
                superseded_by,
                ..
            } => {
                if let Some(record) = self.activations.write().await.get_mut(&configuration_id) {
                    record.preempt(superseded_by);
                }
            }
        }
    }
}

#[async_trait]
impl RuntimeNode<VerticalMasterEvent> for SynodVerticalMaster {
    fn id(&self) -> Uuid {
        Uuid::nil()
    }

    async fn start(&self, mut inbox: Receiver<VerticalMasterEvent>) -> JoinHandle<()> {
        let master = self.clone_for_runtime();
        tokio::spawn(async move {
            while let Some(event) = inbox.recv().await {
                master.handle_event(event).await;
            }
        })
    }

    async fn shutdown(&self) {}
}

impl SynodVerticalMaster {
    fn clone_for_runtime(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            configurations: Arc::clone(&self.configurations),
            activations: Arc::clone(&self.activations),
            active_configuration_id: Arc::clone(&self.active_configuration_id),
        }
    }
}

#[cfg(test)]
mod tests;
