use std::sync::Arc;

use uuid::Uuid;

use crate::cluster::vertical::{
    activation::ActivationSnapshot, configuration::VerticalClusterConfiguration,
};
use crate::common::ballot::Ballot;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InstalledRoles {
    designated_leader: bool,
    member_of_acceptor_set: bool,
    member_of_replica_set: bool,
}

impl InstalledRoles {
    fn from_configuration(node_id: Uuid, configuration: &VerticalClusterConfiguration) -> Self {
        Self {
            designated_leader: configuration.leader() == node_id,
            member_of_acceptor_set: configuration.acceptors().contains(&node_id),
            member_of_replica_set: configuration.replicas().contains(&node_id),
        }
    }

    pub fn designated_leader(&self) -> bool {
        self.designated_leader
    }

    pub fn member_of_acceptor_set(&self) -> bool {
        self.member_of_acceptor_set
    }

    pub fn member_of_replica_set(&self) -> bool {
        self.member_of_replica_set
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AcceptorRuntimeState {
    online: bool,
}

impl AcceptorRuntimeState {
    pub fn online(&self) -> bool {
        self.online
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicaRuntimeState {
    online: bool,
}

impl ReplicaRuntimeState {
    pub fn online(&self) -> bool {
        self.online
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderLifecycle {
    Inactive,
    Assigned {
        configuration_id: Uuid,
    },
    Synchronizing {
        snapshot: Arc<ActivationSnapshot>,
    },
    Ready {
        snapshot: Arc<ActivationSnapshot>,
    },
    Superseded {
        configuration_id: Uuid,
        superseded_by: Ballot,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaLifecycle {
    Inactive,
    Assigned { configuration_id: Uuid },
    Syncing { configuration_id: Uuid },
    Active { configuration_id: Uuid },
    Redirecting {
        installed_configuration_id: Uuid,
        active_configuration_id: Uuid,
    },
}

pub struct VerticalPaxosNodeStateSketch {
    node_id: Uuid,
    installed_configuration: Option<Arc<VerticalClusterConfiguration>>,
    active_configuration: Option<Arc<VerticalClusterConfiguration>>,
    installed_roles: InstalledRoles,
    acceptor_runtime: AcceptorRuntimeState,
    replica_runtime: ReplicaRuntimeState,
    leader: LeaderLifecycle,
    replica: ReplicaLifecycle,
}

impl VerticalPaxosNodeStateSketch {
    pub fn new(node_id: Uuid) -> Self {
        Self {
            node_id,
            installed_configuration: None,
            active_configuration: None,
            installed_roles: InstalledRoles::default(),
            acceptor_runtime: AcceptorRuntimeState::default(),
            replica_runtime: ReplicaRuntimeState::default(),
            leader: LeaderLifecycle::Inactive,
            replica: ReplicaLifecycle::Inactive,
        }
    }

    pub fn install_configuration(&mut self, configuration: Arc<VerticalClusterConfiguration>) {
        let roles = InstalledRoles::from_configuration(self.node_id, &configuration);
        let configuration_id = configuration.id();
        self.installed_configuration = Some(configuration);
        self.active_configuration = None;
        self.installed_roles = roles.clone();
        self.leader = if roles.designated_leader() {
            LeaderLifecycle::Assigned { configuration_id }
        } else {
            LeaderLifecycle::Inactive
        };
        self.replica = if roles.member_of_replica_set() {
            ReplicaLifecycle::Assigned { configuration_id }
        } else {
            ReplicaLifecycle::Inactive
        };
    }

    pub fn set_acceptor_online(&mut self, online: bool) {
        self.acceptor_runtime.online = online;
    }

    pub fn set_replica_online(&mut self, online: bool) {
        self.replica_runtime.online = online;
    }

    pub fn start_activation(&mut self, snapshot: Arc<ActivationSnapshot>) {
        self.leader = LeaderLifecycle::Synchronizing { snapshot };
        if self.installed_roles.member_of_replica_set() {
            if let Some(configuration) = &self.installed_configuration {
                self.replica = ReplicaLifecycle::Syncing {
                    configuration_id: configuration.id(),
                };
            }
        }
    }

    pub fn mark_leader_ready(&mut self, snapshot: Arc<ActivationSnapshot>) {
        self.leader = LeaderLifecycle::Ready { snapshot };
    }

    pub fn activate_replica_set(&mut self, configuration: Arc<VerticalClusterConfiguration>) {
        self.active_configuration = Some(Arc::clone(&configuration));
        if configuration.replicas().contains(&self.node_id) {
            self.replica = ReplicaLifecycle::Active {
                configuration_id: configuration.id(),
            };
        } else if let Some(installed) = &self.installed_configuration {
            if installed.replicas().contains(&self.node_id) {
                self.replica = ReplicaLifecycle::Redirecting {
                    installed_configuration_id: installed.id(),
                    active_configuration_id: configuration.id(),
                };
            } else {
                self.replica = ReplicaLifecycle::Inactive;
            }
        }
    }

    pub fn decommission_to(&mut self, active_configuration: Arc<VerticalClusterConfiguration>) {
        self.active_configuration = Some(Arc::clone(&active_configuration));
        if let Some(installed) = &self.installed_configuration {
            if installed.replicas().contains(&self.node_id) {
                self.replica = ReplicaLifecycle::Redirecting {
                    installed_configuration_id: installed.id(),
                    active_configuration_id: active_configuration.id(),
                };
            }
        }
    }

    pub fn supersede(&mut self, superseded_by: Ballot) {
        let configuration_id = self
            .installed_configuration
            .as_ref()
            .map(|configuration| configuration.id())
            .unwrap_or_default();
        self.leader = LeaderLifecycle::Superseded {
            configuration_id,
            superseded_by,
        };
    }

    pub fn installed_roles(&self) -> &InstalledRoles {
        &self.installed_roles
    }

    pub fn acceptor_runtime(&self) -> &AcceptorRuntimeState {
        &self.acceptor_runtime
    }

    pub fn replica_runtime(&self) -> &ReplicaRuntimeState {
        &self.replica_runtime
    }

    pub fn leader(&self) -> &LeaderLifecycle {
        &self.leader
    }

    pub fn replica(&self) -> &ReplicaLifecycle {
        &self.replica
    }

    pub fn active_configuration(&self) -> Option<Arc<VerticalClusterConfiguration>> {
        self.active_configuration.clone()
    }
}

#[cfg(test)]
mod tests;
