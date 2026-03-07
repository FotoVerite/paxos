use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{
    RwLock,
    mpsc::{self, Receiver, Sender},
};
use uuid::Uuid;

use crate::cluster::{
    cluster_configuration::ClusterConfiguration, configuration_handler::ConfigurationHandler,
    runtime_state::RuntimeState,
};
use crate::{
    cluster::{
        network_fabric::NetworkFabric, network_simulator::NetworkFailure,
        runtime_member::RuntimeMember,
    },
    common::persistence::ClusterPersistence,
    message::ClientMessage,
    monitor::PaxosObserver,
    node::config::{PmmcNodeConfig, Roles},
};

pub struct RuntimeRegistry {
    members: RwLock<HashMap<Uuid, Arc<RuntimeMember>>>,
    member_ids: Vec<Uuid>,
    fabric: Arc<NetworkFabric>,
    handler: Arc<ConfigurationHandler>,
}

const CONFIG_HANDLER_ID: Uuid = Uuid::from_u128(0xC5);

impl RuntimeRegistry {
    pub async fn init(
        members: Vec<(Uuid, PmmcNodeConfig)>,
        fabric: Arc<NetworkFabric>,
        configuration: Arc<ClusterConfiguration>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let mut runtime_members = HashMap::new();
        let mut member_ids = Vec::new();

        for (uuid, roles) in members {
            let (tx, rx) = mpsc::channel(1024);
            let fabric_arc = Arc::clone(&fabric);

            fabric_arc.register(uuid, tx).await;

            let member = RuntimeMember::new(
                uuid,
                roles.roles,
                Arc::clone(&configuration),
                fabric_arc,
                persistence.node(uuid),
                rx,
                Arc::clone(&observer),
            )
            .await?;

            runtime_members.insert(uuid, Arc::new(member));
            member_ids.push(uuid);
        }
        let handler = ConfigurationHandler::new(CONFIG_HANDLER_ID, Arc::clone(&fabric));
        Ok(Self {
            members: RwLock::new(runtime_members),
            member_ids,
            fabric,
            handler,
        })
    }

    pub async fn start(&self) {
        let members: Vec<_> = {
            let members = self.members.read().await;
            members.values().cloned().collect()
        };
        for member in members {
            member.start().await;
        }
    }

    pub async fn spawn_member(
        &self,
        uuid: Uuid,
        roles: Roles,
        configuration: Arc<ClusterConfiguration>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel(1024);
        let fabric_arc = Arc::clone(&self.fabric);

        fabric_arc.register(uuid, tx).await;

        let member = RuntimeMember::new(
            uuid,
            roles,
            Arc::clone(&configuration),
            fabric_arc,
            persistence.node(uuid),
            rx,
            Arc::clone(&observer),
        )
        .await?;
        let arc_member = Arc::new(member);
        let mut members = self.members.write().await;
        members.insert(uuid, Arc::clone(&arc_member));
        drop(members);
        arc_member.start().await;
        return Ok(());
    }

    pub async fn activate_member(&self, uuid: Uuid) {
        if let Some(member) = self.get(uuid).await {
            member.start().await;
        }
    }

    pub async fn stop_member(&self, uuid: Uuid) {
        if let Some(member) = self.get(uuid).await {
            member.stop().await;
        }
    }

    pub async fn update_roles(_uuid: Uuid, _role: Roles) {}

    pub async fn get(&self, uuid: Uuid) -> Option<Arc<RuntimeMember>> {
        let members = self.members.read().await;
        members.get(&uuid).map(|m| Arc::clone(m))
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
