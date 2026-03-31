use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{
    Mutex, RwLock,
    mpsc::{self, Receiver, Sender},
};
use uuid::Uuid;

use crate::{
    cluster::runtime::{
        member::RuntimeMember,
        state::RuntimeState,
        types::{
            ProvisionedInbox, ProvisionedInboxes, RuntimeMemberIds, RuntimeMembers,
            SharedRuntimeFactory, SharedRuntimeMember,
        },
    },
    common::network_fabric::NetworkFabric,
    message::ClientMessage,
};

pub struct NodeRegistry<TMsg>
where
    TMsg: Clone + Send + Sync + 'static,
{
    members: RuntimeMembers<TMsg>,
    member_ids: RuntimeMemberIds,
    inboxes: ProvisionedInboxes<TMsg>,
    fabric: Arc<NetworkFabric<TMsg>>,
    factory: SharedRuntimeFactory<TMsg>,
}

impl<TMsg> NodeRegistry<TMsg>
where
    TMsg: Clone + Send + Sync + 'static,
{
    pub async fn init(
        member_ids: Vec<Uuid>,
        fabric: Arc<NetworkFabric<TMsg>>,
        factory: SharedRuntimeFactory<TMsg>,
    ) -> anyhow::Result<Self> {
        let registry = Self {
            members: Arc::new(RwLock::new(HashMap::new())),
            member_ids: Arc::new(RwLock::new(member_ids)),
            inboxes: RwLock::new(HashMap::new()),
            fabric,
            factory,
        };

        for id in registry.member_ids().await {
            registry.provision_endpoint(id).await;
            let member: SharedRuntimeMember<TMsg> = Arc::new(registry.build_member(id).await?);
            registry.members.write().await.insert(id, member);
        }

        Ok(registry)
    }

    async fn take_provisioned_inbox(&self, id: Uuid) -> anyhow::Result<Receiver<TMsg>> {
        let inbox = {
            let inboxes = self.inboxes.read().await;
            inboxes.get(&id).map(ProvisionedInbox::clone)
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

    async fn build_member(&self, id: Uuid) -> anyhow::Result<RuntimeMember<TMsg>> {
        let rx = self.take_provisioned_inbox(id).await?;
        let node = self.factory.build(id).await?;
        Ok(RuntimeMember::new(id, node, rx))
    }

    pub async fn member_ids(&self) -> Vec<Uuid> {
        self.member_ids.read().await.clone()
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

    pub async fn spawn_process(&self, id: Uuid) -> anyhow::Result<()> {
        self.provision_endpoint(id).await;
        let member: SharedRuntimeMember<TMsg> = Arc::new(self.build_member(id).await?);

        {
            let mut members = self.members.write().await;
            members.insert(id, Arc::clone(&member));
        }
        {
            let mut member_ids = self.member_ids.write().await;
            if !member_ids.contains(&id) {
                member_ids.push(id);
            }
        }

        member.start().await;
        Ok(())
    }

    pub async fn stop_and_remove(&self, id: Uuid) {
        self.stop_process(id).await;
        self.members.write().await.remove(&id);
        self.fabric.unregister(id).await;
        self.member_ids.write().await.retain(|member_id| *member_id != id);
    }

    pub async fn get(&self, id: Uuid) -> Option<SharedRuntimeMember<TMsg>> {
        self.members.read().await.get(&id).cloned()
    }

    pub async fn start(&self) {
        for id in self.member_ids().await {
            if let Some(member) = self.get(id).await {
                member.start().await;
            }
        }
    }

    pub async fn stop_process(&self, id: Uuid) {
        if let Some(member) = self.get(id).await {
            member.stop().await;
        }
    }

    pub async fn state(&self, uuid: Uuid) -> Option<RuntimeState> {
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

    pub async fn connect_client_to(
        &self,
        uuid: Uuid,
        client_id: Uuid,
    ) -> Option<(Sender<ClientMessage>, Receiver<ClientMessage>)> {
        self.get(uuid).await?.connect_client(client_id).await
    }

    pub fn fabric(&self) -> Arc<NetworkFabric<TMsg>> {
        Arc::clone(&self.fabric)
    }
}
