use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{
    Mutex, RwLock,
    mpsc::{self, Receiver, Sender},
};
use uuid::Uuid;

use crate::cluster::runtime::{
    member::RuntimeMember,
    state::RuntimeState,
    types::{ProvisionedInbox, ProvisionedInboxes, RuntimeMemberIds, RuntimeMembers, SharedRuntimeNode},
};

pub struct ControlPlaneRegistry<TMsg>
where
    TMsg: Clone + Send + Sync + 'static,
{
    members: RuntimeMembers<TMsg>,
    member_ids: RuntimeMemberIds,
    inboxes: ProvisionedInboxes<TMsg>,
    senders: Arc<RwLock<HashMap<Uuid, Sender<TMsg>>>>,
}

impl<TMsg> ControlPlaneRegistry<TMsg>
where
    TMsg: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            members: Arc::new(RwLock::new(HashMap::new())),
            member_ids: Arc::new(RwLock::new(Vec::new())),
            inboxes: RwLock::new(HashMap::new()),
            senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn member_ids(&self) -> Vec<Uuid> {
        self.member_ids.read().await.clone()
    }

    pub async fn provision_endpoint(&self, id: Uuid) -> Sender<TMsg> {
        let (tx, rx) = mpsc::channel(1024);

        {
            let mut senders = self.senders.write().await;
            senders.insert(id, tx.clone());
        }

        let inbox = {
            let mut inboxes = self.inboxes.write().await;
            inboxes
                .entry(id)
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let mut inbox_rx = inbox.lock().await;
        *inbox_rx = Some(rx);

        tx
    }

    async fn take_provisioned_inbox(&self, id: Uuid) -> anyhow::Result<Receiver<TMsg>> {
        let inbox = {
            let inboxes = self.inboxes.read().await;
            inboxes.get(&id).map(ProvisionedInbox::clone)
        };
        let Some(inbox) = inbox else {
            anyhow::bail!("control-plane inbox not provisioned for actor {id}");
        };

        let mut rx = inbox.lock().await;
        let Some(inbox_rx) = rx.take() else {
            anyhow::bail!("control-plane inbox already consumed for actor {id}");
        };

        Ok(inbox_rx)
    }

    pub async fn spawn_actor(
        &self,
        id: Uuid,
        node: SharedRuntimeNode<TMsg>,
    ) -> anyhow::Result<()> {
        if !self.senders.read().await.contains_key(&id) {
            self.provision_endpoint(id).await;
        }

        let rx = self.take_provisioned_inbox(id).await?;
        let member = Arc::new(RuntimeMember::new(id, node, rx));

        {
            let mut members = self.members.write().await;
            members.insert(id, member);
        }
        {
            let mut member_ids = self.member_ids.write().await;
            if !member_ids.contains(&id) {
                member_ids.push(id);
            }
        }

        Ok(())
    }

    pub async fn sender(&self, id: Uuid) -> Option<Sender<TMsg>> {
        self.senders.read().await.get(&id).cloned()
    }

    pub async fn get(&self, id: Uuid) -> Option<Arc<RuntimeMember<TMsg>>> {
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

    pub async fn state(&self, id: Uuid) -> Option<RuntimeState> {
        let member = self.get(id).await?;
        Some(member.state().await)
    }
}
