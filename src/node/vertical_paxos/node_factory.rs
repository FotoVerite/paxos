use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::{
    cluster::{
        runtime::{RuntimeNode, RuntimeNodeFactory},
        vertical::master::VerticalMasterEvent,
    },
    common::persistence::ClusterPersistence,
    monitor::PaxosObserver,
    node::vertical_paxos::{
        message::VerticalPaxosMessage, node::VerticalNode, transport::VerticalPaxosFabric,
    },
};

pub struct VerticalNodeFactory {
    fabric: Arc<VerticalPaxosFabric>,
    persistence: Arc<ClusterPersistence>,
    observer: Arc<dyn PaxosObserver>,
    master_inbox: mpsc::Sender<VerticalMasterEvent>,
    built: Mutex<HashMap<Uuid, Arc<VerticalNode>>>,
}

impl VerticalNodeFactory {
    pub fn new(
        fabric: Arc<VerticalPaxosFabric>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
        master_inbox: mpsc::Sender<VerticalMasterEvent>,
    ) -> Self {
        Self {
            fabric,
            persistence,
            observer,
            master_inbox,
            built: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, id: Uuid) -> Option<Arc<VerticalNode>> {
        self.built.lock().await.get(&id).cloned()
    }
}

#[async_trait]
impl RuntimeNodeFactory<VerticalPaxosMessage> for VerticalNodeFactory {
    async fn build(&self, id: Uuid) -> anyhow::Result<Arc<dyn RuntimeNode<VerticalPaxosMessage>>> {
        let node = Arc::new(
            VerticalNode::new(
                id,
                self.persistence.node(id),
                Arc::clone(&self.fabric),
                Arc::clone(&self.observer),
                self.master_inbox.clone(),
            )
            .await?,
        );
        self.built.lock().await.insert(id, Arc::clone(&node));
        Ok(node)
    }
}
