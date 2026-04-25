use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    cluster::{
        pmmc::reconfiguration::ClusterConfiguration,
        runtime::{RuntimeNode, RuntimeNodeFactory},
    },
    common::persistence::ClusterPersistence,
    monitor::PaxosObserver,
    node::{
        config::Roles,
        pmmc::{
            message::PmmcMessage,
            pmmc_node::PmmcNode,
            transport::{PmmcFabric, PmmcHandle},
        },
    },
};

pub struct PmmcNodeFactory {
    fabric: Arc<PmmcFabric>,
    persistence: Arc<ClusterPersistence>,
    observer: Arc<dyn PaxosObserver>,
    configuration: Arc<ClusterConfiguration>,
    roles: HashMap<Uuid, Roles>,
    built: Mutex<HashMap<Uuid, Arc<PmmcNode>>>,
}

impl PmmcNodeFactory {
    pub fn new(
        fabric: Arc<PmmcFabric>,
        persistence: Arc<ClusterPersistence>,
        observer: Arc<dyn PaxosObserver>,
        configuration: Arc<ClusterConfiguration>,
    ) -> Self {
        let roles = configuration
            .members()
            .into_iter()
            .collect::<HashMap<_, _>>();

        Self {
            fabric,
            persistence,
            observer,
            configuration,
            roles,
            built: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, id: Uuid) -> Option<Arc<PmmcNode>> {
        self.built.lock().await.get(&id).cloned()
    }
}

#[async_trait]
impl RuntimeNodeFactory<PmmcMessage> for PmmcNodeFactory {
    async fn build(&self, id: Uuid) -> anyhow::Result<Arc<dyn RuntimeNode<PmmcMessage>>> {
        let roles = self
            .roles
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing PMMC roles for node {id}"))?;
        let handle = Arc::new(PmmcHandle::from_fabric(id, Arc::clone(&self.fabric)));
        let node = Arc::new(
            PmmcNode::new(
                id,
                Arc::clone(&self.observer),
                Arc::clone(&self.fabric),
                handle,
                self.persistence.node(id),
                roles,
                Arc::clone(&self.configuration),
            )
            .await?,
        );
        self.built.lock().await.insert(id, Arc::clone(&node));
        Ok(node)
    }
}
