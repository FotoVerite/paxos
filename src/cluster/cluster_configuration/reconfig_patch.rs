use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::{cluster::cluster_configuration::ConfigurationStrategy, node::config::Roles};


#[derive(Debug, Clone, Default)]
pub struct ReconfigPatch {
    pub alpha_max_inflight: Option<usize>,
    pub strategy: Option<ConfigurationStrategy>,
    pub add: HashMap<Uuid, Roles>,
    pub remove:  HashSet<Uuid>,
}

impl ReconfigPatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn strategy(mut self, strategy: ConfigurationStrategy) -> Self {
        self.strategy = Some(strategy);
        self
    }

    pub fn alpha_max_inflight(mut self, alpha: usize) -> Self {
        self.alpha_max_inflight = Some(alpha);
        self
    }

    pub fn add_node(mut self, uuid: Uuid, roles: Roles) -> Self {
        self.add.insert(uuid, roles);
        self
    }

    pub fn remove_node(mut self, uuid: Uuid) -> Self {
        self.remove.insert(uuid);
        self
    }
}
