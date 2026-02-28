use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::{cluster::cluster_configuration::ConfigurationStrategy, node::config::Roles};


pub struct ReconfigPatch {
    pub strategy: Option<ConfigurationStrategy>,
    pub add: HashMap<Uuid, Roles>,
    pub remove:  HashSet<Uuid>,
}