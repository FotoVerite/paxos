use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::ClientAssignment;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SynodMembership {
    pub assignments: Vec<ClientAssignment>,
    pub bootstrap_node: Option<Uuid>,
}

impl SynodMembership {
    pub fn node_count(&self) -> usize {
        self.assignments.len()
    }
}
