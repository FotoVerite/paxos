use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::ClientId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientAssignment {
    pub client_id: ClientId,
    pub node_id: Uuid,
}
