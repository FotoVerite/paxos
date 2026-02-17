use std::collections::BTreeMap;

use tokio::sync::mpsc::{self, Sender};

use crate::{message::ClientMessage, paxos_command::ClientId};

pub struct ReplicaVolatile {
    clients: BTreeMap<ClientId, mpsc::Sender<ClientMessage>>,
}

impl Default for ReplicaVolatile {
    fn default() -> Self {
        Self {
            clients: BTreeMap::new(),
        }
    }
}

impl ReplicaVolatile {
    pub fn add_client(&mut self, client_id: ClientId, tx: Sender<ClientMessage>) {
        self.clients.insert(client_id, tx);
    }

    pub async fn send_client_response(&self, client_id: ClientId, msg: ClientMessage) {
        if let Some(tx) = self.clients.get(&client_id) {
            tx.send(msg).await;
        }
    }
}
