use tokio::sync::mpsc;
use uuid::Uuid;

use crate::message::ClientMessage;

use super::spec::PmmcClientSpec;

pub struct ClientSession {
    pub id: String,
    pub uuid: Uuid,
    pub replica_index: usize,
    pub tx: Option<mpsc::Sender<ClientMessage>>,
    pub rx: Option<mpsc::Receiver<ClientMessage>>,
    pub next_request_id: u64,
    pub success_count: usize,
    pub attempt_count: usize,
}

impl ClientSession {
    fn from_spec(spec: &PmmcClientSpec) -> Self {
        Self {
            id: spec.id.clone(),
            uuid: Uuid::new_v5(&Uuid::NAMESPACE_OID, spec.id.as_bytes()),
            replica_index: spec.start_replica_index,
            tx: None,
            rx: None,
            next_request_id: 1,
            success_count: 0,
            attempt_count: 0,
        }
    }

    pub fn detach(&mut self) {
        self.tx = None;
        self.rx = None;
    }

    pub fn record_attempt(&mut self) {
        self.attempt_count += 1;
    }

    pub fn is_attached(&self) -> bool {
        self.tx.is_some() && self.rx.is_some()
    }

    pub fn record_success(&mut self) {
        self.next_request_id += 1;
        self.success_count += 1;
    }

    pub fn request_id(&self) -> u64 {
        self.next_request_id
    }

    pub fn sender(&self) -> Option<&mpsc::Sender<ClientMessage>> {
        self.tx.as_ref()
    }

    pub fn receiver_mut(&mut self) -> Option<&mut mpsc::Receiver<ClientMessage>> {
        self.rx.as_mut()
    }

    pub fn attach(&mut self, tx: mpsc::Sender<ClientMessage>, rx: mpsc::Receiver<ClientMessage>) {
        self.tx = Some(tx);
        self.rx = Some(rx);
    }
}

pub struct ClientPool {
    sessions: Vec<ClientSession>,
    next_index: usize,
}

impl ClientPool {
    pub fn new(specs: &[PmmcClientSpec]) -> Self {
        Self {
            sessions: specs.iter().map(ClientSession::from_spec).collect(),
            next_index: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    pub fn next_session_index(&mut self) -> usize {
        let index = self.next_index;
        self.next_index = (self.next_index + 1) % self.sessions.len();
        index
    }

    pub fn session_mut(&mut self, index: usize) -> &mut ClientSession {
        &mut self.sessions[index]
    }

    pub fn move_client(&mut self, client_id: &str, replica_index: usize) -> bool {
        if let Some(session) = self
            .sessions
            .iter_mut()
            .find(|session| session.id == client_id)
        {
            session.replica_index = replica_index;
            session.detach();
            true
        } else {
            false
        }
    }
}
