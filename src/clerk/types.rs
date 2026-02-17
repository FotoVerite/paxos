use std::any::Any;
use uuid::Uuid;

use crate::paxos_command::{ClientId, PaxosCommand, RequestId};

pub struct ClerkRequest {
    pub client_id: ClientId,
    pub request_id: RequestId,
    pub cmd: PaxosCommand,
}

impl ClerkRequest {
    pub fn new(client_id: Uuid, request_id: u64, cmd: PaxosCommand) -> Self {
        Self {
            client_id,
            request_id,
            cmd,
        }
    }

    pub fn into_tagged_command(self) -> PaxosCommand {
        self.cmd.with_client(self.client_id, self.request_id)
    }
}

pub struct ClerkResponse {
    //    error: Option<ClerkResponseError>,
    value: Option<Box<dyn Any>>,
}
