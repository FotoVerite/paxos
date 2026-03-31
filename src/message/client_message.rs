//! Client-facing messages only.
//!
//! Runtime protocol traffic lives under `node/*/message.rs`.

use crate::{paxos_command::PaxosCommand, rsm::kv_store::ReplyOutcome};

#[derive(Debug, Clone, serde::Serialize)]
pub enum ClientMessage {
    PROPOSE {
        cmd: PaxosCommand,
    },
    RESPONSE {
        request_id: u64,
        response: ReplyOutcome,
    },
}
