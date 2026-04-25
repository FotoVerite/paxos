use thiserror::Error;
use uuid::Uuid;

use crate::{node::vertical_paxos::replica::VerticalClientReply, paxos_command::PaxosCommand};

use super::ClientId;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SynodProposalReceipt {
    pub client_id: ClientId,
    pub request_id: u64,
    pub emoji: String,
    pub assigned_node: Uuid,
    pub reply: VerticalClientReply,
}

#[derive(Debug, Error)]
pub enum SynodProposalError {
    #[error("unknown synod client {0}")]
    UnknownClient(ClientId),

    #[error("invalid synod emoji {0}")]
    InvalidEmoji(String),

    #[error("failed to submit synod proposal: {0}")]
    Submit(#[from] anyhow::Error),
}

pub fn emoji_increment_command(emoji: &str) -> PaxosCommand {
    PaxosCommand::INCREMENT {
        key: emoji_counter_key(emoji),
        value: 1,
    }
}

pub fn emoji_counter_key(emoji: &str) -> String {
    format!("synod:emoji:{emoji}")
}
