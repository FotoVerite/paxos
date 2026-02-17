use std::fmt;
use uuid::Uuid;

pub type ClientId = Uuid;
pub type RequestId = u64;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
pub enum PaxosCommand {
    ClientRequest {
        client_id: ClientId,
        request_id: RequestId,
        cmd: Box<PaxosCommand>,
    },
    NOOP,
    EnactDecree {
        author: String,
        law: String,
    },
    Ostracize {
        citizen: String,
    },
    AppointArchon {
        name: String,
        term_length_years: u32,
    },
    BuildAcropolis {
        stones_required: u32,
        architect: String,
    },
    GET {
        key: String,
    },
    PUT {
        key: String,
        version: usize,
        value: usize,
    },
    ADD {
        key: String,
        version: usize,
        value: usize,
    },
    BLANK,
}

impl PaxosCommand {
    pub fn with_client(self, client_id: ClientId, request_id: RequestId) -> Self {
        match self {
            PaxosCommand::ClientRequest { .. } => self,
            cmd => PaxosCommand::ClientRequest {
                client_id,
                request_id,
                cmd: Box::new(cmd),
            },
        }
    }

    pub fn client_identity(&self) -> Option<(ClientId, RequestId)> {
        match self {
            PaxosCommand::ClientRequest {
                client_id,
                request_id,
                ..
            } => Some((*client_id, *request_id)),
            _ => None,
        }
    }

    pub fn operation(&self) -> &PaxosCommand {
        match self {
            PaxosCommand::ClientRequest { cmd, .. } => cmd.operation(),
            _ => self,
        }
    }

    pub fn request_id(&self) -> u64 {
        match self {
            PaxosCommand::ClientRequest { request_id, .. } => request_id.clone(),
            _ => 0,
        }
    }

    pub fn client_id(&self) -> Uuid {
        match self {
            PaxosCommand::ClientRequest { client_id, .. } => client_id.clone(),
            _ => Uuid::nil(),
        }
    }

    pub fn strip_client(self) -> PaxosCommand {
        match self {
            PaxosCommand::ClientRequest { cmd, .. } => cmd.strip_client(),
            _ => self,
        }
    }
}

impl fmt::Display for PaxosCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaxosCommand::ClientRequest {
                client_id,
                request_id,
                cmd,
            } => write!(f, "{} [client={} req={}]", cmd, client_id, request_id),
            PaxosCommand::NOOP => write!(f, "NOOP"),
            PaxosCommand::EnactDecree { author, law } => {
                write!(f, "Enact Decree by {}: '{}'", author, law)
            }
            PaxosCommand::Ostracize { citizen } => write!(f, "Ostracize {}", citizen),
            PaxosCommand::AppointArchon {
                name,
                term_length_years,
            } => {
                write!(f, "Appoint Archon {} for {} years", name, term_length_years)
            }
            PaxosCommand::BuildAcropolis {
                stones_required,
                architect,
            } => {
                write!(
                    f,
                    "Build Acropolis ({} stones) with {}",
                    stones_required, architect
                )
            }
            PaxosCommand::GET { key } => write!(f, "GET {}", key),
            PaxosCommand::PUT {
                key,
                version,
                value,
            } => write!(f, "PUT {} val{} v{}", key, value, version),
            PaxosCommand::ADD {
                key,
                version,
                value,
            } => write!(f, "ADD {} val{} v{}", key, value, version),

            PaxosCommand::BLANK => write!(f, "BLANK"),
        }
    }
}
