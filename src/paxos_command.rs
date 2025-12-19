use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PaxosCommand {
    NOOP,
    EnactDecree { author: String, law: String },
    Ostracize { citizen: String },
    AppointArchon { name: String, term_length_years: u32 },
    BuildAcropolis { stones_required: u32, architect: String },
    GET { key: String },
    PUT { key: String, version: usize },
}

impl fmt::Display for PaxosCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaxosCommand::NOOP => write!(f, "NOOP"),
            PaxosCommand::EnactDecree { author, law } => {
                write!(f, "Enact Decree by {}: '{}'", author, law)
            }
            PaxosCommand::Ostracize { citizen } => write!(f, "Ostracize {}", citizen),
            PaxosCommand::AppointArchon { name, term_length_years } => {
                write!(f, "Appoint Archon {} for {} years", name, term_length_years)
            }
            PaxosCommand::BuildAcropolis { stones_required, architect } => {
                write!(f, "Build Acropolis ({} stones) with {}", stones_required, architect)
            }
            PaxosCommand::GET { key } => write!(f, "GET {}", key),
            PaxosCommand::PUT { key, version } => write!(f, "PUT {} v{}", key, version),
        }
    }
}
