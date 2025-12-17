
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PaxosCommand {
    NOOP,
    GET { key: String },
    PUT { key: String, version: usize },
}
