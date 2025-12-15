#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaxosCommand {
    NOOP,
    GET { key: String },
    PUT { key: String, version: usize },
}
