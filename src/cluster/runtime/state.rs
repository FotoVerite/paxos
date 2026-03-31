#[derive(Debug, PartialEq, Clone)]
pub enum RuntimeState {
    Starting,
    Passive,
    CatchingUp,
    Active,
    Partitioned,
    Retiring,
    Stopped,
    Crashed,
}
