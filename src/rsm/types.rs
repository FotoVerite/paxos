pub enum ClerkResponseError {
    Maybe,
    NotLeader {leader_hint: usize},
    ErrVersion { version: usize},
    ErrKey 
}

pub type KVResult<T> = Result<T, ClerkResponseError>;
