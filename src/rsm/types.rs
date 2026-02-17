use derive_more::{Add, AddAssign, Display, From, Into, SubAssign};
use serde::{Deserialize, Serialize};

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub enum ClerkResponseError {
    Maybe,
    NotLeader { leader_hint: usize },
    ErrVersion { version: KVVersion },
    ErrKey,
}

pub type KVResult<T> = Result<T, ClerkResponseError>;

#[derive(
    Add,
    Display,
    From,
    Into,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Default,
    PartialOrd,
    Ord,
)]
pub struct KVVersion(pub usize);

impl KVVersion {
    pub fn next(self) -> Self {
        KVVersion(self.0 + 1)
    }
}

#[derive(
    Add,
    Display,
    From,
    Into,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Default,
    PartialOrd,
    Ord,
    AddAssign,
    SubAssign,
)]
pub struct KVValue(pub usize);

impl std::ops::MulAssign for KVValue {
    fn mul_assign(&mut self, rhs: Self) {
        self.0 *= rhs.0;
    }
}
