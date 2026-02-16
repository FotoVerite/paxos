use derive_more::{Add, Display, From, Into};
use serde::{Deserialize, Serialize};

#[derive(
    Add, Display, From, Into, Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default, PartialOrd, Ord
)]
pub struct NodeId(pub usize);

#[derive(
    Add, Display, From, Into, Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default, PartialOrd, Ord
)]
pub struct CID(pub usize);

#[derive(
    Add, Display, From, Into, Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default, PartialOrd, Ord
)]
pub struct SID(pub usize);

#[derive(
    Add, Display, From, Into, Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default, PartialOrd, Ord
)]
pub struct DecreeId(pub usize);
