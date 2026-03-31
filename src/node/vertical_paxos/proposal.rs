use std::collections::BTreeMap;

use crate::paxos_command::PaxosCommand;

pub type ProposalsStore = BTreeMap<usize, PaxosCommand>;

#[derive(serde::Serialize, serde::Deserialize, Clone)]

pub struct Proposal {
    pub slot: usize,
    pub cmd: PaxosCommand,
}
