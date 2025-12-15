use crate::{node::ballot::Ballot, paxos_command::PaxosCommand};

#[derive(Debug, Clone)]
pub enum Message {
    Prepare { decree_num: usize, ballot: Ballot },
    Promise { decree_num: usize, ballot: Ballot, accepted_ballot: Ballot, accepted_value: PaxosCommand},
    Accept { decree_num: usize, ballot: Ballot, value: PaxosCommand },
    Accepted { decree_num: usize, ballot: Ballot, value: PaxosCommand },
    NACK,
}
