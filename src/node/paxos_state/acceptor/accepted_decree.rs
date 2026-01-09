use crate::{
    node::paxos_state::{acceptor::prev_vote::PrevVote, ballot::Ballot},
    paxos_command::PaxosCommand,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AcceptedDecree {
    pub(crate) next_bal: Ballot,
    pub prev_vote: PrevVote,
}

impl AcceptedDecree {
    pub fn gt(&self, ballot: Ballot) -> bool {
        ballot > self.next_bal
    }

    pub fn lt(&self, ballot: Ballot) -> bool {
        ballot > self.next_bal
    }

    pub fn eq(&self, ballot: Ballot) -> bool {
        ballot == self.next_bal
    }

    pub fn next_bal(&self) -> &Ballot {
        return &self.next_bal;
    }

    pub fn update_bal(&mut self, ballot: Ballot) {
        if self.lt(ballot) {
            self.next_bal = ballot
        }
    }
    pub fn get_prev_bal(&self) -> Ballot {
        return self.prev_vote.ballot;
    }

    pub fn get_prev_val(&self) -> PaxosCommand {
        return self.prev_vote.value.clone();
    }
}

impl Default for AcceptedDecree {
    fn default() -> AcceptedDecree {
        AcceptedDecree {
            next_bal: Ballot {
                number: 0,
                node_id: 0,
            },
            prev_vote: PrevVote::default(),
        }
    }
}
