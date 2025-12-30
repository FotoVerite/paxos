use colored::*;
use crate::monitor::{Event, PaxosObserver};

pub struct ConsoleObserver;

impl PaxosObserver for ConsoleObserver {
    fn on_event(&self, event: Event) {
        match event {
            Event::Proposal { id, decree_num, value } => {
                println!("{}", format!("[PROPOSER {}] Proposing for decree {}: {}", id, decree_num, value).blue());
            }
            Event::Promise { id, decree_num, ballot } => {
                println!("{}", format!("[ACCEPTOR {}] Promising ballot {} for decree {}", id, ballot, decree_num).cyan());
            }
            Event::Accept { id, decree_num, ballot, value } => {
                println!("{}", format!("[ACCEPTOR {}] Accepting ballot {} for decree {}: {}", id, ballot, decree_num, value).yellow());
            }
            Event::Accepted { id, decree_num, ballot, value } => {
                println!("{}", format!("[ACCEPTOR {}] Voted on ballot {} for decree {}: {}", id, ballot, decree_num, value).green());
            }
            Event::Learn { id, decree_num, value } => {
                println!("{}", format!("[LEARNER {}] Learned value for decree {}: {}", id, decree_num, value).green());
            }
            Event::NodeState { id, role, ballot, learned_count } => {
                println!("{}", format!("[NODE {}] role: {}, ballot: {}, learned: {}", id, role, ballot, learned_count).purple());
            }
        }
    }
}
