use crate::monitor::{Event, PaxosObserver};
use colored::*;

pub struct ConsoleObserver;

impl PaxosObserver for ConsoleObserver {
    fn on_event(&self, event: Event) {
        match event {
            Event::Proposal {
                id,
                decree_num,
                value,
                created_at,
            } => {
                println!(
                    "{}",
                    format!(
                        "[PROPOSER {}] Proposing for decree {}: {} ({}ms)",
                        id, decree_num, value, created_at
                    )
                    .blue()
                );
            }
            Event::Promise {
                id,
                from,
                decree_num,
                ballot,
                created_at,
            } => {
                println!(
                    "{}",
                    format!(
                        "[ACCEPTOR {}] Promising ballot {} for decree {} (from proposer {}) ({}ms)",
                        id, ballot, decree_num, from, created_at
                    )
                    .cyan()
                );
            }
            Event::Accept {
                id,
                decree_num,
                ballot,
                value,
                quorum,
                created_at,
            } => {
                println!("{}", format!("[PROPOSER {}] Beginning ballot {} for decree {}: {} with quorum {:?} ({}ms)", id, ballot, decree_num, value, quorum, created_at).yellow());
            }
            Event::Accepted {
                id,
                from,
                decree_num,
                ballot,
                value,
                created_at,
            } => {
                println!(
                    "{}",
                    format!(
                        "[ACCEPTOR {}] Voted on ballot {} for decree {}: {} (to {}) ({}ms)",
                        id, ballot, decree_num, value, from, created_at
                    )
                    .green()
                );
            }
            Event::Learn {
                id,
                decree_num,
                value,
                created_at,
            } => {
                println!(
                    "{}",
                    format!(
                        "[LEARNER {}] Learned value for decree {}: {} ({}ms)",
                        id, decree_num, value, created_at
                    )
                    .green()
                );
            }
            Event::LearnedValue {
                id,
                decree_num,
                value,
                created_at,
            } => {
                println!(
                    "{}",
                    format!(
                        "[LEARNER {}] Learned value for decree {}: {} ({}ms)",
                        id, decree_num, value, created_at
                    )
                    .green()
                );
            }
            Event::NodeState {
                id,
                role,
                ballot,
                learned_count,
            } => {
                println!(
                    "{}",
                    format!(
                        "[NODE {}] role: {}, ballot: {}, learned: {}",
                        id, role, ballot, learned_count
                    )
                    .purple()
                );
            }
            Event::MessageSent {
                from,
                to,
                message_type,
            } => {
                println!(
                    "{}",
                    format!("[MESSAGE] {} -> {}: {}", from, to, message_type).magenta()
                );
            }
            Event::Success {
                id,
                from,
                decree_num,
                value,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[Success] {} learned from {} -> {}: {}",
                        id, from, decree_num, value
                    )
                    .magenta()
                );
            }
            Event::PartitionCreated {
                partition_a,
                partition_b,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[NETWORK] Partition created: A={:?}, B={:?}",
                        partition_a, partition_b
                    )
                    .red()
                );
            }
            Event::PartitionHealed { .. } => {
                println!("{}", "[NETWORK] Partition healed".green());
            }
            Event::InitialDecree {
                ..
            } => {},
            Event::BatchInitialDecrees {
                id,
                decrees,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[NODE {}] Batch Initial Decrees: {} decrees loaded",
                        id, decrees.len()
                    )
                    .cyan()
                );
            },
            Event::LedgerDump {
                id,
                decrees,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[NODE {}] Ledger Dump: {} total decrees",
                        id, decrees.len()
                    )
                    .cyan()
                );
            },
            Event::NodeCapabilities {
                id,
                roles,
                learning_strategy,
            } => {
                println!(
                    "{}",
                    format!(
                        "[NODE {}] Capabilities: roles={:?}, strategy={}",
                        id, roles, learning_strategy
                    )
                    .purple()
                );
            }
            Event::LeaderElected { id, .. } => {
                println!(
                    "{}",
                    format!("[LEADER] Node {} elected as leader", id).yellow().bold()
                );
            }
        }
    }
}
