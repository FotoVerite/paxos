use crate::{
    message::Message,
    monitor::{Event, PaxosObserver},
};
use colored::*;
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};
use uuid::Uuid;

pub struct ConsoleObserver;

#[derive(Default)]
struct NodeLabelMap {
    map: HashMap<Uuid, usize>,
    next: usize,
}

impl NodeLabelMap {
    fn label(&mut self, id: Uuid) -> usize {
        if let Some(existing) = self.map.get(&id) {
            *existing
        } else {
            let idx = self.next;
            self.next += 1;
            self.map.insert(id, idx);
            idx
        }
    }
}

impl PaxosObserver for ConsoleObserver {
    fn on_message(&self, _index: &[Uuid], message: Message) {
        match message {
            _ => {}
        }
    }

    fn on_event(&self, event: Event) {
        fn labels() -> &'static Mutex<NodeLabelMap> {
            static LABELS: OnceLock<Mutex<NodeLabelMap>> = OnceLock::new();
            LABELS.get_or_init(|| Mutex::new(NodeLabelMap::default()))
        }

        fn node_label(id: Uuid) -> String {
            let mut state = labels().lock().expect("label map lock poisoned");
            format!("N{}", state.label(id))
        }

        fn node_list(ids: &[Uuid]) -> String {
            let mut state = labels().lock().expect("label map lock poisoned");
            let labels: Vec<String> = ids.iter().map(|id| format!("N{}", state.label(*id))).collect();
            format!("[{}]", labels.join(", "))
        }

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
                        node_label(id),
                        decree_num,
                        value,
                        created_at
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
                        node_label(id),
                        ballot,
                        decree_num,
                        node_label(from),
                        created_at
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
                let quorum_labels: Vec<String> = quorum.iter().map(|id| node_label(*id)).collect();
                println!("{}", format!("[PROPOSER {}] Beginning ballot {} for decree {}: {} with quorum {:?} ({}ms)", node_label(id), ballot, decree_num, value, quorum_labels, created_at).yellow());
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
                        node_label(id),
                        ballot,
                        decree_num,
                        value,
                        node_label(from),
                        created_at
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
                        node_label(id),
                        decree_num,
                        value,
                        created_at
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
                        node_label(id),
                        decree_num,
                        value,
                        created_at
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
                        node_label(id),
                        role,
                        ballot,
                        learned_count
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
                    format!(
                        "[MESSAGE] {} -> {}: {}",
                        node_label(from),
                        node_label(to),
                        message_type
                    )
                    .magenta()
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
                        node_label(id),
                        node_label(from),
                        decree_num,
                        value
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
                        node_list(&partition_a),
                        node_list(&partition_b)
                    )
                    .red()
                );
            }
            Event::PartitionHealed { .. } => {
                println!("{}", "[NETWORK] Partition healed".green());
            }
            Event::InitialDecree { .. } => {}
            Event::BatchInitialDecrees { id, decrees, .. } => {
                println!(
                    "{}",
                    format!(
                        "[NODE {}] Batch Initial Decrees: {} decrees loaded",
                        node_label(id),
                        decrees.len()
                    )
                    .cyan()
                );
            }
            Event::LedgerDump { id, decrees, .. } => {
                println!(
                    "{}",
                    format!(
                        "[NODE {}] Ledger Dump: {} total decrees",
                        node_label(id),
                        decrees.len()
                    )
                    .cyan()
                );
            }
            Event::NodeCapabilities {
                id,
                roles,
                learning_strategy,
            } => {
                println!(
                    "{}",
                    format!(
                        "[NODE {}] Capabilities: roles={:?}, strategy={}",
                        node_label(id),
                        roles,
                        learning_strategy
                    )
                    .purple()
                );
            }
            Event::LeaderElected { id, .. } => {
                println!(
                    "{}",
                    format!("[LEADER] Node {} elected as leader", node_label(id))
                        .yellow()
                        .bold()
                );
            }
            Event::LeaderSteppedDown { id, .. } => {
                println!(
                    "{}",
                    format!("[LEADER] Node {} stepped down", node_label(id))
                        .red()
                        .bold()
                );
            }
            Event::BallotAdopted { id, ballot } => {
                println!(
                    "{}",
                    format!("[PMMC][A] {} adopted ballot {}", node_label(id), ballot).cyan()
                );
            }
            Event::ProposalAccepted { id, pvalue } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC][A] {} accepted slot {} at ballot {}",
                        node_label(id),
                        pvalue.slot(),
                        pvalue.ballot()
                    )
                    .green()
                );
            }
            Event::PmmcPropose { id, slot, cmd, .. } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC][R] {} propose slot {}: {}",
                        node_label(id),
                        slot,
                        cmd
                    )
                    .blue()
                );
            }
            Event::PmmcP1A { from, ballot, .. } => {
                println!(
                    "{}",
                    format!("[PMMC][L] {} -> P1A({})", node_label(from), ballot).yellow()
                );
            }
            Event::PmmcP1B {
                from, to, ballot, ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC][A] {} -> {} P1B({})",
                        node_label(from),
                        node_label(to),
                        ballot
                    )
                    .yellow()
                );
            }
            Event::PmmcP2A { from, pvalue, .. } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC][L] {} -> P2A(slot={}, ballot={})",
                        node_label(from),
                        pvalue.slot(),
                        pvalue.ballot()
                    )
                    .yellow()
                );
            }
            Event::PmmcP2B {
                from,
                to,
                ballot,
                pvalue,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC][A] {} -> {} P2B(slot={}, ballot={})",
                        node_label(from),
                        node_label(to),
                        pvalue.slot(),
                        ballot
                    )
                    .yellow()
                );
            }
            Event::PmmcAdopted {
                from, to, ballot, ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC] {} -> {} ADOPTED({})",
                        node_label(from),
                        node_label(to),
                        ballot
                    )
                        .green()
                        .bold()
                );
            }
            Event::PmmcPreempted {
                from, to, ballot, ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC] {} -> {} PREEMPT({})",
                        node_label(from),
                        node_label(to),
                        ballot
                    )
                    .red()
                );
            }
            Event::PmmcHeartbeat { from, ballot, .. } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC][L] {} heartbeat ballot {}",
                        node_label(from),
                        ballot
                    )
                    .purple()
                );
            }
            Event::PmmcAck { from, to, slot, .. } => {
                println!(
                    "{}",
                    format!(
                        "[PMMC][R] {} -> {} ACK(slot={})",
                        node_label(from),
                        node_label(to),
                        slot
                    )
                    .cyan()
                );
            }
        }
    }
}
