use crate::monitor::{Event, MessageTrace, PaxosObserver};
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
    let labels: Vec<String> = ids
        .iter()
        .map(|id| format!("N{}", state.label(*id)))
        .collect();
    format!("[{}]", labels.join(", "))
}

impl ConsoleObserver {
    fn handle_classic_event(&self, event: Event) {
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
            _ => unreachable!("classic event dispatcher received non-classic event"),
        }
    }

    fn handle_system_event(&self, event: Event) {
        match event {
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
            Event::NodeCrashed { id, .. } => {
                println!(
                    "{}",
                    format!("[CRASH] Node {} crashed", node_label(id))
                        .red()
                        .bold()
                );
            }
            Event::ReconfigurationRequested {
                strategy,
                add_nodes,
                remove_nodes,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] Requested {:?}: +{} -{}",
                        strategy,
                        node_list(&add_nodes),
                        node_list(&remove_nodes)
                    )
                    .yellow()
                    .bold()
                );
            }
            Event::ReconfigurationStopStarted {
                strategy,
                target_nodes,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] Stop started ({:?}) for {}",
                        strategy,
                        node_list(&target_nodes)
                    )
                    .yellow()
                );
            }
            Event::ReconfigurationStopCommandSent { to, .. } => {
                println!(
                    "{}",
                    format!("[RECONFIG] stop command sent -> {}", node_label(to)).yellow()
                );
            }
            Event::ReconfigurationStopCompleted {
                strategy,
                stopped_nodes,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] Stop completed ({:?}) for {}",
                        strategy,
                        node_list(&stopped_nodes)
                    )
                    .green()
                );
            }
            Event::ReconfigurationStopDecided {
                id,
                strategy,
                slot,
                delayed_slots,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] stop decided on {} @slot={} ({:?}, delay={})",
                        node_label(id),
                        slot,
                        strategy,
                        delayed_slots
                    )
                    .yellow()
                );
            }
            Event::ReconfigurationStopApplied {
                id,
                strategy,
                slot,
                delayed_slots,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] stop applied on {} @slot={} ({:?}, delay={})",
                        node_label(id),
                        slot,
                        strategy,
                        delayed_slots
                    )
                    .yellow()
                );
            }
            Event::ReconfigurationProposalObserved {
                id,
                strategy,
                outcome,
                slot,
                stop_slot,
                barrier_active,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] proposal gate on {} ({:?}): outcome={:?}, slot={:?}, stop_slot={:?}, barrier_active={}",
                        node_label(id),
                        strategy,
                        outcome,
                        slot,
                        stop_slot,
                        barrier_active
                    )
                    .cyan()
                );
            }
            Event::ReconfigurationCheckpointSelected {
                strategy,
                source_node,
                last_applied_slot,
                ..
            } => {
                let source_text = source_node
                    .map(node_label)
                    .unwrap_or_else(|| "unknown".to_string());
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] Checkpoint selected ({:?}) from {} at slot {:?}",
                        strategy, source_text, last_applied_slot
                    )
                    .cyan()
                );
            }
            Event::ReconfigurationApplied {
                strategy,
                previous_node_count,
                next_node_count,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] Applied {:?}: nodes {} -> {}",
                        strategy, previous_node_count, next_node_count
                    )
                    .green()
                    .bold()
                );
            }
            Event::ReconfigurationReady {
                strategy,
                leader,
                active_nodes,
                ..
            } => {
                let leader_text = leader.map(node_label).unwrap_or_else(|| "none".to_string());
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] Ready ({:?}): leader={}, active={}",
                        strategy,
                        leader_text,
                        node_list(&active_nodes)
                    )
                    .green()
                );
            }
            Event::ReconfigurationNodeRetired { id, .. } => {
                println!(
                    "{}",
                    format!("[RECONFIG] node retired {}", node_label(id)).red()
                );
            }
            Event::ReconfigurationNodeRebooted { id, .. } => {
                println!(
                    "{}",
                    format!("[RECONFIG] node rebooted {}", node_label(id)).green()
                );
            }
            Event::ReconfigurationFailed {
                strategy,
                phase,
                reason,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[RECONFIG] Failed strategy={:?} phase={:?}: {}",
                        strategy, phase, reason
                    )
                    .red()
                    .bold()
                );
            }
            _ => unreachable!("system event dispatcher received non-system event"),
        }
    }

    fn handle_pmmc_event(&self, event: Event) {
        match event {
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
                    format!("[PMMC][L] {} heartbeat ballot {}", node_label(from), ballot).purple()
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
            _ => unreachable!("pmmc event dispatcher received non-pmmc event"),
        }
    }

    fn handle_vertical_event(&self, event: Event) {
        match event {
            Event::VerticalConfigurationInstalled {
                configuration_id,
                leader,
                replicas,
                acceptors,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[VERTICAL] config {} installed leader={} replicas={} acceptors={}",
                        configuration_id,
                        node_label(leader),
                        node_list(&replicas),
                        node_list(&acceptors)
                    )
                    .blue()
                );
            }
            Event::VerticalActivationStarted {
                configuration_id,
                leader,
                ballot,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[VERTICAL] activation started config={} leader={} ballot={}",
                        configuration_id,
                        node_label(leader),
                        ballot
                    )
                    .yellow()
                );
            }
            Event::VerticalActivationReady {
                configuration_id,
                leader,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[VERTICAL] activation ready config={} leader={}",
                        configuration_id,
                        node_label(leader)
                    )
                    .green()
                );
            }
            Event::VerticalActivationSuperseded {
                configuration_id,
                leader,
                superseded_by,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[VERTICAL] activation superseded config={} leader={} by={}",
                        configuration_id,
                        node_label(leader),
                        superseded_by
                    )
                    .red()
                );
            }
            Event::VerticalReplicaSetActivated {
                configuration_id,
                replicas,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[VERTICAL] replica set active config={} replicas={}",
                        configuration_id,
                        node_list(&replicas)
                    )
                    .green()
                );
            }
            Event::VerticalReplicaSetDecommissioned {
                configuration_id,
                replicas,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[VERTICAL] replica set redirecting config={} replicas={}",
                        configuration_id,
                        node_list(&replicas)
                    )
                    .red()
                );
            }
            Event::VerticalReplicaRedirected {
                from_replica,
                active_configuration_id,
                leader,
                ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[VERTICAL] replica {} redirected client to config={} leader={}",
                        node_label(from_replica),
                        active_configuration_id,
                        node_label(leader)
                    )
                    .cyan()
                );
            }
            Event::VerticalReplicaApplied {
                id, slot, value, ..
            } => {
                println!(
                    "{}",
                    format!(
                        "[VERTICAL] replica {} applied slot {} -> {}",
                        node_label(id),
                        slot,
                        value
                    )
                    .green()
                );
            }
            _ => unreachable!("vertical event dispatcher received non-vertical event"),
        }
    }
}

impl PaxosObserver for ConsoleObserver {
    fn on_message_trace(&self, _trace: MessageTrace) {
        // Console observer currently emits only events; message traces are intentionally ignored.
    }

    fn on_event(&self, event: Event) {
        match event.protocol() {
            crate::monitor::EventProtocol::Classic => self.handle_classic_event(event),
            crate::monitor::EventProtocol::Pmmc => self.handle_pmmc_event(event),
            crate::monitor::EventProtocol::Vertical => self.handle_vertical_event(event),
            crate::monitor::EventProtocol::System => self.handle_system_event(event),
        }
    }
}
