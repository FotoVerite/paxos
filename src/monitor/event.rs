use std::collections::HashSet;

use uuid::Uuid;

use crate::cluster::pmmc::reconfiguration::ConfigurationStrategy;
use crate::common::ballot::Ballot;
use crate::node::pvalue::PValue;
use crate::{common::types::DecreeId, paxos_command::PaxosCommand};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum EventProtocol {
    Classic,
    Pmmc,
    Vertical,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconfigurationPhase {
    Requested,
    Stop,
    Checkpoint,
    Apply,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconfigurationProposalOutcome {
    Admitted,
    Queued,
    Stopped,
    CachedResponse,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum Event {
    Proposal {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    Promise {
        id: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        ballot: usize,
        created_at: u64,
    },
    Accept {
        id: Uuid,
        decree_num: DecreeId,
        ballot: usize,
        quorum: HashSet<Uuid>,
        value: PaxosCommand,
        created_at: u64,
    },
    Accepted {
        id: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        ballot: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    Learn {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    Success {
        id: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    LearnedValue {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    InitialDecree {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    BatchInitialDecrees {
        id: Uuid,
        decrees: Vec<(DecreeId, PaxosCommand)>,
        created_at: u64,
    },
    LedgerDump {
        id: Uuid,
        decrees: Vec<(DecreeId, PaxosCommand)>,
        created_at: u64,
    },
    NodeState {
        id: Uuid,
        role: String,
        ballot: usize,
        learned_count: usize,
    },
    NodeCapabilities {
        id: Uuid,
        roles: Vec<String>,
        learning_strategy: String,
    },
    MessageSent {
        from: Uuid,
        to: Uuid,
        message_type: String,
    },
    BallotAdopted {
        id: Uuid,
        ballot: Ballot,
    },
    ProposalAccepted {
        id: Uuid,
        pvalue: PValue,
    },
    PmmcPropose {
        id: Uuid,
        slot: usize,
        cmd: PaxosCommand,
        created_at: u64,
    },
    PmmcP1A {
        from: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcP1B {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcP2A {
        from: Uuid,
        pvalue: PValue,
        created_at: u64,
    },
    PmmcP2B {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        pvalue: PValue,
        created_at: u64,
    },
    PmmcAdopted {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcPreempted {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcHeartbeat {
        from: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcAck {
        from: Uuid,
        to: Uuid,
        slot: usize,
        created_at: u64,
    },
    VerticalConfigurationInstalled {
        configuration_id: Uuid,
        leader: Uuid,
        replicas: Vec<Uuid>,
        acceptors: Vec<Uuid>,
        created_at: u64,
    },
    VerticalActivationStarted {
        configuration_id: Uuid,
        leader: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    VerticalActivationReady {
        configuration_id: Uuid,
        leader: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    VerticalActivationSuperseded {
        configuration_id: Uuid,
        leader: Uuid,
        ballot: Ballot,
        superseded_by: Ballot,
        created_at: u64,
    },
    VerticalReplicaSetActivated {
        configuration_id: Uuid,
        replicas: Vec<Uuid>,
        created_at: u64,
    },
    VerticalReplicaSetDecommissioned {
        configuration_id: Uuid,
        replicas: Vec<Uuid>,
        created_at: u64,
    },
    VerticalReplicaRedirected {
        from_replica: Uuid,
        active_configuration_id: Uuid,
        leader: Uuid,
        replicas: Vec<Uuid>,
        created_at: u64,
    },
    VerticalReplicaApplied {
        id: Uuid,
        configuration_id: Option<Uuid>,
        slot: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    PartitionCreated {
        partition_a: Vec<Uuid>,
        partition_b: Vec<Uuid>,
        created_at: u64,
    },
    PartitionHealed {
        partition_a: Vec<Uuid>,
        partition_b: Vec<Uuid>,
        created_at: u64,
    },
    LeaderElected {
        id: Uuid,
        created_at: u64,
    },
    LeaderSteppedDown {
        id: Uuid,
        created_at: u64,
    },
    NodeCrashed {
        id: Uuid,
        created_at: u64,
    },
    ReconfigurationRequested {
        strategy: ConfigurationStrategy,
        add_nodes: Vec<Uuid>,
        remove_nodes: Vec<Uuid>,
        created_at: u64,
    },
    ReconfigurationStopStarted {
        strategy: ConfigurationStrategy,
        target_nodes: Vec<Uuid>,
        created_at: u64,
    },
    ReconfigurationStopCommandSent {
        to: Uuid,
        created_at: u64,
    },
    ReconfigurationStopCompleted {
        strategy: ConfigurationStrategy,
        stopped_nodes: Vec<Uuid>,
        created_at: u64,
    },
    ReconfigurationStopDecided {
        id: Uuid,
        strategy: ConfigurationStrategy,
        slot: usize,
        delayed_slots: usize,
        created_at: u64,
    },
    ReconfigurationStopApplied {
        id: Uuid,
        strategy: ConfigurationStrategy,
        slot: usize,
        delayed_slots: usize,
        created_at: u64,
    },
    ReconfigurationProposalObserved {
        id: Uuid,
        strategy: ConfigurationStrategy,
        outcome: ReconfigurationProposalOutcome,
        original_cmd: PaxosCommand,
        effective_cmd: Option<PaxosCommand>,
        slot: Option<usize>,
        stop_slot: Option<usize>,
        delayed_slots: usize,
        barrier_active: bool,
        created_at: u64,
    },
    ReconfigurationCheckpointSelected {
        strategy: ConfigurationStrategy,
        source_node: Option<Uuid>,
        last_applied_slot: Option<usize>,
        created_at: u64,
    },
    ReconfigurationApplied {
        strategy: ConfigurationStrategy,
        previous_node_count: usize,
        next_node_count: usize,
        created_at: u64,
    },
    ReconfigurationReady {
        strategy: ConfigurationStrategy,
        leader: Option<Uuid>,
        active_nodes: Vec<Uuid>,
        created_at: u64,
    },
    ReconfigurationNodeRetired {
        id: Uuid,
        created_at: u64,
    },
    ReconfigurationNodeRebooted {
        id: Uuid,
        created_at: u64,
    },
    ReconfigurationFailed {
        strategy: Option<ConfigurationStrategy>,
        phase: ReconfigurationPhase,
        reason: String,
        created_at: u64,
    },
}

impl Event {
    pub fn protocol(&self) -> EventProtocol {
        match self {
            Event::Proposal { .. }
            | Event::Promise { .. }
            | Event::Accept { .. }
            | Event::Accepted { .. }
            | Event::Learn { .. }
            | Event::Success { .. }
            | Event::LearnedValue { .. }
            | Event::InitialDecree { .. }
            | Event::BatchInitialDecrees { .. }
            | Event::LedgerDump { .. }
            | Event::NodeState { .. } => EventProtocol::Classic,

            Event::BallotAdopted { .. }
            | Event::ProposalAccepted { .. }
            | Event::PmmcPropose { .. }
            | Event::PmmcP1A { .. }
            | Event::PmmcP1B { .. }
            | Event::PmmcP2A { .. }
            | Event::PmmcP2B { .. }
            | Event::PmmcAdopted { .. }
            | Event::PmmcPreempted { .. }
            | Event::PmmcHeartbeat { .. }
            | Event::PmmcAck { .. } => EventProtocol::Pmmc,

            Event::VerticalConfigurationInstalled { .. }
            | Event::VerticalActivationStarted { .. }
            | Event::VerticalActivationReady { .. }
            | Event::VerticalActivationSuperseded { .. }
            | Event::VerticalReplicaSetActivated { .. }
            | Event::VerticalReplicaSetDecommissioned { .. }
            | Event::VerticalReplicaRedirected { .. }
            | Event::VerticalReplicaApplied { .. } => EventProtocol::Vertical,

            Event::NodeCapabilities { .. }
            | Event::MessageSent { .. }
            | Event::PartitionCreated { .. }
            | Event::PartitionHealed { .. }
            | Event::LeaderElected { .. }
            | Event::LeaderSteppedDown { .. }
            | Event::NodeCrashed { .. }
            | Event::ReconfigurationRequested { .. }
            | Event::ReconfigurationStopStarted { .. }
            | Event::ReconfigurationStopCommandSent { .. }
            | Event::ReconfigurationStopCompleted { .. }
            | Event::ReconfigurationStopDecided { .. }
            | Event::ReconfigurationStopApplied { .. }
            | Event::ReconfigurationProposalObserved { .. }
            | Event::ReconfigurationCheckpointSelected { .. }
            | Event::ReconfigurationApplied { .. }
            | Event::ReconfigurationReady { .. }
            | Event::ReconfigurationNodeRetired { .. }
            | Event::ReconfigurationNodeRebooted { .. }
            | Event::ReconfigurationFailed { .. } => EventProtocol::System,
        }
    }
}
