use uuid::Uuid;

use crate::{
    cluster::cluster_configuration::ConfigurationStrategy, common::ballot::Ballot,
    node::pvalue::PValue, paxos_command::PaxosCommand, rsm::kv_store::ReplyOutcome,
};

use super::{ProposalPolicy, ReplicaDurable};

#[test]
fn dedup_watermark_ignores_pending_cache_entries() {
    let mut durable = ReplicaDurable::default();
    let client_id = Uuid::new_v4();
    let req1 = PaxosCommand::NOOP.with_client(client_id, 1);
    let req2 = PaxosCommand::NOOP.with_client(client_id, 2);

    durable.add_to_cache(&req1);
    assert!(durable.client_dedup_watermark().is_empty());

    durable.update_cache(&req1, ReplyOutcome::Ok);
    assert_eq!(durable.client_dedup_watermark().get(&client_id), Some(&1));

    durable.add_to_cache(&req2);
    assert_eq!(durable.client_dedup_watermark().get(&client_id), Some(&1));
}

#[test]
fn stop_sign_rejects_new_proposals_after_stop_slot() {
    let node = Uuid::new_v4();
    let mut durable = ReplicaDurable::default();
    durable.set_strategy(ConfigurationStrategy::StopSign);
    let stop = PaxosCommand::STOP.with_client(node, 1);
    let _ = durable.add_decision(PValue::new(0, Ballot::new(1, node), stop));

    assert!(matches!(
        durable.proposal_policy(),
        ProposalPolicy::RejectStopped
    ));
}

#[test]
fn delayed_stop_sign_allows_grace_slots_before_rejecting() {
    let node = Uuid::new_v4();
    let mut durable = ReplicaDurable::default();
    durable.set_strategy(ConfigurationStrategy::DelayedStopSign);
    durable.set_alpha(2);
    let stop = PaxosCommand::STOP.with_client(node, 1);
    let _ = durable.add_decision(PValue::new(0, Ballot::new(1, node), stop));

    assert!(matches!(durable.proposal_policy(), ProposalPolicy::Admit));
    let _ = durable.add_proposal(PaxosCommand::NOOP.with_client(node, 2));
    assert!(matches!(durable.proposal_policy(), ProposalPolicy::Admit));
    let _ = durable.add_proposal(PaxosCommand::NOOP.with_client(node, 3));
    assert!(matches!(
        durable.proposal_policy(),
        ProposalPolicy::RejectStopped
    ));
}

#[test]
fn padding_rewrites_post_stop_proposals_to_noop() {
    let node = Uuid::new_v4();
    let mut durable = ReplicaDurable::default();
    durable.set_strategy(ConfigurationStrategy::Padding);
    let stop = PaxosCommand::STOP.with_client(node, 1);
    let _ = durable.add_decision(PValue::new(0, Ballot::new(1, node), stop));

    let admitted = durable.add_proposal(PaxosCommand::PUT {
        key: "k".to_string(),
        version: 1,
        value: 9,
    });
    assert_eq!(admitted, PaxosCommand::NOOP);
}

#[test]
fn padding_rejects_proposals_after_alpha_boundary() {
    let node = Uuid::new_v4();
    let mut durable = ReplicaDurable::default();
    durable.set_strategy(ConfigurationStrategy::Padding);
    durable.set_alpha(2);
    let stop = PaxosCommand::STOP.with_client(node, 1);
    let _ = durable.add_decision(PValue::new(0, Ballot::new(1, node), stop));

    assert!(matches!(
        durable.proposal_policy(),
        ProposalPolicy::PadWithNoop
    ));
    let _ = durable.add_proposal(PaxosCommand::NOOP.with_client(node, 2));
    assert!(matches!(
        durable.proposal_policy(),
        ProposalPolicy::PadWithNoop
    ));
    let _ = durable.add_proposal(PaxosCommand::NOOP.with_client(node, 3));
    assert!(matches!(
        durable.proposal_policy(),
        ProposalPolicy::RejectStopped
    ));
}

#[test]
fn padding_requires_drain_before_stop_is_considered_drained() {
    let node = Uuid::new_v4();
    let mut durable = ReplicaDurable::default();
    durable.set_strategy(ConfigurationStrategy::Padding);
    durable.set_alpha(1);
    let stop = PaxosCommand::STOP.with_client(node, 1);
    let _ = durable.add_decision(PValue::new(0, Ballot::new(1, node), stop));

    assert!(!durable.is_stop_drained());
    durable.increment_decisions();
    assert!(!durable.is_stop_drained());
    durable.increment_decisions();
    assert!(durable.is_stop_drained());

    let _ = durable.add_proposal(PaxosCommand::NOOP.with_client(node, 2));
    assert!(durable.is_stop_drained());
}

#[test]
fn brick_wall_does_not_wait_for_post_stop_drain() {
    let node = Uuid::new_v4();
    let mut durable = ReplicaDurable::default();
    durable.set_strategy(ConfigurationStrategy::BrickWall);
    let stop = PaxosCommand::STOP.with_client(node, 1);
    let _ = durable.add_decision(PValue::new(0, Ballot::new(1, node), stop));

    assert!(!durable.is_stop_drained());
    durable.increment_decisions();
    assert!(durable.is_stop_drained());
}
