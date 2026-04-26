use crate::{
    cluster::synod::{SynodRequestStage, cluster::SynodCluster, emoji_counter_key},
    common::persistence::ClusterPersistence,
    monitor::{NoOpObserver, PaxosObserver},
    node::vertical_paxos::replica::VerticalClientReply,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::ClientId;

async fn test_cluster(name: &str) -> SynodCluster {
    let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
    SynodCluster::new(Arc::new(ClusterPersistence::for_test(name)), observer)
        .await
        .expect("synod cluster should build")
}

// ========== Membership Module ==========

#[tokio::test]
async fn assigns_new_client_to_server_owned_node() {
    let mut cluster = test_cluster("synod_assign_new_client").await;

    let assignment = cluster
        .assign_client(None)
        .await
        .expect("assignment should work");

    assert!(assignment.client_id.as_str().starts_with("c_"));
    assert_eq!(cluster.assignment_count(), 1);
    assert_eq!(
        cluster.assignment_for(&assignment.client_id),
        Some(&assignment)
    );
    assert_eq!(cluster.server_node_ids().await, vec![assignment.node_id]);
    assert!(cluster.active_configuration_id().await.is_some());
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn reuses_known_client_assignment() {
    let mut cluster = test_cluster("synod_reuse_known_client").await;
    let first = cluster
        .assign_client(None)
        .await
        .expect("assignment should work");

    let second = cluster
        .assign_client(Some(first.client_id.clone()))
        .await
        .expect("rejoin should work");

    assert_eq!(first.client_id, second.client_id);
    assert_eq!(first.node_id, second.node_id);
    assert_eq!(cluster.assignment_count(), 1);
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn unknown_client_gets_fresh_assignment() {
    let mut cluster = test_cluster("synod_unknown_client_assignment").await;

    let fresh = cluster
        .assign_client(Some(ClientId::from_existing("c_unknown")))
        .await
        .expect("fresh assignment should work");

    assert_ne!(fresh.client_id.as_str(), "c_unknown");
    assert!(fresh.client_id.as_str().starts_with("c_"));
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn membership_reports_client_node_assignments() {
    let mut cluster = test_cluster("synod_membership_report").await;
    let c1 = cluster
        .assign_client(None)
        .await
        .expect("c1 assignment should work");
    let c2 = cluster
        .assign_client(None)
        .await
        .expect("c2 assignment should work");

    let member_ids = cluster.assignment_order();
    assert_eq!(member_ids.len(), 2);
    assert_eq!(member_ids[0], c1.client_id);
    assert_eq!(member_ids[1], c2.client_id);
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn heartbeat_refreshes_client_liveness() {
    let mut cluster = test_cluster("synod_heartbeat").await;
    let assignment = cluster
        .assign_client(None)
        .await
        .expect("assignment should work");

    let heartbeat = cluster
        .heartbeat_client(&assignment.client_id)
        .expect("known client should heartbeat");
    let expired = cluster
        .decommission_clients_last_seen_before(Instant::now() - Duration::from_secs(60))
        .await
        .expect("decommission should work");

    assert_eq!(heartbeat, assignment);
    assert!(expired.is_empty());
    assert_eq!(cluster.assignment_count(), 1);
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn idle_client_is_decommissioned_from_active_membership() {
    let mut cluster = test_cluster("synod_decommission_idle").await;
    let expired = cluster
        .assign_client(None)
        .await
        .expect("first assignment should work");
    let active = cluster
        .assign_client(None)
        .await
        .expect("second assignment should work");
    cluster.last_seen.insert(
        expired.client_id.clone(),
        Instant::now() - Duration::from_secs(120),
    );

    let removed = cluster
        .decommission_clients_last_seen_before(Instant::now() - Duration::from_secs(60))
        .await
        .expect("decommission should work");
    let active_config = cluster
        .active_configuration()
        .await
        .expect("remaining membership should have active config");

    assert_eq!(removed, vec![expired.clone()]);
    assert_eq!(cluster.assignment_count(), 1);
    assert_eq!(cluster.assignment_for(&expired.client_id), None);
    assert_eq!(cluster.assignment_for(&active.client_id), Some(&active));
    assert_eq!(cluster.server_node_ids().await, vec![active.node_id]);
    assert_eq!(active_config.replicas(), &[active.node_id]);
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn decommissioning_last_client_resets_to_empty_cluster() {
    let mut cluster = test_cluster("synod_decommission_last").await;
    let expired = cluster
        .assign_client(None)
        .await
        .expect("assignment should work");
    cluster.last_seen.insert(
        expired.client_id.clone(),
        Instant::now() - Duration::from_secs(120),
    );

    let removed = cluster
        .decommission_clients_last_seen_before(Instant::now() - Duration::from_secs(60))
        .await
        .expect("decommission should work");

    assert_eq!(removed, vec![expired]);
    assert_eq!(cluster.assignment_count(), 0);
    assert!(cluster.server_node_ids().await.is_empty());
    assert!(cluster.active_configuration_id().await.is_none());

    let fresh = cluster
        .assign_client(None)
        .await
        .expect("fresh assignment should work after reset");
    assert_eq!(cluster.server_node_ids().await, vec![fresh.node_id]);
    cluster.cleanup().await.expect("cleanup should work");
}

// ========== Proposal Module ==========

#[tokio::test]
async fn joined_client_can_propose_rust_emoji() {
    let mut cluster = test_cluster("synod_propose_emoji").await;
    let assignment = cluster
        .assign_client(None)
        .await
        .expect("assignment should work");

    let receipt = cluster
        .propose_emoji(assignment.client_id.clone(), 1, "🦀".to_string())
        .await
        .expect("proposal should submit");

    assert_eq!(receipt.client_id, assignment.client_id);
    assert_eq!(receipt.request_id, 1);
    assert_eq!(receipt.emoji, "🦀");
    assert_eq!(receipt.assigned_node, assignment.node_id);
    assert!(matches!(
        receipt.reply,
        VerticalClientReply::Accepted { slot: 0, .. }
    ));
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn proposed_and_applied_events_use_recorded_client_name() {
    let mut cluster = test_cluster("synod_client_name_display").await;
    let assignment = cluster
        .assign_client(None)
        .await
        .expect("assignment should work");
    cluster.record_client_name(&assignment.client_id, "Borrow Checker");

    cluster
        .propose_emoji(assignment.client_id.clone(), 1, "🦀".to_string())
        .await
        .expect("proposal should submit");
    wait_for_applied(&cluster, &assignment.client_id, 1).await;

    let status = cluster
        .request_status(&assignment.client_id, 1)
        .expect("status should exist");
    let room_state = cluster.room_state();
    let recent = room_state
        .recent
        .iter()
        .find(|event| event.request_id == Some(1))
        .expect("applied event should exist");

    assert_eq!(status.client_id, "Borrow Checker");
    assert_eq!(recent.client_id.as_deref(), Some("Borrow Checker"));
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn late_joining_node_installs_current_rsm_checkpoint() {
    let mut cluster = test_cluster("synod_late_join_checkpoint").await;
    let first = cluster
        .assign_client(None)
        .await
        .expect("first assignment should work");

    for request_id in 1..=2 {
        cluster
            .propose_emoji(first.client_id.clone(), request_id, "🦀".to_string())
            .await
            .expect("proposal should submit");
    }
    wait_for_applied(&cluster, &first.client_id, 2).await;

    let second = cluster
        .assign_client(None)
        .await
        .expect("late join should work");
    let active_config = cluster
        .active_configuration()
        .await
        .expect("active config should exist after join");
    let checkpoint = active_config
        .checkpoint()
        .expect("new active config should carry checkpoint");

    assert_eq!(active_config.start_index(), 2);
    assert_eq!(checkpoint.manifest().last_applied_slot(), Some(1));
    assert_eq!(
        checkpoint
            .state()
            .kv_store()
            .get(&emoji_counter_key("🦀"))
            .map(|entry| entry.value.0),
        Some(2)
    );

    let receipt = cluster
        .propose_emoji(second.client_id.clone(), 1, "📦".to_string())
        .await
        .expect("late client proposal should submit");
    assert!(matches!(
        receipt.reply,
        VerticalClientReply::Accepted { slot: 2, .. }
    ));
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn proposal_rejects_unknown_client() {
    let cluster = test_cluster("synod_propose_unknown").await;

    let err = cluster
        .propose_emoji(ClientId::from_existing("c_missing"), 1, "🦀".to_string())
        .await
        .expect_err("unknown client should reject");

    assert!(matches!(err, super::SynodProposalError::UnknownClient(_)));
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn proposal_rejects_non_pool_emoji() {
    let mut cluster = test_cluster("synod_propose_invalid").await;
    let assignment = cluster
        .assign_client(None)
        .await
        .expect("assignment should work");

    let err = cluster
        .propose_emoji(assignment.client_id, 1, "🍒".to_string())
        .await
        .expect_err("invalid emoji should reject");

    assert!(matches!(err, super::SynodProposalError::InvalidEmoji(_)));
    cluster.cleanup().await.expect("cleanup should work");
}

#[test]
fn emoji_proposal_uses_rsm_increment_command() {
    let cmd = super::emoji_increment_command("🦀");

    assert_eq!(
        cmd,
        crate::paxos_command::PaxosCommand::INCREMENT {
            key: "synod:emoji:🦀".to_string(),
            value: 1,
        }
    );
}

async fn wait_for_applied(cluster: &SynodCluster, client_id: &ClientId, request_id: u64) {
    for _ in 0..100 {
        if cluster
            .request_status(client_id, request_id)
            .is_some_and(|status| status.stage == SynodRequestStage::Applied)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("request {request_id} was not applied");
}

// ========== Reconfiguration Module ==========

#[tokio::test]
async fn new_client_join_reconfigures_active_membership() {
    let mut cluster = test_cluster("synod_reconfigure_membership").await;
    let first = cluster
        .assign_client(None)
        .await
        .expect("first assignment should work");
    let first_config = cluster
        .active_configuration()
        .await
        .expect("first config should exist");

    let second = cluster
        .assign_client(None)
        .await
        .expect("second assignment should work");
    let second_config = cluster
        .active_configuration()
        .await
        .expect("second config should exist");

    assert_ne!(first_config.id(), second_config.id());
    assert_eq!(second_config.leader(), first.node_id);
    assert_eq!(second_config.acceptors(), &[first.node_id, second.node_id]);
    assert_eq!(second_config.replicas(), &[first.node_id, second.node_id]);
    assert_eq!(
        second_config.read_quorum().members(),
        &[first.node_id, second.node_id]
    );
    assert_eq!(
        second_config.write_quorum().members(),
        &[first.node_id, second.node_id]
    );
    assert_eq!(second_config.complete_bal(), Some(first_config.ballot()));
    assert_eq!(
        cluster.active_configuration_id().await,
        Some(second_config.id())
    );
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn concurrent_proposals_from_different_clients() {
    let mut cluster = test_cluster("synod_concurrent_proposals").await;
    let c1 = cluster
        .assign_client(None)
        .await
        .expect("c1 assignment should work");
    let c2 = cluster
        .assign_client(None)
        .await
        .expect("c2 assignment should work");
    let c3 = cluster
        .assign_client(None)
        .await
        .expect("c3 assignment should work");

    // Submit proposals concurrently (all at once, don't await between)
    let fut1 = cluster.propose_emoji(c1.client_id, 1, "🦀".to_string());
    let fut2 = cluster.propose_emoji(c2.client_id, 1, "📦".to_string());
    let fut3 = cluster.propose_emoji(c3.client_id, 1, "🔒".to_string());

    let (r1, r2, r3) = tokio::join!(fut1, fut2, fut3);

    let r1 = r1.expect("c1 proposal should submit");
    let r2 = r2.expect("c2 proposal should submit");
    let r3 = r3.expect("c3 proposal should submit");

    // All should be accepted
    assert_eq!(r1.status.stage, SynodRequestStage::Accepted);
    assert_eq!(r2.status.stage, SynodRequestStage::Accepted);
    assert_eq!(r3.status.stage, SynodRequestStage::Accepted);

    // All should have different slots
    let s1 = match &r1.reply {
        VerticalClientReply::Accepted { slot, .. } => *slot,
        _ => panic!("expected Accepted"),
    };
    let s2 = match &r2.reply {
        VerticalClientReply::Accepted { slot, .. } => *slot,
        _ => panic!("expected Accepted"),
    };
    let s3 = match &r3.reply {
        VerticalClientReply::Accepted { slot, .. } => *slot,
        _ => panic!("expected Accepted"),
    };

    assert_ne!(s1, s2, "concurrent proposals should get different slots");
    assert_ne!(s2, s3, "concurrent proposals should get different slots");
    assert_ne!(s1, s3, "concurrent proposals should get different slots");

    // All should eventually apply
    tokio::time::sleep(Duration::from_millis(500)).await;
    let state = cluster.room_state();
    assert!(
        state.cluster_slot >= 3,
        "should have applied at least 3 slots, got {}",
        state.cluster_slot
    );
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn duplicate_proposal_is_idempotent() {
    let mut cluster = test_cluster("synod_duplicate_proposal").await;
    let client = cluster
        .assign_client(None)
        .await
        .expect("assignment should work");

    // First proposal
    let r1 = cluster
        .propose_emoji(client.client_id.clone(), 1, "🦀".to_string())
        .await
        .expect("first proposal should submit");

    assert_eq!(r1.status.stage, SynodRequestStage::Accepted);

    // Wait for it to be applied so the cache is populated
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Submit identical proposal again (same request_id, same emoji)
    let r2 = cluster
        .propose_emoji(client.client_id.clone(), 1, "🦀".to_string())
        .await
        .expect("duplicate proposal should submit");

    // Should be idempotent: same status, retrieved from cache
    assert_eq!(r2.status.stage, SynodRequestStage::Accepted);

    // The important invariant: room state should still have just one application
    let state = cluster.room_state();
    assert_eq!(
        state.cluster_slot, 1,
        "should have applied exactly 1 proposal, idempotent duplicates should not create new slots"
    );
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn four_clients_sequential_all_propose_accepted() {
    let mut cluster = test_cluster("synod_four_clients").await;
    let c1 = cluster
        .assign_client(None)
        .await
        .expect("c1 assignment should work");
    let c2 = cluster
        .assign_client(None)
        .await
        .expect("c2 assignment should work");
    let c3 = cluster
        .assign_client(None)
        .await
        .expect("c3 assignment should work");
    let c4 = cluster
        .assign_client(None)
        .await
        .expect("c4 assignment should work");

    let r1 = cluster
        .propose_emoji(c1.client_id, 1, "🦀".to_string())
        .await
        .expect("c1 proposal should submit");
    let r2 = cluster
        .propose_emoji(c2.client_id, 1, "📦".to_string())
        .await
        .expect("c2 proposal should submit");
    let r3 = cluster
        .propose_emoji(c3.client_id, 1, "🔒".to_string())
        .await
        .expect("c3 proposal should submit");
    let r4 = cluster
        .propose_emoji(c4.client_id, 1, "🧵".to_string())
        .await
        .expect("c4 proposal should submit");

    assert_eq!(r1.status.stage, SynodRequestStage::Accepted);
    assert_eq!(r2.status.stage, SynodRequestStage::Accepted);
    assert_eq!(r3.status.stage, SynodRequestStage::Accepted);
    assert_eq!(r4.status.stage, SynodRequestStage::Accepted);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let state = cluster.room_state();
    assert!(
        state.cluster_slot >= 4,
        "should have applied at least 4 slots, got {}",
        state.cluster_slot
    );
    cluster.cleanup().await.expect("cleanup should work");
}

#[tokio::test]
async fn three_clients_can_propose_and_apply() {
    let mut cluster = test_cluster("synod_three_clients").await;
    let c1 = cluster
        .assign_client(None)
        .await
        .expect("c1 assignment should work");
    let c2 = cluster
        .assign_client(None)
        .await
        .expect("c2 assignment should work");
    let c3 = cluster
        .assign_client(None)
        .await
        .expect("c3 assignment should work");

    let active_cfg = cluster.active_configuration().await;
    println!(
        "Active config leader: {:?}",
        active_cfg.as_ref().map(|cfg| cfg.leader())
    );
    println!("C1 assigned to: {}", c1.node_id);
    println!("C2 assigned to: {}", c2.node_id);
    println!("C3 assigned to: {}", c3.node_id);

    let r1 = cluster
        .propose_emoji(c1.client_id, 1, "🦀".to_string())
        .await
        .expect("c1 proposal should submit");
    println!("R1 stage: {:?}", r1.status.stage);
    let r2 = cluster
        .propose_emoji(c2.client_id, 1, "📦".to_string())
        .await
        .expect("c2 proposal should submit");
    println!("R2 stage: {:?}", r2.status.stage);
    let r3 = cluster
        .propose_emoji(c3.client_id, 1, "🔒".to_string())
        .await
        .expect("c3 proposal should submit");
    println!("R3 stage: {:?}", r3.status.stage);

    assert_eq!(
        r1.status.stage,
        SynodRequestStage::Accepted,
        "c1 should be accepted"
    );
    assert_eq!(
        r2.status.stage,
        SynodRequestStage::Accepted,
        "c2 should be accepted"
    );
    assert_eq!(
        r3.status.stage,
        SynodRequestStage::Accepted,
        "c3 should be accepted"
    );

    // Wait briefly for learning
    tokio::time::sleep(Duration::from_millis(500)).await;

    let state = cluster.room_state();
    assert!(
        state.cluster_slot >= 3,
        "should have applied at least 3 slots, got {}",
        state.cluster_slot
    );
    cluster.cleanup().await.expect("cleanup should work");
}
