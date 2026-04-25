use uuid::Uuid;

use crate::{
    cluster::synod_vertical::configuration::{
        VerticalClusterConfiguration, VerticalPaxosConfigError, VerticalPaxosVariant,
    },
    cluster::synod_vertical::quorum::Quorum,
    common::ballot::Ballot,
};

fn ids(count: usize) -> Vec<Uuid> {
    (0..count).map(|_| Uuid::new_v4()).collect()
}

#[test]
fn quorum_rejects_empty_members() {
    let err = Quorum::new(vec![]).expect_err("empty quorum should fail");
    assert_eq!(err, VerticalPaxosConfigError::EmptyQuorum);
}

#[test]
fn quorum_rejects_duplicate_members() {
    let member = Uuid::new_v4();
    let err = Quorum::new(vec![member, member]).expect_err("duplicate quorum should fail");
    assert_eq!(err, VerticalPaxosConfigError::DuplicateQuorumMember(member));
}

#[test]
fn configuration_accepts_valid_v1_shape() {
    let id = Uuid::new_v4();
    let leader = Uuid::new_v4();
    let acceptors = ids(3);
    let read_quorum = Quorum::new(vec![acceptors[0]]).expect("read quorum should build");
    let write_quorum = Quorum::new(acceptors.clone()).expect("write quorum should build");
    let complete_bal = Some(Ballot::new(2, Uuid::new_v4()));

    let cfg = VerticalClusterConfiguration::new(
        id,
        leader,
        VerticalPaxosVariant::V1,
        Ballot::new(3, leader),
        7,
        acceptors.clone(),
        vec![leader],
        read_quorum.clone(),
        write_quorum.clone(),
        complete_bal,
    )
    .expect("config should be valid");

    assert_eq!(cfg.id(), id);
    assert_eq!(cfg.leader(), leader);
    assert_eq!(cfg.variant(), VerticalPaxosVariant::V1);
    assert_eq!(cfg.ballot(), Ballot::new(3, leader));
    assert_eq!(cfg.start_index(), 7);
    assert_eq!(cfg.acceptors(), acceptors.as_slice());
    assert_eq!(cfg.replicas(), &[leader]);
    assert_eq!(cfg.read_quorum(), &read_quorum);
    assert_eq!(cfg.write_quorum(), &write_quorum);
    assert_eq!(cfg.complete_bal(), complete_bal);
}

#[test]
fn configuration_accepts_valid_v2_shape() {
    let id = Uuid::new_v4();
    let leader = Uuid::new_v4();
    let acceptors = ids(4);
    let read_quorum =
        Quorum::new(vec![acceptors[0], acceptors[1]]).expect("read quorum should build");
    let write_quorum = Quorum::new(vec![acceptors[1], acceptors[2], acceptors[3]])
        .expect("write quorum should build");
    let complete_bal = Some(Ballot::with_epoch(7, Uuid::new_v4(), 2));

    let cfg = VerticalClusterConfiguration::new(
        id,
        leader,
        VerticalPaxosVariant::V2,
        Ballot::with_epoch(9, leader, 3),
        12,
        acceptors.clone(),
        vec![leader, Uuid::new_v4()],
        read_quorum.clone(),
        write_quorum.clone(),
        complete_bal,
    )
    .expect("config should be valid");

    assert_eq!(cfg.id(), id);
    assert_eq!(cfg.leader(), leader);
    assert_eq!(cfg.variant(), VerticalPaxosVariant::V2);
    assert_eq!(cfg.ballot(), Ballot::with_epoch(9, leader, 3));
    assert_eq!(cfg.start_index(), 12);
    assert_eq!(cfg.acceptors(), acceptors.as_slice());
    assert_eq!(cfg.read_quorum(), &read_quorum);
    assert_eq!(cfg.write_quorum(), &write_quorum);
    assert_eq!(cfg.complete_bal(), complete_bal);
}

#[test]
fn configuration_rejects_quorum_members_outside_acceptors() {
    let leader = Uuid::new_v4();
    let acceptors = ids(2);
    let outsider = Uuid::new_v4();
    let read_quorum =
        Quorum::new(vec![acceptors[0], outsider]).expect("quorum shape is syntactically valid");
    let write_quorum =
        Quorum::new(vec![acceptors[0], acceptors[1]]).expect("write quorum should build");

    let err = VerticalClusterConfiguration::new(
        Uuid::new_v4(),
        leader,
        VerticalPaxosVariant::V1,
        Ballot::new(1, leader),
        0,
        acceptors,
        vec![],
        read_quorum,
        write_quorum,
        None,
    )
    .expect_err("outsider quorum member should fail");

    assert_eq!(
        err,
        VerticalPaxosConfigError::ReadQuorumOutsideAcceptors(outsider)
    );
}

#[test]
fn configuration_rejects_complete_ballot_not_earlier_than_ballot() {
    let leader = Uuid::new_v4();
    let acceptors = ids(3);
    let read_quorum = Quorum::new(vec![acceptors[0]]).expect("read quorum should build");
    let write_quorum = Quorum::new(acceptors.clone()).expect("write quorum should build");
    let ballot = Ballot::with_epoch(5, leader, 1);
    let complete_bal = ballot;

    let err = VerticalClusterConfiguration::new(
        Uuid::new_v4(),
        leader,
        VerticalPaxosVariant::V2,
        ballot,
        0,
        acceptors,
        vec![],
        read_quorum,
        write_quorum,
        Some(complete_bal),
    )
    .expect_err("complete ballot must be earlier");

    assert_eq!(
        err,
        VerticalPaxosConfigError::CompleteBallotNotEarlier {
            complete_bal,
            ballot,
        }
    );
}
