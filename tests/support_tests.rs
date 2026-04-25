mod support;

use std::time::Duration;

use paxos::{
    common::types::DecreeId,
    monitor::{Event, PaxosObserver},
    paxos_command::PaxosCommand,
};
use uuid::Uuid;

use support::{EventBarrier, RecordingObserver, test_uuid};

#[tokio::test]
async fn test_event_barrier_multiple_events() {
    let barrier = EventBarrier::new();

    barrier
        .record(Event::Proposal {
            id: test_uuid(1),
            decree_num: DecreeId(0),
            value: PaxosCommand::NOOP,
            created_at: 0,
        })
        .await;
    barrier
        .record(Event::Promise {
            id: test_uuid(1),
            from: Uuid::nil(),
            decree_num: DecreeId(0),
            ballot: 1,
            created_at: 0,
        })
        .await;

    let proposals = barrier
        .wait_for(
            |e| matches!(e, Event::Proposal { .. }),
            1,
            Duration::from_secs(1),
        )
        .await;

    assert!(proposals.is_ok());
    assert_eq!(proposals.unwrap().len(), 1);
}

#[tokio::test]
async fn test_event_barrier_count_matching() {
    let barrier = EventBarrier::new();

    for i in 0..3 {
        barrier
            .record(Event::LearnedValue {
                id: test_uuid(1),
                decree_num: DecreeId(i),
                value: PaxosCommand::NOOP,
                created_at: 0,
            })
            .await;
    }

    let count = barrier
        .count_matching(|e| matches!(e, Event::LearnedValue { .. }))
        .await;

    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_event_barrier_clear() {
    let barrier = EventBarrier::new();

    barrier
        .record(Event::Proposal {
            id: test_uuid(1),
            decree_num: DecreeId(0),
            value: PaxosCommand::NOOP,
            created_at: 0,
        })
        .await;

    assert_eq!(barrier.get_events().await.len(), 1);

    barrier.clear().await;

    assert_eq!(barrier.get_events().await.len(), 0);
}

#[tokio::test]
async fn test_recording_observer_with_barrier() {
    let obs = RecordingObserver::new().arc();
    let barrier = obs.barrier.clone();

    obs.on_event(Event::LearnedValue {
        id: test_uuid(1),
        decree_num: DecreeId(0),
        value: PaxosCommand::NOOP,
        created_at: 0,
    });

    let learned = barrier
        .wait_for(
            |e| matches!(e, Event::LearnedValue { .. }),
            1,
            Duration::from_secs(1),
        )
        .await;

    assert!(learned.is_ok());
}
