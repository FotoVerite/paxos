use paxos::{
    common::persistence::ClusterPersistence,
    common::types::DecreeId,
    console_observer::ConsoleObserver,
    node::classic_paxos::{decree_notes::DecreeNotes, proposer::Proposer},
    paxos_command::PaxosCommand,
};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[tokio::test]
async fn test_proposer_persistence() -> anyhow::Result<()> {
    let uuid = Uuid::new_v4();
    let persistence = ClusterPersistence::for_test("persistence_test").node(uuid);
    let observer = Arc::new(ConsoleObserver);

    // 1. Init DecreeNotes and Proposer
    // Use load_or_init to ensure it starts fresh (or loads empty)
    let decree_notes = Arc::new(Mutex::new(DecreeNotes::load_or_init(&persistence).await?));

    // We expect it to be empty initially
    {
        let notes = decree_notes.lock().await;
        assert!(notes.state.is_empty());
    }

    let proposer = Proposer::new(
        uuid,
        3,
        persistence.clone(),
        Arc::clone(&decree_notes),
        observer.clone(),
    );

    // 2. Propose a value -> should bump ballot to (1, 1) and save
    proposer.propose(DecreeId(0), PaxosCommand::NOOP).await;

    // 3. Check persistence file exists under the node-scoped test directory.
    let filename = persistence.dir().join("decree_notes.bin");
    assert!(
        Path::new(&filename).exists(),
        "Persistence file {} should exist",
        filename.display()
    );

    // 4. Simulate restart: Load fresh DecreeNotes from disk
    // Drop logic not strictly needed as we load a new instance
    let recovered_notes = DecreeNotes::load_or_init(&persistence).await?;
    let recovered_note = recovered_notes
        .state
        .get(&DecreeId(0))
        .expect("Should have note for decree 0");

    // 5. Verify ballot
    // Initial was (0,0). propose -> (1,id) = (1,1).
    assert_eq!(recovered_note.last_tried.number, 1);
    assert_eq!(recovered_note.last_tried.node_id, uuid);

    // Cleanup
    std::fs::remove_file(filename).ok();
    Ok(())
}
