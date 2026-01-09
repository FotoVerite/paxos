use paxos::{
    node::paxos_state::{decree_notes::DecreeNotes, proposer::Proposer},
    paxos_command::PaxosCommand,
    console_observer::ConsoleObserver,
    common::types::{NodeId, DecreeId},
};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;
use std::path::Path;

#[tokio::test]
async fn test_proposer_persistence() -> anyhow::Result<()> {
    // Determine a temp dir for this test to avoid conflicts, or just use .paxos
    // The code hardcodes ".paxos", so we must expect files there.
    
    // Setup
    let uuid = Uuid::new_v4();
    let id = 1;
    let observer = Arc::new(ConsoleObserver);
    
    // 1. Init DecreeNotes and Proposer
    // Use load_or_init to ensure it starts fresh (or loads empty)
    let decree_notes = Arc::new(Mutex::new(DecreeNotes::load_or_init(uuid).await?));
    
    // We expect it to be empty initially
    {
        let notes = decree_notes.lock().await;
        assert!(notes.state.is_empty());
    }

    let proposer = Proposer::new(NodeId(id), uuid, 3, Arc::clone(&decree_notes), observer.clone());

    // 2. Propose a value -> should bump ballot to (1, 1) and save
    proposer.propose(DecreeId(0), PaxosCommand::NOOP).await;
    
    // 3. Check persistence file exists
    // The path is defined in src/common/persistence.rs as .paxos/
    let filename = format!(".paxos/decree_notes_{}.bin", uuid);
    assert!(Path::new(&filename).exists(), "Persistence file {} should exist", filename);

    // 4. Simulate restart: Load fresh DecreeNotes from disk
    // Drop logic not strictly needed as we load a new instance
    let recovered_notes = DecreeNotes::load_or_init(uuid).await?;
    let recovered_note = recovered_notes.state.get(&DecreeId(0)).expect("Should have note for decree 0");
    
    // 5. Verify ballot
    // Initial was (0,0). propose -> (1,id) = (1,1).
    assert_eq!(recovered_note.last_tried.number, 1);
    assert_eq!(recovered_note.last_tried.node_id, NodeId(id));

    // Cleanup
    std::fs::remove_file(filename).ok();
    Ok(())
}
