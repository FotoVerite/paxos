mod test_helpers;
use test_helpers::create_ledger;

#[tokio::test]
async fn test_ledger_vote_returns_value() {
    use paxos::node::ballot::Ballot;
    use paxos::paxos_command::PaxosCommand;

    // Clean up any previous state
    let _ = std::fs::remove_dir_all(".paxos");
    let _ = std::fs::create_dir_all(".paxos");

    let ledger = create_ledger();
    let _b = Ballot::new(1, 1); // Ballot is no longer used for ledger.insert
    let cmd = PaxosCommand::NOOP;

    println!("Before insert");
    let inserted = ledger.insert(0, cmd.clone()).await;
    println!("Insert result: {:?}", inserted);
    
    match inserted {
        true => println!("Successfully inserted"),
        false => println!("Insertion failed (already present)"),
    }

    assert!(inserted, "Expected insert to succeed");
    let retrieved_cmd = ledger.get(0).await;
    assert_eq!(retrieved_cmd, Some(cmd), "Expected to retrieve the inserted command");
}
