#[tokio::test]
async fn test_ledger_vote_returns_value() {
    use paxos::node::ballot::Ballot;
    use paxos::paxos_command::PaxosCommand;
    use paxos::node::ledger::Ledger;

    let ledger = Ledger::init(1, 1).await.unwrap();
    let b = Ballot::new(1, 1);
    let cmd = PaxosCommand::NOOP;

    println!("Before vote");
    let result = ledger.vote(0, b, cmd.clone(), 0).await;
    println!("Vote result: {:?}", result);
    
    match result {
        Some(v) => println!("Got Some: {:?}", v),
        None => println!("Got None"),
    }
}
