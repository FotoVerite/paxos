use std::sync::Arc;

use tokio::{select, time};
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    message::Message,
    monitor::PaxosObserver,
    node::{
        classic_paxos::ballot::Ballot,
        pmmc::{leader::commander::state::CommanderState, proposal::ProposalsStore},
        pvalue::PValue,
    },
    paxos_command::PaxosCommand,
};

mod state;
#[derive(Clone)]
pub struct Commander {
    uuid: Uuid,
    quorum: usize,
    ballot: Ballot,
    observer: Arc<dyn PaxosObserver>,
    state: Arc<CommanderState>,
    peers: Arc<NetworkSimulator>,
}

impl Commander {
    pub fn new(
        uuid: Uuid,
        quorum: usize,
        ballot: Ballot,
        proposals: ProposalsStore,
        peers: Arc<NetworkSimulator>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        let state = Arc::new(CommanderState::new(uuid, ballot, quorum, proposals));
        Self {
            uuid,
            observer,
            quorum,
            ballot,
            state,
            peers,
        }
    }

    pub async fn add_pending(&self, slot: usize, cmd: PaxosCommand){
        self.state.add_pending(slot, cmd).await
    }
    async fn p2b(&mut self, acceptor: Uuid, pvalue: PValue, ballot: Ballot) -> Message {
        if self.ballot < ballot {
            return Message::PREEMPT {
                from: self.uuid,
                to: self.uuid,
                ballot,
            };
        }
        if self.ballot == ballot {
            return self.state.process(acceptor, pvalue).await;
        }
        Message::NACK
    }
    
    pub async fn handle_message(&mut self, msg: Message) -> Message {
        match msg {
            Message::P2B {
                from,
                ballot,
                pvalue,
                ..
            } => self.p2b(from, pvalue, ballot).await,
            _ => Message::NACK,
        }
    }

    pub async fn run(&self) {
        let state = Arc::clone(&self.state);
        let peers = Arc::clone(&self.peers);
        loop {
            select! {
                _ = time::sleep_until(state.deadline().await) => {
                    let mut pvalues = state.proposals().await;
                    for (slot, (cmd, quorum)) in pvalues.drain() {
                        if quorum.len() >= self.quorum {
                            peers
                                .broadcast(Message::ACCEPTED {
                                    from: self.uuid,
                                    pvalue: PValue::new(slot, self.ballot, cmd),
                                })
                                .await;
                        } else {
                            peers
                                .broadcast(Message::P2A {
                                    from: self.uuid,
                                    pvalue: PValue::new(slot, self.ballot, cmd),
                                })
                                .await;
                        }
                    }
                    state.reset_deadline().await;
                }


                else => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::{BTreeMap, HashMap}, sync::Arc, time::Duration};

    use tokio::{sync::mpsc, time::sleep};
    use uuid::Uuid;

    use crate::{
        cluster::network_simulator::NetworkSimulator,
        message::Message,
        monitor::{NoOpObserver, PaxosObserver},
        node::{classic_paxos::ballot::Ballot, pvalue::PValue},
        paxos_command::PaxosCommand,
    };

    use super::Commander;

    fn cmd(v: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value: v,
        }
    }

    fn mk_commander(quorum: usize, ballot: Ballot, proposals: BTreeMap<usize, PaxosCommand>) -> Commander {
        let id = ballot.node_id;
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peers = Arc::new(NetworkSimulator::new(id, HashMap::new(), Arc::clone(&observer)));
        Commander::new(id, quorum, ballot, proposals, peers, observer)
    }

    #[tokio::test]
    async fn p2b_returns_nack_until_quorum_then_accepted() {
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(5, leader);
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();
        let mut proposals = BTreeMap::new();
        proposals.insert(3, cmd(10));
        let mut commander = mk_commander(2, ballot, proposals);

        let first = commander
            .handle_message(Message::P2B {
                from: a1,
                to: leader,
                ballot,
                pvalue: PValue::new(3, ballot, cmd(10)),
            })
            .await;
        assert!(matches!(first, Message::NACK));

        let second = commander
            .handle_message(Message::P2B {
                from: a2,
                to: leader,
                ballot,
                pvalue: PValue::new(3, ballot, cmd(10)),
            })
            .await;
        assert!(matches!(
            second,
            Message::ACCEPTED { from, pvalue } if from == leader && pvalue.slot() == 3 && pvalue.ballot() == ballot && pvalue.cmd() == cmd(10)
        ));
    }

    #[tokio::test]
    async fn duplicate_acceptor_ack_does_not_count_twice() {
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(7, leader);
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();
        let mut proposals = BTreeMap::new();
        proposals.insert(1, cmd(1));
        let mut commander = mk_commander(2, ballot, proposals);

        let r1 = commander
            .handle_message(Message::P2B {
                from: a1,
                to: leader,
                ballot,
                pvalue: PValue::new(1, ballot, cmd(1)),
            })
            .await;
        assert!(matches!(r1, Message::NACK));

        let dup = commander
            .handle_message(Message::P2B {
                from: a1,
                to: leader,
                ballot,
                pvalue: PValue::new(1, ballot, cmd(1)),
            })
            .await;
        assert!(matches!(dup, Message::NACK));

        let quorum = commander
            .handle_message(Message::P2B {
                from: a2,
                to: leader,
                ballot,
                pvalue: PValue::new(1, ballot, cmd(1)),
            })
            .await;
        assert!(matches!(quorum, Message::ACCEPTED { .. }));
    }

    #[tokio::test]
    async fn higher_ballot_p2b_preempts_commander() {
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(8, leader);
        let mut proposals = BTreeMap::new();
        proposals.insert(2, cmd(2));
        let mut commander = mk_commander(2, ballot, proposals);
        let higher = Ballot::new(9, Uuid::new_v4());

        let reply = commander
            .handle_message(Message::P2B {
                from: Uuid::new_v4(),
                to: leader,
                ballot: higher,
                pvalue: PValue::new(2, ballot, cmd(2)),
            })
            .await;

        assert!(
            matches!(reply, Message::PREEMPT { ballot, .. } if ballot == higher),
            "PMMC commander should preempt on a higher ballot in p2b"
        );
    }

    #[tokio::test]
    async fn lower_ballot_p2b_is_ignored() {
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(11, leader);
        let lower = Ballot::new(10, Uuid::new_v4());
        let mut proposals = BTreeMap::new();
        proposals.insert(2, cmd(2));
        let mut commander = mk_commander(2, ballot, proposals);

        let reply = commander
            .handle_message(Message::P2B {
                from: Uuid::new_v4(),
                to: leader,
                ballot: lower,
                pvalue: PValue::new(2, lower, cmd(2)),
            })
            .await;

        assert!(
            matches!(reply, Message::NACK),
            "PMMC commander should ignore lower-ballot p2b responses"
        );
    }

    #[tokio::test]
    async fn unhandled_message_returns_nack() {
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(1, leader);
        let mut proposals = BTreeMap::new();
        proposals.insert(0, cmd(0));
        let mut commander = mk_commander(1, ballot, proposals);

        let reply = commander
            .handle_message(Message::P1A {
                from: leader,
                ballot,
                start_index: 0,
            })
            .await;
        assert!(matches!(reply, Message::NACK));
    }

    #[tokio::test]
    async fn unknown_slot_p2b_is_ignored() {
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(3, leader);
        let mut proposals = BTreeMap::new();
        proposals.insert(0, cmd(0));
        let mut commander = mk_commander(2, ballot, proposals);

        let reply = commander
            .handle_message(Message::P2B {
                from: Uuid::new_v4(),
                to: leader,
                ballot,
                pvalue: PValue::new(42, ballot, cmd(42)),
            })
            .await;
        assert!(matches!(reply, Message::NACK));
    }

    #[tokio::test]
    async fn run_rebroadcasts_p2a_periodically_until_quorum() {
        let leader = Uuid::new_v4();
        let peer = Uuid::new_v4();
        let ballot = Ballot::new(1, leader);
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let mut proposals = BTreeMap::new();
        proposals.insert(0, cmd(1));

        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        let mut peers_map = HashMap::new();
        peers_map.insert(peer, peer_tx);
        let peers = Arc::new(NetworkSimulator::new(leader, peers_map, Arc::clone(&observer)));
        let commander = Commander::new(leader, 2, ballot, proposals, peers, observer);
        let runner = commander.clone();
        tokio::spawn(async move {
            runner.run().await;
        });

        sleep(Duration::from_millis(700)).await;
        let mut p2a_count = 0usize;
        while let Ok(msg) = peer_rx.try_recv() {
            if matches!(msg, Message::P2A { .. }) {
                p2a_count += 1;
            }
        }

        assert!(
            p2a_count >= 2,
            "commander should keep re-broadcasting p2a while quorum is not reached"
        );
    }
}
