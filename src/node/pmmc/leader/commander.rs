use std::sync::Arc;

use std::collections::HashSet;
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
        replicas: Vec<Uuid>,
        proposals: ProposalsStore,
        peers: Arc<NetworkSimulator>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        let state = Arc::new(CommanderState::new(uuid, ballot, quorum, replicas, proposals));
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

    pub async fn record_replica_ack(&self, from: Uuid, slot: usize) {
        self.state.record_replica_ack(from, slot).await;
    }

    pub fn stop(&self) {
        self.state.stop();
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
            if state.is_stopped() {
                break;
            }
            select! {
                _ = time::sleep_until(state.deadline().await) => {
                    let (mut pvalues, all_replicas) = state.tick_snapshot().await;
                    for (slot, pending) in pvalues.drain() {
                        if pending.decided {
                            let unsent: HashSet<Uuid> = all_replicas
                                .difference(&pending.replica_acks)
                                .copied()
                                .collect();
                            if !unsent.is_empty() {
                                let msg = Message::ACCEPTED {
                                    from: self.uuid,
                                    pvalue: PValue::new(slot, self.ballot, pending.cmd.clone()),
                                };
                                peers.broadcast_to(&msg, &unsent).await;
                            }
                        } else {
                            peers
                                .broadcast(Message::P2A {
                                    from: self.uuid,
                                    pvalue: PValue::new(slot, self.ballot, pending.cmd.clone()),
                                })
                                .await;
                        }
                    }
                    state.reset_deadline().await;
                }
                _ = state.wait_for_stop() => {
                    break;
                }


                else => break,
            }
        }
    }
}

#[cfg(test)]
#[path = "commander_tests.rs"]
mod tests;
