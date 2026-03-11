use std::sync::Arc;

use std::collections::HashSet;
use tokio::{select, time};
use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    monitor::PaxosObserver,
    node::{
        pmmc::{
            leader::commander::state::CommanderState, message::PmmcMessage,
            proposal::ProposalsStore, transport::PmmcHandle,
        },
        pvalue::PValue,
    },
    paxos_command::PaxosCommand,
};

mod state;
#[derive(Clone)]
pub struct Commander {
    uuid: Uuid,
    ballot: Ballot,
    state: Arc<CommanderState>,
    peers: Arc<PmmcHandle>,
}

impl Commander {
    pub fn new(
        uuid: Uuid,
        quorum: usize,
        ballot: Ballot,
        replicas: Vec<Uuid>,
        proposals: ProposalsStore,
        peers: Arc<PmmcHandle>,
        _observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        let state = Arc::new(CommanderState::new(
            uuid, ballot, quorum, replicas, proposals,
        ));
        Self {
            uuid,
            ballot,
            state,
            peers,
        }
    }

    pub async fn add_pending(&self, slot: usize, cmd: PaxosCommand) {
        self.state.add_pending(slot, cmd).await
    }

    pub async fn record_replica_ack(&self, from: Uuid, slot: usize) {
        self.state.record_replica_ack(from, slot).await;
    }

    pub fn stop(&self) {
        self.state.stop();
    }
    async fn p2b(&mut self, acceptor: Uuid, pvalue: PValue, ballot: Ballot) -> Option<PmmcMessage> {
        if self.ballot < ballot {
            return Some(PmmcMessage::PREEMPT {
                from: self.uuid,
                to: self.uuid,
                ballot,
            });
        }
        if self.ballot == ballot {
            return self.state.process(acceptor, pvalue).await;
        }
        None
    }

    pub async fn handle_message(&mut self, msg: PmmcMessage) -> Option<PmmcMessage> {
        match msg {
            PmmcMessage::P2B {
                from,
                ballot,
                pvalue,
                ..
            } => self.p2b(from, pvalue, ballot).await,
            _ => None,
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
                                let msg = PmmcMessage::ACCEPTED {
                                    from: self.uuid,
                                    pvalue: PValue::new(slot, self.ballot, pending.cmd.clone()),
                                };
                                peers.broadcast_to(msg.clone(), &unsent).await;
                            }
                        } else {
                            peers
                                .broadcast(PmmcMessage::P2A {
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
