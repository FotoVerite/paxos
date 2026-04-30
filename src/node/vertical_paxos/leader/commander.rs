use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    node::{
        pvalue::PValue,
        vertical_paxos::{
            message::VerticalPaxosMessage, proposal::ProposalsStore, transport::VerticalPaxosHandle,
        },
    },
    paxos_command::PaxosCommand,
};

#[derive(Clone)]
struct PendingProposal {
    cmd: PaxosCommand,
    acceptor_acks: HashSet<Uuid>,
    replica_acks: HashSet<Uuid>,
    decided: bool,
}

pub struct Commander {
    id: Uuid,
    ballot: Ballot,
    write_quorum: HashSet<Uuid>,
    replicas: HashSet<Uuid>,
    peers: Arc<VerticalPaxosHandle>,
    pending: Arc<Mutex<HashMap<usize, PendingProposal>>>,
}

impl Commander {
    pub fn new(
        id: Uuid,
        ballot: Ballot,
        write_quorum: Vec<Uuid>,
        replicas: Vec<Uuid>,
        proposals: ProposalsStore,
        peers: Arc<VerticalPaxosHandle>,
    ) -> Self {
        let pending = proposals
            .into_iter()
            .map(|(slot, cmd)| {
                (
                    slot,
                    PendingProposal {
                        cmd,
                        acceptor_acks: HashSet::new(),
                        replica_acks: HashSet::new(),
                        decided: false,
                    },
                )
            })
            .collect();
        Self {
            id,
            ballot,
            write_quorum: write_quorum.into_iter().collect(),
            replicas: replicas.into_iter().collect(),
            peers,
            pending: Arc::new(Mutex::new(pending)),
        }
    }

    pub async fn add_pending(&self, slot: usize, cmd: PaxosCommand) {
        let should_send = {
            let mut pending = self.pending.lock().await;
            if pending.contains_key(&slot) {
                tracing::warn!(
                    target: "synod::inflight",
                    leader = %self.id,
                    ballot = ?self.ballot,
                    slot,
                    "dropping duplicate commander proposal for occupied pending slot"
                );
                false
            } else {
                pending.insert(
                    slot,
                    PendingProposal {
                        cmd: cmd.clone(),
                        acceptor_acks: HashSet::new(),
                        replica_acks: HashSet::new(),
                        decided: false,
                    },
                );
                true
            }
        };

        if should_send {
            self.peers
                .broadcast_to(
                    VerticalPaxosMessage::P2A {
                        from: self.id,
                        pvalue: PValue::new(slot, self.ballot, cmd),
                    },
                    &self.write_quorum,
                )
                .await;
        }
    }

    pub async fn record_replica_ack(&self, from: Uuid, slot: usize) {
        let mut pending = self.pending.lock().await;
        let replicas = self.replicas.clone();
        if let Some(entry) = pending.get_mut(&slot) {
            entry.replica_acks.insert(from);
            if entry.decided && entry.replica_acks.is_superset(&replicas) {
                pending.remove(&slot);
            }
        } else {
            tracing::warn!(
                target: "synod::inflight",
                leader = %self.id,
                from = %from,
                slot,
                "ignoring replica ACK for unknown commander slot"
            );
        }
    }

    pub async fn handle_message(&self, msg: VerticalPaxosMessage) -> Option<VerticalPaxosMessage> {
        match msg {
            VerticalPaxosMessage::P2B {
                from,
                ballot,
                pvalue,
                ..
            } => self.handle_p2b(from, ballot, pvalue).await,
            _ => None,
        }
    }

    async fn handle_p2b(
        &self,
        from: Uuid,
        ballot: Ballot,
        pvalue: PValue,
    ) -> Option<VerticalPaxosMessage> {
        if ballot > self.ballot {
            tracing::warn!(
                target: "synod::inflight",
                leader = %self.id,
                from = %from,
                slot = pvalue.slot(),
                commander_ballot = ?self.ballot,
                observed_ballot = ?ballot,
                "commander preempted by higher ballot P2B"
            );
            return Some(VerticalPaxosMessage::PREEMPT {
                from: self.id,
                to: self.id,
                ballot,
            });
        }
        if ballot < self.ballot {
            tracing::warn!(
                target: "synod::inflight",
                leader = %self.id,
                from = %from,
                slot = pvalue.slot(),
                commander_ballot = ?self.ballot,
                observed_ballot = ?ballot,
                "ignoring stale lower-ballot P2B"
            );
            return None;
        }
        if !self.write_quorum.contains(&from) {
            tracing::warn!(
                target: "synod::inflight",
                leader = %self.id,
                from = %from,
                slot = pvalue.slot(),
                ballot = ?ballot,
                write_quorum = ?self.write_quorum,
                "ignoring P2B from acceptor outside active commander write quorum"
            );
            return None;
        }

        let decided_cmd = {
            let mut pending = self.pending.lock().await;
            let Some(entry) = pending.get_mut(&pvalue.slot()) else {
                tracing::warn!(
                    target: "synod::inflight",
                    leader = %self.id,
                    from = %from,
                    slot = pvalue.slot(),
                    ballot = ?ballot,
                    "ignoring P2B for unknown commander pending slot"
                );
                return None;
            };
            entry.acceptor_acks.insert(from);
            if entry.decided || !entry.acceptor_acks.is_superset(&self.write_quorum) {
                return None;
            }
            entry.decided = true;
            entry.cmd.clone()
        };

        self.peers
            .broadcast_to(
                VerticalPaxosMessage::ACCEPTED {
                    from: self.id,
                    pvalue: PValue::new(pvalue.slot(), self.ballot, decided_cmd),
                },
                &self.replicas,
            )
            .await;

        None
    }
}
