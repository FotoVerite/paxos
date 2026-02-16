use std::{
    collections::{HashMap, HashSet, VecDeque, hash_map::Entry},
    sync::Arc,
    time::Duration,
};

use rand::Rng;
use tokio::{
    select,
    sync::Mutex,
    time::{self, Instant},
};
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    common::aimd_timeout::AimdTimeout,
    message::Message,
    monitor::PaxosObserver,
    node::{paxos_state::ballot::Ballot, pmmc::leader::leader_state::ProposalsMap, pvalue::PValue},
    paxos_command::PaxosCommand,
};
pub struct Commander {
    uuid: Uuid,
    quorum: usize,
    ballot: Ballot,
    observer: Arc<dyn PaxosObserver>,
    proposals: Mutex<HashMap<usize, (PaxosCommand, HashSet<Uuid>)>>,
    peers: Arc<NetworkSimulator>,
    send_deadline: Instant,
    send_aimd: AimdTimeout,
}

impl Commander {
    pub fn new(
        uuid: Uuid,
        quorum: usize,
        ballot: Ballot,
        proposals: ProposalsMap,
        peers: Arc<NetworkSimulator>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        let initial = proposals
            .iter()
            .map(|(&slot, cmd)| (slot, (cmd.clone(), HashSet::new())))
            .collect();
        let mut commander = Self {
            uuid,
            observer,
            quorum,
            ballot,
            proposals: Mutex::new(initial),
            peers,
            send_deadline: Instant::now(),
            send_aimd: AimdTimeout::default(),
        };
        commander.run();
        commander
    }

    async fn on_send(&mut self) {
        let mut pvalues = self.proposals.lock().await.clone();
        for (slot, (cmd, quorum)) in pvalues.drain() {
            if quorum.len() >= self.quorum {
                self.peers
                    .broadcast(Message::ACCEPTED {
                        from: self.uuid,
                        pvalue: PValue::new(slot, self.ballot, cmd),
                    })
                    .await;
            } else {
                self.peers
                    .broadcast(Message::P2A {
                        from: self.uuid,
                        pvalue: PValue::new(slot, self.ballot, cmd),
                    })
                    .await;
            }
        }
        self.reset_send();
    }

    async fn p2b(&mut self, acceptor: Uuid, pvalue: PValue, ballot: Ballot) -> Message {
        if self.ballot < ballot {
            return Message::NACK;
        }
        if self.ballot == ballot {
            self.send_aimd.success();
            let pval = {
                let mut guard = self.proposals.lock().await;
                match guard.get_mut(&pvalue.slot()) {
                    Some(val) => {
                        val.1.insert(acceptor);
                        val.clone()
                    }
                    None => panic!("Invariant Error"),
                }
            };
            if pval.1.len() >= self.quorum {
                return Message::ACCEPTED {
                    from: self.uuid,
                    pvalue: PValue::new(pvalue.slot(), ballot, pval.0),
                };
            }
        }
        return Message::PREEMPT {
            from: self.uuid,
            to: self.uuid,
            ballot,
        };
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

    fn reset_send(&mut self) {
        let base = self.send_aimd.interval();
        let jitter = rand::rng().random_range(0..=((base.as_millis() as u64 / 5).max(1)));
        self.send_deadline = Instant::now() + base + Duration::from_millis(jitter);
    }

    pub async fn run(&mut self) {
        self.reset_send();

        loop {
            select! {
                _ = time::sleep_until(self.send_deadline) => {
                    self.on_send();
                }


                else => break,
            }
        }
    }
}
