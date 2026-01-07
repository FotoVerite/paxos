use std::{collections::HashSet, sync::Arc, time::Duration, usize::MAX};

use anyhow::Ok;
use tokio::sync::{Notify, mpsc::Sender};

use crate::{message::Message, node::ballot::Ballot, paxos_command::PaxosCommand};

struct ProposedDecree {
    proposed_value: PaxosCommand,
    quorum: Quorum,
    promise_notifier: Arc<Notify>,
    cached_message: Message,
    tx: Sender<Message>
}

struct Quorum {
    promises: HashSet<usize>,
    highest_accepted: Ballot,
    quorum_size: usize,
}

impl Default for Quorum {
    fn default() -> Quorum {
        Quorum {
            promises: HashSet::new(),
            highest_accepted: Ballot::default(),
            quorum_size: MAX,
        }
    }
}

impl Quorum {
    pub fn has_met_quorum(&self) -> bool {
        self.promises.len() >= self.quorum_size
    }
}

impl Default for ProposedDecree {
    fn default() -> ProposedDecree {
        ProposedDecree {
            proposed_value: PaxosCommand::BLANK,
            cached_message: Message::NACK,
            promise_notifier: Arc::new(Notify::new()),
            quorum: Quorum::default(),
        }
    }
}

impl ProposedDecree {
    pub fn has_met_quorum(&self) -> bool {
        return self.quorum.has_met_quorum();
    }
}

impl ProposedDecree {
    async fn retry(&self) -> anyhow::Result<()> {
        let mut delay_ms: u64 = 100;

        loop {
            if self.has_met_quorum() {
                return Ok(());
            }
            tokio::select! {
                _ = self.promise_notifier.notified() => {

                }
                _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {
                // retry the current ballot
                    if self.has_met_quorum() {
                        return Ok(());
                    }
                    delay_ms *= 2; // cap backoff at 5s
                    if delay_ms <= 800 {
                        self.tx.send(self.cached_message.clone());
                    }

                }
            }
        }
        
    }
}
