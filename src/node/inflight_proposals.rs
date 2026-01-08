use std::collections::HashMap;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::paxos_command::PaxosCommand;

#[derive(Debug, Clone)]
pub struct InflightProposal {
    pub token: CancellationToken,
    pub decree_num: usize,
    pub cmd: PaxosCommand,
}

pub struct InflightProposals {
    inflight: Mutex<HashMap<usize, InflightProposal>>,
}

impl InflightProposals {

    pub fn new() -> Self {
        Self {inflight: Mutex::new(HashMap::new())}
    }
    pub async fn insert(&self, decree_num: usize, cmd: PaxosCommand) -> InflightProposal {
        let (old_token, entry) = {
            let mut state = self.inflight.lock().await;
            let token = CancellationToken::new();

            match state.get_mut(&decree_num) {
            Some(entry) => {
                let old = entry.token.clone();
                entry.token = token.clone();
                (Some(old), entry.clone())
            }
            None => {
                let entry = InflightProposal {
                        token: token.clone(),
                        decree_num,
                        cmd
                };
                state.insert(
                    decree_num,
                    entry.clone()
                );
                (None, entry)
            }
        }
        };

        if let Some(old) = old_token {
            old.cancel();
        }
        entry
    }

    pub async fn cancel(&self, decree_num: usize) {
        let token = {
            let mut state = self.inflight.lock().await;
            state.remove(&decree_num).map(|entry| entry.token)
        };

        if let Some(token) = token {
            token.cancel();
        }
    }
}
