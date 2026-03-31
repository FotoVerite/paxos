use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::Arc,
    time::Duration,
};

use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::sleep,
};
use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    node::{pvalue::PValue, vertical_paxos::message::VerticalPaxosMessage},
};

const RETRY_INTERVAL: Duration = Duration::from_millis(50);

struct ScoutRoundState {
    ballot: Ballot,
    targets: HashSet<Uuid>,
    responders: HashSet<Uuid>,
    settled_values: HashMap<usize, PValue>,
    superseded_by: Option<Ballot>,
    complete: bool,
}

pub struct ScoutRound {
    state: Arc<Mutex<ScoutRoundState>>,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ScoutRound {
    pub fn new(
        leader_id: Uuid,
        peers: Arc<crate::node::vertical_paxos::transport::VerticalPaxosHandle>,
        ballot: Ballot,
        start_index: usize,
        targets: HashSet<Uuid>,
    ) -> Self {
        let state = Arc::new(Mutex::new(ScoutRoundState {
            ballot,
            targets: targets.clone(),
            responders: HashSet::new(),
            settled_values: HashMap::new(),
            superseded_by: None,
            complete: false,
        }));
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let p1a = VerticalPaxosMessage::P1A {
                from: leader_id,
                ballot,
                start_index,
            };

            loop {
                {
                    let state = task_state.lock().await;
                    if state.complete || state.superseded_by.is_some() || *stop_rx.borrow() {
                        break;
                    }
                }

                peers.broadcast_to(p1a.clone(), &targets).await;

                tokio::select! {
                    _ = sleep(RETRY_INTERVAL) => {}
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        Self {
            state,
            stop_tx,
            task,
        }
    }

    pub async fn handle_message(&self, msg: &VerticalPaxosMessage) {
        let VerticalPaxosMessage::P1B {
            from,
            ballot,
            pvalues,
            ..
        } = msg
        else {
            return;
        };

        let mut state = self.state.lock().await;
        if state.complete || state.superseded_by.is_some() {
            return;
        }

        if *ballot > state.ballot {
            state.superseded_by = Some(*ballot);
            return;
        }

        if *ballot != state.ballot || !state.targets.contains(from) {
            return;
        }

        state.responders.insert(*from);
        merge_pvalues(&mut state.settled_values, pvalues);
        if state.responders.is_superset(&state.targets) {
            state.complete = true;
        }
    }

    pub async fn is_complete(&self) -> bool {
        self.state.lock().await.complete
    }

    pub async fn settled_values(&self) -> HashMap<usize, PValue> {
        self.state.lock().await.settled_values.clone()
    }

    pub async fn superseded_by(&self) -> Option<Ballot> {
        self.state.lock().await.superseded_by
    }

    pub async fn shutdown(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.task.await;
    }
}

fn merge_pvalues(settled_values: &mut HashMap<usize, PValue>, pvalues: &[PValue]) {
    for pvalue in pvalues.iter().cloned() {
        match settled_values.entry(pvalue.slot()) {
            Entry::Vacant(entry) => {
                entry.insert(pvalue);
            }
            Entry::Occupied(mut entry) => {
                if entry.get().ballot() < pvalue.ballot() {
                    entry.insert(pvalue);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
