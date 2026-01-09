use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::paxos_state::{ballot::Ballot, learner::learner_quorum::LearnerQuorum},
    paxos_command::PaxosCommand,
};

pub struct Decrees {
    decrees: Mutex<HashMap<usize, LearnerQuorum>>,
}

impl Decrees {
    pub fn init() -> Self {
        Self {
            decrees: Mutex::new(HashMap::new()),
        }
    }

    pub async fn add_vote(
        &self,
        id: usize,
        from: usize,
        decree_num: usize,
        ballot: Ballot,
        quorum_size: usize,
        observer: Arc<dyn PaxosObserver>,
        value: PaxosCommand,
    ) -> Message {
        let mut decrees = self.decrees.lock().await;
        let proposer_id = ballot.node_id;

        let quorum = decrees
            .entry(decree_num)
            .or_insert_with(|| LearnerQuorum::new(quorum_size, ballot));
        quorum.add_vote(from, ballot);
        if quorum.has_met_quorum() {
            observer.on_event(Event::LearnedValue {
                decree_num,
                id: id,
                value: value.clone(),
                created_at: crate::monitor::current_timestamp_millis(),
            });
            tracing::info!(
                "[Node {}] Learner reached quorum for decree {}: {:?} from proposer {} (votes: {:?})",
                id,
                decree_num,
                value,
                proposer_id,
                quorum.quorum_set()
            );
            return Message::Success {
                from: id,
                decree_num,
                value: value.clone(),
                ballot_proposer: proposer_id,
            };
        }
        return Message::NACK;
    }
}
