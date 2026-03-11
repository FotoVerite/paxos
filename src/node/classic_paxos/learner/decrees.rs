use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    common::ballot::Ballot,
    common::types::DecreeId,
    monitor::PaxosObserver,
    node::classic_paxos::{learner::learner_quorum::LearnerQuorum, message::ClassicMessage},
    paxos_command::PaxosCommand,
};
use uuid::Uuid;

pub struct Decrees {
    decrees: Mutex<HashMap<DecreeId, LearnerQuorum>>,
}

impl Decrees {
    pub fn init() -> Self {
        Self {
            decrees: Mutex::new(HashMap::new()),
        }
    }

    pub async fn add_vote(
        &self,
        id: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        ballot: Ballot,
        quorum_size: usize,
        _observer: Arc<dyn PaxosObserver>,
        value: PaxosCommand,
    ) -> Option<ClassicMessage> {
        let mut decrees = self.decrees.lock().await;
        let proposer_id = ballot.node_id;

        let quorum = decrees
            .entry(decree_num)
            .or_insert_with(|| LearnerQuorum::new(quorum_size, ballot));
        quorum.add_vote(from, ballot);

        if quorum.has_met_quorum() {
            tracing::info!(
                "[Node {}] Learner reached quorum for decree {}: {:?} from proposer {} (votes: {:?})",
                id,
                decree_num,
                value,
                proposer_id,
                quorum.quorum_set()
            );
            return Some(ClassicMessage::Success {
                from: id,
                decree_num,
                value: value.clone(),
                ballot_proposer: proposer_id,
            });
        }
        None
    }
}
