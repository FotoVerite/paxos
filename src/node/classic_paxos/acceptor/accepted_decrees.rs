use std::{collections::HashMap, sync::Arc};

use uuid::Uuid;

use crate::{
    common::{persistence::Persistence, types::DecreeId},
    message::Message,
    monitor::{Event, PaxosObserver},
    node::classic_paxos::{
        acceptor::{accepted_decree::AcceptedDecree, prev_vote::PrevVote},
        ballot::Ballot,
    },
    paxos_command::PaxosCommand,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AcceptedDecrees {
    decrees: HashMap<DecreeId, AcceptedDecree>,
}

impl AcceptedDecrees {
    pub fn promise(
        &mut self,
        id_uuid: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        ballot: Ballot,
        observer: Arc<dyn PaxosObserver>,
    ) -> Message {
        let entry = self.decrees.entry(decree_num).or_default();
        if !entry.lt(ballot) {
            tracing::info!(
                "[Node {}] Prepare: ballot ({}) <= next_bal ({}) - NACK",
                id_uuid,
                ballot,
                entry.next_bal()
            );
            return Message::NACK;
        }

        tracing::info!(
            "[Node {}] Prepare: ballot ({}) > next_bal ({}) - PROMISE",
            id_uuid,
            ballot,
            entry.next_bal()
        );
        entry.update_bal(ballot);

        observer.on_event(Event::Promise {
            decree_num,
            id: id_uuid,
            from,
            ballot: ballot.number,
            created_at: crate::monitor::current_timestamp_millis(),
        });

        Message::Promise {
            from: id_uuid,
            decree_num,
            ballot,
            accepted_ballot: entry.get_prev_bal(),
            accepted_value: entry.get_prev_val(),
        }
    }

    pub fn accept(
        &mut self,
        id_uuid: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        ballot: Ballot,
        value: PaxosCommand,
        observer: Arc<dyn PaxosObserver>,
    ) -> Message {
        let entry = self.decrees.entry(decree_num).or_default();
        if !entry.eq(ballot) {
            tracing::info!(
                "[Node {}] Accept: ballot ({}) != next_bal ({}) - NACK",
                id_uuid,
                ballot,
                entry.next_bal()
            );
            return Message::NACK;
        }

        entry.prev_vote = PrevVote {
            ballot,
            value: value.clone(),
        };
        tracing::info!(
            "[Node {}] Accept: ballot ({}) == next_bal - ACCEPTED decree {} from node {}: {:?}",
            id_uuid,
            ballot,
            decree_num,
            from,
            value.clone()
        );
        observer.on_event(Event::Accepted {
            decree_num,
            id: id_uuid,
            from,
            ballot: ballot.number,
            value: value.clone(),
            created_at: crate::monitor::current_timestamp_millis(),
        });

        Message::Accepted {
            from: id_uuid,
            decree_num,
            ballot,
            value,
        }
    }

    pub async fn prepopulate(
        uuid: Uuid,
        initial_decrees: Vec<(DecreeId, PaxosCommand)>,
    ) -> anyhow::Result<()> {
        let mut state = HashMap::new();

        let high_ballot = Ballot {
            number: 1,
            node_id: Uuid::nil(),
        };

        for (decree_num, cmd) in initial_decrees {
            state.insert(
                decree_num,
                AcceptedDecree {
                    next_bal: high_ballot.clone(),
                    prev_vote: PrevVote {
                        ballot: high_ballot,
                        value: cmd.clone(),
                    },
                },
            );
        }

        Persistence::save(&format!("acceptor_{}.bin", uuid), &state).await
    }
}

impl Default for AcceptedDecrees {
    fn default() -> AcceptedDecrees {
        AcceptedDecrees {
            decrees: HashMap::new(),
        }
    }
}
