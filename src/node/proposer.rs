use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::ballot::Ballot;
use crate::paxos_command::PaxosCommand;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

const DATA_DIR: &str = ".paxos";

pub struct Proposer {
    id: usize,
    quorum: usize,
    state: HashMap<usize, ProposedDecree>,
    observer: Arc<dyn PaxosObserver>,
    highest_ballot_number: usize,
}

/// Represents the persistent state of a proposed decree.
/// This struct is used for serialization and deserialization to/from disk.
/// It omits transient runtime state like the `votes` map, as that information
/// is not relevant across restarts and should be re-established during a new proposal phase.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct ProposedDecreeState {
    ballot: Ballot,
    highest_seen_ballot: Ballot,
    proposed_value: PaxosCommand,
    chosen: bool,
}

/// Represents the full in-memory runtime state of a proposed decree managed by the Proposer.
/// The `votes` field is transient and not persisted, as it represents promises received
/// during a specific proposal round which become invalid upon a Proposer restart.
struct ProposedDecree {
    ballot: Ballot,
    highest_seen_ballot: Ballot,
    proposed_value: PaxosCommand,
    chosen: bool,
    votes: HashMap<usize, HashSet<usize>>, // Transient state, not persisted
}

impl Proposer {
    pub async fn new(id: usize, quorum: usize, observer: Arc<dyn PaxosObserver>) -> Result<Self> {
        let state = Self::load_or_init(id).await?;
        Ok(Self {
            id,
            quorum,
            state,
            observer,
            highest_ballot_number: 0,
        })
    }

    #[allow(dead_code)]
    fn state_path(&self) -> String {
        format!("{}/proposer_state_{}.bin", DATA_DIR, self.id)
    }

    #[allow(dead_code)]
    async fn ensure_dir_exists() -> Result<()> {
        tokio::fs::create_dir_all(DATA_DIR).await?;
        Ok(())
    }

    /// Saves the current state of the Proposer to disk.
    /// Only the persistent fields (`ProposedDecreeState`) are serialized,
    /// omitting transient runtime data like the `votes` map.
    #[allow(dead_code)]
    async fn save(&self) -> Result<()> {
        Self::ensure_dir_exists().await?;
        let state_to_save: HashMap<usize, ProposedDecreeState> = self
            .state
            .iter()
            .map(|(k, v)| {
                (
                    *k,
                    ProposedDecreeState {
                        ballot: v.ballot,
                        highest_seen_ballot: v.highest_seen_ballot,
                        proposed_value: v.proposed_value.clone(),
                        chosen: v.chosen,
                    },
                )
            })
            .collect();
        let encoded = bincode::serialize(&state_to_save)?;
        tokio::fs::write(self.state_path(), encoded).await?;
        Ok(())
    }

    /// Loads the Proposer's state from disk or initializes it if no state is found.
    /// When loading, it reconstructs the in-memory `ProposedDecree` from the
    /// `ProposedDecreeState`, re-initializing transient fields (like `votes`) to their default
    /// or empty values, as they are not persisted.
    async fn load_or_init(node_id: usize) -> Result<HashMap<usize, ProposedDecree>> {
        let path_str = format!("{}/proposer_state_{}.bin", DATA_DIR, node_id);
        let path = Path::new(&path_str);

        if !path.exists() {
            return Ok(HashMap::new());
        }

        let data = tokio::fs::read(&path).await?;
        if data.is_empty() {
            return Ok(HashMap::new());
        }

        let loaded_state: HashMap<usize, ProposedDecreeState> = bincode::deserialize(&data)?;
        let state = loaded_state
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    ProposedDecree {
                        ballot: v.ballot,
                        highest_seen_ballot: v.highest_seen_ballot,
                        proposed_value: v.proposed_value,
                        chosen: v.chosen,
                        votes: HashMap::new(), // Votes are re-initialized as they are transient
                    },
                )
            })
            .collect();
        Ok(state)
    }

    pub fn promise(
        &mut self,
        decree_num: usize,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
        from_node: usize,
    ) -> Message {
        tracing::debug!(
            "Proposer {} processing promise for decree {} from node {}, current votes: {:?}",
            self.id,
            decree_num,
            from_node,
            self.state.get(&decree_num).map(|d| d
                .votes
                .get(&ballot.number)
                .map(|v| v.len())
                .unwrap_or(0))
        );
        if let Some(proposed_decree) = self.state.get_mut(&decree_num) {
            if ballot != proposed_decree.ballot {
                // Out of sync, we've already sent a Promise
                return Message::NACK;
            }
            let votes = proposed_decree.votes.entry(ballot.number).or_default();
            if proposed_decree.chosen == true {
                if votes.contains(&from_node) {
                    return Message::Accept {
                        from: self.id,
                        decree_num,
                        ballot,
                        value: proposed_decree.proposed_value.clone(),
                        quorum: votes.clone(),
                    };
                } else {
                    return Message::NACK;
                }
            }

            if accepted_ballot > proposed_decree.highest_seen_ballot {
                proposed_decree.highest_seen_ballot = accepted_ballot;
                proposed_decree.proposed_value = accepted_value.clone();
            }

            votes.insert(from_node);

            if votes.len() >= self.quorum {
                proposed_decree.chosen = true;

                tracing::debug!(
                    "Proposer {} reached quorum for decree {} with quorum: {:?}",
                    self.id,
                    decree_num,
                    votes
                );

                self.observer.on_event(Event::Accept {
                    decree_num,
                    id: self.id,
                    ballot: ballot.number,
                    quorum: votes.clone(),
                    value: proposed_decree.proposed_value.clone(),
                    created_at: crate::monitor::current_timestamp_millis(),
                });

                return Message::Accept {
                    from: self.id,
                    decree_num,
                    ballot,
                    value: proposed_decree.proposed_value.clone(),
                    quorum: votes.clone(),
                };
            }
            return Message::NACK;
        }
        // We are in a bad state
        return Message::NACK;
    }

    pub fn propose(&mut self, decree_num: usize, cmd: PaxosCommand) -> Message {
        // Increment the ballot number for every new proposal attempt.
        self.highest_ballot_number += 1;
        let ballot = Ballot {
            number: self.highest_ballot_number,
            node_id: self.id,
        };

        // Create or update the state for this decree.
        let proposed_decree = ProposedDecree {
            ballot,
            chosen: false,
            highest_seen_ballot: Ballot {
                number: 0,
                node_id: 0,
            }, // Initialize to default, not proposer's own ballot
            proposed_value: cmd.clone(),
            votes: HashMap::new(),
        };
        self.state.insert(decree_num, proposed_decree);

        self.observer.on_event(Event::Proposal {
            decree_num,
            id: self.id,
            value: cmd.clone(),
            created_at: crate::monitor::current_timestamp_millis(),
        });

        Message::Prepare {
            from: self.id,
            decree_num,
            ballot: ballot,
        }
    }

    pub async fn handle_message(&mut self, msg: Message) -> Message {
        match msg {
            Message::Promise {
                from,
                decree_num,
                ballot,
                accepted_ballot,
                accepted_value,
            } => self.promise(decree_num, ballot, accepted_ballot, accepted_value, from),
            _ => Message::NACK,
        }
    }
}
