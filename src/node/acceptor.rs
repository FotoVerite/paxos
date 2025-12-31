use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::ballot::Ballot;
use crate::paxos_command::PaxosCommand;
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

const DATA_DIR: &str = ".paxos";

pub struct Acceptor {
    id: usize,
    state: HashMap<usize, AcceptedDecree>,
    observer: Arc<dyn PaxosObserver>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AcceptedDecree {
    min_ballot: Ballot,
    accepted_ballot: Ballot,
    accepted_value: PaxosCommand,
}

impl Default for AcceptedDecree {
    fn default() -> AcceptedDecree {
        AcceptedDecree {
            min_ballot: Ballot {
                number: usize::MIN,
                node_id: 0,
            },
            accepted_ballot: Ballot {
                number: usize::MIN,
                node_id: 0,
            },
            accepted_value: PaxosCommand::BLANK,
        }
    }
}

impl Acceptor {
    pub async fn new(id: usize, observer: Arc<dyn PaxosObserver>) -> Result<Self> {
        let state = Acceptor::load_or_init(id).await?;
        return Ok(Self {
            id,
            state,
            observer,
        });
    }



    async fn load_or_init(node_id: usize) -> Result<HashMap<usize, AcceptedDecree>> {
        let path_str = format!("{}/state_{}.bin", DATA_DIR, node_id);
        let path = Path::new(&path_str);

        if !path.exists() {
            return Ok(HashMap::new());
        }

        let data = tokio::fs::read(&path).await?;
        if data.is_empty() {
            return Ok(HashMap::new());
        }

        Ok(bincode::deserialize(&data)?)
    }

    fn prepare(&mut self, decree_num: usize, ballot: Ballot, from: usize) -> Message {
        let decree = self.state.entry(decree_num).or_default();
        if ballot > decree.min_ballot {
            decree.min_ballot = ballot;

            self.observer.on_event(Event::Promise {
                decree_num,
                id: self.id,
                from,  // Track who initiated the prepare
                ballot: ballot.number,
                created_at: crate::monitor::current_timestamp_millis(),
            });
            decree.min_ballot = ballot;

            return Message::Promise {
                from: self.id,
                decree_num,
                ballot,
                accepted_ballot: decree.accepted_ballot,
                accepted_value: decree.accepted_value.clone(),
            };
        }
        return Message::NACK;
    }

    fn accept(&mut self, decree_num: usize, ballot: Ballot, cmd: PaxosCommand, from: usize) -> Message {
        let decree = self.state.get_mut(&decree_num);
        match decree {
            Some(decree) => {

                if ballot >= decree.min_ballot {
                    decree.min_ballot = ballot;
                    decree.accepted_ballot = ballot;
                    decree.accepted_value = cmd.clone();

                    self.observer.on_event(Event::Accepted {
                        decree_num,
                        id: self.id,
                        from,  // Track who initiated the accept
                        ballot: ballot.number,
                        value: cmd.clone(),
                        created_at: crate::monitor::current_timestamp_millis(),
                    });

                    return Message::Accepted {
                        from: self.id,
                        decree_num,
                        ballot,
                        value: cmd,
                    };
                }
                return Message::NACK;
            }

            None => return Message::NACK,
        }
    }

    pub async fn handle_message(&mut self, msg: Message) -> Message {
        match msg {
            Message::Prepare {
                decree_num, ballot, from
            } => return self.prepare(decree_num, ballot, from),
            Message::Accept {
                decree_num,
                ballot,
                value,
                from, ..
            } => return self.accept(decree_num, ballot, value, from),
            _ => Message::NACK,
        }
    }
}
