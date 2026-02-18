use crate::common::persistence::Persistence;
use crate::cluster::network_simulator::NetworkSimulator;
use crate::common::types::DecreeId;
use crate::message::{ClientMessage, Message};
use crate::monitor::{Event, PaxosObserver, current_timestamp_millis};
use crate::node::pmmc::replica::replica_state::ReplicaState;
use crate::node::pmmc::replica::replica_state::durable::ReplicaDurable;
use crate::node::pvalue::PValue;
use crate::rsm::kv_store::KVStore;
use anyhow::Result;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc;
use uuid::Uuid;
mod replica_state;

pub struct Replica {
    uuid: Uuid,
    store: Arc<KVStore>,
    pub state: Arc<ReplicaState>,
    observer: Arc<dyn PaxosObserver>,
    peers: Arc<NetworkSimulator>,
}
impl Replica {
    pub async fn new(
        uuid: Uuid,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<NetworkSimulator>,
    ) -> Result<Self> {
        let data: ReplicaDurable = Persistence::load(&format!("replica_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state = ReplicaDurable::default();
        let replica = Self {
            uuid,
            store: Arc::new(KVStore::init(uuid).await?),
            state: Arc::new(ReplicaState::init(data)),
            observer: Arc::clone(&observer),
            peers,
        };
        replica.spawn_applier().await;
        Ok(replica)
    }

    pub async fn accepted(&self, pvalue: PValue) -> Message {
        self.state.add_decision(pvalue.clone()).await;
        Message::ACK {
            from: self.uuid,
            to: pvalue.ballot().node_id,
            slot: pvalue.slot(),
        }
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::ACCEPTED { pvalue, .. } => self.accepted(pvalue).await,
            _ => Message::NACK,
        }
    }

    pub async fn spawn_client_handler(
        &self,
        client_id: Uuid,
        mut rx: mpsc::Receiver<ClientMessage>,
        tx: Sender<ClientMessage>,
    ) {
        let state = Arc::clone(&self.state);
        let peers = Arc::clone(&self.peers);
        let uuid = self.uuid;

        state.add_client(client_id, tx).await;
        tokio::spawn(async move {
            loop {
                select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            ClientMessage::PROPOSE { cmd } => {
                                // Add to local proposals store (handles dedup/caching)
                                state.proposal_handler(cmd.clone()).await;
                                // Per PMMC §3: broadcast propose(s, c) to ALL leaders.
                                // Passive leaders ignore it; only the active one runs a commander.
                                let slot = state.execution_slot().await;
                                peers.broadcast(Message::PROPOSE {
                                    from: uuid,
                                    slot,
                                    cmd,
                                }).await;
                            },
                            _ => {},
                        }
                    }

                    else => break,
                }
            }
        });
    }

    pub async fn spawn_applier(&self) {
        let state = Arc::clone(&self.state);
        let store = Arc::clone(&self.store);
        let observer = Arc::clone(&self.observer);
        let node_id = self.uuid;
        tokio::spawn(async move {
            loop {
                while let Some(cmd) = state.next_decision().await {
                    let slot = state.execution_slot().await;
                    let response = store.apply(cmd.operation().clone()).await;
                    match response {
                        Ok(response) => {
                            state.increment_execution_slot().await;
                            observer.on_event(Event::LearnedValue {
                                id: node_id,
                                decree_num: DecreeId(slot),
                                value: cmd.clone(),
                                created_at: current_timestamp_millis(),
                            });
                            state.send_client_response(
                                cmd.client_id(),
                                ClientMessage::RESPONSE {
                                    request_id: cmd.request_id(),
                                    response,
                                },
                            ).await;
                        }
                        _ => {}
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};
    use std::time::Duration;

    use tokio::{sync::mpsc, time::timeout};
    use uuid::Uuid;

    use crate::{
        cluster::network_simulator::NetworkSimulator,
        message::{ClientMessage, Message},
        monitor::{NoOpObserver, PaxosObserver},
        node::{classic_paxos::ballot::Ballot, pvalue::PValue},
        paxos_command::PaxosCommand,
        rsm::kv_store::KVStore,
    };

    use super::{Replica, ReplicaDurable, ReplicaState};

    async fn new_replica() -> Replica {
        let uuid = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        Replica {
            uuid,
            store: Arc::new(KVStore::init(uuid).await.expect("store init should work")),
            state: Arc::new(ReplicaState::init(ReplicaDurable::default())),
            observer: Arc::clone(&observer),
            peers: Arc::new(NetworkSimulator::new(uuid, HashMap::new(), observer)),
        }
    }

    fn client_cmd(value: usize, request_id: u64) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value,
        }
        .with_client(Uuid::from_u128(7), request_id)
    }

    #[tokio::test]
    async fn accepted_records_decision_and_replies_with_ack() {
        let replica = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(3, leader);
        let cmd = client_cmd(10, 1);
        let pvalue = PValue::new(0, ballot, cmd.clone());

        let reply = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue,
            })
            .await;

        assert!(matches!(
            reply,
            Message::ACK { from, to, slot } if from == replica.uuid && to == leader && slot == 0
        ));
        assert_eq!(replica.state.next_decision().await, Some(cmd));
    }

    #[tokio::test]
    async fn decisions_are_only_executable_in_slot_order() {
        let replica = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(5, leader);
        let cmd = client_cmd(20, 2);

        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(1, ballot, cmd),
            })
            .await;

        assert_eq!(
            replica.state.next_decision().await,
            None,
            "PMMC replica executes only when decision for slot_out is present"
        );
    }

    #[tokio::test]
    async fn stale_decisions_for_executed_slots_are_ignored() {
        let replica = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(7, leader);
        let first = client_cmd(30, 3);
        let late = client_cmd(99, 4);

        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, first.clone()),
            })
            .await;
        assert_eq!(replica.state.next_decision().await, Some(first));
        replica.state.increment_execution_slot().await;

        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, late),
            })
            .await;

        assert_eq!(
            replica.state.next_decision().await,
            None,
            "decision for an already executed slot should have no effect"
        );
    }

    #[tokio::test]
    async fn decided_slot_removes_conflicting_local_proposal() {
        let replica = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(9, leader);
        let local = client_cmd(40, 5);
        let decided = client_cmd(50, 6);

        replica.state.add_proposal(local).await;
        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, decided.clone()),
            })
            .await;
        assert_eq!(replica.state.next_decision().await, Some(decided));
        replica.state.increment_execution_slot().await;

        let proposals = replica.state.proposal().await;
        assert!(
            !proposals.contains_key(&0),
            "PMMC replica should clear proposal at slot_out when a decision arrives for that slot"
        );
    }

    #[tokio::test]
    async fn conflicting_decision_requeues_local_proposal_to_new_slot() {
        let replica = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(12, leader);
        let local = client_cmd(60, 10);
        let decided = client_cmd(70, 11);

        replica.state.add_proposal(local.clone()).await;
        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, decided),
            })
            .await;

        let proposals = replica.state.proposal().await;
        assert_eq!(proposals.get(&1), Some(&local));
    }

    #[tokio::test]
    async fn gap_fill_executes_in_order_once_missing_slot_arrives() {
        let replica = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(13, leader);
        let cmd0 = client_cmd(80, 12);
        let cmd1 = client_cmd(81, 13);

        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(1, ballot, cmd1.clone()),
            })
            .await;
        assert_eq!(replica.state.next_decision().await, None);

        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, cmd0.clone()),
            })
            .await;
        assert_eq!(replica.state.next_decision().await, Some(cmd0));
        replica.state.increment_execution_slot().await;
        assert_eq!(replica.state.next_decision().await, Some(cmd1));
    }

    #[tokio::test]
    async fn duplicate_client_request_returns_cached_response_after_first_execution() {
        let uuid = Uuid::new_v4();
        let replica = Replica::new(uuid, Arc::new(NoOpObserver))
            .await
            .expect("replica init should work");
        let client_id = Uuid::new_v4();
        let cmd = PaxosCommand::PUT {
            key: "dup".to_string(),
            version: 1,
            value: 1,
        }
        .with_client(client_id, 99);

        let (client_tx, client_rx) = mpsc::channel(8);
        let (resp_tx, mut resp_rx) = mpsc::channel(8);
        replica.spawn_client_handler(client_id, client_rx, resp_tx).await;

        client_tx
            .send(ClientMessage::PROPOSE { cmd: cmd.clone() })
            .await
            .expect("client send should work");

        let leader = Uuid::new_v4();
        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: leader,
                pvalue: PValue::new(0, Ballot::new(1, leader), cmd.clone()),
            })
            .await;

        let first = timeout(Duration::from_millis(400), resp_rx.recv())
            .await
            .expect("first response should arrive")
            .expect("channel should stay open");
        assert!(matches!(first, ClientMessage::RESPONSE { .. }));

        client_tx
            .send(ClientMessage::PROPOSE { cmd: cmd.clone() })
            .await
            .expect("duplicate send should work");

        let second = timeout(Duration::from_millis(400), resp_rx.recv())
            .await
            .expect("duplicate request should return cached response")
            .expect("channel should stay open");
        assert!(matches!(second, ClientMessage::RESPONSE { .. }));
    }

    #[tokio::test]
    async fn unhandled_message_returns_nack() {
        let replica = new_replica().await;
        let reply = replica
            .handle_message(Message::P1A {
                from: Uuid::new_v4(),
                ballot: Ballot::new(1, Uuid::new_v4()),
                start_index: 0,
            })
            .await;
        assert!(matches!(reply, Message::NACK));
    }
}
