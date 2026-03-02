use crate::cluster::network_fabric::NetworkFabric;
use crate::common::persistence::NodePersistence;
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
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
mod replica_state;

pub struct Replica {
    uuid: Uuid,
    store: Arc<KVStore>,
    pub state: Arc<ReplicaState>,
    observer: Arc<dyn PaxosObserver>,
    fabric: Arc<NetworkFabric>,
}
impl Replica {
    pub async fn new(
        uuid: Uuid,
        persistence: NodePersistence,
        observer: Arc<dyn PaxosObserver>,
        fabric: Arc<NetworkFabric>,
    ) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let data: ReplicaDurable = persistence.load("replica.bin").await?;

        #[cfg(not(feature = "persistence"))]
        let data: ReplicaDurable = ReplicaDurable::default();
        let replica = Self {
            uuid,
            store: Arc::new(KVStore::init(uuid, persistence).await?),
            state: Arc::new(ReplicaState::init(data)),
            observer: Arc::clone(&observer),
            fabric,
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
        let fabric = Arc::clone(&self.fabric);
        let uuid = self.uuid;
        let observer = Arc::clone(&self.observer);

        state.add_client(client_id, tx).await;
        tokio::spawn(async move {
            loop {
                select! {
                        Some(msg) = rx.recv() => {
                            match msg {
                                ClientMessage::PROPOSE { cmd } => {
                                    // Add to local proposals store (handles dedup/caching)
                                    let slot = match state.proposal_handler(cmd.clone()).await {
                                        Some(slot) => slot,
                                        None => continue,
                                    };
                                    let cmd_for_event = cmd.clone();
                                    // Per PMMC §3: broadcast propose(s, c) to ALL leaders.
                                    // Passive leaders ignore it; only the active one runs a commander.
                                    fabric.broadcast(uuid, Message::PROPOSE {
                                        from: uuid,
                                        slot,
                                        cmd,
                                    }).await;
                                    observer.on_event(Event::PmmcPropose {
                                        id: uuid,
                                        slot,
                                        cmd: cmd_for_event,
                                        created_at: current_timestamp_millis(),
                                    });
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
                let mut progressed = false;
                while let Some(cmd) = state.next_decision().await {
                    progressed = true;
                    let slot = state.execution_slot().await;
                    let response = store.apply(cmd.operation().clone()).await;
                    match response {
                        Ok(response) => {
                            state.update_cache(&cmd, response.clone()).await;
                            state.increment_execution_slot().await;
                            observer.on_event(Event::LearnedValue {
                                id: node_id,
                                decree_num: DecreeId(slot),
                                value: cmd.clone(),
                                created_at: current_timestamp_millis(),
                            });
                            state
                                .send_client_response(
                                    cmd.client_id(),
                                    ClientMessage::RESPONSE {
                                        request_id: cmd.request_id(),
                                        response,
                                    },
                                )
                                .await;
                        }
                        _ => {}
                    }
                }
                if !progressed {
                    state.wait_for_decision().await;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::sync::Arc;

    use tokio::{sync::mpsc, time::timeout};
    use uuid::Uuid;

    use crate::{
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
        let fabric = Arc::new(crate::cluster::network_fabric::NetworkFabric::new(
            Arc::clone(&observer),
        ));
        Replica {
            uuid,
            store: Arc::new(
                KVStore::init(
                    uuid,
                    crate::common::persistence::ClusterPersistence::for_test("pmmc_replica")
                        .node(uuid),
                )
                .await
                .expect("store init should work"),
            ),
            state: Arc::new(ReplicaState::init(ReplicaDurable::default())),
            observer: Arc::clone(&observer),
            fabric,
        }
    }

    async fn new_replica_with_peer() -> (Replica, Uuid, mpsc::Receiver<Message>) {
        let uuid = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peer = Uuid::new_v4();
        let (peer_tx, peer_rx) = mpsc::channel(16);
        let fabric = Arc::new(crate::cluster::network_fabric::NetworkFabric::new(
            Arc::clone(&observer),
        ));
        fabric.register(peer, peer_tx).await;
        let replica = Replica::new(
            uuid,
            crate::common::persistence::ClusterPersistence::for_test("replica_with_peer")
                .node(uuid),
            observer,
            fabric,
        )
            .await
            .expect("replica init should work");

        (replica, peer, peer_rx)
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
    async fn learned_decision_advances_proposal_slot_for_failover_replicas() {
        let replica = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(14, leader);
        let learned = client_cmd(82, 14);
        let fresh = client_cmd(83, 15);

        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, learned),
            })
            .await;

        let slot = replica.state.add_proposal(fresh).await;
        assert_eq!(
            slot, 1,
            "replica must not repropose slot 0 after learning it"
        );
    }

    #[tokio::test]
    async fn duplicate_client_request_returns_cached_response_after_first_execution() {
        let uuid = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let fabric = Arc::new(crate::cluster::network_fabric::NetworkFabric::new(
            Arc::clone(&observer),
        ));
        let replica = Replica::new(
            uuid,
            crate::common::persistence::ClusterPersistence::for_test("replica_client")
                .node(uuid),
            observer,
            fabric,
        )
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
        replica
            .spawn_client_handler(client_id, client_rx, resp_tx)
            .await;

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
    async fn duplicate_inflight_request_does_not_rebroadcast_propose() {
        let (replica, _leader, mut leader_rx) = new_replica_with_peer().await;
        let client_id = Uuid::new_v4();
        let cmd = PaxosCommand::PUT {
            key: "dup-inflight".to_string(),
            version: 1,
            value: 5,
        }
        .with_client(client_id, 111);

        let (client_tx, client_rx) = mpsc::channel(8);
        let (resp_tx, _resp_rx) = mpsc::channel(8);
        replica
            .spawn_client_handler(client_id, client_rx, resp_tx)
            .await;

        client_tx
            .send(ClientMessage::PROPOSE { cmd: cmd.clone() })
            .await
            .expect("first propose should send");

        let first = timeout(Duration::from_millis(300), leader_rx.recv())
            .await
            .expect("first propose should be broadcast")
            .expect("leader channel should stay open");
        assert!(
            matches!(first, Message::PROPOSE { slot, .. } if slot == 0),
            "first broadcast proposal should reserve slot 0"
        );

        client_tx
            .send(ClientMessage::PROPOSE { cmd })
            .await
            .expect("duplicate propose should send");

        let second = timeout(Duration::from_millis(200), leader_rx.recv()).await;
        assert!(
            second.is_err(),
            "duplicate in-flight request should not rebroadcast PROPOSE"
        );
    }

    #[tokio::test]
    async fn duplicate_cached_request_does_not_rebroadcast_propose() {
        let (replica, leader, mut leader_rx) = new_replica_with_peer().await;
        let client_id = Uuid::new_v4();
        let cmd = PaxosCommand::PUT {
            key: "dup-cached".to_string(),
            version: 1,
            value: 8,
        }
        .with_client(client_id, 222);

        let (client_tx, client_rx) = mpsc::channel(8);
        let (resp_tx, mut resp_rx) = mpsc::channel(8);
        replica
            .spawn_client_handler(client_id, client_rx, resp_tx)
            .await;

        client_tx
            .send(ClientMessage::PROPOSE { cmd: cmd.clone() })
            .await
            .expect("first propose should send");

        let first = timeout(Duration::from_millis(300), leader_rx.recv())
            .await
            .expect("first propose should be broadcast")
            .expect("leader channel should stay open");
        assert!(
            matches!(first, Message::PROPOSE { slot, .. } if slot == 0),
            "first broadcast proposal should reserve slot 0"
        );

        let _ = replica
            .handle_message(Message::ACCEPTED {
                from: leader,
                pvalue: PValue::new(0, Ballot::new(1, leader), cmd.clone()),
            })
            .await;

        let _ = timeout(Duration::from_millis(400), resp_rx.recv())
            .await
            .expect("first response should arrive")
            .expect("response channel should stay open");

        client_tx
            .send(ClientMessage::PROPOSE { cmd })
            .await
            .expect("duplicate propose should send");

        let _cached = timeout(Duration::from_millis(400), resp_rx.recv())
            .await
            .expect("cached response should arrive")
            .expect("response channel should stay open");

        let second = timeout(Duration::from_millis(200), leader_rx.recv()).await;
        assert!(
            second.is_err(),
            "duplicate cached request should not rebroadcast PROPOSE"
        );
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
