use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use crate::cluster::cluster_configuration::ClusterConfiguration;
use crate::common::persistence::NodePersistence;
use crate::common::types::DecreeId;
use crate::message::ClientMessage;
use crate::monitor::{Event, PaxosObserver, current_timestamp_millis};
use crate::node::pmmc::message::PmmcMessage;
use crate::node::pmmc::replica::replica_state::ReplicaState;
use crate::node::pmmc::replica::replica_state::durable::ReplicaDurable;
use crate::node::pmmc::transport::PmmcFabric;
use crate::node::pvalue::PValue;
use crate::paxos_command::{ClientId, PaxosCommand};
use crate::rsm::checkpoint::{CheckpointManifest, CheckpointState, RsmCheckpoint};
use crate::rsm::kv_store::KVStore;

mod replica_state;
pub use replica_state::ReplicaAdmissionOutcome;

pub struct Replica {
    uuid: Uuid,
    store: Arc<KVStore>,
    pub state: Arc<ReplicaState>,
    configuration: Arc<ClusterConfiguration>,
    observer: Arc<dyn PaxosObserver>,
    fabric: Arc<PmmcFabric>,
    client_sink: Arc<dyn ClientReplySink>,
}

#[async_trait]
pub trait ClientReplySink: Send + Sync {
    async fn send(&self, client_id: ClientId, message: ClientMessage);
}

impl Replica {
    pub async fn new(
        uuid: Uuid,
        persistence: NodePersistence,
        observer: Arc<dyn PaxosObserver>,
        fabric: Arc<PmmcFabric>,
        configuration: Arc<ClusterConfiguration>,
        client_sink: Arc<dyn ClientReplySink>,
    ) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let data: ReplicaDurable = persistence.load("replica.bin").await?;

        #[cfg(not(feature = "persistence"))]
        let data: ReplicaDurable = ReplicaDurable::default();

        let kv_snapshot = configuration.kv_store();
        let state = Arc::new(ReplicaState::init(data, Arc::clone(&configuration)));
        let replica = Self {
            uuid,
            store: Arc::new(KVStore::init(uuid, persistence, kv_snapshot).await?),
            client_sink,
            state,
            configuration,
            observer: Arc::clone(&observer),
            fabric,
        };
        replica.spawn_applier().await;
        Ok(replica)
    }

    pub async fn is_stopped(&self) -> bool {
        self.state.is_stopped().await
    }

    pub(crate) async fn emit_checkpoint(&self) -> RsmCheckpoint {
        let next_execution_slot = self.state.execution_slot().await;
        let last_applied_slot = next_execution_slot.checked_sub(1);
        let rsm_state = self.store.emit_checkpoint_state().await;
        let client_dedup_watermark = self.state.client_dedup_watermark().await;
        let manifest = CheckpointManifest::new(
            Uuid::new_v4(),
            self.uuid,
            self.configuration.id(),
            self.configuration.epoch() as u64,
            last_applied_slot,
            current_timestamp_millis(),
        );
        let state = CheckpointState::new(rsm_state, client_dedup_watermark);
        RsmCheckpoint::new(manifest, state)
    }

    pub async fn propose_from_client(&self, cmd: PaxosCommand) {
        match self.state.proposal_handler(cmd).await {
            ReplicaAdmissionOutcome::Admitted { slot, cmd } => {
                Self::send_proposal(
                    self.uuid,
                    Arc::clone(&self.fabric),
                    slot,
                    cmd,
                    Arc::clone(&self.observer),
                )
                .await;
            }
            ReplicaAdmissionOutcome::Queued => {}
            ReplicaAdmissionOutcome::Stopped => {}
            ReplicaAdmissionOutcome::CachedResponse {
                client_id,
                request_id,
                response,
            } => {
                if let Some(outcome) = response {
                    self.client_sink
                        .send(
                            client_id,
                            ClientMessage::RESPONSE {
                                request_id,
                                response: outcome,
                            },
                        )
                        .await;
                }
            }
        }
    }

    pub async fn accepted(&self, pvalue: PValue) -> PmmcMessage {
        if let Some(outcome) = self.state.add_decision(pvalue.clone()).await {
            if let ReplicaAdmissionOutcome::Admitted { slot, cmd } = outcome {
                Self::send_proposal(
                    self.uuid,
                    Arc::clone(&self.fabric),
                    slot,
                    cmd,
                    Arc::clone(&self.observer),
                )
                .await;
            }
        }
        PmmcMessage::ACK {
            from: self.uuid,
            to: pvalue.ballot().node_id,
            slot: pvalue.slot(),
        }
    }

    pub async fn max_inflight_exceeded(&self) -> bool {
        self.state.max_inflight_exceeded().await
    }

    pub async fn handle_message(&self, msg: PmmcMessage) -> Option<PmmcMessage> {
        match msg {
            PmmcMessage::ACCEPTED { pvalue, .. } => Some(self.accepted(pvalue).await),
            _ => None,
        }
    }

    async fn send_proposal(
        uuid: Uuid,
        fabric: Arc<PmmcFabric>,
        slot: usize,
        cmd: PaxosCommand,
        observer: Arc<dyn PaxosObserver>,
    ) {
        fabric
            .broadcast(
                uuid,
                PmmcMessage::PROPOSE {
                    from: uuid,
                    slot,
                    cmd: cmd.clone(),
                },
            )
            .await;
        observer.on_event(Event::PmmcPropose {
            id: uuid,
            slot,
            cmd,
            created_at: current_timestamp_millis(),
        })
    }

    pub async fn spawn_applier(&self) {
        let state = Arc::clone(&self.state);
        let store = Arc::clone(&self.store);
        let observer = Arc::clone(&self.observer);
        let client_sink = Arc::clone(&self.client_sink);
        let node_id = self.uuid;
        tokio::spawn(async move {
            loop {
                let mut progressed = false;
                while let Some(cmd) = state.next_decision().await {
                    progressed = true;
                    let slot = state.execution_slot().await;
                    let response = store.apply(cmd.operation().clone()).await;
                    if let Ok(response) = response {
                        state.update_cache(&cmd, response.clone()).await;
                        state.increment_execution_slot().await;
                        observer.on_event(Event::LearnedValue {
                            id: node_id,
                            decree_num: DecreeId(slot),
                            value: cmd.clone(),
                            created_at: current_timestamp_millis(),
                        });
                        client_sink
                            .send(
                                cmd.client_id(),
                                ClientMessage::RESPONSE {
                                    request_id: cmd.request_id(),
                                    response,
                                },
                            )
                            .await;
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
    use std::collections::HashMap;
    use std::time::Duration;
    use std::{net::IpAddr, sync::Arc};

    use async_trait::async_trait;
    use tokio::sync::{Mutex, mpsc};
    use tokio::time::timeout;
    use uuid::Uuid;

    use crate::{
        cluster::cluster_configuration::ClusterConfiguration,
        common::ballot::Ballot,
        message::ClientMessage,
        monitor::{NoOpObserver, PaxosObserver},
        node::{
            config::PmmcNodeConfig, pmmc::message::PmmcMessage, pmmc::transport::new_pmmc_fabric,
            pvalue::PValue,
        },
        paxos_command::PaxosCommand,
        rsm::kv_store::KVStore,
    };

    use super::{ClientReplySink, Replica, ReplicaAdmissionOutcome, ReplicaDurable, ReplicaState};

    struct TestClientSink {
        clients: Mutex<HashMap<Uuid, mpsc::Sender<ClientMessage>>>,
    }

    impl TestClientSink {
        fn new() -> Self {
            Self {
                clients: Mutex::new(HashMap::new()),
            }
        }

        async fn register(&self, client_id: Uuid, tx: mpsc::Sender<ClientMessage>) {
            self.clients.lock().await.insert(client_id, tx);
        }
    }

    #[async_trait]
    impl ClientReplySink for TestClientSink {
        async fn send(&self, client_id: Uuid, message: ClientMessage) {
            let tx = self.clients.lock().await.get(&client_id).cloned();
            if let Some(tx) = tx {
                let _ = tx.send(message).await;
            }
        }
    }

    fn config_for_test() -> Arc<ClusterConfiguration> {
        let ip = IpAddr::V4([127, 0, 0, 1].into());
        Arc::new(
            ClusterConfiguration::bootstrap_pmmc(ip, vec![PmmcNodeConfig::default()])
                .expect("test config should bootstrap"),
        )
    }

    async fn new_replica() -> (Replica, Arc<TestClientSink>) {
        let uuid = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let configuration = config_for_test();
        let fabric = Arc::new(new_pmmc_fabric(Arc::clone(&observer)));
        let state = Arc::new(ReplicaState::init(
            ReplicaDurable::default(),
            Arc::clone(&configuration),
        ));
        let sink = Arc::new(TestClientSink::new());
        let sink_trait: Arc<dyn ClientReplySink> = sink.clone();

        (
            Replica {
                uuid,
                store: Arc::new(
                    KVStore::init(
                        uuid,
                        crate::common::persistence::ClusterPersistence::for_test("pmmc_replica")
                            .node(uuid),
                        None,
                    )
                    .await
                    .expect("store init should work"),
                ),
                state: Arc::clone(&state),
                configuration,
                observer: Arc::clone(&observer),
                fabric,
                client_sink: sink_trait,
            },
            sink,
        )
    }

    async fn new_replica_with_peer() -> (
        Replica,
        Arc<TestClientSink>,
        Uuid,
        mpsc::Receiver<PmmcMessage>,
    ) {
        let uuid = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peer = Uuid::new_v4();
        let (peer_tx, peer_rx) = mpsc::channel(16);
        let fabric = Arc::new(new_pmmc_fabric(Arc::clone(&observer)));
        fabric.register(peer, peer_tx).await;
        let sink = Arc::new(TestClientSink::new());
        let sink_trait: Arc<dyn ClientReplySink> = sink.clone();
        let replica = Replica::new(
            uuid,
            crate::common::persistence::ClusterPersistence::for_test("replica_with_peer")
                .node(uuid),
            observer,
            fabric,
            config_for_test(),
            sink_trait,
        )
        .await
        .expect("replica init should work");

        (replica, sink, peer, peer_rx)
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
        let (replica, _sink) = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(3, leader);
        let cmd = client_cmd(10, 1);
        let pvalue = PValue::new(0, ballot, cmd.clone());

        let reply = replica
            .handle_message(PmmcMessage::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue,
            })
            .await;

        assert!(matches!(
            reply,
            Some(PmmcMessage::ACK { from, to, slot }) if from == replica.uuid && to == leader && slot == 0
        ));
        assert_eq!(replica.state.next_decision().await, Some(cmd));
    }

    #[tokio::test]
    async fn decisions_are_only_executable_in_slot_order() {
        let (replica, _sink) = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(5, leader);
        let cmd = client_cmd(20, 2);

        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
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
        let (replica, _sink) = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(7, leader);
        let first = client_cmd(30, 3);
        let late = client_cmd(99, 4);

        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, first.clone()),
            })
            .await;
        assert_eq!(replica.state.next_decision().await, Some(first));
        replica.state.increment_execution_slot().await;

        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
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
        let (replica, _sink) = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(9, leader);
        let local = client_cmd(40, 5);
        let decided = client_cmd(50, 6);

        let outcome = replica.state.proposal_handler(local).await;
        assert!(matches!(
            outcome,
            ReplicaAdmissionOutcome::Admitted { slot: 0, .. }
        ));
        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
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
        let (replica, _sink) = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(12, leader);
        let local = client_cmd(60, 10);
        let decided = client_cmd(70, 11);

        let outcome = replica.state.proposal_handler(local.clone()).await;
        assert!(matches!(
            outcome,
            ReplicaAdmissionOutcome::Admitted { slot: 0, .. }
        ));
        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, decided),
            })
            .await;

        let proposals = replica.state.proposal().await;
        assert_eq!(proposals.get(&1), Some(&local));
    }

    #[tokio::test]
    async fn gap_fill_executes_in_order_once_missing_slot_arrives() {
        let (replica, _sink) = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(13, leader);
        let cmd0 = client_cmd(80, 12);
        let cmd1 = client_cmd(81, 13);

        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(1, ballot, cmd1.clone()),
            })
            .await;
        assert_eq!(replica.state.next_decision().await, None);

        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
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
        let (replica, _sink) = new_replica().await;
        let leader = Uuid::new_v4();
        let ballot = Ballot::new(14, leader);
        let learned = client_cmd(82, 14);
        let fresh = client_cmd(83, 15);

        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
                from: Uuid::new_v4(),
                pvalue: PValue::new(0, ballot, learned),
            })
            .await;

        let slot = match replica.state.proposal_handler(fresh).await {
            ReplicaAdmissionOutcome::Admitted { slot, .. } => slot,
            _ => panic!("fresh proposal should be admitted"),
        };
        assert_eq!(
            slot, 1,
            "replica must not repropose slot 0 after learning it"
        );
    }

    #[tokio::test]
    async fn duplicate_client_request_returns_cached_response_after_first_execution() {
        let uuid = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let fabric = Arc::new(new_pmmc_fabric(Arc::clone(&observer)));
        let sink = Arc::new(TestClientSink::new());
        let sink_trait: Arc<dyn ClientReplySink> = sink.clone();
        let replica = Replica::new(
            uuid,
            crate::common::persistence::ClusterPersistence::for_test("replica_client").node(uuid),
            observer,
            fabric,
            config_for_test(),
            sink_trait,
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

        let (resp_tx, mut resp_rx) = mpsc::channel(8);
        sink.register(client_id, resp_tx).await;

        replica.propose_from_client(cmd.clone()).await;

        let leader = Uuid::new_v4();
        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
                from: leader,
                pvalue: PValue::new(0, Ballot::new(1, leader), cmd.clone()),
            })
            .await;

        let first = timeout(Duration::from_millis(400), resp_rx.recv())
            .await
            .expect("first response should arrive")
            .expect("channel should stay open");
        assert!(matches!(first, ClientMessage::RESPONSE { .. }));

        replica.propose_from_client(cmd.clone()).await;

        let second = timeout(Duration::from_millis(400), resp_rx.recv())
            .await
            .expect("duplicate request should return cached response")
            .expect("channel should stay open");
        assert!(matches!(second, ClientMessage::RESPONSE { .. }));
    }

    #[tokio::test]
    async fn duplicate_inflight_request_does_not_rebroadcast_propose() {
        let (replica, _sink, _leader, mut leader_rx) = new_replica_with_peer().await;
        let client_id = Uuid::new_v4();
        let cmd = PaxosCommand::PUT {
            key: "dup-inflight".to_string(),
            version: 1,
            value: 5,
        }
        .with_client(client_id, 111);

        replica.propose_from_client(cmd.clone()).await;

        let first = timeout(Duration::from_millis(300), leader_rx.recv())
            .await
            .expect("first propose should be broadcast")
            .expect("leader channel should stay open");
        assert!(
            matches!(first, PmmcMessage::PROPOSE { slot, .. } if slot == 0),
            "first broadcast proposal should reserve slot 0"
        );

        replica.propose_from_client(cmd).await;

        let second = timeout(Duration::from_millis(200), leader_rx.recv()).await;
        assert!(
            second.is_err(),
            "duplicate in-flight request should not rebroadcast PROPOSE"
        );
    }

    #[tokio::test]
    async fn duplicate_cached_request_does_not_rebroadcast_propose() {
        let (replica, sink, leader, mut leader_rx) = new_replica_with_peer().await;
        let client_id = Uuid::new_v4();
        let cmd = PaxosCommand::PUT {
            key: "dup-cached".to_string(),
            version: 1,
            value: 8,
        }
        .with_client(client_id, 222);

        let (resp_tx, mut resp_rx) = mpsc::channel(8);
        sink.register(client_id, resp_tx).await;

        replica.propose_from_client(cmd.clone()).await;

        let first = timeout(Duration::from_millis(300), leader_rx.recv())
            .await
            .expect("first propose should be broadcast")
            .expect("leader channel should stay open");
        assert!(
            matches!(first, PmmcMessage::PROPOSE { slot, .. } if slot == 0),
            "first broadcast proposal should reserve slot 0"
        );

        let _ = replica
            .handle_message(PmmcMessage::ACCEPTED {
                from: leader,
                pvalue: PValue::new(0, Ballot::new(1, leader), cmd.clone()),
            })
            .await;

        let _ = timeout(Duration::from_millis(400), resp_rx.recv())
            .await
            .expect("first response should arrive")
            .expect("response channel should stay open");

        replica.propose_from_client(cmd).await;

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
    async fn unhandled_message_returns_none() {
        let (replica, _sink) = new_replica().await;
        let reply = replica
            .handle_message(PmmcMessage::P1A {
                from: Uuid::new_v4(),
                ballot: Ballot::new(1, Uuid::new_v4()),
                start_index: 0,
            })
            .await;
        assert!(reply.is_none());
    }
}
