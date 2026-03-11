use std::sync::Arc;

use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    cluster::cluster_configuration::ClusterConfiguration,
    common::ballot::Ballot,
    common::persistence::NodePersistence,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::pmmc::{
        leader::leader_state::{LeaderState, durable::LeaderDurable},
        message::PmmcMessage,
        transport::PmmcHandle,
    },
    paxos_command::PaxosCommand,
};

mod commander;
mod leader_state;
mod scout;

pub struct Leader {
    uuid: Uuid,
    persistence: NodePersistence,
    quorum: usize,
    replicas: Vec<Uuid>,
    peers: Arc<PmmcHandle>,
    state: Arc<LeaderState>,
    observer: Arc<dyn PaxosObserver>,
}

impl Leader {
    pub async fn new(
        uuid: Uuid,
        persistence: NodePersistence,
        configuration: Arc<ClusterConfiguration>,
        peers: Arc<PmmcHandle>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        #[cfg(feature = "persistence")]
        let state: LeaderDurable = persistence.load("leader.bin").await?;

        #[cfg(not(feature = "persistence"))]
        let state: LeaderDurable = LeaderDurable::default();

        let state = LeaderState::init(state, uuid, configuration.epoch());
        let leader = Self {
            uuid,
            persistence,
            peers,
            quorum: configuration.quorum(),
            replicas: configuration.replicas(),
            observer,
            state: Arc::new(state),
        };
        Ok(leader)
    }

    pub async fn start_election(&self) {
        let ballot = self
            .state
            .start_scout(self.quorum, Arc::clone(&self.observer))
            .await;
        self.peers
            .broadcast(PmmcMessage::P1A {
                from: self.uuid,
                ballot,
                start_index: 0,
            })
            .await;
    }

    async fn become_active(&self, ballot: Ballot) {
        let activated = self
            .state
            .set_as_active(
                self.quorum,
                ballot,
                self.replicas.clone(),
                Arc::clone(&self.observer),
                Arc::clone(&self.peers),
            )
            .await;
        if activated {
            self.observer.on_event(Event::LeaderElected {
                id: self.uuid,
                created_at: current_timestamp_millis(),
            });
        }
    }

    pub async fn send_heartbeat(&self) {
        if !self.state.is_active().await {
            return;
        }
        let ballot = self.state.ballot().await;
        self.peers
            .broadcast(PmmcMessage::HEARTBEAT {
                from: self.uuid,
                ballot: ballot.clone(),
            })
            .await;
    }

    pub async fn propose_handler(&self, slot: usize, cmd: PaxosCommand) {
        if !self.state.is_active().await {
            return;
        }
        self.state.add(slot, cmd).await;
    }

    async fn heartbeat_handler(&self, from: Uuid, ballot: Ballot) {
        if from == self.uuid {
            return;
        }
        let was_leader = self.state.is_active().await;
        self.state.heartbeat_handler(ballot).await;
        if was_leader && !self.state.is_active().await {
            self.observer.on_event(Event::LeaderSteppedDown {
                id: self.uuid,
                created_at: current_timestamp_millis(),
            });
        }
    }

    async fn preempt(&self, ballot: Ballot) {
        let was_leader = self.state.is_active().await;
        self.state.preempt(ballot).await;
        if was_leader && !self.state.is_active().await {
            self.observer.on_event(Event::LeaderSteppedDown {
                id: self.uuid,
                created_at: current_timestamp_millis(),
            });
        }
    }

    #[allow(dead_code)]
    async fn save(&self) -> anyhow::Result<()> {
        let state = self.state.dump().await;
        self.persistence.save("leader.bin", &state).await?;
        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        self.state.is_active().await
    }

    pub async fn election_deadline(&self) -> Instant {
        self.state.election_deadline().await
    }

    pub async fn handle_message<M>(&self, msg: M) -> Option<PmmcMessage>
    where
        M: TryInto<PmmcMessage>,
    {
        let msg = msg.try_into().ok()?;
        match msg {
            PmmcMessage::ADOPTED { ballot, .. } => {
                self.become_active(ballot).await;
                None
            }
            PmmcMessage::HEARTBEAT { from, ballot } => {
                self.heartbeat_handler(from, ballot).await;
                None
            }
            PmmcMessage::PROPOSE { from: _, slot, cmd } => {
                self.propose_handler(slot, cmd).await;
                None
            }
            PmmcMessage::ACK { from, slot, .. } => {
                self.state.handle_ack(from, slot).await;
                None
            }
            msg @ PmmcMessage::P1B { .. } => self.state.handle_p1b(msg).await,
            msg @ PmmcMessage::P2B { .. } => self.state.handle_p2b(msg).await,
            PmmcMessage::PREEMPT { ballot, .. } => {
                self.preempt(ballot).await;
                None
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, net::IpAddr, sync::Arc};

    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };
    use uuid::Uuid;

    use crate::{
        cluster::cluster_configuration::ClusterConfiguration,
        common::ballot::Ballot,
        message::Message,
        monitor::{NoOpObserver, PaxosObserver},
        node::{
            config::PmmcNodeConfig,
            pmmc::{message::PmmcMessage, transport::PmmcHandle},
            pvalue::PValue,
        },
        paxos_command::PaxosCommand,
    };

    use super::{Leader, LeaderDurable, LeaderState};

    fn cleanup_leader_files(root: &crate::common::persistence::NodePersistence) {
        let _ = fs::remove_file(root.dir().join("leader.bin"));
    }

    fn config_for_test() -> Arc<ClusterConfiguration> {
        let ip = IpAddr::V4([127, 0, 0, 1].into());
        Arc::new(
            ClusterConfiguration::bootstrap_pmmc(ip, vec![PmmcNodeConfig::default()])
                .expect("test config should bootstrap"),
        )
    }

    async fn mk_leader() -> Leader {
        let uuid = Uuid::nil();
        let persistence =
            crate::common::persistence::ClusterPersistence::for_test("pmmc_leader").node(uuid);
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peers = Arc::new(PmmcHandle::new(uuid, HashMap::new()).await);
        let state = Arc::new(LeaderState::init(LeaderDurable::default(), uuid, 0));

        Leader {
            uuid,
            persistence,
            quorum: 2,
            replicas: vec![],
            peers,
            state,
            observer,
        }
    }

    async fn mk_leader_with_peer() -> (Leader, Uuid, mpsc::Receiver<PmmcMessage>) {
        let uuid = Uuid::new_v4();
        let persistence =
            crate::common::persistence::ClusterPersistence::for_test("pmmc_leader").node(uuid);
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peer = Uuid::new_v4();
        let (tx, rx) = mpsc::channel(8);
        let mut peers_map = HashMap::new();
        peers_map.insert(peer, tx);
        let peers = Arc::new(PmmcHandle::new(uuid, peers_map).await);
        let state = Arc::new(LeaderState::init(LeaderDurable::default(), uuid, 0));

        (
            Leader {
                uuid,
                persistence,
                quorum: 2,
                replicas: vec![],
                peers,
                state,
                observer,
            },
            peer,
            rx,
        )
    }

    fn cmd(value: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value,
        }
    }

    #[tokio::test]
    async fn adopted_for_current_ballot_activates_leader() {
        let leader = mk_leader().await;
        let ballot = leader.state.ballot().await;
        assert!(!leader.is_leader().await);

        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;

        assert!(leader.is_leader().await);
    }

    #[tokio::test]
    async fn adopted_with_mismatched_ballot_does_not_activate_leader() {
        let leader = mk_leader().await;
        assert!(!leader.is_leader().await);

        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot: Ballot::new(1, Uuid::new_v4()),
                pvalues: vec![],
            })
            .await;

        assert!(
            !leader.is_leader().await,
            "leader should only activate for ADOPTED at its current ballot"
        );
    }

    #[tokio::test]
    async fn heartbeat_message_updates_leader_timers_when_ballot_is_higher() {
        let leader = mk_leader().await;
        let before = leader.election_deadline().await;

        let _ = leader
            .handle_message(Message::HEARTBEAT {
                from: Uuid::new_v4(),
                ballot: Ballot::new(1, Uuid::new_v4()),
            })
            .await;

        let after = leader.election_deadline().await;
        assert!(
            after > before,
            "leader should process higher-ballot heartbeat and reset election deadline"
        );
    }

    #[tokio::test]
    async fn persistence_round_trip_preserves_active_state() {
        let uuid = Uuid::nil();
        let persistence =
            crate::common::persistence::ClusterPersistence::for_test("leader_reload").node(uuid);
        cleanup_leader_files(&persistence);

        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peers = Arc::new(PmmcHandle::new(uuid, HashMap::new()).await);
        let leader = Leader::new(
            uuid,
            persistence.clone(),
            config_for_test(),
            peers,
            Arc::clone(&observer),
        )
        .await
        .expect("leader init should work");
        let ballot = leader.state.ballot().await;

        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: uuid,
                ballot,
                pvalues: vec![],
            })
            .await;
        assert!(leader.is_leader().await);
        leader.save().await.expect("save should work");
        drop(leader);

        let peers2 = Arc::new(PmmcHandle::new(uuid, HashMap::new()).await);
        let reloaded = Leader::new(
            uuid,
            persistence.clone(),
            config_for_test(),
            peers2,
            observer,
        )
        .await
        .expect("leader reload should work");

        assert!(
            reloaded.is_leader().await,
            "active status should survive leader restart"
        );
        cleanup_leader_files(&persistence);
    }

    #[tokio::test]
    async fn unhandled_message_returns_none() {
        let leader = mk_leader().await;
        let reply = leader
            .handle_message(Message::PROPOSE {
                from: Uuid::new_v4(),
                slot: 0,
                cmd: crate::paxos_command::PaxosCommand::NOOP,
            })
            .await;
        assert!(reply.is_none());
    }

    #[tokio::test]
    async fn preempt_with_higher_ballot_deactivates_leader() {
        let leader = mk_leader().await;
        let ballot = leader.state.ballot().await;
        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;
        assert!(leader.is_leader().await);

        let _ = leader
            .handle_message(Message::PREEMPT {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot: Ballot::new(ballot.number + 1, Uuid::new_v4()),
            })
            .await;

        assert!(!leader.is_leader().await);
    }

    #[tokio::test]
    async fn delayed_adopted_after_preempt_does_not_reactivate_leader() {
        let leader = mk_leader().await;
        let ballot = leader.state.ballot().await;
        let higher = Ballot::new(ballot.number + 1, Uuid::new_v4());

        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;
        assert!(leader.is_leader().await);

        let _ = leader
            .handle_message(Message::PREEMPT {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot: higher,
            })
            .await;
        assert!(!leader.is_leader().await);

        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;
        assert!(!leader.is_leader().await);
    }

    #[tokio::test]
    async fn commander_respects_leader_quorum_for_p2b() {
        let leader = mk_leader().await;
        let ballot = leader.state.ballot().await;
        let slot = 4usize;
        let command = cmd(42);
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();

        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;

        let _ = leader
            .handle_message(Message::PROPOSE {
                from: Uuid::new_v4(),
                slot,
                cmd: command.clone(),
            })
            .await;

        let first = leader
            .handle_message(Message::P2B {
                from: a1,
                to: leader.uuid,
                ballot,
                pvalue: PValue::new(slot, ballot, command.clone()),
            })
            .await;
        assert!(first.is_none());

        let second = leader
            .handle_message(Message::P2B {
                from: a2,
                to: leader.uuid,
                ballot,
                pvalue: PValue::new(slot, ballot, command),
            })
            .await;
        assert!(matches!(second, Some(PmmcMessage::ACCEPTED { .. })));
    }

    #[tokio::test]
    async fn start_election_broadcasts_p1a() {
        let (leader, _peer, mut rx) = mk_leader_with_peer().await;

        leader.start_election().await;

        let msg = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("expected p1a broadcast")
            .expect("receiver should get message");

        let expected_ballot = leader.state.ballot().await;
        assert!(matches!(
            msg,
            PmmcMessage::P1A {
                from,
                ballot,
                start_index
            } if from == leader.uuid && ballot == expected_ballot && start_index == 0
        ));
    }

    #[tokio::test]
    async fn heartbeat_is_only_sent_when_active() {
        let (leader, _peer, mut rx) = mk_leader_with_peer().await;

        leader.send_heartbeat().await;
        let no_msg = timeout(Duration::from_millis(20), rx.recv()).await;
        assert!(no_msg.is_err(), "passive leader should not send heartbeat");

        let ballot = leader.state.ballot().await;
        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;
        leader.send_heartbeat().await;

        let msg = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("active leader should send heartbeat")
            .expect("receiver should get heartbeat");
        assert!(matches!(
            msg,
            PmmcMessage::HEARTBEAT { from, ballot: hb } if from == leader.uuid && hb == ballot
        ));
    }

    #[tokio::test]
    async fn p1b_quorum_emits_adopted_but_does_not_activate_until_adopted_message() {
        let (leader, _peer, mut rx) = mk_leader_with_peer().await;
        leader.start_election().await;
        let _ = timeout(Duration::from_millis(100), rx.recv()).await;
        let ballot = leader.state.ballot().await;

        let _ = leader
            .handle_message(Message::P1B {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;
        let second = leader
            .handle_message(Message::P1B {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;

        assert!(
            matches!(second, Some(PmmcMessage::ADOPTED { .. })),
            "scout should emit ADOPTED at quorum"
        );
        assert!(
            !leader.is_leader().await,
            "leader must not activate until ADOPTED is delivered to leader handler"
        );

        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;
        assert!(leader.is_leader().await);
    }

    #[tokio::test]
    async fn higher_ballot_p2b_emits_preempt_but_does_not_deactivate_until_preempt_message() {
        let (leader, _peer, _rx) = mk_leader_with_peer().await;
        let ballot = leader.state.ballot().await;
        let cmd = cmd(9);

        let _ = leader
            .handle_message(Message::ADOPTED {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot,
                pvalues: vec![],
            })
            .await;
        assert!(leader.is_leader().await);

        let _ = leader
            .handle_message(Message::PROPOSE {
                from: Uuid::new_v4(),
                slot: 1,
                cmd: cmd.clone(),
            })
            .await;

        let higher = Ballot::new(ballot.number + 1, Uuid::new_v4());
        let reply = leader
            .handle_message(Message::P2B {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot: higher,
                pvalue: PValue::new(1, ballot, cmd),
            })
            .await;
        assert!(
            matches!(reply, Some(PmmcMessage::PREEMPT { ballot, .. }) if ballot == higher),
            "commander should surface preempt on higher-ballot p2b"
        );
        assert!(
            leader.is_leader().await,
            "leader must not deactivate until PREEMPT is delivered to leader handler"
        );

        let _ = leader
            .handle_message(Message::PREEMPT {
                from: Uuid::new_v4(),
                to: leader.uuid,
                ballot: higher,
            })
            .await;
        assert!(!leader.is_leader().await);
    }
}
