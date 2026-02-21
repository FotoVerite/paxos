use std::sync::Arc;

use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    common::persistence::Persistence,
    message::Message,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        classic_paxos::ballot::Ballot,
        pmmc::leader::leader_state::{durable::LeaderDurable, LeaderState},
    },
    paxos_command::PaxosCommand,
};

mod commander;
mod leader_state;
mod scout;

pub struct Leader {
    uuid: Uuid,
    quorum: usize,
    replicas: Vec<Uuid>,
    peers: Arc<NetworkSimulator>,
    state: Arc<LeaderState>,
    observer: Arc<dyn PaxosObserver>,
}

impl Leader {
    pub async fn new(
        uuid: Uuid,
        quorum: usize,
        replicas: Vec<Uuid>,
        peers: Arc<NetworkSimulator>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        #[cfg(feature = "persistence")]
        let state: LeaderDurable = Persistence::load(&format!("leader_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state: LeaderDurable = LeaderDurable::default();

        let state = LeaderState::init(uuid, state);
        let leader = Self {
            uuid,
            peers,
            quorum,
            replicas,
            observer,
            state: Arc::new(state),
        };
        Ok(leader)
    }

    pub async fn start_election(&self) {
        let ballot = self
            .state
            .start_scout(self.uuid, self.quorum, Arc::clone(&self.observer))
            .await;
        self.peers
            .broadcast(Message::P1A {
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
                self.uuid,
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
            .broadcast(Message::HEARTBEAT {
                from: self.uuid,
                ballot: ballot.clone(),
            })
            .await;
    }

    pub async fn propose_handler(&self, slot: usize, cmd: PaxosCommand) -> Message {
        if !self.state.is_active().await {
            return Message::NACK;
        }
        self.state.add(slot, cmd).await;
        return Message::NACK;
    }

    async fn heartbeat_handler(&self, from: Uuid, ballot: Ballot) -> Message {
        if from == self.uuid {
            return Message::NACK;
        }
        let was_leader = self.state.is_active().await;
        self.state.heartbeat_handler(ballot).await;
        if was_leader && !self.state.is_active().await {
            self.observer.on_event(Event::LeaderSteppedDown {
                id: self.uuid,
                created_at: current_timestamp_millis(),
            });
        }
        return Message::NACK;
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
        Persistence::save(&format!("leader_{}.bin", self.uuid), &state).await?;
        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        self.state.is_active().await
    }

    pub async fn election_deadline(&self) -> Instant {
        self.state.election_deadline().await
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::ADOPTED { ballot, .. } => {
                self.become_active(ballot).await;
                return Message::NACK;
            }
            Message::HEARTBEAT { from, ballot } => {
                self.heartbeat_handler(from, ballot).await;
                return Message::NACK;
            }
            Message::PROPOSE { from: _, slot, cmd } => self.propose_handler(slot, cmd).await,
            Message::ACK { from, slot, .. } => {
                self.state.handle_ack(from, slot).await;
                Message::NACK
            }
            Message::P1B { .. } => self.state.handle_p1b(msg.clone()).await,

            Message::P2B { .. } => self.state.handle_p2b(msg.clone()).await,
            Message::PREEMPT { ballot, .. } => {
                self.preempt(ballot).await;
                return Message::NACK;
            }
            _ => Message::NACK,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, sync::Arc};

    use tokio::{sync::mpsc, time::{timeout, Duration}};
    use uuid::Uuid;

    use crate::{
        cluster::network_simulator::NetworkSimulator,
        message::Message,
        monitor::{NoOpObserver, PaxosObserver},
        node::{classic_paxos::ballot::Ballot, pvalue::PValue},
        paxos_command::PaxosCommand,
    };

    use super::{Leader, LeaderDurable, LeaderState};

    fn cleanup_leader_files(uuid: Uuid) {
        let _ = fs::remove_file(format!(".paxos/leader_{}.bin", uuid));
        let _ = fs::remove_file(format!(".paxos/Leader_{}.bin", uuid));
    }

    fn mk_leader() -> Leader {
        let uuid = Uuid::nil();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peers = Arc::new(NetworkSimulator::new(
            uuid,
            HashMap::new(),
            Arc::clone(&observer),
        ));
        let state = Arc::new(LeaderState::init(uuid, LeaderDurable::default()));

        Leader {
            uuid,
            quorum: 2,
            replicas: vec![],
            peers,
            state,
            observer,
        }
    }

    fn mk_leader_with_peer() -> (Leader, Uuid, mpsc::Receiver<Message>) {
        let uuid = Uuid::new_v4();
        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peer = Uuid::new_v4();
        let (tx, rx) = mpsc::channel(8);
        let mut peers_map = HashMap::new();
        peers_map.insert(peer, tx);
        let peers = Arc::new(NetworkSimulator::new(uuid, peers_map, Arc::clone(&observer)));
        let state = Arc::new(LeaderState::init(uuid, LeaderDurable::default()));

        (
            Leader {
                uuid,
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
        let leader = mk_leader();
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
        let leader = mk_leader();
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
        let leader = mk_leader();
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
        cleanup_leader_files(uuid);

        let observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let peers = Arc::new(NetworkSimulator::new(
            uuid,
            HashMap::new(),
            Arc::clone(&observer),
        ));
        let leader = Leader::new(uuid, 2, vec![], peers, Arc::clone(&observer))
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

        let peers2 = Arc::new(NetworkSimulator::new(
            uuid,
            HashMap::new(),
            Arc::clone(&observer),
        ));
        let reloaded = Leader::new(uuid, 2, vec![], peers2, observer)
            .await
            .expect("leader reload should work");

        assert!(
            reloaded.is_leader().await,
            "active status should survive leader restart"
        );
        cleanup_leader_files(uuid);
    }

    #[tokio::test]
    async fn unhandled_message_returns_nack() {
        let leader = mk_leader();
        let reply = leader
            .handle_message(Message::PROPOSE {
                from: Uuid::new_v4(),
                slot: 0,
                cmd: crate::paxos_command::PaxosCommand::NOOP,
            })
            .await;
        assert!(matches!(reply, Message::NACK));
    }

    #[tokio::test]
    async fn preempt_with_higher_ballot_deactivates_leader() {
        let leader = mk_leader();
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
        let leader = mk_leader();
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
        let leader = mk_leader();
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
        assert!(matches!(
            first,
            Message::NACK
        ));

        let second = leader
            .handle_message(Message::P2B {
                from: a2,
                to: leader.uuid,
                ballot,
                pvalue: PValue::new(slot, ballot, command),
            })
            .await;
        assert!(matches!(second, Message::ACCEPTED { .. }));
    }

    #[tokio::test]
    async fn start_election_broadcasts_p1a() {
        let (leader, _peer, mut rx) = mk_leader_with_peer();

        leader.start_election().await;

        let msg = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("expected p1a broadcast")
            .expect("receiver should get message");

        let expected_ballot = leader.state.ballot().await;
        assert!(matches!(
            msg,
            Message::P1A {
                from,
                ballot,
                start_index
            } if from == leader.uuid && ballot == expected_ballot && start_index == 0
        ));
    }

    #[tokio::test]
    async fn heartbeat_is_only_sent_when_active() {
        let (leader, _peer, mut rx) = mk_leader_with_peer();

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
            Message::HEARTBEAT { from, ballot: hb } if from == leader.uuid && hb == ballot
        ));
    }

    #[tokio::test]
    async fn p1b_quorum_emits_adopted_but_does_not_activate_until_adopted_message() {
        let (leader, _peer, mut rx) = mk_leader_with_peer();
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
            matches!(second, Message::ADOPTED { .. }),
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
        let (leader, _peer, _rx) = mk_leader_with_peer();
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
            matches!(reply, Message::PREEMPT { ballot, .. } if ballot == higher),
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
