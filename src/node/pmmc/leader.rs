use std::{cmp::max, future::pending, sync::Arc, time::Duration};

use rand::Rng;
use tokio::{
    select,
    time::{self, Instant, Interval, MissedTickBehavior},
};
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    common::{aimd_timeout::AimdTimeout, persistence::Persistence},
    message::Message,
    monitor::PaxosObserver,
    node::{
        paxos_state::ballot::Ballot,
        pmmc::leader::{
            commander::Commander,
            leader_state::{LeaderData, LeaderState},
            scout::Scout,
        },
    },
};

mod commander;
mod leader_state;
mod scout;

pub struct Leader {
    uuid: Uuid,
    quorum: usize,
    peers: Arc<NetworkSimulator>,

    state: Arc<LeaderState>,
    highest_seen: Ballot,
    scout: Option<Scout>,

    election_aimd: AimdTimeout,
    election_deadline: Instant,

    // Heartbeat is fixed period (non-AIMD).
    heartbeat_period: Duration,
    heartbeat: Option<Interval>,

    commander: Option<Commander>,
    observer: Arc<dyn PaxosObserver>,
}

impl Leader {
    pub async fn new(
        uuid: Uuid,
        quorum: usize,
        peers: Arc<NetworkSimulator>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let state: LeaderData = Persistence::load(&format!("leader_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state = LeaderData::default();

        let state = LeaderState::init(uuid, state);
        let mut leader = Self {
            uuid,
            peers,
            quorum,
            highest_seen: state.ballot().await,
            heartbeat_period: Duration::from_millis(150),
            election_aimd: AimdTimeout::default(),
            election_deadline: Instant::now(),
            observer,
            state: Arc::new(state),
            heartbeat: None,
            scout: None,
            commander: None,
        };
        leader.run().await;
        Ok(leader)
    }

    fn reset_election_deadline(&mut self) {
        let base = self.election_aimd.interval();
        let jitter = rand::rng().random_range(0..=((base.as_millis() as u64 / 5).max(1)));
        self.election_deadline = Instant::now() + base + Duration::from_millis(jitter);
    }

    async fn start_scout(&mut self) {
        let ballot = self.state.bump_ballot(self.highest_seen).await;
        self.highest_seen = ballot;
        self.scout = Some(Scout::new(self.uuid, 5, ballot, Arc::clone(&self.observer)));
        self.peers
            .broadcast(Message::P1A {
                from: self.uuid,
                ballot,
                start_index: 0,
            })
            .await;
        self.reset_election_deadline();
    }

    async fn become_active(&mut self) {
        self.state.set_as_active().await;
        self.election_aimd.success();

        let mut hb = time::interval_at(
            Instant::now() + self.heartbeat_period,
            self.heartbeat_period,
        );
        hb.set_missed_tick_behavior(MissedTickBehavior::Skip);
        self.heartbeat = Some(hb);
        self.start_commander().await;
    }

    async fn start_commander(&mut self) {
        let ballot = self.state.ballot().await;
        let proposals = self.state.proposal().await;
        self.commander = Some(Commander::new(
            self.uuid,
            self.quorum,
            ballot,
            proposals,
            Arc::clone(&self.peers),
            Arc::clone(&self.observer),
        ));
    }

    async fn wait_heartbeat_tick(&mut self) {
        if let Some(hb) = self.heartbeat.as_mut() {
            hb.tick().await;
        } else {
            pending::<()>().await;
        }
    }

    async fn on_heartbeat_tick(&mut self) {
        let ballot = self.state.ballot().await;
        self.peers
            .broadcast(Message::HEARTBEAT {
                from: self.uuid,
                ballot,
            })
            .await;
    }

    async fn heartbeat_handler(&mut self, ballot: Ballot) -> Message {
        if self.state.is_active().await || self.state.is_stale_ballot(ballot).await {
            return Message::NACK;
        }
        self.state.set_as_passive();
        self.scout = None;
        self.commander = None;
        self.highest_seen = max(ballot, self.highest_seen);
        return Message::NACK;
    }

    async fn preempt(&mut self, ballot: Ballot) {
        if !self.state.is_stale_ballot(ballot).await {
            self.election_aimd.backoff();
            self.scout = None;
            self.commander = None;
            self.highest_seen = max(ballot, self.highest_seen);
        }
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let state = self.state.dump().await;
        Persistence::save(&format!("Leader_{}.bin", self.uuid), &state).await?;
        Ok(())
    }

    pub async fn handle_message(&mut self, msg: Message) -> Message {
        match msg {
            Message::ADOPTED { .. } => {
                self.become_active().await;
                return Message::NACK;
            }
            Message::P1B { .. } => {
                if let Some(scout) = self.scout.as_mut() {
                    return scout.handle_message(msg).await;
                }
                return Message::NACK;
            }

            Message::P2B { .. } => {
                if let Some(commander) = self.commander.as_mut() {
                    return commander.handle_message(msg).await;
                }
                return Message::NACK;
            }
            Message::PREEMPT { ballot, .. } => {
                self.preempt(ballot).await;
                return Message::NACK;
            }
            _ => Message::NACK,
        }
    }

    pub async fn run(&mut self) {
        self.reset_election_deadline();
        let state = Arc::clone(&self.state);
        loop {
            select! {
                _ = time::sleep_until(self.election_deadline), if !state.is_active().await => {
                    self.start_scout().await;
               }

                _ = self.wait_heartbeat_tick(),if state.is_active().await => {
                    self.on_heartbeat_tick().await;
                }


                else => break,
            }
        }
    }
}
