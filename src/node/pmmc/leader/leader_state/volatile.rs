use std::{cmp::max, future::pending, sync::Arc, time::Duration};

use rand::Rng;
use tokio::time::{Instant, Interval};
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    common::aimd_timeout::AimdTimeout,
    message::Message,
    monitor::PaxosObserver,
    node::{
        classic_paxos::ballot::Ballot,
        pmmc::{leader::{
            commander::{Commander},
            leader_state::LeaderDurable,
            scout::Scout,
        }, proposal::ProposalsStore},
    },
};

pub struct LeaderVolatile {
    active: bool,
    pub highest_seen: Ballot,
    election_deadline: Instant,
    election_aimd: AimdTimeout,
    heartbeat: Option<Interval>,
    scout: Option<Scout>,
    commander: Option<Commander>,
}

impl Default for LeaderVolatile {
    fn default() -> Self {
        Self {
            active: false,
            highest_seen: Ballot::default(),
            election_deadline: Instant::now(),
            election_aimd: AimdTimeout::default(),
            heartbeat: None,
            scout: None,
            commander: None,
        }
    }
}

impl LeaderVolatile {
    pub fn init(data: LeaderDurable) -> Self {
        let mut state = Self::default();
        state.highest_seen = data.ballot();
        state.reset_election_deadline();
        state
    }

    pub fn reset_election_deadline(&mut self) {
        let base = self.election_aimd.interval();
        let jitter = rand::rng().random_range(0..=((base.as_millis() as u64 / 5).max(1)));
        self.election_deadline = Instant::now() + base + Duration::from_millis(jitter);
    }

    pub fn drop_scount(&mut self) {
        self.scout = None;
    }

    pub fn drop_commander(&mut self) {
        self.commander = None;
    }

    pub fn start_commanders(
        &mut self,
        uuid: Uuid,
        ballot: Ballot,
        proposals: ProposalsStore,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<NetworkSimulator>,
    ) {
        self.commander = Some(Commander::new(uuid, 1, ballot, proposals, peers, observer));
    }
    pub async fn start_scout(
        &mut self,
        uuid: Uuid,
        ballot: Ballot,
        quorum: usize,
        observer: Arc<dyn PaxosObserver>,
    ) {
        self.scout = Some(Scout::new(uuid, quorum, ballot, Arc::clone(&observer)));
        self.highest_seen = ballot;
        self.reset_election_deadline();
    }

    pub fn set_highest_seen(&mut self, ballot: Ballot) {
        self.highest_seen = max(ballot, self.highest_seen);
    }

    pub fn aimd_backoff(&mut self) {
        self.election_aimd.backoff();
    }

    pub fn aimd_success(&mut self) {
        self.election_aimd.success();
    }

    pub fn heartbeat_interval(&self) -> Option<&Interval> {
        self.heartbeat.as_ref()
    }

    pub fn election_deadline(&self) -> Instant {
        self.election_deadline
    }

    pub async fn wait_heartbeat_tick(&mut self) {
        if let Some(hb) = self.heartbeat.as_mut() {
            hb.tick().await;
        } else {
            pending::<()>().await;
        }
    }

    pub async fn p1b(&mut self, msg: Message) -> Message {
        if let Some(scout) = self.scout.as_mut() {
            return scout.handle_message(msg).await
        }
        Message::NACK
    }

    pub async fn p2b(&mut self, msg: Message) -> Message {
        if let Some(commander) = self.commander.as_mut() {
            return commander.handle_message(msg).await
        }
        Message::NACK
    }
}
