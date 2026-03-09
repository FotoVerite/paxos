use std::{cmp::max, sync::Arc, time::Duration};

use rand::Rng;
use tokio::{task::JoinHandle, time::Instant};
use uuid::Uuid;

use crate::{
    cluster::network_handle::NetworkHandle,
    common::aimd_timeout::AimdTimeout,
    message::Message,
    monitor::PaxosObserver,
    node::{
        classic_paxos::ballot::Ballot,
        pmmc::{
            leader::{commander::Commander, scout::Scout},
            proposal::ProposalsStore,
        },
        pvalue::PValue,
    },
    paxos_command::PaxosCommand,
};

pub struct LeaderVolatile {
    pub highest_seen: Ballot,
    election_deadline: Instant,
    election_aimd: AimdTimeout,
    scout: Option<Scout>,
    commander: Option<CommanderRuntime>,
}

struct CommanderRuntime {
    commander: Commander,
    task: JoinHandle<()>,
}

impl Default for LeaderVolatile {
    fn default() -> Self {
        Self {
            highest_seen: Ballot::default(),
            election_deadline: Instant::now() + Duration::from_millis(100),
            election_aimd: AimdTimeout::default(),
            scout: None,
            commander: None,
        }
    }
}

impl LeaderVolatile {
    pub fn reset_election_deadline(&mut self) {
        let base = self.election_aimd.interval();
        let jitter = rand::rng().random_range(0..=((base.as_millis() as u64 / 5).max(1)));
        self.election_deadline = Instant::now() + base + Duration::from_millis(jitter);
    }

    pub fn drop_scout(&mut self) {
        self.scout = None;
    }

    pub async fn drop_commander(&mut self) {
        if let Some(runtime) = self.commander.take() {
            runtime.commander.stop();
            runtime.task.abort();
            let _ = runtime.task.await;
        }
    }

    pub fn start_commander(
        &mut self,
        uuid: Uuid,
        quorum: usize,
        ballot: Ballot,
        replicas: Vec<Uuid>,
        proposals: ProposalsStore,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<NetworkHandle>,
    ) {
        if let Some(runtime) = self.commander.take() {
            runtime.commander.stop();
            runtime.task.abort();
        }

        let commander = Commander::new(uuid, quorum, ballot, replicas, proposals, peers, observer);
        let runner = commander.clone();
        let task = tokio::spawn(async move {
            runner.run().await;
        });
        self.commander = Some(CommanderRuntime { commander, task });
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

    pub fn scout_pvalues(&self) -> Vec<PValue> {
        if let Some(scout) = &self.scout {
            return scout.pvalues();
        }
        return Vec::new();
    }

    pub fn set_highest_seen(&mut self, ballot: Ballot) {
        self.highest_seen = max(ballot, self.highest_seen);
    }

    pub fn aimd_backoff(&mut self) {
        self.election_aimd.backoff();
    }

    pub fn election_deadline(&self) -> Instant {
        self.election_deadline
    }

    pub async fn add_to_commander(&mut self, slot: usize, cmd: PaxosCommand) {
        if let Some(runtime) = self.commander.as_mut() {
            return runtime.commander.add_pending(slot, cmd).await;
        }
    }

    pub async fn p1b(&mut self, msg: Message) -> Message {
        if let Some(scout) = self.scout.as_mut() {
            return scout.handle_message(msg).await;
        }
        Message::NACK
    }

    pub async fn p2b(&mut self, msg: Message) -> Message {
        if let Some(runtime) = self.commander.as_mut() {
            return runtime.commander.handle_message(msg).await;
        }
        Message::NACK
    }

    pub async fn ack(&mut self, from: Uuid, slot: usize) {
        if let Some(runtime) = self.commander.as_mut() {
            runtime.commander.record_replica_ack(from, slot).await;
        }
    }
}

#[cfg(test)]
impl LeaderVolatile {
    pub(crate) fn has_scout(&self) -> bool {
        self.scout.is_some()
    }
}
