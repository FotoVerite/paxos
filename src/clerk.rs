mod types;

use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    clerk::types::{ClerkRequest, ClerkResponse},
    cluster::network_simulator::NetworkSimulator,
    paxos_command::PaxosCommand,
};

pub struct Clerk {
    cid: usize,
    sid: usize,
    leader_id: Option<usize>,
    replicas: Arc<NetworkSimulator>,
    tx: Sender<ClerkRequest>,
    rx: Receiver<ClerkResponse>,
}

impl Clerk {
    pub fn create(
        &self,
        cid: usize,
        replicas: Arc<NetworkSimulator>,
        tx: Sender<ClerkRequest>,
        rx: Receiver<ClerkResponse>,
    ) -> Self {
        Self {
            cid,
            sid: 0,
            leader_id: None,
            replicas,
            tx,
            rx,
        }
    }
    pub fn Request(&self, cmd: PaxosCommand) {}

    //pub fn HandleResponse(&self) -> usize {}

    fn increment_sid(&mut self) {
        self.sid += 1;
    }

    // pub fn start(&mut self) {
    //     let mut rx = self.rx.take().expect("worker already started");
    //     tokio::spawn(async move {
    //         while let Some(msg) = rx.recv().await {
    //         }
    //     });
    // }
}
