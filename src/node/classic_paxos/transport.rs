use std::sync::Arc;

use uuid::Uuid;

use crate::{
    common::{network_fabric::NetworkFabric, network_handle::NetworkHandle},
    monitor::{MessageTrace, PaxosObserver},
    node::classic_paxos::message::ClassicMessage,
};

pub type ClassicFabric = NetworkFabric<ClassicMessage>;
pub type ClassicHandle = NetworkHandle<ClassicMessage>;

pub fn new_classic_fabric(observer: Arc<dyn PaxosObserver>) -> ClassicFabric {
    let trace = Arc::new(move |targets: &[Uuid], msg: &ClassicMessage| {
        observer.on_message_trace(MessageTrace::from_message(targets, msg));
    });
    NetworkFabric::with_trace(trace)
}
