use std::sync::Arc;

use uuid::Uuid;

use crate::{
    common::{network_fabric::NetworkFabric, network_handle::NetworkHandle},
    monitor::{MessageProtocol, MessageTrace, ObservedMessage, PaxosObserver},
    node::vertical_paxos::message::VerticalPaxosMessage,
};

pub type VerticalPaxosFabric = NetworkFabric<VerticalPaxosMessage>;
pub type VerticalPaxosHandle = NetworkHandle<VerticalPaxosMessage>;

pub fn new_vertical_paxos_fabric(observer: Arc<dyn PaxosObserver>) -> VerticalPaxosFabric {
    let trace = Arc::new(move |targets: &[Uuid], msg: &VerticalPaxosMessage| {
        observer.on_message_trace(MessageTrace::new(
            targets.to_vec(),
            MessageProtocol::Vertical,
            ObservedMessage::from(msg),
        ));
    });
    NetworkFabric::with_trace(trace)
}
