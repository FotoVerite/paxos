use std::sync::Arc;

use uuid::Uuid;

use crate::{
    common::{network_fabric::NetworkFabric, network_handle::NetworkHandle},
    message::{MessageTrace, trace::MessageTracePayload},
    monitor::PaxosObserver,
    node::pmmc::message::PmmcMessage,
};

pub type PmmcFabric = NetworkFabric<PmmcMessage>;
pub type PmmcHandle = NetworkHandle<PmmcMessage>;

pub fn new_pmmc_fabric(observer: Arc<dyn PaxosObserver>) -> PmmcFabric {
    let trace = Arc::new(move |targets: &[Uuid], msg: &PmmcMessage| {
        observer.on_message_trace(MessageTrace::new(
            targets.to_vec(),
            MessageTracePayload::Pmmc(msg.clone()),
        ));
    });
    NetworkFabric::with_trace(trace)
}
