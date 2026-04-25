use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tracing::debug;
use uuid::Uuid;

use crate::monitor::{
    Event, EventProtocol, MessageProtocol, MessageTrace, ObservedMessage, PaxosObserver,
};
use crate::web::{ClusterInfo, VisualizerMessage};

/// A PaxosObserver that broadcasts events to all connected WebSocket clients.
pub struct WebSocketObserver {
    sender: broadcast::Sender<String>,
    cluster_info: RwLock<Option<ClusterInfo>>,
}

#[derive(Serialize)]
struct IndexedMessage<'a> {
    indexes: &'a [Uuid],
    message: &'a ObservedMessage,
}

impl WebSocketObserver {
    /// Creates a new WebSocketObserver.
    /// The `capacity` determines the buffer size for the broadcast channel.
    pub fn new(capacity: usize) -> Self {
        let (sender, _receiver) = broadcast::channel(capacity);
        Self {
            sender,
            cluster_info: RwLock::new(None),
        }
    }

    /// Sets the cluster information (called during initialization)
    pub async fn set_cluster_info(
        &self,
        total_nodes: usize,
        quorum_size: usize,
        node_uuids: Vec<Uuid>,
    ) {
        let cluster_info = ClusterInfo {
            total_nodes,
            quorum_size,
            node_uuids,
        };

        let mut info = self.cluster_info.write().await;
        *info = Some(cluster_info.clone());

        // Send cluster info to all connected clients
        let msg = VisualizerMessage::ClusterInitialized(cluster_info);
        if let Ok(json) = serde_json::to_string(&msg) {
            let _ = self.sender.send(json);
        }
    }

    /// Returns a new `tokio::sync::broadcast::Receiver` that can be used by a WebSocket client
    /// to receive events. Also sends the cluster info immediately if available.
    pub async fn subscribe(&self) -> broadcast::Receiver<String> {
        let receiver = self.sender.subscribe();

        // Don't send cluster info on subscribe - let set_cluster_info handle it
        // This prevents duplicate messages when scenarios start

        receiver
    }

    /// Clears the cluster info and prepares for a new scenario
    pub async fn clear(&self) {
        let mut info = self.cluster_info.write().await;
        *info = None;
    }

    fn broadcast_event(&self, event: Event, protocol: EventProtocol) {
        let event_debug = format!("{:?}", event);
        let event_type = event_debug.split('(').next().unwrap_or("Unknown");

        let event_json =
            serde_json::to_value(&event).expect("Failed to convert Paxos event to JSON value");
        let msg = VisualizerMessage::Event(event_json);
        let json_event =
            serde_json::to_string(&msg).expect("Failed to serialize visualizer message to JSON");

        match self.sender.send(json_event) {
            Ok(subscriber_count) => {
                debug!(
                    "Broadcast {:?} {} event to {} subscribers",
                    protocol, event_type, subscriber_count
                );
            }
            Err(e) => {
                debug!("No subscribers for {} event: {}", event_type, e);
            }
        }
    }

    fn handle_classic_event(&self, event: Event) {
        self.broadcast_event(event, EventProtocol::Classic);
    }

    fn handle_pmmc_event(&self, event: Event) {
        self.broadcast_event(event, EventProtocol::Pmmc);
    }

    fn handle_vertical_event(&self, event: Event) {
        self.broadcast_event(event, EventProtocol::Vertical);
    }

    fn handle_system_event(&self, event: Event) {
        self.broadcast_event(event, EventProtocol::System);
    }

    fn broadcast_message(
        &self,
        indexes: &[Uuid],
        message: &ObservedMessage,
        protocol: MessageProtocol,
    ) {
        let payload = IndexedMessage { indexes, message };
        let debug = format!("{:?}", message);

        let message_json =
            serde_json::to_value(&payload).expect("Failed to convert Paxos event to JSON value");
        let message_type = debug.split('(').next().unwrap_or("Unknown");

        let msg = VisualizerMessage::Message(message_json);
        let json_message =
            serde_json::to_string(&msg).expect("Failed to serialize visualizer message to JSON");
        match self.sender.send(json_message) {
            Ok(subscriber_count) => {
                debug!(
                    "Broadcast {:?} {}-{:?} message to {} subscribers",
                    protocol, message_type, indexes, subscriber_count
                );
            }
            Err(e) => {
                debug!("No subscribers for {} message: {}", message_type, e);
            }
        }
    }

    fn handle_classic_message(&self, indexes: &[Uuid], message: &ObservedMessage) {
        self.broadcast_message(indexes, message, MessageProtocol::Classic);
    }

    fn handle_pmmc_message(&self, indexes: &[Uuid], message: &ObservedMessage) {
        self.broadcast_message(indexes, message, MessageProtocol::Pmmc);
    }

    fn handle_vertical_message(&self, indexes: &[Uuid], message: &ObservedMessage) {
        self.broadcast_message(indexes, message, MessageProtocol::Vertical);
    }

    fn handle_system_message(&self, indexes: &[Uuid], message: &ObservedMessage) {
        if !message.should_broadcast_to_visualizer() {
            return;
        }
        self.broadcast_message(indexes, message, MessageProtocol::System);
    }
}

impl PaxosObserver for WebSocketObserver {
    /// Called when a Paxos event occurs.
    /// Serializes the event to JSON and broadcasts it to all subscribed clients.
    fn on_event(&self, event: Event) {
        match event.protocol() {
            EventProtocol::Classic => self.handle_classic_event(event),
            EventProtocol::Pmmc => self.handle_pmmc_event(event),
            EventProtocol::Vertical => self.handle_vertical_event(event),
            EventProtocol::System => self.handle_system_event(event),
        }
    }

    fn on_message_trace(&self, trace: MessageTrace) {
        match trace.protocol() {
            MessageProtocol::Classic => {
                self.handle_classic_message(trace.indexes(), trace.message())
            }
            MessageProtocol::Pmmc => self.handle_pmmc_message(trace.indexes(), trace.message()),
            MessageProtocol::Vertical => {
                self.handle_vertical_message(trace.indexes(), trace.message())
            }
            MessageProtocol::System => self.handle_system_message(trace.indexes(), trace.message()),
        }
    }
}

// Allow easy conversion to Arc<dyn PaxosObserver>
impl From<WebSocketObserver> for Arc<dyn PaxosObserver> {
    fn from(observer: WebSocketObserver) -> Self {
        Arc::new(observer)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::sync::broadcast;
    use tokio::time::timeout;
    use uuid::Uuid;

    use super::WebSocketObserver;
    use crate::cluster::pmmc::reconfiguration::ConfigurationStrategy;
    use crate::common::ballot::Ballot;
    use crate::common::types::DecreeId;
    use crate::monitor::{Event, MessageTrace, ObservedMessage, PaxosObserver};
    use crate::paxos_command::PaxosCommand;

    async fn recv_json(receiver: &mut broadcast::Receiver<String>) -> serde_json::Value {
        let payload = timeout(Duration::from_millis(250), receiver.recv())
            .await
            .expect("timed out waiting for websocket payload")
            .expect("websocket payload channel closed");
        serde_json::from_str(&payload).expect("failed to parse websocket payload")
    }

    #[tokio::test]
    async fn event_payload_contract_is_stable() {
        let observer = WebSocketObserver::new(8);
        let mut receiver = observer.subscribe().await;
        let id = Uuid::from_u128(1);

        observer.on_event(Event::Proposal {
            id,
            decree_num: DecreeId(7),
            value: PaxosCommand::NOOP,
            created_at: 42,
        });

        let got = recv_json(&mut receiver).await;
        let expected = json!({
            "Event": {
                "Proposal": {
                    "id": id,
                    "decree_num": 7,
                    "value": "NOOP",
                    "created_at": 42
                }
            }
        });
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn message_payload_contract_is_stable() {
        let observer = WebSocketObserver::new(8);
        let mut receiver = observer.subscribe().await;
        let from = Uuid::from_u128(11);
        let to = Uuid::from_u128(22);

        observer.on_message_trace(MessageTrace::from_message(
            &[from, to],
            &ObservedMessage::P1A {
                from,
                ballot: Ballot::with_epoch(3, from, 9),
                start_index: 5,
            },
        ));

        let got = recv_json(&mut receiver).await;
        let expected = json!({
            "Message": {
                "indexes": [from, to],
                "message": {
                    "P1A": {
                        "from": from,
                        "ballot": {
                            "epoch": 9,
                            "number": 3,
                            "node_id": from
                        },
                        "start_index": 5
                    }
                }
            }
        });
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn reconfiguration_event_payload_contract_is_stable() {
        let observer = WebSocketObserver::new(8);
        let mut receiver = observer.subscribe().await;
        let add_node = Uuid::from_u128(101);
        let remove_node = Uuid::from_u128(202);

        observer.on_event(Event::ReconfigurationRequested {
            strategy: ConfigurationStrategy::StopSign,
            add_nodes: vec![add_node],
            remove_nodes: vec![remove_node],
            created_at: 77,
        });

        let got = recv_json(&mut receiver).await;
        let expected = json!({
            "Event": {
                "ReconfigurationRequested": {
                    "strategy": "StopSign",
                    "add_nodes": [add_node],
                    "remove_nodes": [remove_node],
                    "created_at": 77
                }
            }
        });
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn vertical_event_payload_contract_is_stable() {
        let observer = WebSocketObserver::new(8);
        let mut receiver = observer.subscribe().await;
        let configuration_id = Uuid::from_u128(301);
        let leader = Uuid::from_u128(302);

        observer.on_event(Event::VerticalActivationReady {
            configuration_id,
            leader,
            ballot: Ballot::with_epoch(4, leader, 2),
            created_at: 88,
        });

        let got = recv_json(&mut receiver).await;
        let expected = json!({
            "Event": {
                "VerticalActivationReady": {
                    "configuration_id": configuration_id,
                    "leader": leader,
                    "ballot": {
                        "epoch": 2,
                        "number": 4,
                        "node_id": leader
                    },
                    "created_at": 88
                }
            }
        });
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn vertical_message_payload_contract_is_stable() {
        let observer = WebSocketObserver::new(8);
        let mut receiver = observer.subscribe().await;
        let from = Uuid::from_u128(401);
        let to = Uuid::from_u128(402);

        observer.on_message_trace(MessageTrace::new(
            vec![to],
            crate::monitor::MessageProtocol::Vertical,
            ObservedMessage::PROPOSE {
                from,
                slot: 3,
                cmd: PaxosCommand::PUT {
                    key: "olive".to_string(),
                    version: 0,
                    value: 1,
                },
            },
        ));

        let got = recv_json(&mut receiver).await;
        let expected = json!({
            "Message": {
                "indexes": [to],
                "message": {
                    "PROPOSE": {
                        "from": from,
                        "slot": 3,
                        "cmd": {
                            "PUT": {
                                "key": "olive",
                                "version": 0,
                                "value": 1
                            }
                        }
                    }
                }
            }
        });
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn cluster_initialized_payload_contract_is_stable() {
        let observer = WebSocketObserver::new(8);
        let mut receiver = observer.subscribe().await;
        let n1 = Uuid::from_u128(101);
        let n2 = Uuid::from_u128(102);
        let n3 = Uuid::from_u128(103);

        observer.set_cluster_info(3, 2, vec![n1, n2, n3]).await;

        let got = recv_json(&mut receiver).await;
        let expected = json!({
            "ClusterInitialized": {
                "total_nodes": 3,
                "quorum_size": 2,
                "node_uuids": [n1, n2, n3]
            }
        });
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn nack_is_not_broadcast() {
        let observer = WebSocketObserver::new(8);
        let mut receiver = observer.subscribe().await;

        observer.on_message_trace(MessageTrace::from_message(
            &[Uuid::from_u128(44)],
            &ObservedMessage::NACK,
        ));

        let result = timeout(Duration::from_millis(100), receiver.recv()).await;
        assert!(result.is_err(), "unexpected websocket payload for NACK");
    }
}
