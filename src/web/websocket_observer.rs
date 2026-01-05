use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug};

use crate::monitor::{Event, PaxosObserver};
use crate::web::{ClusterInfo, VisualizerMessage};

/// A PaxosObserver that broadcasts events to all connected WebSocket clients.
pub struct WebSocketObserver {
    sender: broadcast::Sender<String>,
    cluster_info: RwLock<Option<ClusterInfo>>,
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
    pub async fn set_cluster_info(&self, total_nodes: usize, quorum_size: usize) {
        let cluster_info = ClusterInfo {
            total_nodes,
            quorum_size,
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
}

impl PaxosObserver for WebSocketObserver {
    /// Called when a Paxos event occurs.
    /// Serializes the event to JSON and broadcasts it to all subscribed clients.
    fn on_event(&self, event: Event) {
        let event_debug = format!("{:?}", event);
        let event_type = event_debug.split('(').next().unwrap_or("Unknown");
        
        let event_json = serde_json::to_value(&event)
            .expect("Failed to convert Paxos event to JSON value");
        let msg = VisualizerMessage::Event(event_json);
        let json_event = serde_json::to_string(&msg)
            .expect("Failed to serialize visualizer message to JSON");

        // Broadcast to all subscribers
        match self.sender.send(json_event) {
            Ok(subscriber_count) => {
                debug!("Broadcast {} event to {} subscribers", event_type, subscriber_count);
            }
            Err(e) => {
                // This is expected when no clients are listening
                debug!("No subscribers for {} event: {}", event_type, e);
            }
        }
    }
}

// Allow easy conversion to Arc<dyn PaxosObserver>
impl From<WebSocketObserver> for Arc<dyn PaxosObserver> {
    fn from(observer: WebSocketObserver) -> Self {
        Arc::new(observer)
    }
}