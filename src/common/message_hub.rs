use std::collections::HashMap;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct MessageHub<T, I> {
    inbox: mpsc::Sender<I>,
    listeners: RwLock<HashMap<Uuid, JoinHandle<()>>>,
    clients: RwLock<HashMap<Uuid, mpsc::Sender<T>>>,
}

pub enum HubInbound<T> {
    Client { from: Uuid, msg: T },
}

impl<T: Send + 'static, I: From<HubInbound<T>> + Send + 'static> MessageHub<T, I> {
    pub fn new(inbox: mpsc::Sender<I>) -> Self {
        Self {
            listeners: RwLock::new(HashMap::new()),
            clients: RwLock::new(HashMap::new()),
            inbox,
        }
    }

    pub async fn register(&self, uuid: Uuid, sender: mpsc::Sender<T>) -> mpsc::Sender<T> {
        let (tx, rx) = mpsc::channel(1024);
        self.clients.write().await.insert(uuid, sender);

        if let Some(handle) = self.listeners.write().await.remove(&uuid) {
            handle.abort();
        }
        let handle = self.spawn_listener(uuid, rx);
        self.listeners.write().await.insert(uuid, handle);

        tx
    }

    pub async fn unregister(&self, uuid: Uuid) {
        self.clients.write().await.remove(&uuid);
        if let Some(handle) = self.listeners.write().await.remove(&uuid) {
            handle.abort();
        }
    }

    pub async fn clients(&self) -> Vec<Uuid> {
        self.clients.read().await.keys().copied().collect()
    }

    pub async fn send(&self, to: Uuid, msg: T) {
        if let Some(client) = self.clients.read().await.get(&to).cloned() {
            let _ = client.send(msg).await;
        }
    }

    pub fn spawn_listener(&self, from: Uuid, mut rx: mpsc::Receiver<T>) -> JoinHandle<()> {
        let inbox = self.inbox.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if inbox
                    .send(I::from(HubInbound::Client { from, msg }))
                    .await
                    .is_err()
                {
                    // Parent inbox closed: exit listener so we don't hot-spin on a dead path.
                    break;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{HubInbound, MessageHub};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};
    use uuid::Uuid;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Inbound {
        Client { from: Uuid, msg: String },
    }

    impl From<HubInbound<String>> for Inbound {
        fn from(value: HubInbound<String>) -> Self {
            match value {
                HubInbound::Client { from, msg } => Self::Client { from, msg },
            }
        }
    }

    #[tokio::test]
    async fn send_delivers_to_registered_client() {
        let (inbox_tx, _inbox_rx) = mpsc::channel(8);
        let hub: MessageHub<String, Inbound> = MessageHub::new(inbox_tx);
        let client_id = Uuid::new_v4();
        let (to_client_tx, mut to_client_rx) = mpsc::channel(8);

        let _client_inbox = hub.register(client_id, to_client_tx).await;
        hub.send(client_id, "ping".to_string()).await;

        let got = timeout(Duration::from_millis(100), to_client_rx.recv())
            .await
            .expect("timed out waiting for client message")
            .expect("client channel closed");
        assert_eq!(got, "ping");
    }

    #[tokio::test]
    async fn listener_wraps_source_and_forwards_to_hub_inbox() {
        let (inbox_tx, mut inbox_rx) = mpsc::channel(8);
        let hub: MessageHub<String, Inbound> = MessageHub::new(inbox_tx);
        let client_id = Uuid::new_v4();
        let (to_client_tx, _to_client_rx) = mpsc::channel(8);

        let from_client_tx = hub.register(client_id, to_client_tx).await;
        from_client_tx
            .send("cmd".to_string())
            .await
            .expect("send to listener");

        let inbound = timeout(Duration::from_millis(100), inbox_rx.recv())
            .await
            .expect("timed out waiting for inbound")
            .expect("hub inbox closed");

        assert_eq!(
            inbound,
            Inbound::Client {
                from: client_id,
                msg: "cmd".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn listener_exits_when_inbox_is_closed() {
        let (inbox_tx, inbox_rx) = mpsc::channel(8);
        let hub: MessageHub<String, Inbound> = MessageHub::new(inbox_tx);
        let client_id = Uuid::new_v4();
        let (to_client_tx, _to_client_rx) = mpsc::channel(8);

        let from_client_tx = hub.register(client_id, to_client_tx).await;
        drop(inbox_rx);

        // This should not panic/hang even with closed parent inbox.
        from_client_tx
            .send("first".to_string())
            .await
            .expect("send to listener after inbox close");
        from_client_tx
            .send("second".to_string())
            .await
            .expect("send to listener after inbox close");

        tokio::time::sleep(Duration::from_millis(30)).await;
        hub.unregister(client_id).await;
    }
}
