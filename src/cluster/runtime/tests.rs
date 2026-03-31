use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use tokio::{
    sync::{Mutex, mpsc::Receiver},
    time::{Duration, timeout},
};
use uuid::Uuid;

use crate::{
    cluster::{
        runtime::{ControlPlaneRegistry, NodeRegistry, RuntimeNode, RuntimeNodeFactory, RuntimeState},
    },
    common::network_fabric::NetworkFabric,
};

#[derive(Default)]
struct DummyShared {
    started: AtomicUsize,
    shut_down: AtomicUsize,
    seen_messages: Mutex<Vec<(Uuid, usize)>>,
}

struct DummyNode {
    id: Uuid,
    shared: Arc<DummyShared>,
}

#[async_trait]
impl RuntimeNode<usize> for DummyNode {
    fn id(&self) -> Uuid {
        self.id
    }

    async fn start(&self, mut inbox: Receiver<usize>) -> tokio::task::JoinHandle<()> {
        self.shared.started.fetch_add(1, Ordering::SeqCst);
        let shared = Arc::clone(&self.shared);
        let id = self.id;
        tokio::spawn(async move {
            while let Some(msg) = inbox.recv().await {
                shared.seen_messages.lock().await.push((id, msg));
            }
        })
    }

    async fn shutdown(&self) {
        self.shared.shut_down.fetch_add(1, Ordering::SeqCst);
    }
}

struct DummyFactory {
    shared: Arc<DummyShared>,
    built: Mutex<HashMap<Uuid, Arc<DummyNode>>>,
}

impl DummyFactory {
    fn new(shared: Arc<DummyShared>) -> Self {
        Self {
            shared,
            built: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl RuntimeNodeFactory<usize> for DummyFactory {
    async fn build(&self, id: Uuid) -> anyhow::Result<Arc<dyn RuntimeNode<usize>>> {
        let node = Arc::new(DummyNode {
            id,
            shared: Arc::clone(&self.shared),
        });
        self.built.lock().await.insert(id, Arc::clone(&node));
        Ok(node)
    }
}

#[tokio::test]
async fn registry_starts_members_and_tracks_state() {
    let shared = Arc::new(DummyShared::default());
    let factory = Arc::new(DummyFactory::new(Arc::clone(&shared)));
    let fabric = Arc::new(NetworkFabric::new());
    let ids = vec![Uuid::new_v4(), Uuid::new_v4()];

    let registry = NodeRegistry::init(ids.clone(), fabric, factory)
        .await
        .expect("registry should initialize");

    registry.start().await;

    assert_eq!(shared.started.load(Ordering::SeqCst), 2);
    assert_eq!(registry.state(ids[0]).await, Some(RuntimeState::Active));
    assert_eq!(registry.state(ids[1]).await, Some(RuntimeState::Active));
}

#[tokio::test]
async fn registry_provisions_fabric_endpoints_for_members() {
    let shared = Arc::new(DummyShared::default());
    let factory = Arc::new(DummyFactory::new(Arc::clone(&shared)));
    let fabric = Arc::new(NetworkFabric::new());
    let from = Uuid::new_v4();
    let target = Uuid::new_v4();

    let registry = NodeRegistry::init(vec![target], Arc::clone(&fabric), factory)
        .await
        .expect("registry should initialize");
    registry.start().await;

    fabric.send(from, target, 42usize).await;

    timeout(Duration::from_millis(200), async {
        loop {
            if shared
                .seen_messages
                .lock()
                .await
                .iter()
                .any(|(id, msg)| *id == target && *msg == 42)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("message should be delivered");
}

#[tokio::test]
async fn stop_process_shuts_runtime_down() {
    let shared = Arc::new(DummyShared::default());
    let factory = Arc::new(DummyFactory::new(Arc::clone(&shared)));
    let fabric = Arc::new(NetworkFabric::new());
    let id = Uuid::new_v4();

    let registry = NodeRegistry::init(vec![id], fabric, factory)
        .await
        .expect("registry should initialize");
    registry.start().await;
    registry.stop_process(id).await;

    assert_eq!(shared.shut_down.load(Ordering::SeqCst), 1);
    assert_eq!(registry.state(id).await, Some(RuntimeState::Stopped));
}

#[tokio::test]
async fn spawn_process_builds_and_starts_new_member() {
    let shared = Arc::new(DummyShared::default());
    let factory = Arc::new(DummyFactory::new(Arc::clone(&shared)));
    let fabric = Arc::new(NetworkFabric::new());
    let existing = Uuid::new_v4();
    let spawned = Uuid::new_v4();

    let registry = NodeRegistry::init(vec![existing], fabric, factory)
        .await
        .expect("registry should initialize");
    registry.start().await;
    registry
        .spawn_process(spawned)
        .await
        .expect("spawn should work");

    assert_eq!(shared.started.load(Ordering::SeqCst), 2);
    assert_eq!(registry.state(spawned).await, Some(RuntimeState::Active));
}

#[tokio::test]
async fn control_plane_registry_starts_local_actor_and_delivers_messages() {
    let shared = Arc::new(DummyShared::default());
    let actor_id = Uuid::new_v4();
    let actor = Arc::new(DummyNode {
        id: actor_id,
        shared: Arc::clone(&shared),
    });
    let registry = ControlPlaneRegistry::new();
    let sender = registry.provision_endpoint(actor_id).await;
    registry
        .spawn_actor(actor_id, actor)
        .await
        .expect("actor should register");

    registry.start().await;
    sender
        .send(7usize)
        .await
        .expect("control-plane inbox should accept messages");

    timeout(Duration::from_millis(200), async {
        loop {
            if shared
                .seen_messages
                .lock()
                .await
                .iter()
                .any(|(id, msg)| *id == actor_id && *msg == 7)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("control-plane actor should receive local message");

    assert_eq!(registry.state(actor_id).await, Some(RuntimeState::Active));
}
