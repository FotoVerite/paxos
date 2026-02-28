use std::collections::HashMap;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    common::persistence::NodePersistence,
    paxos_command::PaxosCommand,
    rsm::{
        entry::KVEntry,
        types::{ClerkResponseError, KVResult, KVValue, KVVersion},
    },
};

pub struct KVStore {
    persistence: NodePersistence,
    data: Mutex<HashMap<String, KVEntry>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub enum ReplyOutcome {
    GetOk { value: KVValue, version: KVVersion },
    WriteOk { version: KVVersion }, // PUT/ADD success
    Ok,                             // commands with no payload
    Err(ClerkResponseError),
}

impl KVStore {
    pub async fn init(_uuid: Uuid, persistence: NodePersistence) -> anyhow::Result<Self> {
        let state = persistence.load("store.bin").await?;

        #[cfg(not(feature = "persistence"))]
        let state = HashMap::new();

        Ok(Self {
            persistence,
            data: Mutex::new(state),
        })
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let data = self.data.lock().await;
        let state = data.clone();
        drop(data);
        self.persistence.save("store.bin", &state).await?;
        Ok(())
    }

    pub async fn apply(&self, cmd: PaxosCommand) -> KVResult<ReplyOutcome> {
        match cmd.operation() {
            PaxosCommand::GET { key } => self.get(key).await,
            PaxosCommand::PUT {
                key,
                value,
                ..
            } => self.set(key, KVValue(*value)).await,
            PaxosCommand::ADD {
                key,
                version,
                value,
            } => self
                .add(key, KVValue(*value), KVVersion(*version))
                .await
                .map(|v| ReplyOutcome::WriteOk { version: v }),
            _ => Ok(ReplyOutcome::Ok),
        }
    }

    pub async fn get(&self, key: &str) -> KVResult<ReplyOutcome> {
        let data = self.data.lock().await;
        if let Some(entry) = data.get(key) {
            return Ok(ReplyOutcome::GetOk {
                value: entry.value,
                version: entry.version,
            });
        }
        return Err(ClerkResponseError::ErrKey);
    }

    pub async fn set(&self, key: &str, value: KVValue) -> KVResult<ReplyOutcome> {
        let entry_version = {
            let mut data = self.data.lock().await;
            let entry = data
                .entry(key.to_string())
                .and_modify(|e| e.value = value)
                .or_insert_with(|| KVEntry::new(value));
            entry.version
        }; // lock is dropped here

        let _ = self.save().await;

        Ok(ReplyOutcome::WriteOk {
            version: entry_version,
        })
    }

    pub async fn add(&self, key: &str, value: KVValue, version: KVVersion) -> KVResult<KVVersion> {
        let entry_version = {
            let mut data = self.data.lock().await;

            match data.get_mut(key) {
                Some(entry) => {
                    entry.add(value, version)?;
                    entry.version
                }
                None => return Err(ClerkResponseError::ErrKey),
            }
        };
        let _ = self.save().await;
        Ok(entry_version)
    }

    pub async fn delete(&self, key: &str) -> KVResult<()> {
        {
            let mut data = self.data.lock().await;
            data.remove(key)
                .map(|_| ())
                .ok_or(ClerkResponseError::ErrKey)?;
        }
        let _ = self.save().await;
        Ok(())
    }
}
