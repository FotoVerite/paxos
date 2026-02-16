use std::collections::{HashMap, hash_map::Entry};

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    common::persistence::Persistence,
    rsm::{
        entry::KVEntry,
        types::{ClerkResponseError, KVResult, KVValue, KVVersion},
    },
};

pub struct KVStore {
    uuid: Uuid,
    data: Mutex<HashMap<String, KVEntry>>,
}

impl KVStore {
    pub async fn init(uuid: Uuid) -> anyhow::Result<Self> {
        let state = Persistence::load(&format!("store_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state = HashMap::new();

        Ok(Self {
            uuid,
            data: Mutex::new(state),
        })
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let data = self.data.lock().await;
        let state = data.clone();
        drop(data);
        Persistence::save(&format!("store_{}.bin", self.uuid), &state).await?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> KVResult<(KVValue, KVVersion)> {
        let data = self.data.lock().await;
        if let Some(entry) = data.get(key) {
            return Ok((entry.value, entry.version));
        }
        return Err(ClerkResponseError::ErrKey);
    }

    pub async fn set(&self, key: &str, value: KVValue) -> KVResult<KVVersion> {
        let entry_version = {
            let mut data = self.data.lock().await;

            match data.entry(key.to_string()) {
                Entry::Occupied(_) => return Err(ClerkResponseError::ErrKey),
                Entry::Vacant(v) => {
                    let entry = v.insert(KVEntry::new(value));
                    entry.version
                }
            }
        }; // lock is dropped here

        // Now you can safely call save without holding the lock
        self.save().await;

        Ok(entry_version)
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
        self.save().await;
        Ok(entry_version)
    }

    pub async fn delete(&self, key: &str) -> KVResult<()> {
        {
            let mut data = self.data.lock().await;
            data.remove(key)
                .map(|_| ())
                .ok_or(ClerkResponseError::ErrKey)?;
        }
        self.save().await;
        Ok(())
    }
}
