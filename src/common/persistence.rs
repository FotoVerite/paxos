use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::path::Path;
use tokio::fs;
use tracing::warn;

pub const DATA_DIR: &str = ".paxos";

pub struct Persistence;

impl Persistence {
    pub async fn ensure_dir_exists() -> Result<()> {
        if !Path::new(DATA_DIR).exists() {
            fs::create_dir_all(DATA_DIR).await?;
        }
        Ok(())
    }

    pub async fn load<T: DeserializeOwned + Default>(filename: &str) -> Result<T> {
        let path_str = format!("{}/{}", DATA_DIR, filename);
        let path = Path::new(&path_str);

        if !path.exists() {
            return Ok(T::default());
        }

        let data = fs::read(&path).await?;
        if data.is_empty() {
            return Ok(T::default());
        }

        match bincode::deserialize(&data) {
            Ok(state) => Ok(state),
            Err(err) => {
                // Recovery path for schema migrations (e.g., usize -> UUID).
                // Keep the old file for debugging and start from default state.
                let backup_path = format!("{}.corrupt", path_str);
                let _ = fs::rename(&path_str, &backup_path).await;
                warn!(
                    "Failed to load persisted state from {} ({}). Backed up to {} and using default state.",
                    path_str, err, backup_path
                );
                Ok(T::default())
            }
        }
    }

    pub async fn save<T: Serialize>(filename: &str, state: &T) -> Result<()> {
        Self::ensure_dir_exists().await?;

        let path_str = format!("{}/{}", DATA_DIR, filename);
        let temp_path = format!("{}.tmp", path_str);

        let encoded = bincode::serialize(state)?;

        // Write to temp file
        fs::write(&temp_path, encoded).await?;

        // Atomic rename
        fs::rename(&temp_path, &path_str).await?;

        Ok(())
    }

    pub async fn purge_known_state_files() -> Result<usize> {
        if !Path::new(DATA_DIR).exists() {
            return Ok(0);
        }

        let prefixes = [
            "ledger_",
            "acceptor_",
            "leader_",
            "replica_",
            "decree_notes_",
            "store_",
        ];

        let mut removed = 0usize;
        let mut entries = fs::read_dir(DATA_DIR).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                continue;
            }

            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };

            if prefixes.iter().any(|prefix| name.starts_with(prefix)) {
                if fs::remove_file(entry.path()).await.is_ok() {
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }
}
