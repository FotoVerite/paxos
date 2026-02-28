use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::warn;
use uuid::Uuid;

pub const DATA_DIR: &str = ".paxos";

pub struct Persistence;

#[derive(Debug, Clone)]
pub struct ClusterPersistence {
    dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct NodePersistence {
    dir: PathBuf,
}

impl Persistence {
    pub fn cluster(ip: IpAddr) -> ClusterPersistence {
        ClusterPersistence::for_ip(ip)
    }
}

impl ClusterPersistence {
    pub fn for_ip(ip: IpAddr) -> Self {
        let ip_dir = ip.to_string().replace(':', "_");
        Self {
            dir: Path::new(DATA_DIR).join("ip").join(ip_dir),
        }
    }

    pub fn for_test(name: &str) -> Self {
        Self {
            dir: Path::new(DATA_DIR).join("test").join(name),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn node(&self, uuid: Uuid) -> NodePersistence {
        NodePersistence {
            dir: self.dir.join("nodes").join(uuid.to_string()),
        }
    }

    pub async fn ensure_dir_exists(&self) -> Result<()> {
        if !self.dir.exists() {
            fs::create_dir_all(&self.dir).await?;
        }
        Ok(())
    }

    pub async fn purge_known_state_files(&self) -> Result<usize> {
        let nodes_dir = self.dir.join("nodes");
        if !nodes_dir.exists() {
            return Ok(0);
        }

        let mut removed = 0usize;
        let mut entries = fs::read_dir(nodes_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_dir() {
                continue;
            }

            let node = NodePersistence { dir: entry.path() };
            removed += node.purge_known_state_files().await?;
        }

        Ok(removed)
    }
}

impl NodePersistence {
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub async fn ensure_dir_exists(&self) -> Result<()> {
        if !self.dir.exists() {
            fs::create_dir_all(&self.dir).await?;
        }
        Ok(())
    }

    pub async fn load<T: DeserializeOwned + Default>(&self, filename: &str) -> Result<T> {
        let path = self.dir.join(filename);

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
                let backup_path = PathBuf::from(format!("{}.corrupt", path.display()));
                let _ = fs::rename(&path, &backup_path).await;
                warn!(
                    "Failed to load persisted state from {} ({}). Backed up to {} and using default state.",
                    path.display(),
                    err,
                    backup_path.display()
                );
                Ok(T::default())
            }
        }
    }

    pub async fn save<T: Serialize>(&self, filename: &str, state: &T) -> Result<()> {
        self.ensure_dir_exists().await?;

        let path = self.dir.join(filename);
        let temp_path = self.dir.join(format!("{}.tmp", filename));
        let encoded = bincode::serialize(state)?;

        fs::write(&temp_path, encoded).await?;
        fs::rename(&temp_path, &path).await?;

        Ok(())
    }

    pub async fn remove(&self, filename: &str) -> Result<()> {
        let path = self.dir.join(filename);
        if path.exists() {
            fs::remove_file(path).await?;
        }
        Ok(())
    }

    pub async fn purge_known_state_files(&self) -> Result<usize> {
        if !self.dir.exists() {
            return Ok(0);
        }

        let names = [
            "ledger.bin",
            "acceptor.bin",
            "leader.bin",
            "replica.bin",
            "decree_notes.bin",
            "store.bin",
        ];

        let mut removed = 0usize;
        for name in names {
            let path = self.dir.join(name);
            if path.exists() && self.remove(name).await.is_ok() {
                removed += 1;
            }
        }

        Ok(removed)
    }
}
