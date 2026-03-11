use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::rsm::entry::KVEntry;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct RsmCheckpoint {
    manifest: CheckpointManifest,
    state: CheckpointState,
}

impl PartialOrd for RsmCheckpoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RsmCheckpoint {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.manifest.cmp(&other.manifest) {
            ord => ord,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default)]
pub(crate) struct RsmCheckpointState {
    kv_store: HashMap<String, KVEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CheckpointManifest {
    snapshot_id: Uuid,
    source_node: Uuid,
    config_id: u64,
    epoch: u64,
    last_applied_slot: Option<usize>,
    created_at_ms: u64,
    checksum: String,
}

impl PartialOrd for CheckpointManifest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CheckpointManifest {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.epoch.cmp(&other.epoch) {
            Ordering::Equal => match self.config_id.cmp(&other.config_id) {
                Ordering::Equal => self.last_applied_slot.cmp(&other.last_applied_slot),
                ord => ord,
            },
            ord => ord,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct CheckpointState {
    kv_store: HashMap<String, KVEntry>,
    // Highest applied request_id per client_id. Used to suppress duplicate
    // retries after restore/reconfiguration without replaying prior writes.
    client_dedup_watermark: HashMap<Uuid, u64>,
}

impl RsmCheckpoint {
    pub(crate) fn new(mut manifest: CheckpointManifest, state: CheckpointState) -> Self {
        manifest.set_checksum(Self::compute_checksum(&state));
        Self { manifest, state }
    }

    pub(crate) fn manifest(&self) -> &CheckpointManifest {
        &self.manifest
    }

    pub(crate) fn state(&self) -> &CheckpointState {
        &self.state
    }

    fn compute_checksum(state: &CheckpointState) -> String {
        let canonical = CanonicalCheckpointState::from(state);
        let encoded =
            serde_json::to_vec(&canonical).expect("checkpoint state serialization failed");
        let digest = Sha256::digest(encoded);
        to_hex(&digest)
    }
}

impl RsmCheckpointState {
    pub(crate) fn new(kv_store: HashMap<String, KVEntry>) -> Self {
        Self { kv_store }
    }

    pub(crate) fn into_kv_store(self) -> HashMap<String, KVEntry> {
        self.kv_store
    }
}

impl CheckpointManifest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        snapshot_id: Uuid,
        source_node: Uuid,
        config_id: u64,
        epoch: u64,
        last_applied_slot: Option<usize>,
        created_at_ms: u64,
    ) -> Self {
        Self {
            snapshot_id,
            source_node,
            config_id,
            epoch,
            last_applied_slot,
            created_at_ms,
            checksum: String::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_id(&self) -> Uuid {
        self.snapshot_id
    }

    #[cfg(test)]
    pub(crate) fn source_node(&self) -> Uuid {
        self.source_node
    }

    #[cfg(test)]
    pub(crate) fn config_id(&self) -> u64 {
        self.config_id
    }

    #[cfg(test)]
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(crate) fn last_applied_slot(&self) -> Option<usize> {
        self.last_applied_slot
    }

    #[cfg(test)]
    pub(crate) fn created_at_ms(&self) -> u64 {
        self.created_at_ms
    }

    #[cfg(test)]
    pub(crate) fn checksum(&self) -> &str {
        &self.checksum
    }

    fn set_checksum(&mut self, checksum: String) {
        self.checksum = checksum;
    }
}

impl CheckpointState {
    pub(crate) fn new(
        rsm_state: RsmCheckpointState,
        client_dedup_watermark: HashMap<Uuid, u64>,
    ) -> Self {
        Self {
            kv_store: rsm_state.into_kv_store(),
            client_dedup_watermark,
        }
    }

    pub(crate) fn kv_store(&self) -> &HashMap<String, KVEntry> {
        &self.kv_store
    }

    #[cfg(test)]
    pub(crate) fn client_dedup_watermark(&self) -> &HashMap<Uuid, u64> {
        &self.client_dedup_watermark
    }
}

#[derive(Serialize)]
struct CanonicalCheckpointState {
    kv_store: BTreeMap<String, KVEntry>,
    client_dedup_watermark: BTreeMap<String, u64>,
}

impl From<&CheckpointState> for CanonicalCheckpointState {
    fn from(value: &CheckpointState) -> Self {
        let kv_store = value
            .kv_store
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<BTreeMap<_, _>>();
        let client_dedup_watermark = value
            .client_dedup_watermark
            .iter()
            .map(|(client_id, request_id)| (client_id.to_string(), *request_id))
            .collect::<BTreeMap<_, _>>();
        Self {
            kv_store,
            client_dedup_watermark,
        }
    }
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::types::{KVValue, KVVersion};

    #[test]
    fn checkpoint_roundtrip_json() {
        let source_node = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();

        let mut kv = HashMap::new();
        kv.insert(
            "alpha".to_string(),
            KVEntry {
                value: KVValue(7),
                version: KVVersion(2),
            },
        );
        let mut dedup = HashMap::new();
        dedup.insert(client_id, 42);

        let manifest =
            CheckpointManifest::new(snapshot_id, source_node, 3, 9, Some(17), 1_742_123_456_789);
        let state = CheckpointState::new(RsmCheckpointState::new(kv), dedup);
        let checkpoint = RsmCheckpoint::new(manifest, state);
        let encoded = serde_json::to_string(&checkpoint).unwrap();
        let decoded: RsmCheckpoint = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded.manifest().snapshot_id(), snapshot_id);
        assert_eq!(decoded.manifest().source_node(), source_node);
        assert_eq!(decoded.manifest().config_id(), 3);
        assert_eq!(decoded.manifest().epoch(), 9);
        assert_eq!(decoded.manifest().last_applied_slot(), Some(17));
        assert_eq!(decoded.manifest().created_at_ms(), 1_742_123_456_789);
        assert!(!decoded.manifest().checksum().is_empty());
        assert_eq!(decoded.manifest().checksum().len(), 64);

        let entry = decoded
            .state()
            .kv_store()
            .get("alpha")
            .expect("alpha key missing");
        assert_eq!(entry.value, KVValue(7));
        assert_eq!(entry.version, KVVersion(2));
        assert_eq!(
            decoded.state().client_dedup_watermark().get(&client_id),
            Some(&42)
        );
    }

    #[test]
    fn checksum_changes_when_state_changes() {
        let source_node = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let mut kv_a = HashMap::new();
        kv_a.insert(
            "alpha".to_string(),
            KVEntry {
                value: KVValue(7),
                version: KVVersion(2),
            },
        );
        let state_a = CheckpointState::new(RsmCheckpointState::new(kv_a), HashMap::new());
        let ckpt_a = RsmCheckpoint::new(
            CheckpointManifest::new(snapshot_id, source_node, 3, 9, Some(17), 1_742_123_456_789),
            state_a,
        );

        let mut kv_b = HashMap::new();
        kv_b.insert(
            "alpha".to_string(),
            KVEntry {
                value: KVValue(8),
                version: KVVersion(2),
            },
        );
        let state_b = CheckpointState::new(RsmCheckpointState::new(kv_b), HashMap::new());
        let ckpt_b = RsmCheckpoint::new(
            CheckpointManifest::new(snapshot_id, source_node, 3, 9, Some(17), 1_742_123_456_789),
            state_b,
        );

        assert_ne!(ckpt_a.manifest().checksum(), ckpt_b.manifest().checksum());
    }

    #[test]
    fn checkpoint_roundtrip_preserves_empty_last_applied_slot() {
        let checkpoint = RsmCheckpoint::new(
            CheckpointManifest::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                3,
                9,
                None,
                1_742_123_456_789,
            ),
            CheckpointState::new(RsmCheckpointState::new(HashMap::new()), HashMap::new()),
        );

        let encoded = serde_json::to_string(&checkpoint).unwrap();
        let decoded: RsmCheckpoint = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.manifest().last_applied_slot(), None);
    }
}
