use crate::rsm::types::{ClerkResponseError, KVResult, KVValue, KVVersion};

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct KVEntry {
    pub value: KVValue,
    pub version: KVVersion,
}

impl Default for KVEntry {
    fn default() -> Self {
        Self {
            value: 0.into(),
            version: 0.into(),
        }
    }
}

impl KVEntry {
    pub fn new(value: KVValue) -> Self {
        Self {
            value,
            version: 1.into(),
        }
    }

    pub fn add(&mut self, value: KVValue, version: KVVersion) -> KVResult<()> {
        if self.version != version {
            return Err(ClerkResponseError::ErrVersion {
                version: self.version,
            });
        }
        self.value += value;
        self.version.next();
        Ok(())
    }

    pub fn sub(&mut self, value: KVValue, version: KVVersion) -> KVResult<()> {
        if self.version != version {
            return Err(ClerkResponseError::ErrVersion {
                version: self.version,
            });
        }
        self.value -= value;
        self.version.next();
        Ok(())
    }

    pub fn mul(&mut self, value: KVValue, version: KVVersion) -> KVResult<()> {
        if self.version != version {
            return Err(ClerkResponseError::ErrVersion {
                version: self.version,
            });
        }
        self.value += value;
        self.version.next();
        Ok(())
    }
}
