use crate::rsm::types::{ClerkResponseError, KVResult};

pub struct KVEntry {
    pub value: usize,
    pub version: usize,
}

impl Default for KVEntry {
    fn default() -> Self {
        Self {
            value: 0,
            version: 0,
        }
    }
}

impl KVEntry {
    pub fn new(value: usize) -> Self {
        Self { value, version: 1 }
    }
    

    pub fn add(&mut self, value: usize, version: usize) -> KVResult<()> {
        if self.version != version {
            return Err(ClerkResponseError::ErrVersion {
                version: self.version,
            });
        }
        self.value += value;
        self.version += 1;
        Ok(())
    }

    pub fn sub(&mut self, value: usize, version: usize) -> KVResult<()> {
        if self.version != version {
            return Err(ClerkResponseError::ErrVersion {
                version: self.version,
            });
        }
        self.value -= value;
        self.version += 1;
        Ok(())
    }

    pub fn mul(&mut self, value: usize, version: usize) -> KVResult<()> {
        if self.version != version {
            return Err(ClerkResponseError::ErrVersion {
                version: self.version,
            });
        }
        self.value *= value;
        self.version += 1;
        Ok(())
    }
}
