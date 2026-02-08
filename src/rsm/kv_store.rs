use std::collections::{HashMap, hash_map::Entry};

use crate::rsm::{
    entry::KVEntry,
    types::{ClerkResponseError, KVResult},
};

struct KVStore {
    data: HashMap<String, KVEntry>,
}

impl KVStore {
    pub fn init() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> KVResult<(usize, usize)> {
        if let Some(entry) = self.data.get(key) {
            return Ok((entry.value, entry.version));
        }
        return Err(ClerkResponseError::ErrKey);
    }

    pub fn set(&mut self, key: &str, value: usize) -> KVResult<usize> {
        match self.data.entry(key.to_string()) {
            Entry::Occupied(_) => Err(ClerkResponseError::ErrKey),
            Entry::Vacant(v) => Ok(v.insert(KVEntry::new(value)).version),
        }
    }

    pub fn append(&mut self, key: &str, value: usize, version: usize) -> KVResult<usize> {
        match self.data.get_mut(key) {
            Some(entry) => {
                entry.add(value, version)?;
                Ok(entry.version)
            }
            None => Err(ClerkResponseError::ErrKey),
        }
    }

    pub fn delete(&mut self, key: &str) -> KVResult<()> {
        self.data
            .remove(key)
            .map(|_| ())
            .ok_or(ClerkResponseError::ErrKey)
    }
}
