

use tokio::sync::Mutex;

use crate::node::pvalue::PValue;


pub struct PValues {
    gc_index: usize,
    base_index: usize,
    next_idx: usize,
    data: Mutex<Vec<PValue>>,
}

impl PValues {
    fn init(gc_index: usize, data: &[PValue]) -> Self {
        let initial = data.to_vec();
        let len = initial.len();
        Self {
            gc_index,
            data: Mutex::new(initial),
            next_idx: gc_index + len,
        }
    }

    pub async fn append(&mut self, val: PValue) {
        let mut data = self.data.lock().await;
        data.push(val);
        self.next_idx += 1;
    }

    pub async fn next_idx(&self) -> usize {
        let data = self.data.lock().await;
        data.len() + self.gc_index
    }

    pub async fn get(&self, index: usize) -> Option<PValue> {
        let local_index = index - self.gc_index;
        let data = self.data.lock().await;
        data.get(local_index).cloned()
    }

    pub async fn update(&self, index: usize, value: PValue) -> Option<PValue> {
        let local_index = index.checked_sub(self.gc_index)?;

        let mut data = self.data.lock().await;

        let original = data.get(local_index)?.clone();

        if original > value {
            return Some(original);
        }

        data[local_index] = value.clone();
        Some(value)
    }

    pub async fn compact(&self, index: usize) {
        let local_index = index.checked_sub(self.gc_index);

        let mut data = self.data.lock().await;
    }

    pub async fn all(&self) -> Vec<PValue> {
        let data = self.data.lock().await;
        data.clone()
    }

    fn rebuild(&mut self) {
        let offset = self.base_index - self.starting_index_of_vec;
        self.entries = self.entries[offset..].to_vec();
        self.starting_index_of_vec = self.base_index;
    }
}
