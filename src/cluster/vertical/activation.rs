use std::sync::Arc;

use super::configuration::VerticalClusterConfiguration;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PredecessorChain {
    entries: Vec<Arc<VerticalClusterConfiguration>>,
}

impl PredecessorChain {
    pub fn new(entries: Vec<Arc<VerticalClusterConfiguration>>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[Arc<VerticalClusterConfiguration>] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationSnapshot {
    configuration: Arc<VerticalClusterConfiguration>,
    predecessor_chain: Arc<PredecessorChain>,
}

impl ActivationSnapshot {
    pub fn new(
        configuration: Arc<VerticalClusterConfiguration>,
        predecessor_chain: Arc<PredecessorChain>,
    ) -> Self {
        Self {
            configuration,
            predecessor_chain,
        }
    }

    pub fn configuration(&self) -> &Arc<VerticalClusterConfiguration> {
        &self.configuration
    }

    pub fn predecessor_chain(&self) -> &Arc<PredecessorChain> {
        &self.predecessor_chain
    }
}

#[cfg(test)]
mod tests;
