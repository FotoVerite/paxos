use std::net::IpAddr;

use uuid::Uuid;

use crate::{
    common::network_fabric::NetworkFailure,
    monitor::{Event, current_timestamp_millis},
};

use super::PmmcCluster;

impl PmmcCluster {
    pub async fn enable_failures(&self) {
        self.fabric.set_enabled(true).await;
    }

    pub async fn disable_failures(&self) {
        self.fabric.set_enabled(false).await;
    }

    pub async fn partition(&self, node1: usize, node2: usize) {
        if let (Some(node1), Some(node2)) = (
            self.configuration.member(node1),
            self.configuration.member(node2),
        ) {
            self.fabric
                .set_failure(node1, node2, NetworkFailure::Partition)
                .await;
            self.fabric
                .set_failure(node2, node1, NetworkFailure::Partition)
                .await;
        }
    }

    pub async fn heal_partition(&self, node1: usize, node2: usize) {
        if let (Some(node1), Some(node2)) = (
            self.configuration.member(node1),
            self.configuration.member(node2),
        ) {
            self.fabric.clear_failure(node1, node2).await;
            self.fabric.clear_failure(node2, node1).await;
        }
    }

    pub async fn crash_node(&self, node: usize) -> Option<Uuid> {
        let crashed_uuid = self.configuration.member(node)?;
        if !self.runtime_registry.crash_node(crashed_uuid).await {
            return None;
        }
        self.observer.on_event(Event::NodeCrashed {
            id: crashed_uuid,
            created_at: current_timestamp_millis(),
        });

        Some(crashed_uuid)
    }

    pub async fn isolate_node(&self, node: usize) -> Option<Uuid> {
        let isolated_uuid = self.configuration.member(node)?;
        if !self.runtime_registry.isolate_node(isolated_uuid).await {
            return None;
        }
        Some(isolated_uuid)
    }

    pub async fn heal_node(&self, node: usize) -> Option<Uuid> {
        let to_heal = self.configuration.member(node)?;
        if !self.runtime_registry.heal_node(to_heal).await {
            return None;
        }
        Some(to_heal)
    }

    pub fn set_cleanup_on_drop(&mut self, enabled: bool) {
        self.cleanup_on_drop = enabled;
    }

    pub async fn cleanup(&mut self) -> anyhow::Result<()> {
        self.runtime_registry.reset().await;
        self.persistence.purge_cluster_dir().await?;
        Ok(())
    }

    pub fn node_uuid(ip: IpAddr, node_id: usize) -> Uuid {
        let namespace = Uuid::NAMESPACE_DNS;
        let name = format!("{}:pmmc:{}", ip, node_id);
        Uuid::new_v5(&namespace, name.as_bytes())
    }
}
