use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;

use crate::cluster::cluster_configuration::reconfig_patch::ReconfigPatch;
use crate::common::persistence::Persistence;
use crate::monitor::{NoOpObserver, PaxosObserver};
use crate::node::config::PmmcNodeConfig;
use crate::paxos_command::PaxosCommand;
use crate::rsm::kv_store::ReplyOutcome;

use super::PmmcCluster;

static NEXT_TEST_IP: AtomicU32 = AtomicU32::new(1);

fn unique_test_ip() -> IpAddr {
    let seq = NEXT_TEST_IP.fetch_add(1, Ordering::Relaxed);
    let octet3 = ((seq / 250) % 250 + 1) as u8;
    let octet4 = (seq % 250 + 1) as u8;
    IpAddr::V4(Ipv4Addr::new(127, 120, octet3, octet4))
}

pub struct PmmcTestCluster {
    ip: IpAddr,
    cluster: PmmcCluster,
}

impl PmmcTestCluster {
    pub async fn new(node_count: usize) -> Result<Self> {
        Self::new_with_observer(node_count, Arc::new(NoOpObserver)).await
    }

    pub async fn with_configs(configs: Vec<PmmcNodeConfig>) -> Result<Self> {
        Self::with_configs_and_observer(configs, Arc::new(NoOpObserver)).await
    }

    pub async fn new_with_observer(
        node_count: usize,
        observer: Arc<dyn PaxosObserver>,
    ) -> Result<Self> {
        let ip = unique_test_ip();
        Persistence::cluster(ip).purge_cluster_dir().await?;
        let mut cluster = PmmcCluster::new(ip, node_count, observer).await?;
        cluster.set_cleanup_on_drop(true);
        Ok(Self { ip, cluster })
    }

    pub async fn with_configs_and_observer(
        configs: Vec<PmmcNodeConfig>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Result<Self> {
        let ip = unique_test_ip();
        Persistence::cluster(ip).purge_cluster_dir().await?;
        let mut cluster = PmmcCluster::new_with_configs(ip, configs, observer).await?;
        cluster.set_cleanup_on_drop(true);
        Ok(Self { ip, cluster })
    }

    pub fn cleanup_on_drop(mut self, enabled: bool) -> Self {
        self.cluster.set_cleanup_on_drop(enabled);
        self
    }

    pub async fn start_ready(&mut self, timeout: Duration) -> Result<&mut Self> {
        self.cluster.start_all_ready(timeout).await?;
        Ok(self)
    }

    pub async fn reconfigure_and_ready(
        &mut self,
        patch: ReconfigPatch,
        timeout_duration: Duration,
    ) -> Result<&mut Self> {
        self.cluster
            .update_configuration_ready(patch, timeout_duration)
            .await?;
        Ok(self)
    }

    pub async fn request_any_replica(
        &self,
        request_id: u64,
        cmd: PaxosCommand,
        timeout_duration: Duration,
    ) -> Result<ReplyOutcome> {
        self.cluster
            .request_any_replica(request_id, cmd, timeout_duration)
            .await
    }

    pub fn ip(&self) -> IpAddr {
        self.ip
    }

    pub fn cluster(&self) -> &PmmcCluster {
        &self.cluster
    }

    pub fn cluster_mut(&mut self) -> &mut PmmcCluster {
        &mut self.cluster
    }
}
