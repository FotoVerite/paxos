use crate::cluster::cluster_configuration::{
    ClusterConfiguration, reconfig_errors::ReconfigError, reconfig_patch::ReconfigPatch,
};

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Reconfig(#[from] ReconfigError),
}

pub struct ClusterReconciler {
    next_config: ClusterConfiguration,
}

impl ClusterReconciler {
    pub fn next_config(&self) -> &ClusterConfiguration {
        &self.next_config
    }
}
impl TryFrom<(&ClusterConfiguration, ReconfigPatch)> for ClusterReconciler {
    type Error = ReconcileError;

    fn try_from(
        (prev, patch): (&ClusterConfiguration, ReconfigPatch),
    ) -> Result<Self, Self::Error> {
        let next_config = ClusterConfiguration::try_from((prev, patch))?;
        Ok(ClusterReconciler { next_config })
    }
}
