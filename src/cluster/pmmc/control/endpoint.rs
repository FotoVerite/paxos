use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use super::types::{
    ConfigurationCommand, ConfigurationHandlerError, ConfigurationOperationId,
    ConfigurationOperationStatus, ConfigurationReplyOutcome,
};

#[async_trait]
pub trait ConfigurationEndpoint: Send + Sync {
    async fn submit(
        &self,
        endpoint: Uuid,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationOperationId, ConfigurationHandlerError>;

    async fn await_outcome(
        &self,
        operation_id: ConfigurationOperationId,
        timeout: Duration,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError>;

    async fn status(
        &self,
        operation_id: ConfigurationOperationId,
    ) -> Result<ConfigurationOperationStatus, ConfigurationHandlerError>;
}
