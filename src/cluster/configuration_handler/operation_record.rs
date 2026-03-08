use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::types::{
    ConfigurationHandlerError, ConfigurationOperationStatus, ConfigurationReplyOutcome,
};

type OperationResult = Result<ConfigurationReplyOutcome, ConfigurationHandlerError>;

pub(crate) struct OperationRecord {
    status: ConfigurationOperationStatus,
    tx: Option<oneshot::Sender<OperationResult>>,
    rx: Option<oneshot::Receiver<OperationResult>>,
    retry_token: CancellationToken,
}

impl OperationRecord {
    pub(crate) fn submitted_with_retry_token(retry_token: CancellationToken) -> Self {
        let (tx, rx) = oneshot::channel();
        Self {
            status: ConfigurationOperationStatus::Submitted,
            tx: Some(tx),
            rx: Some(rx),
            retry_token,
        }
    }

    pub(crate) fn status(&self) -> ConfigurationOperationStatus {
        self.status.clone()
    }

    pub(crate) fn set_status(&mut self, status: ConfigurationOperationStatus) {
        self.status = status;
    }

    pub(crate) fn take_sender(&mut self) -> Option<oneshot::Sender<OperationResult>> {
        self.tx.take()
    }

    pub(crate) fn take_receiver(&mut self) -> Option<oneshot::Receiver<OperationResult>> {
        self.rx.take()
    }

    pub(crate) fn cancel_retry(&self) {
        self.retry_token.cancel();
    }
}
