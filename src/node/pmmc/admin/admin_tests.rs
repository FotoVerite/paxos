use super::*;
use crate::rsm::kv_store::ReplyOutcome;

#[tokio::test]
async fn wait_for_stop_response_ignores_non_matching_messages() {
    let (tx, mut rx) = mpsc::channel(8);
    let request_id = 77;

    tokio::spawn(async move {
        let _ = tx
            .send(ClientMessage::RESPONSE {
                request_id: request_id - 1,
                response: ReplyOutcome::Ok,
            })
            .await;
        let _ = tx
            .send(ClientMessage::PROPOSE {
                cmd: PaxosCommand::NOOP,
            })
            .await;
        let _ = tx
            .send(ClientMessage::RESPONSE {
                request_id,
                response: ReplyOutcome::Ok,
            })
            .await;
    });

    let outcome =
        NodeAdmin::wait_for_stop_response(&mut rx, Instant::now() + Duration::from_secs(1), request_id)
            .await
            .expect("matching stop response should be accepted");

    assert_eq!(outcome, ConfigurationReplyOutcome::Stopped);
}

#[tokio::test]
async fn wait_for_stop_response_times_out_without_match() {
    let (_tx, mut rx) = mpsc::channel(1);
    let result =
        NodeAdmin::wait_for_stop_response(&mut rx, Instant::now() + Duration::from_millis(30), 12)
            .await;
    assert!(matches!(result, Err(ConfigurationHandlerError::Timeout)));
}

#[tokio::test]
async fn wait_for_stop_response_errors_when_channel_closes() {
    let (tx, mut rx) = mpsc::channel(1);
    drop(tx);

    let result =
        NodeAdmin::wait_for_stop_response(&mut rx, Instant::now() + Duration::from_millis(200), 15)
            .await;
    assert!(matches!(
        result,
        Err(ConfigurationHandlerError::ResponseChannelClosed)
    ));
}
