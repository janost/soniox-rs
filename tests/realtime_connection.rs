mod support;

use futures_util::{SinkExt, StreamExt};
use support::{recv_text, send_json, spawn_mock};
use tokio_tungstenite::tungstenite::Message;

/// The harness must capture what the client sent and deliver what the server
/// replied. Every later session test leans on both halves of that.
#[tokio::test]
async fn harness_round_trips_a_frame_in_each_direction() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let first = recv_text(&mut ws).await;
        send_json(&mut ws, r#"{"tokens":[],"finished":true}"#).await;
        vec![Message::Text(first.into())]
    })
    .await;

    let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await.unwrap();
    ws.send(Message::Text(r#"{"hello":1}"#.into())).await.unwrap();

    let reply = ws.next().await.unwrap().unwrap();
    assert!(reply.to_text().unwrap().contains("finished"));

    let received = server.await.unwrap();
    assert_eq!(received.len(), 1);
    assert!(received[0].to_text().unwrap().contains("hello"));
}
