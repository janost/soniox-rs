mod support;

use soniox::{Client, MAX_STREAMS_PER_CONNECTION, TtsConfig};
use support::{recv_text, send_json, spawn_mock};
use tokio_tungstenite::tungstenite::Message;

fn client_for(url: &str) -> Client {
    Client::new("sk-test").with_tts_url(url::Url::parse(url).unwrap())
}

fn config(stream_id: &str) -> TtsConfig {
    TtsConfig::pcm_s16le(stream_id, "tts-rt-v2", "en", "Adrian", 24_000)
}

#[tokio::test]
async fn interleaved_events_reach_the_stream_they_name() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let a = recv_text(&mut ws).await;
        let b = recv_text(&mut ws).await;
        send_json(&mut ws, r#"{"stream_id":"b","audio":"BAUGBw=="}"#).await;
        send_json(&mut ws, r#"{"stream_id":"a","audio":"AAECAw=="}"#).await;
        send_json(&mut ws, r#"{"stream_id":"a","terminated":true}"#).await;
        send_json(&mut ws, r#"{"stream_id":"b","terminated":true}"#).await;
        vec![Message::Text(a.into()), Message::Text(b.into())]
    })
    .await;

    let connection = client_for(&url).connect_tts_multi().await.unwrap();
    let mut a = connection.open_stream(&config("a")).await.unwrap();
    let mut b = connection.open_stream(&config("b")).await.unwrap();

    // Read `a` to its end first, although `b`'s audio arrived before it.
    assert_eq!(a.next_audio().await.unwrap().unwrap().audio, vec![0, 1, 2, 3]);
    assert!(a.next_audio().await.unwrap().unwrap().terminated);
    assert!(a.next_audio().await.is_none(), "a terminated stream is over");

    assert_eq!(b.next_audio().await.unwrap().unwrap().audio, vec![4, 5, 6, 7]);
    assert!(b.next_audio().await.unwrap().unwrap().terminated);

    let configs = server.await.unwrap();
    let first: serde_json::Value = serde_json::from_str(configs[0].to_text().unwrap()).unwrap();
    assert_eq!(first["stream_id"], "a");
    assert!(first.get("api_key").is_none());
}

#[tokio::test]
async fn text_and_cancel_frames_name_their_stream() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let _config = recv_text(&mut ws).await;
        let text = recv_text(&mut ws).await;
        let end = recv_text(&mut ws).await;
        let cancel = recv_text(&mut ws).await;
        vec![Message::Text(text.into()), Message::Text(end.into()), Message::Text(cancel.into())]
    })
    .await;

    let connection = client_for(&url).connect_tts_multi().await.unwrap();
    let stream = connection.open_stream(&config("a")).await.unwrap();
    stream.send_text("Hello", false).await.unwrap();
    stream.finish().await.unwrap();
    stream.cancel().await.unwrap();

    let got = server.await.unwrap();
    let frame = |i: usize| -> serde_json::Value {
        serde_json::from_str(got[i].to_text().unwrap()).unwrap()
    };
    assert_eq!(frame(0), serde_json::json!({"stream_id": "a", "text": "Hello", "text_end": false}));
    assert_eq!(frame(1), serde_json::json!({"stream_id": "a", "text": "", "text_end": true}));
    assert_eq!(frame(2), serde_json::json!({"stream_id": "a", "cancel": true}));
}

#[tokio::test]
async fn an_error_for_one_stream_leaves_the_others_running() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let _a = recv_text(&mut ws).await;
        let _b = recv_text(&mut ws).await;
        send_json(&mut ws, r#"{"stream_id":"a","error_code":400,"error_message":"bad voice"}"#)
            .await;
        send_json(&mut ws, r#"{"stream_id":"b","audio":"AAECAw=="}"#).await;
        // Hold the socket until the client has opened the replacement.
        let c = recv_text(&mut ws).await;
        vec![Message::Text(c.into())]
    })
    .await;

    let connection = client_for(&url).connect_tts_multi().await.unwrap();
    let mut a = connection.open_stream(&config("a")).await.unwrap();
    let mut b = connection.open_stream(&config("b")).await.unwrap();

    let err = a.next_audio().await.unwrap().unwrap_err();
    assert_eq!(err.api().map(|e| e.code), Some(400));
    assert!(a.next_audio().await.is_none(), "a failed stream is over");
    assert_eq!(b.next_audio().await.unwrap().unwrap().audio, vec![0, 1, 2, 3]);

    // The failed stream's id is free again.
    let _again = connection.open_stream(&config("a")).await.unwrap();
    let _ = server.await.unwrap();
}

#[tokio::test]
async fn a_connection_error_reaches_every_stream() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let _a = recv_text(&mut ws).await;
        let _b = recv_text(&mut ws).await;
        // The server sends connection-level errors with an empty stream id.
        send_json(&mut ws, r#"{"stream_id":"","error_code":401,"error_message":"bad key"}"#).await;
        Vec::new()
    })
    .await;

    let connection = client_for(&url).connect_tts_multi().await.unwrap();
    let mut a = connection.open_stream(&config("a")).await.unwrap();
    let mut b = connection.open_stream(&config("b")).await.unwrap();

    for stream in [&mut a, &mut b] {
        let err = stream.next_audio().await.unwrap().unwrap_err();
        assert_eq!(err.api().map(|e| e.code), Some(401), "got {err:?}");
        assert!(stream.next_audio().await.is_none());
    }
    assert!(
        matches!(connection.open_stream(&config("c")).await, Err(soniox::Error::Closed)),
        "a failed connection opens no more streams"
    );
    let _ = server.await.unwrap();
}

#[tokio::test]
async fn duplicate_ids_and_a_sixth_stream_are_rejected_before_the_wire() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let mut configs = Vec::new();
        for _ in 0..MAX_STREAMS_PER_CONNECTION {
            configs.push(Message::Text(recv_text(&mut ws).await.into()));
        }
        configs
    })
    .await;

    let connection = client_for(&url).connect_tts_multi().await.unwrap();
    let mut streams = Vec::new();
    streams.push(connection.open_stream(&config("s0")).await.unwrap());
    assert!(matches!(
        connection.open_stream(&config("s0")).await,
        Err(soniox::Error::InvalidInput(_))
    ));
    for i in 1..MAX_STREAMS_PER_CONNECTION {
        streams.push(connection.open_stream(&config(&format!("s{i}"))).await.unwrap());
    }
    assert!(matches!(
        connection.open_stream(&config("one-too-many")).await,
        Err(soniox::Error::InvalidInput(_))
    ));

    assert_eq!(server.await.unwrap().len(), MAX_STREAMS_PER_CONNECTION);
}
