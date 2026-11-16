mod support;

use soniox::{Client, TtsConfig};
use support::{recv_text, send_json, spawn_mock, spawn_mock_with_headers};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

fn client_for(url: &str) -> Client {
    Client::new("sk-test").with_tts_url(url::Url::parse(url).unwrap())
}

fn config() -> TtsConfig {
    TtsConfig::pcm_s16le("stream-1", "tts-rt-v2", "hu", "Adrian", 24_000)
}

#[tokio::test]
async fn sends_the_key_on_the_handshake_and_the_config_frame_then_the_text_sequence() {
    let (url, server, headers) = spawn_mock_with_headers(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let first = recv_text(&mut ws).await;
        let second = recv_text(&mut ws).await;
        vec![Message::Text(cfg.into()), Message::Text(first.into()), Message::Text(second.into())]
    })
    .await;

    let mut session = client_for(&url).connect_tts(&config()).await.unwrap();

    let headers = headers.await.expect("the handshake happened");
    let auth = headers.get(AUTHORIZATION).expect("an Authorization header");
    assert_eq!(auth.to_str().unwrap(), "Bearer sk-test");

    session.send_text("Ez az elso resz.", false).await.unwrap();
    session.send_text(" Ez a masodik.", true).await.unwrap();

    let got = server.await.unwrap();
    let cfg: serde_json::Value = serde_json::from_str(got[0].to_text().unwrap()).unwrap();
    assert!(cfg.get("api_key").is_none(), "deprecated field must not be sent: {cfg}");
    assert_eq!(cfg["stream_id"], "stream-1");
    assert_eq!(cfg["model"], "tts-rt-v2");
    assert_eq!(cfg["language"], "hu");
    assert_eq!(cfg["voice"], "Adrian");
    assert_eq!(cfg["audio_format"], "pcm_s16le");
    assert_eq!(cfg["sample_rate"], 24_000);

    let a: serde_json::Value = serde_json::from_str(got[1].to_text().unwrap()).unwrap();
    let b: serde_json::Value = serde_json::from_str(got[2].to_text().unwrap()).unwrap();
    assert_eq!(a["text"], "Ez az elso resz.");
    assert_eq!(a["text_end"], false);
    assert_eq!(b["text_end"], true, "only the last chunk ends the utterance");
    assert_eq!(a["stream_id"], "stream-1");
}

#[tokio::test]
async fn decodes_streamed_audio_and_reports_the_end_flags() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        send_json(&mut ws, r#"{"stream_id":"stream-1","audio":"AAECAw=="}"#).await;
        send_json(&mut ws, r#"{"stream_id":"stream-1","audio":"BAUGBw=="}"#).await;
        send_json(&mut ws, r#"{"stream_id":"stream-1","audio_end":true}"#).await;
        send_json(&mut ws, r#"{"stream_id":"stream-1","terminated":true}"#).await;
        vec![Message::Text(cfg.into())]
    })
    .await;

    let mut session = client_for(&url).connect_tts(&config()).await.unwrap();

    let first = session
        .next_audio()
        .await
        .expect("server closed early; check the mock handler's assertions")
        .unwrap();
    assert_eq!(first.audio, vec![0, 1, 2, 3]);
    let second = session
        .next_audio()
        .await
        .expect("server closed early; check the mock handler's assertions")
        .unwrap();
    assert_eq!(second.audio, vec![4, 5, 6, 7]);

    let end = session
        .next_audio()
        .await
        .expect("server closed early; check the mock handler's assertions")
        .unwrap();
    assert!(end.audio_end);
    assert!(end.audio.is_empty());

    let done = session
        .next_audio()
        .await
        .expect("server closed early; check the mock handler's assertions")
        .unwrap();
    assert!(done.terminated);

    let _ = server.await.unwrap();
}

#[tokio::test]
async fn cancel_sends_the_cancel_frame_for_the_stream() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let cancel = recv_text(&mut ws).await;
        vec![Message::Text(cfg.into()), Message::Text(cancel.into())]
    })
    .await;

    let mut session = client_for(&url).connect_tts(&config()).await.unwrap();
    session.cancel().await.unwrap();

    let got = server.await.unwrap();
    let v: serde_json::Value = serde_json::from_str(got[1].to_text().unwrap()).unwrap();
    assert_eq!(v["stream_id"], "stream-1");
    assert_eq!(v["cancel"], true);
}

#[tokio::test]
async fn keepalive_sends_the_keep_alive_frame() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let keepalive = recv_text(&mut ws).await;
        vec![Message::Text(cfg.into()), Message::Text(keepalive.into())]
    })
    .await;

    let mut session = client_for(&url).connect_tts(&config()).await.unwrap();
    session.keepalive().await.unwrap();

    let got = server.await.unwrap();
    let v: serde_json::Value = serde_json::from_str(got[1].to_text().unwrap()).unwrap();
    assert_eq!(v["keep_alive"], true);
}

#[tokio::test]
async fn an_error_frame_becomes_an_api_error() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        send_json(
            &mut ws,
            r#"{"error_code":400,"error_type":"invalid_request","error_message":"unknown voice"}"#,
        )
        .await;
        vec![Message::Text(cfg.into())]
    })
    .await;

    let mut session = client_for(&url).connect_tts(&config()).await.unwrap();
    match session
        .next_audio()
        .await
        .expect("server closed early; check the mock handler's assertions")
    {
        Err(soniox::Error::Api(e)) => {
            assert_eq!(e.code, 400);
            assert_eq!(e.message, "unknown voice");
        }
        other => panic!("expected Error::Api, got {other:?}"),
    }

    let _ = server.await.unwrap();
}

#[tokio::test]
async fn text_over_the_limit_is_rejected_before_it_reaches_the_wire() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let sentinel = recv_text(&mut ws).await;
        vec![Message::Text(cfg.into()), Message::Text(sentinel.into())]
    })
    .await;

    let mut session = client_for(&url).connect_tts(&config()).await.unwrap();
    let too_long = "a".repeat(soniox::MAX_TEXT_CHARS + 1);
    match session.send_text(&too_long, true).await {
        Err(soniox::Error::InvalidInput(m)) => assert!(m.contains("5000")),
        other => panic!("expected InvalidInput, got {other:?}"),
    }
    // A sentinel proves the rejected call sent nothing: if `send_text` had
    // truncated and sent the oversized payload anyway, this would be the
    // second frame the server receives instead of the sentinel.
    session.send_text("ok", true).await.unwrap();

    let got = server.await.unwrap();
    let sentinel: serde_json::Value = serde_json::from_str(got[1].to_text().unwrap()).unwrap();
    assert_eq!(
        sentinel["text"], "ok",
        "nothing beyond the config frame may be sent for the rejected call"
    );
}

#[tokio::test]
async fn finish_ends_the_utterance_with_an_empty_final_chunk() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let end = recv_text(&mut ws).await;
        vec![Message::Text(cfg.into()), Message::Text(end.into())]
    })
    .await;

    let mut session = client_for(&url).connect_tts(&config()).await.unwrap();
    session.finish().await.unwrap();

    let got = server.await.unwrap();
    let end: serde_json::Value = serde_json::from_str(got[1].to_text().unwrap()).unwrap();
    assert_eq!(end, serde_json::json!({"stream_id": "stream-1", "text": "", "text_end": true}));
}
