mod support;

use soniox::{Client, SttConfig};
use support::{recv_any, recv_text, send_json, spawn_mock, spawn_mock_with_headers};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

fn client_for(url: &str) -> Client {
    Client::new("sk-test").with_stt_url(url::Url::parse(url).unwrap())
}

#[tokio::test]
async fn sends_the_key_on_the_handshake_and_the_config_frame_before_any_audio() {
    let (url, server, headers) = spawn_mock_with_headers(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let audio = recv_any(&mut ws).await;
        vec![Message::Text(cfg.into()), audio]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1)
        .with_language_hints(vec!["hu".into(), "en".into()]);
    let session = client_for(&url).connect_stt(&config).await.unwrap();
    let (mut sink, _events) = session.split();
    sink.send_audio(vec![1, 2, 3, 4]).await.unwrap();

    let headers = headers.await.expect("the handshake happened");
    let auth = headers.get(AUTHORIZATION).expect("an Authorization header");
    assert_eq!(auth.to_str().unwrap(), "Bearer sk-test");

    let got = server.await.unwrap();
    let cfg: serde_json::Value = serde_json::from_str(got[0].to_text().unwrap()).unwrap();
    assert!(cfg.get("api_key").is_none(), "deprecated field must not be sent: {cfg}");
    assert_eq!(cfg["model"], "stt-rt-v6");
    assert_eq!(cfg["audio_format"], "pcm_s16le");
    assert_eq!(cfg["sample_rate"], 16_000);
    assert_eq!(cfg["num_channels"], 1);
    assert_eq!(cfg["language_hints"][1], "en");

    match &got[1] {
        Message::Binary(b) => assert_eq!(b.as_ref(), &[1, 2, 3, 4]),
        other => panic!("audio must be a binary frame, got {other:?}"),
    }
}

#[tokio::test]
async fn surfaces_partial_then_final_tokens() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        send_json(&mut ws, r#"{"tokens":[{"text":" szia","is_final":false}]}"#).await;
        send_json(&mut ws, r#"{"tokens":[{"text":" szia","is_final":true}]}"#).await;
        send_json(&mut ws, r#"{"tokens":[],"finished":true}"#).await;
        vec![Message::Text(cfg.into())]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1);
    let (_sink, mut events) = client_for(&url).connect_stt(&config).await.unwrap().split();

    let first = events.next().await.unwrap().unwrap();
    assert!(!first.tokens[0].is_final());

    let second = events.next().await.unwrap().unwrap();
    assert!(second.tokens[0].is_final());
    assert_eq!(second.tokens[0].text, " szia", "leading space is preserved");

    let last = events.next().await.unwrap().unwrap();
    assert!(last.finished);

    let _ = server.await.unwrap();
}

#[tokio::test]
async fn finalize_sends_the_control_frame_and_the_fin_marker_comes_back() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let control = recv_text(&mut ws).await;
        send_json(&mut ws, r#"{"tokens":[{"text":"<fin>","is_final":true}]}"#).await;
        vec![Message::Text(cfg.into()), Message::Text(control.into())]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1);
    let (mut sink, mut events) = client_for(&url).connect_stt(&config).await.unwrap().split();
    sink.finalize().await.unwrap();

    let ev = events.next().await.unwrap().unwrap();
    assert_eq!(ev.tokens[0].marker(), Some(soniox::Marker::Finalized));
    assert!(!ev.tokens[0].is_text());

    let got = server.await.unwrap();
    assert_eq!(got[1].to_text().unwrap(), r#"{"type":"finalize"}"#);
}

#[tokio::test]
async fn finish_sends_an_empty_text_frame() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let end = recv_any(&mut ws).await;
        vec![Message::Text(cfg.into()), end]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1);
    let (mut sink, _events) = client_for(&url).connect_stt(&config).await.unwrap().split();
    sink.finish().await.unwrap();

    let got = server.await.unwrap();
    // End-of-audio is an empty TEXT frame, matching the upstream Python and JS
    // clients (`ws.send("")`). An empty BINARY frame is indistinguishable from
    // zero bytes of audio: the server keeps the stream open waiting for more,
    // never emits `finished: true`, and the caller sits until its own timeout.
    match &got[1] {
        Message::Text(t) => {
            assert!(t.as_str().is_empty(), "end of audio is a zero-length text frame")
        }
        other => panic!("expected an empty text frame, got {other:?}"),
    }
}

#[tokio::test]
async fn an_error_frame_becomes_an_api_error() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        send_json(
            &mut ws,
            r#"{"error_code":401,"error_type":"unauthorized","error_message":"bad key"}"#,
        )
        .await;
        vec![Message::Text(cfg.into())]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1);
    let (_sink, mut events) = client_for(&url).connect_stt(&config).await.unwrap().split();

    match events.next().await.unwrap() {
        Err(soniox::Error::Api(e)) => {
            assert_eq!(e.code, 401);
            assert_eq!(e.message, "bad key");
        }
        other => panic!("expected Error::Api, got {other:?}"),
    }

    let _ = server.await.unwrap();
}

#[tokio::test]
async fn an_abrupt_close_ends_the_stream_without_panicking() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        drop(ws);
        vec![Message::Text(cfg.into())]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1);
    let (_sink, mut events) = client_for(&url).connect_stt(&config).await.unwrap().split();

    // Either a transport error or a clean end is acceptable; a panic is not.
    match events.next().await {
        None | Some(Err(_)) => {}
        Some(Ok(ev)) => panic!("expected the stream to end, got {ev:?}"),
    }

    let _ = server.await.unwrap();
}

#[tokio::test]
async fn agent_text_goes_out_as_a_typed_frame() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let first = recv_text(&mut ws).await;
        let second = recv_text(&mut ws).await;
        vec![Message::Text(cfg.into()), Message::Text(first.into()), Message::Text(second.into())]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1).with_agent_text_input(true);
    let (mut sink, _events) = client_for(&url).connect_stt(&config).await.unwrap().split();
    sink.send_agent_text("Your code is 42 ", false).await.unwrap();
    sink.send_agent_text("", true).await.unwrap();

    let got = server.await.unwrap();
    let cfg: serde_json::Value = serde_json::from_str(got[0].to_text().unwrap()).unwrap();
    assert_eq!(cfg["enable_agent_text_input"], true);
    let first: serde_json::Value = serde_json::from_str(got[1].to_text().unwrap()).unwrap();
    assert_eq!(
        first,
        serde_json::json!({"type": "agent_text", "text": "Your code is 42 ", "end_of_segment": false})
    );
    let second: serde_json::Value = serde_json::from_str(got[2].to_text().unwrap()).unwrap();
    assert_eq!(second["end_of_segment"], true, "an empty piece can close the segment");
}

/// The server refuses agent text on a session that did not ask for it, with
/// an error that ends the session. Catching it here keeps the session alive.
#[tokio::test]
async fn agent_text_is_rejected_locally_when_the_session_did_not_enable_it() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let sentinel = recv_text(&mut ws).await;
        vec![Message::Text(cfg.into()), Message::Text(sentinel.into())]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1);
    let (mut sink, _events) = client_for(&url).connect_stt(&config).await.unwrap().split();
    assert!(matches!(sink.send_agent_text("hi", true).await, Err(soniox::Error::InvalidInput(_))));
    sink.keepalive().await.unwrap();

    let got = server.await.unwrap();
    assert_eq!(got[1].to_text().unwrap(), r#"{"type":"keepalive"}"#, "nothing else was sent");
}

#[tokio::test]
async fn a_raw_format_without_a_sample_rate_never_opens_a_connection() {
    // Nothing listens here; a connect attempt would fail with a different error.
    let client = client_for("ws://127.0.0.1:9/");
    let config = SttConfig::new("stt-rt-v6").with_audio_format(soniox::AudioFormat::PcmS16Le);
    assert!(matches!(client.connect_stt(&config).await, Err(soniox::Error::InvalidInput(_))));
}

#[tokio::test]
async fn the_event_stream_is_a_futures_stream() {
    use futures_util::{StreamExt, TryStreamExt};

    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        send_json(&mut ws, r#"{"tokens":[{"text":"a","is_final":true}]}"#).await;
        send_json(&mut ws, r#"{"tokens":[],"finished":true}"#).await;
        vec![Message::Text(cfg.into())]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1);
    let (_sink, events) = client_for(&url).connect_stt(&config).await.unwrap().split();
    // The mock drops the socket without a closing handshake, so stop at the
    // two events it sent rather than reading into the reset.
    let all: Vec<soniox::Event> = events.take(2).try_collect().await.unwrap();
    assert_eq!(all.len(), 2);
    assert!(all[1].finished);

    let _ = server.await.unwrap();
}

#[tokio::test]
async fn finalize_can_declare_the_trailing_silence_already_sent() {
    let (url, server) = spawn_mock(|mut ws| async move {
        let cfg = recv_text(&mut ws).await;
        let control = recv_text(&mut ws).await;
        vec![Message::Text(cfg.into()), Message::Text(control.into())]
    })
    .await;

    let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1);
    let (mut sink, _events) = client_for(&url).connect_stt(&config).await.unwrap().split();
    sink.finalize_with_trailing_silence(300).await.unwrap();

    let got = server.await.unwrap();
    let control: serde_json::Value = serde_json::from_str(got[1].to_text().unwrap()).unwrap();
    assert_eq!(control, serde_json::json!({"type": "finalize", "trailing_silence_ms": 300}));
}
