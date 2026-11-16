mod support;

use std::time::Duration;

use futures_util::TryStreamExt;
use soniox::rest::{
    ApiKeyPermission, ConcurrentStreamsPeriod, SharedVoiceFilter, StreamKind,
    TemporaryApiKeyRequest, TemporaryApiKeyUsage, UsageLogsQuery, VoiceGender,
};
use soniox::{
    ApiErrorKind, Client, CreateTranscription, Error, FileId, Timeouts, TranscriptionId,
    TranscriptionStatus, TtsRequest, Upload, VoiceId, WaitOptions,
};
use support::http::{Reply, serve};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use url::Url;

fn client_for(base: &Url) -> Client {
    Client::new("sk-test").with_api_url(base.clone()).with_tts_api_url(base.clone())
}

fn job(status: &str) -> Reply {
    Reply::json(&format!(r#"{{"id":"t-1","status":"{status}","file_id":"f-1"}}"#))
}

#[tokio::test]
async fn requests_carry_the_bearer_key_and_a_listing_decodes_into_a_page() {
    let (base, server) = serve(vec![Reply::json(
        r#"{"files":[{"id":"f-1","filename":"mic.opus","size":1024,"created_at":"2026-11-03T10:00:00Z"}],
            "next_page_cursor":"c-2"}"#,
    )])
    .await;

    let page = client_for(&base).files().list(Some(1), None).await.unwrap();
    assert_eq!(page.items[0].id, FileId::from("f-1"));
    assert_eq!(page.items[0].size, 1024);
    assert_eq!(page.next_page_cursor.as_deref(), Some("c-2"));

    let requests = server.await.unwrap();
    assert_eq!(requests[0].line(), "GET /v1/files?limit=1");
    assert!(requests[0].head.contains("authorization: bearer sk-test"), "{}", requests[0].head);
}

#[tokio::test]
async fn a_non_2xx_response_becomes_a_typed_api_error() {
    let (base, server) = serve(vec![Reply::status(
        403,
        r#"{"status_code":403,"error_type":"permission_denied","message":"Missing files:read.","request_id":"req-42"}"#,
    )])
    .await;

    let err = client_for(&base).files().count().await.expect_err("a 403 must be an error");
    let api = err.api().expect("an API error");
    assert_eq!(api.code, 403);
    assert_eq!(api.kind(), ApiErrorKind::PermissionDenied);
    assert_eq!(api.message, "Missing files:read.");
    assert_eq!(api.request_id.as_deref(), Some("req-42"));
    assert!(!err.is_retryable());
    let _ = server.await.unwrap();
}

#[tokio::test]
async fn an_in_memory_upload_is_a_multipart_file_with_its_name_and_reference_id() {
    let (base, server) = serve(vec![Reply::json(r#"{"id":"f-9"}"#)]).await;

    let upload = Upload::from_bytes("mic.opus", b"OggS-audio-bytes".to_vec());
    let file =
        client_for(&base).files().upload_with_reference_id(upload, "meeting-9").await.unwrap();
    assert_eq!(file.id, FileId::from("f-9"));

    let requests = server.await.unwrap();
    assert_eq!(requests[0].line(), "POST /v1/files");
    let body = requests[0].body_text();
    assert!(body.contains(r#"name="file"; filename="mic.opus""#), "{body}");
    assert!(body.contains("OggS-audio-bytes"), "{body}");
    assert!(body.contains(r#"name="client_reference_id""#) && body.contains("meeting-9"), "{body}");
}

#[tokio::test]
async fn an_upload_from_disk_streams_the_file_under_its_own_name() {
    let dir = std::env::temp_dir().join(format!("soniox-rs-upload-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("clip.wav");
    std::fs::write(&path, b"RIFF-on-disk").unwrap();
    let (base, server) = serve(vec![Reply::json(r#"{"id":"f-9"}"#)]).await;

    let result = client_for(&base).files().upload(Upload::from_path(&path)).await;
    std::fs::remove_dir_all(&dir).unwrap();
    result.unwrap();

    let body = server.await.unwrap()[0].body_text();
    assert!(body.contains(r#"filename="clip.wav""#), "{body}");
    assert!(body.contains("RIFF-on-disk"), "{body}");
}

#[tokio::test]
async fn a_missing_upload_path_is_an_io_error_that_names_it() {
    let client = client_for(&Url::parse("http://127.0.0.1:9/").unwrap());
    let err = client.files().upload(Upload::from_path("/nonexistent/clip.wav")).await.unwrap_err();
    match err {
        Error::Io { path, .. } => assert_eq!(path, "/nonexistent/clip.wav"),
        other => panic!("expected Error::Io, got {other:?}"),
    }
}

#[tokio::test]
async fn creating_a_job_posts_the_request_and_returns_the_job() {
    let (base, server) = serve(vec![job("queued")]).await;

    let request =
        CreateTranscription::from_file(FileId::from("f-1"), "stt-async-v6").with_diarization(true);
    let created = client_for(&base).transcriptions().create(&request).await.unwrap();
    assert_eq!(created.id, TranscriptionId::from("t-1"));
    assert_eq!(created.status, TranscriptionStatus::Queued);

    let requests = server.await.unwrap();
    assert_eq!(requests[0].line(), "POST /v1/transcriptions");
    assert_eq!(
        requests[0].json(),
        serde_json::json!({
            "model": "stt-async-v6", "file_id": "f-1", "enable_speaker_diarization": true,
        })
    );
}

/// The async transcript endpoint omits `is_final` entirely: it only ever
/// serves a completed job, so every token it returns is confirmed. Body is
/// the documented example response, plus one token with an explicit value to
/// show that one is left alone.
#[tokio::test]
async fn transcript_tokens_are_final_unless_the_server_says_otherwise() {
    let (base, server) = serve(vec![Reply::json(
        r#"{"id":"19b6d61d","text":"Hello","tokens":[
             {"text":"Hel","start_ms":10,"end_ms":90,"confidence":0.95},
             {"text":"lo","start_ms":110,"end_ms":160,"confidence":0.98},
             {"text":"?","is_final":false}]}"#,
    )])
    .await;

    let transcript =
        client_for(&base).transcriptions().transcript(&TranscriptionId::from("t-1")).await.unwrap();
    assert_eq!(transcript.text, "Hello");
    let finals: Vec<bool> = transcript.tokens.iter().map(|t| t.is_final()).collect();
    assert_eq!(finals, [true, true, false]);

    assert_eq!(server.await.unwrap()[0].line(), "GET /v1/transcriptions/t-1/transcript");
}

#[tokio::test]
async fn wait_polls_until_the_job_reaches_a_terminal_status() {
    let (base, server) = serve(vec![job("queued"), job("processing"), job("completed")]).await;

    let options = WaitOptions { interval: Duration::from_millis(5), timeout: None };
    let done = client_for(&base)
        .transcriptions()
        .wait(&TranscriptionId::from("t-1"), options)
        .await
        .unwrap();
    assert_eq!(done.status, TranscriptionStatus::Completed);
    assert_eq!(server.await.unwrap().len(), 3);
}

/// A failed job is a result, not a transport error: the caller reads the
/// status, and the job's own error fields say why.
#[tokio::test]
async fn wait_returns_a_failed_job_rather_than_an_error() {
    let (base, server) = serve(vec![Reply::json(
        r#"{"id":"t-1","status":"error","error_type":"invalid_audio","error_message":"unsupported audio"}"#,
    )])
    .await;

    let done = client_for(&base)
        .transcriptions()
        .wait(&TranscriptionId::from("t-1"), WaitOptions::default())
        .await
        .unwrap();
    assert_eq!(done.status, TranscriptionStatus::Error);
    assert_eq!(done.error_type.as_deref(), Some("invalid_audio"));
    let _ = server.await.unwrap();
}

#[tokio::test]
async fn wait_gives_up_at_its_timeout_while_the_job_is_still_running() {
    let (base, server) = serve((0..50).map(|_| job("processing")).collect()).await;

    let options = WaitOptions {
        interval: Duration::from_millis(10),
        timeout: Some(Duration::from_millis(60)),
    };
    let err = client_for(&base)
        .transcriptions()
        .wait(&TranscriptionId::from("t-1"), options)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Timeout(ref what) if what.contains("t-1")), "got {err:?}");
    server.abort();
}

#[tokio::test]
async fn destroy_deletes_the_job_and_then_its_file_and_tolerates_a_file_already_gone() {
    let (base, server) = serve(vec![
        job("completed"),
        Reply::status(204, ""),
        Reply::status(404, r#"{"status_code":404,"error_type":"not_found","message":"No file."}"#),
    ])
    .await;

    client_for(&base).transcriptions().destroy(&TranscriptionId::from("t-1")).await.unwrap();

    let lines: Vec<String> = server.await.unwrap().iter().map(|r| r.line()).collect();
    assert_eq!(
        lines,
        ["GET /v1/transcriptions/t-1", "DELETE /v1/transcriptions/t-1", "DELETE /v1/files/f-1"]
    );
}

#[tokio::test]
async fn list_all_follows_the_cursor_across_pages() {
    let (base, server) = serve(vec![
        Reply::json(
            r#"{"transcriptions":[{"id":"t-1","status":"completed"}],"next_page_cursor":"c 2"}"#,
        ),
        Reply::json(
            r#"{"transcriptions":[{"id":"t-2","status":"queued"}],"next_page_cursor":null}"#,
        ),
    ])
    .await;

    let client = client_for(&base);
    let jobs: Vec<_> = client.transcriptions().list_all().try_collect().await.unwrap();
    assert_eq!(jobs.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(), ["t-1", "t-2"]);

    let lines: Vec<String> = server.await.unwrap().iter().map(|r| r.line()).collect();
    assert_eq!(lines, ["GET /v1/transcriptions", "GET /v1/transcriptions?cursor=c+2"]);
}

#[tokio::test]
async fn rest_tts_posts_to_the_tts_host_and_returns_the_audio_whole_or_streamed() {
    let audio = b"RIFF....WAVEfmt ";
    let (base, server) = serve(vec![Reply::bytes(audio), Reply::bytes(audio)]).await;
    let client = client_for(&base);
    let request = TtsRequest::new("tts-rt-v2", "en", "Adrian", "Hello there");

    let whole = client.tts().generate(&request).await.unwrap();
    assert_eq!(&whole[..], audio);

    let chunks: Vec<_> =
        client.tts().generate_stream(&request).await.unwrap().try_collect().await.unwrap();
    assert_eq!(chunks.concat(), audio);

    let requests = server.await.unwrap();
    assert_eq!(requests[0].line(), "POST /tts", "the TTS endpoint has no v1 prefix");
    assert_eq!(requests[0].json()["text"], "Hello there");
    assert_eq!(requests[0].json()["audio_format"], "wav");
}

/// The TTS REST endpoint reports errors in the websocket field names.
#[tokio::test]
async fn a_rest_tts_error_in_the_websocket_shape_is_still_an_api_error() {
    let (base, server) = serve(vec![Reply::status(
        400,
        r#"{"error_code":400,"error_type":"bad_request","error_message":"unknown voice"}"#,
    )])
    .await;

    let request = TtsRequest::new("tts-rt-v2", "en", "Nobody", "Hello");
    let err = client_for(&base).tts().generate(&request).await.unwrap_err();
    assert_eq!(err.api().map(|e| e.message.as_str()), Some("unknown voice"));
    let _ = server.await.unwrap();
}

#[tokio::test]
async fn shared_voice_filters_become_query_parameters() {
    let (base, server) = serve(vec![Reply::json(
        r#"{"voices":[{"id":"Maya","description":"Warm","gender":"female","age":"young",
                       "accent":"british","use_case":["narration"],"style":["warm"]}]}"#,
    )])
    .await;

    let filter = SharedVoiceFilter {
        gender: Some(VoiceGender::Female),
        accent: Some("british".into()),
        use_case: vec!["narration".into(), "ads".into()],
        ..Default::default()
    };
    let page =
        client_for(&base).voices().list_shared("tts-rt-v2", &filter, Some(10), None).await.unwrap();
    assert_eq!(page.items[0].id, "Maya");
    assert_eq!(page.next_page_cursor, None);

    assert_eq!(
        server.await.unwrap()[0].line(),
        "GET /v1/shared-voices?model=tts-rt-v2&gender=female&accent=british\
         &use_case=narration&use_case=ads&limit=10"
    );
}

#[tokio::test]
async fn cloning_a_voice_uploads_the_clip_with_its_name() {
    let (base, server) = serve(vec![Reply::json(
        r#"{"id":"v-1","name":"Narrator","filename":"clip.wav","models":[]}"#,
    )])
    .await;

    let clip = Upload::from_bytes("clip.wav", b"RIFF-clip".to_vec());
    let voice = client_for(&base).voices().create("Narrator", clip).await.unwrap();
    assert_eq!(voice.id, VoiceId::from("v-1"));

    let requests = server.await.unwrap();
    assert_eq!(requests[0].line(), "POST /v1/voices");
    let body = requests[0].body_text();
    assert!(body.contains(r#"name="name""#) && body.contains("Narrator"), "{body}");
    assert!(body.contains(r#"name="file"; filename="clip.wav""#), "{body}");
}

/// Pins the method and path of every endpoint not exercised above. The
/// replies are empty objects: response types decode leniently, so this is
/// about routing alone.
#[tokio::test]
async fn every_remaining_endpoint_is_routed_to_its_documented_path() {
    let start = chrono::DateTime::parse_from_rfc3339("2026-11-01T00:00:00Z").unwrap().to_utc();
    let end = chrono::DateTime::parse_from_rfc3339("2026-11-02T00:00:00Z").unwrap().to_utc();
    let window = "start_time=2026-11-01T00%3A00%3A00Z&end_time=2026-11-02T00%3A00%3A00Z";
    let key = r#"{"id":"k-1","key":"sk-new"}"#;
    let temporary = r#"{"api_key":"temp:1"}"#;
    let voice = r#"{"id":"v-1"}"#;
    let file = r#"{"id":"f-1"}"#;

    let expected = [
        ("GET /v1/files/f-1".to_owned(), file),
        ("DELETE /v1/files/f-1".to_owned(), ""),
        ("GET /v1/transcriptions/count".to_owned(), "{}"),
        ("GET /v1/models".to_owned(), "{}"),
        ("GET /v1/tts-models".to_owned(), "{}"),
        ("GET /v1/voices/v-1".to_owned(), voice),
        ("GET /v1/voices/count".to_owned(), r#"{"total":3}"#),
        ("POST /v1/voices/v-1/recompute".to_owned(), voice),
        ("DELETE /v1/voices/v-1".to_owned(), ""),
        ("POST /v1/auth/temporary-api-key".to_owned(), temporary),
        (format!("GET /v1/usage-logs?{window}&sort=end_time_asc"), "{}"),
        (format!("GET /v1/usage/summary?{window}"), "{}"),
        ("GET /v1/concurrency-limits".to_owned(), "{}"),
        (format!("GET /v1/concurrent-streams-history?{window}&period_sec=3600&kind=tts"), "{}"),
        ("GET /v1/organization".to_owned(), "{}"),
        ("GET /v1/organization/balance".to_owned(), "{}"),
        ("GET /v1/organization/limits".to_owned(), "{}"),
        ("GET /v1/organization/usage/monthly?month=2026-10".to_owned(), "{}"),
        (format!("GET /v1/organization/usage/summary?{window}"), "{}"),
        ("GET /v1/organization/projects".to_owned(), "{}"),
        ("GET /v1/organization/projects/p-1/api-keys".to_owned(), "{}"),
        ("POST /v1/organization/projects/p-1/api-keys".to_owned(), key),
        ("DELETE /v1/organization/projects/p-1/api-keys/k-1".to_owned(), ""),
    ];
    let (base, server) = serve(expected.iter().map(|(_, body)| Reply::json(body)).collect()).await;
    let client = client_for(&base);
    let (file_id, voice_id) = (FileId::from("f-1"), VoiceId::from("v-1"));

    client.files().get(&file_id).await.unwrap();
    client.files().delete(&file_id).await.unwrap();
    client.transcriptions().count().await.unwrap();
    client.models().list().await.unwrap();
    client.models().list_tts().await.unwrap();
    client.voices().get(&voice_id).await.unwrap();
    assert_eq!(client.voices().count().await.unwrap(), 3);
    client.voices().recompute(&voice_id, Some("tts-rt-v3")).await.unwrap();
    client.voices().delete(&voice_id).await.unwrap();
    let request = TemporaryApiKeyRequest::new(TemporaryApiKeyUsage::TranscribeWebsocket, 60);
    client.auth().create_temporary_api_key(&request).await.unwrap();
    client.usage().logs(&UsageLogsQuery::new(start, end), None).await.unwrap();
    client.usage().summary(start, end).await.unwrap();
    client.usage().concurrency_limits().await.unwrap();
    client
        .usage()
        .concurrent_streams_history(start, end, ConcurrentStreamsPeriod::Hour, StreamKind::Tts)
        .await
        .unwrap();
    let organization = client.organization();
    organization.get().await.unwrap();
    organization.balance().await.unwrap();
    organization.limits().await.unwrap();
    organization.monthly_usage(Some("2026-10")).await.unwrap();
    organization.usage_summary(start, end).await.unwrap();
    organization.projects().await.unwrap();
    organization.list_project_api_keys("p-1").await.unwrap();
    organization
        .create_project_api_key("p-1", "ci", &[ApiKeyPermission::SttAsyncWrite])
        .await
        .unwrap();
    organization.delete_project_api_key("p-1", "k-1").await.unwrap();

    let requests = server.await.unwrap();
    let lines: Vec<String> = requests.iter().map(|r| r.line()).collect();
    let wanted: Vec<&str> = expected.iter().map(|(line, _)| line.as_str()).collect();
    assert_eq!(lines, wanted);
    assert_eq!(requests[7].json(), serde_json::json!({"model": "tts-rt-v3"}));
    assert_eq!(
        requests[21].json(),
        serde_json::json!({"name": "ci", "permissions": ["stt_async:write"]})
    );
}

/// A server that accepts the connection and then says nothing must not hold
/// the caller open. A poll loop's own deadline is only checked between
/// requests, so without a per-request budget one wedged request hangs it.
#[tokio::test]
async fn a_request_that_never_answers_gives_up_rather_than_hanging() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Accept, read the request, then hold the socket open forever.
    let _accepting = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await;
        std::future::pending::<()>().await;
        drop(stream);
    });

    let short = Duration::from_millis(200);
    let client = client_for(&Url::parse(&format!("http://{addr}/")).unwrap())
        .with_timeouts(Timeouts { connect: short, request: short, transfer: short });
    let jobs = client.transcriptions();
    let id = TranscriptionId::from("t-1");
    let err = tokio::time::timeout(Duration::from_secs(5), jobs.get(&id))
        .await
        .expect("the request must give up on its own, not sit there")
        .expect_err("a server that never answers must surface as an error");
    assert!(matches!(err, Error::Http(_)), "got {err:?}");
    assert!(err.is_retryable());
}

/// A server that drains a request body continuously, never silent for longer
/// than `gap`, and answers only once it has the whole thing. Stands in for a
/// slow uplink, which is not the same thing as a stalled one.
async fn slow_reader(gap: Duration) -> Url {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.split();
        let mut reader = tokio::io::BufReader::new(reader);
        let mut length = 0usize;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                return;
            }
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = value.trim().parse().unwrap_or(0);
            }
        }
        let mut read = 0usize;
        let mut chunk = vec![0u8; 64 * 1024];
        while read < length {
            let want = chunk.len().min(length - read);
            match reader.read(&mut chunk[..want]).await {
                Ok(0) | Err(_) => break,
                Ok(n) => read += n,
            }
            tokio::time::sleep(gap).await;
        }
        let body = r#"{"id":"file-1"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = writer.write_all(response.as_bytes()).await;
        let _ = writer.shutdown().await;
    });
    Url::parse(&format!("http://{addr}/")).unwrap()
}

/// An upload's duration scales with the audio, not with the round trip, so it
/// runs under the transfer budget. The server here is never silent for more
/// than 50 ms yet takes over a second in total: the request budget would kill
/// it for being slow, not for being stuck.
#[tokio::test]
async fn a_slow_upload_runs_under_the_transfer_budget_not_the_request_budget() {
    // 2 MB drained 64 KB at a time with a 50 ms pause between reads.
    let payload = vec![0u8; 2 * 1024 * 1024];
    let gap = Duration::from_millis(50);
    let short = Duration::from_millis(400);
    let budgets = |transfer| Timeouts { connect: Duration::from_secs(5), request: short, transfer };

    let strict = client_for(&slow_reader(gap).await).with_timeouts(budgets(short));
    let err = strict
        .files()
        .upload(Upload::from_bytes("mic.opus", payload.clone()))
        .await
        .expect_err("a short budget kills an upload that is merely slow");
    assert!(matches!(err, Error::Http(_)), "got {err:?}");

    let generous =
        client_for(&slow_reader(gap).await).with_timeouts(budgets(Duration::from_secs(30)));
    let file = generous
        .files()
        .upload(Upload::from_bytes("mic.opus", payload))
        .await
        .expect("the transfer budget lets the same upload through");
    assert_eq!(file.id, FileId::from("file-1"));
}
