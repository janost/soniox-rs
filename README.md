# soniox-rs

Rust client for the [Soniox](https://soniox.com) speech APIs. Soniox ships official SDKs for [JavaScript](https://github.com/soniox/soniox-js) and [Python](https://github.com/soniox/soniox-python) only; this crate covers the same API surface for Rust.

This is an unofficial client. It is not affiliated with or endorsed by Soniox.

Status: pre-1.0. The API can change between commits, so pin a `rev` if that matters to you.

- Realtime speech-to-text over a websocket, with translation, diarization, endpoint detection and agent text input.
- Realtime text-to-speech, one stream per connection or up to five multiplexed on one.
- The REST API: files, asynchronous transcriptions, models, text-to-speech, voice cloning and the shared voice catalogue, temporary API keys, usage and concurrency, organization administration.
- Helpers: token segmentation into segments and utterances, webhook verification, the supported language table.

Async only, on tokio. TLS is rustls with the `ring` provider. Needs Rust 1.98 or later.

## Install

```toml
[dependencies]
soniox = { git = "https://github.com/janost/soniox-rs.git", branch = "master" }
```

## Realtime speech-to-text

```rust
use soniox::{Client, Marker, SttConfig};

let client = Client::new(api_key);
let config = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1)
    .with_language_hints(vec!["en".into()])
    .with_endpoint_detection(true);

let (mut audio, mut events) = client.connect_stt(&config).await?.split();

tokio::spawn(async move {
    while let Some(chunk) = microphone.recv().await {
        audio.send_audio(chunk).await?;
    }
    audio.finish().await
});

while let Some(event) = events.next().await {
    let event = event?;
    for token in event.text_tokens().filter(|t| t.is_final()) {
        print!("{}", token.text);
    }
    if event.has_marker(Marker::Endpoint) {
        println!();
    }
}
```

Non-final tokens are provisional: every event resends the current non-final tail in full. Append final tokens, replace the tail.

The server closes a session that receives nothing for about 20 seconds. While no audio flows, call `audio.keepalive()` every few seconds from the task that owns the sink.

## Realtime text-to-speech

```rust
use soniox::{Client, TtsConfig};

let config = TtsConfig::pcm_s16le("reply-1", "tts-rt-v2", "en", "Adrian", 24_000);
let mut session = client.connect_tts(&config).await?;
session.send_text("Hello there.", true).await?;

while let Some(chunk) = session.next_audio().await {
    let chunk = chunk?;
    speaker.write(&chunk.audio);
    if chunk.terminated {
        break;
    }
}
```

`client.connect_tts_multi()` opens a connection that carries several streams; `open_stream` returns a handle per stream and events are routed by stream id.

## Asynchronous transcription

```rust
use soniox::{Client, CreateTranscription, DEFAULT_ASYNC_MODEL, TranscriptionStatus, Upload, WaitOptions};

let file = client.files().upload(Upload::from_path("meeting.opus")).await?;
let request = CreateTranscription::from_file(file.id.clone(), DEFAULT_ASYNC_MODEL)
    .with_diarization(true);

let jobs = client.transcriptions();
let job = jobs.create(&request).await?;
let job = jobs.wait(&job.id, WaitOptions::default()).await?;
if job.status == TranscriptionStatus::Completed {
    let transcript = jobs.transcript(&job.id).await?;
    println!("{}", transcript.text);
}
jobs.destroy(&job.id).await?; // the job and its uploaded file
```

## Configuration

`Client::new(key)` targets the US region. `with_region(Region::Eu)` switches the whole host family; `with_stt_url`, `with_tts_url`, `with_api_url` and `with_tts_api_url` override single endpoints, for a proxy or a test server. A key issued in one region is rejected by every other region's hosts.

`Client::from_env()` reads the same variables as the official SDKs: `SONIOX_API_KEY`, `SONIOX_REGION`, `SONIOX_BASE_DOMAIN`, `SONIOX_API_BASE_URL`, `SONIOX_WS_URL`, `SONIOX_TTS_API_URL`, `SONIOX_TTS_WS_URL`.

`with_timeouts` sets three budgets: `connect` (10 s), `request` for an ordinary REST round trip (60 s), and `transfer` for uploads and generated audio (15 min).

## Errors

Everything returns `soniox::Result`. A websocket error frame and a non-2xx REST response both become `Error::Api`, carrying the status code, the error type, the server's message, any validation errors and the request id. `ApiError::kind()` classifies it; `Error::is_retryable()` says whether trying again can help.

The server's `message` can echo request content, such as an uploaded file name. Where that must stay out of logs, render the code and `request_id` instead of the error's `Display`.

## Secrets

The API key is held as a `secrecy::SecretString` and only ever sent in an `Authorization` header. It is in no config type, request body or URL, so `{:?}` on any type in this crate is safe to log. Keys the API returns (temporary keys, new project keys) are `SecretString` too.

## Differences from the official SDKs

- No pause and resume. They exist upstream to run a keepalive timer for you; here the task that owns the sink sends `keepalive()` from its own `select!` loop.
- No `delete_all` / `destroy_all`. Mass deletion is `list_all()` plus `delete()`, written out where it is used.
- No one-call `transcribe_and_wait`. The four steps are separate so each can be retried and timed on its own.
- Client-side range checks are limited to what the server reports badly (a raw audio format without a sample rate, text over the size limit). Model-dependent ranges are left to the server, which names the offending field.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

The tests run against local mock servers and need no API key or network.

## License

[MIT](LICENSE)
