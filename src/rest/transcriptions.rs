//! Asynchronous transcription: upload a file (or name a URL), create a job
//! against an async model, wait for it, fetch the transcript.
//!
//! The server sees the whole file, so it can cluster speakers globally and
//! reach a quality a realtime session cannot.

use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::stream::BoxStream;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use super::{Count, Page, empty, json, page_query, paginate};
use crate::Client;
use crate::error::{Error, Result};
use crate::types::{Context, FileId, Token, TranscriptionId, TranslationConfig, is_false};
use crate::webhooks::WebhookAuth;

/// The current asynchronous model. Realtime sessions use a different one,
/// `stt-rt-v6` at the time of writing; [`crate::rest::Models::list`] returns
/// what a key can use.
pub const DEFAULT_ASYNC_MODEL: &str = "stt-async-v6";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum TranscriptionStatus {
    Queued,
    Processing,
    Completed,
    Error,
    /// A status this crate does not know. Not terminal, so polling continues.
    #[serde(other)]
    Unknown,
}

impl TranscriptionStatus {
    /// Whether polling can stop.
    pub fn is_terminal(self) -> bool {
        matches!(self, TranscriptionStatus::Completed | TranscriptionStatus::Error)
    }
}

/// A transcription job. Soniox deletes jobs 30 days after creation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub struct Transcription {
    pub id: TranscriptionId,
    pub status: TranscriptionStatus,
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub model: String,
    pub audio_url: Option<String>,
    pub file_id: Option<FileId>,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub language_hints: Option<Vec<String>>,
    #[serde(default)]
    pub enable_speaker_diarization: bool,
    #[serde(default)]
    pub enable_language_identification: bool,
    /// How much audio the job covers, which is what Soniox bills. Present
    /// once processing has begun.
    pub audio_duration_ms: Option<u64>,
    pub error_type: Option<String>,
    /// Free text from Soniox for a failed job. It arrives in a 200 response,
    /// and like an HTTP error message it can echo request content, so keep it
    /// out of places where that content must not appear.
    pub error_message: Option<String>,
    pub webhook_url: Option<String>,
    pub webhook_auth_header_name: Option<String>,
    /// Always masked by the server.
    pub webhook_auth_header_value: Option<String>,
    /// The status the webhook receiver answered with, once delivered.
    pub webhook_status_code: Option<u16>,
    pub client_reference_id: Option<String>,
    /// Not in the published schema, so usually absent. When the server does
    /// send it, it is safe to surface: it carries no request content, only
    /// what support needs to look a failure up.
    pub request_id: Option<String>,
}

/// The result of a completed job.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[non_exhaustive]
pub struct Transcript {
    #[serde(default)]
    pub id: Option<TranscriptionId>,
    /// The whole transcript as one string.
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub tokens: Vec<Token>,
}

/// Where the audio of a job comes from. Exactly one source, by construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum AudioSource {
    /// A file uploaded through [`Client::files`].
    #[serde(rename = "file_id")]
    File(FileId),
    /// A publicly reachable URL the server downloads.
    #[serde(rename = "audio_url")]
    Url(String),
}

/// A transcription job request.
///
/// Deliberately does not hold the API key, exactly as with `SttConfig`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct CreateTranscription {
    pub model: String,
    #[serde(flatten)]
    pub source: AudioSource,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub language_hints: Vec<String>,
    #[serde(skip_serializing_if = "is_false")]
    pub language_hints_strict: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub enable_speaker_diarization: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub enable_language_identification: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub translation: Option<TranslationConfig>,
    #[serde(skip_serializing_if = "Context::is_empty")]
    pub context: Context,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_auth_header_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_auth_header_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_reference_id: Option<String>,
}

impl CreateTranscription {
    pub fn new(source: AudioSource, model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            source,
            language_hints: Vec::new(),
            language_hints_strict: false,
            enable_speaker_diarization: false,
            enable_language_identification: false,
            translation: None,
            context: Context::default(),
            webhook_url: None,
            webhook_auth_header_name: None,
            webhook_auth_header_value: None,
            client_reference_id: None,
        }
    }

    pub fn from_file(file: FileId, model: impl Into<String>) -> Self {
        Self::new(AudioSource::File(file), model)
    }

    pub fn from_url(audio_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self::new(AudioSource::Url(audio_url.into()), model)
    }

    pub fn with_diarization(mut self, on: bool) -> Self {
        self.enable_speaker_diarization = on;
        self
    }

    pub fn with_language_identification(mut self, on: bool) -> Self {
        self.enable_language_identification = on;
        self
    }

    pub fn with_language_hints(mut self, hints: Vec<String>) -> Self {
        self.language_hints = hints;
        self
    }

    /// Restrict recognition to the hinted languages.
    pub fn with_language_hints_strict(mut self, on: bool) -> Self {
        self.language_hints_strict = on;
        self
    }

    /// Bias recognition toward these words and phrases.
    pub fn with_terms(mut self, terms: Vec<String>) -> Self {
        self.context.terms = terms;
        self
    }

    /// Replace the whole recognition context.
    pub fn with_context(mut self, context: Context) -> Self {
        self.context = context;
        self
    }

    /// Translate as well as transcribe. Translated tokens are told apart by
    /// their `language`, so language identification is usually wanted with it.
    pub fn with_translation(mut self, translation: Option<TranslationConfig>) -> Self {
        self.translation = translation;
        self
    }

    /// Have Soniox call `url` when the job completes or fails, presenting
    /// `auth` as a header so the receiver can tell the call is genuine.
    pub fn with_webhook(mut self, url: impl Into<String>, auth: Option<&WebhookAuth>) -> Self {
        self.webhook_url = Some(url.into());
        self.webhook_auth_header_name = auth.map(|auth| auth.header_name().to_owned());
        self.webhook_auth_header_value = auth.map(|auth| auth.expose_value().to_owned());
        self
    }

    /// A tracking id echoed in usage logs. Need not be unique.
    pub fn with_client_reference_id(mut self, id: impl Into<String>) -> Self {
        self.client_reference_id = Some(id.into());
        self
    }
}

/// How [`Transcriptions::wait`] polls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitOptions {
    /// Pause between polls.
    pub interval: Duration,
    /// Give up after this long. `None` waits for as long as the job takes.
    pub timeout: Option<Duration>,
}

impl Default for WaitOptions {
    fn default() -> Self {
        Self { interval: Duration::from_secs(5), timeout: None }
    }
}

#[derive(Deserialize)]
struct TranscriptionsPage {
    #[serde(default)]
    transcriptions: Vec<Transcription>,
    next_page_cursor: Option<String>,
}

/// Asynchronous transcription jobs. Get one from [`Client::transcriptions`].
#[derive(Debug, Clone, Copy)]
pub struct Transcriptions<'a> {
    client: &'a Client,
}

impl<'a> Transcriptions<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn create(&self, request: &CreateTranscription) -> Result<Transcription> {
        json(self.client.request(Method::POST, &["transcriptions"])?.json(request)).await
    }

    pub async fn get(&self, id: &TranscriptionId) -> Result<Transcription> {
        json(self.client.request(Method::GET, &["transcriptions", id.as_str()])?).await
    }

    /// One page of jobs. `limit` is at most 1000.
    pub async fn list(
        &self,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<Transcription>> {
        let request = self.client.request(Method::GET, &["transcriptions"])?;
        let page: TranscriptionsPage = json(request.query(&page_query(limit, cursor))).await?;
        Ok(Page { items: page.transcriptions, next_page_cursor: page.next_page_cursor })
    }

    /// Every job, fetched page by page as the stream is consumed.
    pub fn list_all(&self) -> BoxStream<'a, Result<Transcription>> {
        let jobs = *self;
        paginate(move |cursor| async move { jobs.list(None, cursor.as_deref()).await })
    }

    pub async fn count(&self) -> Result<Count> {
        json(self.client.request(Method::GET, &["transcriptions", "count"])?).await
    }

    /// Delete a job and its transcript. A job that is still processing
    /// answers with a `409`. The uploaded file is left alone; see
    /// [`destroy`](Self::destroy).
    pub async fn delete(&self, id: &TranscriptionId) -> Result<()> {
        empty(self.client.request(Method::DELETE, &["transcriptions", id.as_str()])?).await
    }

    /// Delete a job and the uploaded file it transcribed, if it had one.
    pub async fn destroy(&self, id: &TranscriptionId) -> Result<()> {
        let job = self.get(id).await?;
        self.delete(id).await?;
        let Some(file) = job.file_id else { return Ok(()) };
        match self.client.files().delete(&file).await {
            Err(e) if e.is_not_found() => Ok(()),
            other => other,
        }
    }

    /// The transcript of a completed job. Tokens come back with `is_final`
    /// filled in.
    ///
    /// `Token` is shared with the realtime schema, where a missing `is_final`
    /// means "provisional". This endpoint has no such state: it only serves a
    /// completed job, so every token is confirmed, and it omits the field
    /// rather than sending `true`. Left alone, a whole batch transcript would
    /// look provisional to any consumer that folds realtime tokens. An
    /// explicit value is kept, so this fills the gap rather than overruling
    /// the server.
    pub async fn transcript(&self, id: &TranscriptionId) -> Result<Transcript> {
        let request =
            self.client.request(Method::GET, &["transcriptions", id.as_str(), "transcript"])?;
        let mut transcript: Transcript = json(request).await?;
        for token in &mut transcript.tokens {
            token.is_final.get_or_insert(true);
        }
        Ok(transcript)
    }

    /// Poll a job until it completes or fails. A job that failed is returned,
    /// not turned into an error: check `status`.
    pub async fn wait(&self, id: &TranscriptionId, options: WaitOptions) -> Result<Transcription> {
        let deadline = options.timeout.map(|timeout| Instant::now() + timeout);
        loop {
            let job = self.get(id).await?;
            if job.status.is_terminal() {
                return Ok(job);
            }
            let mut pause = options.interval;
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(Error::Timeout(format!("transcription {id}")));
                }
                pause = pause.min(remaining);
            }
            tokio::time::sleep(pause).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_create_payload_names_one_source_and_omits_what_is_unset() {
        let from_file = CreateTranscription::from_file(FileId::from("f-1"), "stt-async-v6");
        assert_eq!(
            serde_json::to_value(&from_file).unwrap(),
            serde_json::json!({"model": "stt-async-v6", "file_id": "f-1"})
        );
        let from_url = CreateTranscription::from_url("https://example.test/a.mp3", "stt-async-v6");
        assert_eq!(
            serde_json::to_value(&from_url).unwrap(),
            serde_json::json!({"model": "stt-async-v6", "audio_url": "https://example.test/a.mp3"})
        );
    }

    #[test]
    fn options_serialize_when_asked_for() {
        let payload = CreateTranscription::from_file(FileId::from("f-1"), "stt-async-v6")
            .with_diarization(true)
            .with_language_hints(vec!["hu".into(), "en".into()])
            .with_language_hints_strict(true)
            .with_terms(vec!["Kovács".into()])
            .with_client_reference_id("meeting-9");
        let v = serde_json::to_value(&payload).unwrap();
        assert_eq!(v["enable_speaker_diarization"], true);
        assert_eq!(v["language_hints"], serde_json::json!(["hu", "en"]));
        assert_eq!(v["language_hints_strict"], true);
        assert_eq!(v["context"], serde_json::json!({"terms": ["Kovács"]}));
        assert_eq!(v["client_reference_id"], "meeting-9");
    }

    #[test]
    fn translation_serializes_under_its_own_block() {
        let payload = CreateTranscription::from_file(FileId::from("f-1"), "stt-async-v6")
            .with_translation(Some(TranslationConfig::two_way("en", "hu")))
            .with_language_identification(true);
        let v = serde_json::to_value(&payload).unwrap();
        assert_eq!(v["translation"]["type"], "two_way");
        assert_eq!(v["enable_language_identification"], true);
    }

    #[test]
    fn a_webhook_carries_its_auth_header_only_when_given_one() {
        let base = CreateTranscription::from_file(FileId::from("f-1"), "stt-async-v6");
        let auth = WebhookAuth::new("X-Hook-Secret", "s3cret");
        let v = serde_json::to_value(base.clone().with_webhook("https://a.test/hook", Some(&auth)))
            .unwrap();
        assert_eq!(v["webhook_url"], "https://a.test/hook");
        assert_eq!(v["webhook_auth_header_name"], "X-Hook-Secret");
        assert_eq!(v["webhook_auth_header_value"], "s3cret");

        let v = serde_json::to_value(base.with_webhook("https://a.test/hook", None)).unwrap();
        assert!(v.get("webhook_auth_header_name").is_none());
    }

    #[test]
    fn a_job_decodes_from_a_minimal_and_from_a_full_body() {
        let minimal: Transcription =
            serde_json::from_str(r#"{"id":"t-1","status":"queued"}"#).unwrap();
        assert_eq!(minimal.status, TranscriptionStatus::Queued);
        assert_eq!(minimal.audio_duration_ms, None);

        let full: Transcription = serde_json::from_str(
            r#"{"id":"t-1","status":"error","created_at":"2026-11-03T10:00:00Z","model":"stt-async-v6",
                "audio_url":null,"file_id":"f-1","filename":"mic.opus","language_hints":["hu"],
                "enable_speaker_diarization":true,"enable_language_identification":false,
                "audio_duration_ms":61500,"error_type":"invalid_audio","error_message":"unsupported audio",
                "webhook_status_code":null,"client_reference_id":"m-9"}"#,
        )
        .unwrap();
        assert_eq!(full.status, TranscriptionStatus::Error);
        assert_eq!(full.file_id, Some(FileId::from("f-1")));
        assert_eq!(full.audio_duration_ms, Some(61_500));
        assert_eq!(full.error_message.as_deref(), Some("unsupported audio"));
        assert!(full.created_at.is_some());
    }

    #[test]
    fn only_completed_and_error_are_terminal_and_an_unknown_status_keeps_polling() {
        let parse = |s: &str| serde_json::from_str::<TranscriptionStatus>(s).unwrap();
        assert!(!parse("\"queued\"").is_terminal());
        assert!(!parse("\"processing\"").is_terminal());
        assert!(parse("\"completed\"").is_terminal());
        assert!(parse("\"error\"").is_terminal());
        assert_eq!(parse("\"paused\""), TranscriptionStatus::Unknown);
        assert!(!parse("\"paused\"").is_terminal());
    }
}
