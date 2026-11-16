//! Realtime speech-to-text over a websocket.

use std::pin::Pin;
use std::task::{Context as TaskContext, Poll, ready};

use bytes::Bytes;
use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::Message;

use super::{Connection, WsSink, WsStream, poll_text, send_json, send_static};
use crate::error::{ApiError, Error, FrameError, Result};
use crate::types::{AudioFormat, Context, Marker, Token, TranslationConfig, is_false};

/// Configuration for one realtime transcription session.
///
/// Deliberately does not hold the API key: it travels on the websocket
/// handshake, so `{:?}` on this type is safe by construction.
///
/// Value ranges (endpoint delay, sensitivity, latency level) are not checked
/// here. They differ per model, see [`crate::rest::Model`], and the server
/// rejects a bad value with an error that names it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct SttConfig {
    pub model: String,
    pub audio_format: AudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_channels: Option<u16>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub language_hints: Vec<String>,
    #[serde(skip_serializing_if = "is_false")]
    pub language_hints_strict: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub enable_language_identification: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub enable_speaker_diarization: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub enable_endpoint_detection: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub enable_agent_text_input: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_endpoint_delay_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_latency_adjustment_level: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_sensitivity: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_reference_id: Option<String>,
    #[serde(skip_serializing_if = "Context::is_empty")]
    pub context: Context,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub translation: Option<TranslationConfig>,
}

impl SttConfig {
    /// A session whose audio is a container format the server detects.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            audio_format: AudioFormat::Auto,
            sample_rate: None,
            num_channels: None,
            language_hints: Vec::new(),
            language_hints_strict: false,
            enable_language_identification: false,
            enable_speaker_diarization: false,
            enable_endpoint_detection: false,
            enable_agent_text_input: false,
            max_endpoint_delay_ms: None,
            endpoint_latency_adjustment_level: None,
            endpoint_sensitivity: None,
            client_reference_id: None,
            context: Context::default(),
            translation: None,
        }
    }

    /// A raw little-endian 16-bit PCM stream.
    pub fn pcm_s16le(model: impl Into<String>, sample_rate: u32, num_channels: u16) -> Self {
        Self::new(model).with_raw_audio(AudioFormat::PcmS16Le, sample_rate, num_channels)
    }

    /// Declare a headerless audio format. Raw formats carry no header, so the
    /// sample rate and channel count come with them.
    pub fn with_raw_audio(mut self, format: AudioFormat, sample_rate: u32, channels: u16) -> Self {
        self.audio_format = format;
        self.sample_rate = Some(sample_rate);
        self.num_channels = Some(channels);
        self
    }

    /// Declare a container format instead of leaving it to detection.
    pub fn with_audio_format(mut self, format: AudioFormat) -> Self {
        self.audio_format = format;
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

    /// Bias recognition toward these languages. Leaving
    /// `language_hints_strict` off is what allows code-switching within one
    /// utterance.
    pub fn with_language_hints(mut self, hints: Vec<String>) -> Self {
        self.language_hints = hints;
        self
    }

    /// Restrict recognition to the hinted languages. Works best with a
    /// single hint.
    pub fn with_language_hints_strict(mut self, on: bool) -> Self {
        self.language_hints_strict = on;
        self
    }

    /// Ask the server to label tokens with a speaker id. Ids are per session:
    /// two sessions both number their speakers from 1, and those are different
    /// people.
    pub fn with_diarization(mut self, on: bool) -> Self {
        self.enable_speaker_diarization = on;
        self
    }

    /// Translate as well as transcribe.
    pub fn with_translation(mut self, translation: Option<TranslationConfig>) -> Self {
        self.translation = translation;
        self
    }

    /// Ask for a `language` on every token.
    pub fn with_language_identification(mut self, on: bool) -> Self {
        self.enable_language_identification = on;
        self
    }

    /// Ask for `<end>` markers at utterance boundaries.
    pub fn with_endpoint_detection(mut self, on: bool) -> Self {
        self.enable_endpoint_detection = on;
        self
    }

    /// Accept [`AudioSink::send_agent_text`] on this session. The model must
    /// report `supports_agent_text_input`, or the server refuses the session.
    pub fn with_agent_text_input(mut self, on: bool) -> Self {
        self.enable_agent_text_input = on;
        self
    }

    /// Upper bound on how long the server waits after speech stops before it
    /// emits `<end>`.
    pub fn with_max_endpoint_delay_ms(mut self, ms: Option<u32>) -> Self {
        self.max_endpoint_delay_ms = ms;
        self
    }

    /// Endpoint latency adjustment level, higher is faster.
    pub fn with_endpoint_latency_adjustment_level(mut self, level: Option<u8>) -> Self {
        self.endpoint_latency_adjustment_level = level;
        self
    }

    /// Endpoint sensitivity, higher ends utterances more eagerly.
    pub fn with_endpoint_sensitivity(mut self, value: Option<f32>) -> Self {
        self.endpoint_sensitivity = value;
        self
    }

    /// A tracking id echoed in usage logs. Need not be unique.
    pub fn with_client_reference_id(mut self, id: impl Into<String>) -> Self {
        self.client_reference_id = Some(id.into());
        self
    }

    /// Catch the one mistake the server reports confusingly: a raw format
    /// with no sample rate or channel count is read as garbage audio.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.audio_format.is_raw() && (self.sample_rate.is_none() || self.num_channels.is_none())
        {
            return Err(Error::InvalidInput(format!(
                "audio format {} has no header, so sample_rate and num_channels must be set",
                self.audio_format
            )));
        }
        Ok(())
    }
}

/// One message from the server: the tokens recognised since the last one.
///
/// Non-final tokens are provisional. Each event resends the current
/// non-final tokens in full, so a consumer replaces its non-final tail with
/// the new one and appends only tokens whose `is_final` is set.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Event {
    #[serde(default)]
    pub tokens: Vec<Token>,
    pub final_audio_proc_ms: Option<u64>,
    pub total_audio_proc_ms: Option<u64>,
    /// The session is over; the server closes the connection next.
    #[serde(default)]
    pub finished: bool,
}

impl Event {
    /// Whether this event carries the given control marker.
    pub fn has_marker(&self, marker: Marker) -> bool {
        self.tokens.iter().any(|token| token.marker() == Some(marker))
    }

    /// The tokens that are text, with control markers skipped.
    pub fn text_tokens(&self) -> impl Iterator<Item = &Token> {
        self.tokens.iter().filter(|token| token.is_text())
    }
}

/// An event as it arrives, error fields included, so a frame is decoded once
/// whether it turns out to be an event or an error.
#[derive(Deserialize)]
struct Frame {
    #[serde(default)]
    tokens: Vec<Token>,
    final_audio_proc_ms: Option<u64>,
    total_audio_proc_ms: Option<u64>,
    #[serde(default)]
    finished: bool,
    error_code: Option<u64>,
    error_type: Option<String>,
    error_message: Option<String>,
    request_id: Option<String>,
}

/// Decode one server frame. An error frame becomes `Error::Api`, so callers
/// never inspect events for `error_code` themselves.
fn parse_event(raw: &str) -> Result<Event> {
    let frame: Frame = serde_json::from_str(raw)?;
    let error = FrameError {
        error_code: frame.error_code,
        error_type: frame.error_type,
        error_message: frame.error_message,
        request_id: frame.request_id,
    };
    if let Some(error) = ApiError::from_frame(error) {
        return Err(error.into());
    }
    Ok(Event {
        tokens: frame.tokens,
        final_audio_proc_ms: frame.final_audio_proc_ms,
        total_audio_proc_ms: frame.total_audio_proc_ms,
        finished: frame.finished,
    })
}

/// One realtime transcription session. Split it to run the audio pump and the
/// event reader as independent tasks.
pub struct SttSession {
    conn: Connection,
    agent_text: bool,
}

impl SttSession {
    pub(crate) fn new(conn: Connection, config: &SttConfig) -> Self {
        Self { conn, agent_text: config.enable_agent_text_input }
    }

    pub fn split(self) -> (AudioSink, EventStream) {
        let (sink, stream) = self.conn.into_split();
        (AudioSink { sink, agent_text: self.agent_text }, EventStream { stream })
    }
}

#[derive(Serialize)]
struct AgentText<'a> {
    r#type: &'static str,
    text: &'a str,
    end_of_segment: bool,
}

/// The outbound half: audio frames and control messages.
///
/// The server closes a session that receives nothing for about 20 seconds.
/// While no audio is flowing, send [`keepalive`](Self::keepalive) every few
/// seconds from the task that owns the sink.
pub struct AudioSink {
    sink: WsSink,
    agent_text: bool,
}

impl AudioSink {
    /// Send one chunk of audio in the format the config declared. A
    /// `Vec<u8>` or `Bytes` is sent without copying.
    pub async fn send_audio(&mut self, audio: impl Into<Bytes>) -> Result<()> {
        self.sink.send(Message::Binary(audio.into())).await?;
        Ok(())
    }

    /// Finalize outstanding non-final tokens while keeping the session open.
    /// The server answers with a `<fin>` marker token once it has drained.
    pub async fn finalize(&mut self) -> Result<()> {
        send_static(&mut self.sink, r#"{"type":"finalize"}"#).await
    }

    /// [`finalize`](Self::finalize), telling the server how much silence
    /// already follows the last speech in the audio sent. The server then
    /// does not wait for that silence itself, so the `<fin>` comes sooner.
    pub async fn finalize_with_trailing_silence(&mut self, trailing_silence_ms: u32) -> Result<()> {
        #[derive(Serialize)]
        struct Finalize {
            r#type: &'static str,
            trailing_silence_ms: u32,
        }
        send_json(&mut self.sink, &Finalize { r#type: "finalize", trailing_silence_ms }).await
    }

    pub async fn keepalive(&mut self) -> Result<()> {
        send_static(&mut self.sink, r#"{"type":"keepalive"}"#).await
    }

    /// Tell Soniox what the agent is saying, so those words are recognised
    /// when the user says them back.
    ///
    /// Soniox places the text by its position among the audio frames, so send
    /// each piece 500 ms to 1 s ahead of when the caller hears it, paced by
    /// playback rather than by how fast the reply was generated. Pieces are
    /// joined exactly as sent, spaces included.
    ///
    /// A segment is one agent turn. Close every turn with `end_of_segment`,
    /// on the last piece or on an empty one. Do the same when the caller
    /// interrupts: send what was already played, close the segment, and drop
    /// the rest.
    pub async fn send_agent_text(&mut self, text: &str, end_of_segment: bool) -> Result<()> {
        if !self.agent_text {
            return Err(Error::InvalidInput(
                "set enable_agent_text_input on the session config to send agent text".into(),
            ));
        }
        send_json(&mut self.sink, &AgentText { r#type: "agent_text", text, end_of_segment }).await
    }

    /// Signal end of audio with an empty text frame. The server finalizes what
    /// is pending, emits `finished: true`, and closes.
    ///
    /// The frame type is load-bearing. An empty *binary* frame is
    /// indistinguishable from zero bytes of audio, so the server keeps the
    /// stream open waiting for more, never emits `finished`, and the caller
    /// blocks until its own timeout expires.
    pub async fn finish(&mut self) -> Result<()> {
        send_static(&mut self.sink, "").await
    }
}

/// The inbound half. Server error frames surface as `Err(Error::Api(..))`.
pub struct EventStream {
    stream: WsStream,
}

impl EventStream {
    /// The next event, or `None` once the connection closes.
    pub async fn next(&mut self) -> Option<Result<Event>> {
        StreamExt::next(self).await
    }
}

impl Stream for EventStream {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let frame = ready!(poll_text(&mut self.stream, cx));
        Poll::Ready(frame.map(|text| parse_event(text?.as_str())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pcm_config_serializes_only_what_is_set() {
        let cfg = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1)
            .with_language_hints(vec!["hu".into(), "en".into()]);
        assert_eq!(
            serde_json::to_value(&cfg).unwrap(),
            serde_json::json!({
                "model": "stt-rt-v6",
                "audio_format": "pcm_s16le",
                "sample_rate": 16_000,
                "num_channels": 1,
                "language_hints": ["hu", "en"],
            })
        );
        assert!(!format!("{cfg:?}").contains("api_key"));
    }

    /// Diarization costs money and changes what a session returns, and the
    /// other switches are opt-in the same way: `false` sends nothing rather
    /// than an explicit `false`.
    #[test]
    fn switches_are_sent_only_when_on() {
        let on = SttConfig::new("stt-rt-v6")
            .with_diarization(true)
            .with_language_identification(true)
            .with_endpoint_detection(true)
            .with_agent_text_input(true)
            .with_language_hints_strict(true);
        let v = serde_json::to_value(&on).unwrap();
        for key in [
            "enable_speaker_diarization",
            "enable_language_identification",
            "enable_endpoint_detection",
            "enable_agent_text_input",
            "language_hints_strict",
        ] {
            assert_eq!(v[key], true, "{key}");
        }

        let off = on
            .with_diarization(false)
            .with_language_identification(false)
            .with_endpoint_detection(false)
            .with_agent_text_input(false)
            .with_language_hints_strict(false);
        assert_eq!(
            serde_json::to_value(&off).unwrap(),
            serde_json::json!({"model": "stt-rt-v6", "audio_format": "auto"})
        );
    }

    #[test]
    fn endpoint_tuning_context_and_translation_serialize_when_set() {
        let cfg = SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1)
            .with_max_endpoint_delay_ms(Some(1000))
            .with_endpoint_latency_adjustment_level(Some(2))
            .with_endpoint_sensitivity(Some(0.5))
            .with_terms(vec!["Kovács".into()])
            .with_translation(Some(TranslationConfig::one_way("hu")))
            .with_client_reference_id("call-7");
        let v = serde_json::to_value(&cfg).unwrap();
        assert_eq!(v["max_endpoint_delay_ms"], 1000);
        assert_eq!(v["endpoint_latency_adjustment_level"], 2);
        assert_eq!(v["endpoint_sensitivity"], 0.5);
        assert_eq!(v["context"], serde_json::json!({"terms": ["Kovács"]}));
        assert_eq!(v["translation"]["target_language"], "hu");
        assert_eq!(v["client_reference_id"], "call-7");

        let cleared = cfg.with_terms(vec![]).with_translation(None);
        let v = serde_json::to_value(&cleared).unwrap();
        assert!(v.get("context").is_none(), "an empty context is not sent");
        assert!(v.get("translation").is_none());
    }

    #[test]
    fn a_raw_format_without_rate_and_channels_is_rejected_before_connecting() {
        let bad = SttConfig::new("stt-rt-v6").with_audio_format(AudioFormat::PcmS16Le);
        assert!(matches!(bad.validate(), Err(Error::InvalidInput(_))));
        assert!(SttConfig::new("stt-rt-v6").validate().is_ok(), "auto needs neither");
        assert!(SttConfig::pcm_s16le("stt-rt-v6", 16_000, 1).validate().is_ok());
    }

    #[test]
    fn decodes_an_event_with_partial_fields() {
        let ev = parse_event(
            r#"{"tokens":[{"text":" szia","is_final":true}],"total_audio_proc_ms":880}"#,
        )
        .unwrap();
        assert_eq!(ev.tokens.len(), 1);
        assert!(ev.tokens[0].is_final());
        assert_eq!(ev.total_audio_proc_ms, Some(880));
        assert!(!ev.finished, "finished defaults to false");
    }

    #[test]
    fn an_error_frame_becomes_an_api_error() {
        let err = parse_event(
            r#"{"tokens":[],"error_code":401,"error_type":"unauthorized","error_message":"nope","request_id":"r-1"}"#,
        )
        .expect_err("must be an error");
        let api = err.api().expect("an API error");
        assert_eq!(api.code, 401);
        assert_eq!(api.message, "nope");
        assert_eq!(api.error_type.as_deref(), Some("unauthorized"));
        assert_eq!(api.request_id.as_deref(), Some("r-1"));
    }

    #[test]
    fn malformed_json_is_a_decode_error() {
        assert!(matches!(parse_event("not json"), Err(Error::Decode(_))));
    }

    #[test]
    fn markers_are_found_and_left_out_of_the_text_tokens() {
        let ev = parse_event(
            r#"{"tokens":[{"text":"Hi","is_final":true},{"text":"<end>","is_final":true}]}"#,
        )
        .unwrap();
        assert!(ev.has_marker(Marker::Endpoint));
        assert!(!ev.has_marker(Marker::Finalized));
        assert_eq!(ev.text_tokens().map(|t| t.text.as_str()).collect::<Vec<_>>(), ["Hi"]);
    }
}
