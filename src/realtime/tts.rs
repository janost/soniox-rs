//! Realtime text-to-speech over a websocket, one stream per connection. For
//! several streams on one socket see [`super::tts_multi`].

use std::borrow::Cow;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use base64::Engine;
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use super::{Connection, WsSink, WsStream, poll_text, send_json, send_static};
use crate::error::{ApiError, Error, FrameError, Result};
use crate::types::{is_false, wire_enum};

/// Soniox accepts at most this many characters in one text frame.
pub const MAX_TEXT_CHARS: usize = 5000;

wire_enum! {
    /// Audio encodings text-to-speech can produce, realtime or REST.
    pub enum TtsAudioFormat {
        PcmS16Le => "pcm_s16le",
        PcmS16Be => "pcm_s16be",
        PcmF32Le => "pcm_f32le",
        PcmMulaw => "pcm_mulaw",
        PcmAlaw => "pcm_alaw",
        Wav => "wav",
        Aac => "aac",
        Mp3 => "mp3",
        Opus => "opus",
        Flac => "flac",
    }
}

impl TtsAudioFormat {
    /// The conventional file extension for audio in this format.
    pub const fn file_extension(self) -> &'static str {
        match self {
            Self::PcmS16Le | Self::PcmS16Be | Self::PcmF32Le | Self::PcmMulaw | Self::PcmAlaw => {
                "pcm"
            }
            Self::Wav => "wav",
            Self::Aac => "aac",
            Self::Mp3 => "mp3",
            Self::Opus => "opus",
            Self::Flac => "flac",
        }
    }
}

/// Configuration for one realtime synthesis stream.
///
/// Like `SttConfig`, this deliberately does not hold the API key.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct TtsConfig {
    /// Identifies the stream on its connection. Unique among the streams
    /// active on one socket.
    pub stream_id: String,
    pub model: String,
    pub language: String,
    pub voice: String,
    pub audio_format: TtsAudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    /// Bits per second, for the compressed formats.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bitrate: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f32>,
    #[serde(skip_serializing_if = "is_false")]
    pub reduce_silence: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub return_timestamps: bool,
}

impl TtsConfig {
    pub fn new(
        stream_id: impl Into<String>,
        model: impl Into<String>,
        language: impl Into<String>,
        voice: impl Into<String>,
        audio_format: TtsAudioFormat,
    ) -> Self {
        Self {
            stream_id: stream_id.into(),
            model: model.into(),
            language: language.into(),
            voice: voice.into(),
            audio_format,
            sample_rate: None,
            bitrate: None,
            speed: None,
            reduce_silence: false,
            return_timestamps: false,
        }
    }

    /// Raw little-endian 16-bit PCM, which feeds an output device directly with
    /// no decoding step.
    pub fn pcm_s16le(
        stream_id: impl Into<String>,
        model: impl Into<String>,
        language: impl Into<String>,
        voice: impl Into<String>,
        sample_rate: u32,
    ) -> Self {
        Self::new(stream_id, model, language, voice, TtsAudioFormat::PcmS16Le)
            .with_sample_rate(sample_rate)
    }

    pub fn with_sample_rate(mut self, sample_rate: u32) -> Self {
        self.sample_rate = Some(sample_rate);
        self
    }

    pub fn with_bitrate(mut self, bitrate: u32) -> Self {
        self.bitrate = Some(bitrate);
        self
    }

    /// Speaking rate multiplier. The range is per model, see
    /// [`crate::rest::TtsModel`]; 0.7 to 1.3 at the time of writing.
    pub fn with_speed(mut self, speed: f32) -> Self {
        self.speed = Some(speed);
        self
    }

    /// Shorten the pauses between words, on models that support it.
    pub fn with_reduce_silence(mut self, on: bool) -> Self {
        self.reduce_silence = on;
        self
    }

    /// Ask for character-to-audio alignment on every chunk.
    pub fn with_timestamps(mut self, on: bool) -> Self {
        self.return_timestamps = on;
        self
    }
}

/// Character-to-audio alignment for one chunk. The three lists are parallel:
/// index `i` maps one character of the spoken text to the span that
/// pronounces it.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[non_exhaustive]
pub struct TtsTimestamps {
    #[serde(default)]
    pub characters: Vec<String>,
    #[serde(default)]
    pub character_start_times_seconds: Vec<f64>,
    #[serde(default)]
    pub character_end_times_seconds: Vec<f64>,
}

/// One decoded audio event.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TtsChunk {
    /// Audio in the configured format. Empty on an event that only carries
    /// flags.
    pub audio: Vec<u8>,
    /// Present when the config asked for timestamps.
    pub timestamps: Option<TtsTimestamps>,
    /// No further audio is coming. Playback must still drain what is buffered.
    pub audio_end: bool,
    /// The stream is fully finished.
    pub terminated: bool,
}

#[derive(Deserialize)]
struct Frame<'a> {
    stream_id: Option<String>,
    /// Borrowed from the frame when it holds no escapes, so the base64 text
    /// is decoded in place instead of being copied into a `String` first.
    #[serde(borrow)]
    audio: Option<Cow<'a, str>>,
    timestamps: Option<TtsTimestamps>,
    #[serde(default)]
    audio_end: bool,
    #[serde(default)]
    terminated: bool,
    error_code: Option<u64>,
    error_type: Option<String>,
    error_message: Option<String>,
    request_id: Option<String>,
}

/// Decode one server frame into the stream it belongs to and its chunk. The
/// stream id comes back even when the frame is an error, so a multiplexed
/// connection can route the failure.
pub(crate) fn parse_frame(raw: &str) -> (Option<String>, Result<TtsChunk>) {
    let frame: Frame<'_> = match serde_json::from_str(raw) {
        Ok(frame) => frame,
        Err(e) => return (None, Err(e.into())),
    };
    let error = FrameError {
        error_code: frame.error_code,
        error_type: frame.error_type,
        error_message: frame.error_message,
        request_id: frame.request_id,
    };
    let chunk = match ApiError::from_frame(error) {
        Some(error) => Err(error.into()),
        None => decode_audio(frame.audio.as_deref()).map(|audio| TtsChunk {
            audio,
            timestamps: frame.timestamps,
            audio_end: frame.audio_end,
            terminated: frame.terminated,
        }),
    };
    // The server reports connection-level errors with an empty stream id.
    (frame.stream_id.filter(|id| !id.is_empty()), chunk)
}

fn decode_audio(encoded: Option<&str>) -> Result<Vec<u8>> {
    match encoded {
        None => Ok(Vec::new()),
        Some(encoded) => Ok(base64::engine::general_purpose::STANDARD.decode(encoded)?),
    }
}

#[derive(Serialize)]
struct TextFrame<'a> {
    stream_id: &'a str,
    text: &'a str,
    text_end: bool,
}

#[derive(Serialize)]
struct CancelFrame<'a> {
    stream_id: &'a str,
    cancel: bool,
}

/// Send one chunk of text for `stream_id`.
pub(crate) async fn send_text(
    sink: &mut WsSink,
    stream_id: &str,
    text: &str,
    text_end: bool,
) -> Result<()> {
    let count = text.chars().count();
    if count > MAX_TEXT_CHARS {
        return Err(Error::InvalidInput(format!(
            "text chunk of {count} characters exceeds the {MAX_TEXT_CHARS} character limit"
        )));
    }
    send_json(sink, &TextFrame { stream_id, text, text_end }).await
}

pub(crate) async fn send_cancel(sink: &mut WsSink, stream_id: &str) -> Result<()> {
    send_json(sink, &CancelFrame { stream_id, cancel: true }).await
}

pub(crate) async fn send_keepalive(sink: &mut WsSink) -> Result<()> {
    send_static(sink, r#"{"keep_alive":true}"#).await
}

/// One realtime synthesis stream on its own connection. Unlike STT this is
/// not split, because the text side is short and bursty rather than a
/// continuous pump.
///
/// An idle connection is closed by the server; send
/// [`keepalive`](Self::keepalive) every 20 to 30 seconds while holding one
/// open with nothing to say.
pub struct TtsSession {
    sink: WsSink,
    stream: WsStream,
    stream_id: String,
}

impl TtsSession {
    pub(crate) fn new(conn: Connection, stream_id: String) -> Self {
        let (sink, stream) = conn.into_split();
        Self { sink, stream, stream_id }
    }

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Send one chunk of text. Set `text_end` on the final chunk; that is what
    /// ends the utterance.
    pub async fn send_text(&mut self, text: &str, text_end: bool) -> Result<()> {
        send_text(&mut self.sink, &self.stream_id, text, text_end).await
    }

    /// End the utterance without sending more text.
    pub async fn finish(&mut self) -> Result<()> {
        self.send_text("", true).await
    }

    /// Abandon the stream server-side so no further audio is generated.
    pub async fn cancel(&mut self) -> Result<()> {
        send_cancel(&mut self.sink, &self.stream_id).await
    }

    pub async fn keepalive(&mut self) -> Result<()> {
        send_keepalive(&mut self.sink).await
    }

    /// The next audio chunk, or `None` once the connection closes.
    pub async fn next_audio(&mut self) -> Option<Result<TtsChunk>> {
        StreamExt::next(self).await
    }
}

impl Stream for TtsSession {
    type Item = Result<TtsChunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let frame = ready!(poll_text(&mut self.stream, cx));
        Poll::Ready(frame.map(|text| parse_frame(text?.as_str()).1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_serializes_only_what_is_set() {
        let cfg = TtsConfig::pcm_s16le("s1", "tts-rt-v2", "hu", "Adrian", 24_000);
        assert_eq!(
            serde_json::to_value(&cfg).unwrap(),
            serde_json::json!({
                "stream_id": "s1",
                "model": "tts-rt-v2",
                "language": "hu",
                "voice": "Adrian",
                "audio_format": "pcm_s16le",
                "sample_rate": 24_000,
            })
        );
        assert!(!format!("{cfg:?}").contains("api_key"));

        let cfg = TtsConfig::new("s1", "tts-rt-v2", "en", "Adrian", TtsAudioFormat::Mp3)
            .with_bitrate(128_000)
            .with_speed(1.25)
            .with_reduce_silence(true)
            .with_timestamps(true);
        let v = serde_json::to_value(&cfg).unwrap();
        assert_eq!(v["audio_format"], "mp3");
        assert_eq!(v["bitrate"], 128_000);
        assert_eq!(v["speed"], 1.25);
        assert_eq!(v["reduce_silence"], true);
        assert_eq!(v["return_timestamps"], true);
    }

    #[test]
    fn decodes_base64_audio_and_names_its_stream() {
        let (stream, chunk) = parse_frame(r#"{"stream_id":"s1","audio":"AAECAw=="}"#);
        let chunk = chunk.unwrap();
        assert_eq!(stream.as_deref(), Some("s1"));
        assert_eq!(chunk.audio, vec![0, 1, 2, 3]);
        assert!(!chunk.audio_end);
        assert!(!chunk.terminated);
    }

    /// Some encoders escape `/`, which base64 uses. The escaped form cannot be
    /// borrowed from the frame, and must decode all the same.
    #[test]
    fn decodes_audio_whose_json_string_carries_escapes() {
        let (_, chunk) = parse_frame(r#"{"stream_id":"s1","audio":"\/\/8="}"#);
        assert_eq!(chunk.unwrap().audio, vec![0xff, 0xff]);
    }

    #[test]
    fn an_event_without_audio_yields_an_empty_chunk_with_its_flags() {
        let (_, chunk) = parse_frame(r#"{"stream_id":"s1","audio_end":true}"#);
        let chunk = chunk.unwrap();
        assert!(chunk.audio.is_empty());
        assert!(chunk.audio_end, "audio_end means no more audio, not stop now");

        let (_, chunk) = parse_frame(r#"{"stream_id":"s1","terminated":true}"#);
        assert!(chunk.unwrap().terminated);
    }

    #[test]
    fn timestamps_ride_along_with_the_audio() {
        let (_, chunk) = parse_frame(
            r#"{"stream_id":"s1","audio":"AAEC","timestamps":{"characters":["H","i"],
                "character_start_times_seconds":[0.0,0.1],"character_end_times_seconds":[0.1,0.2]}}"#,
        );
        let timestamps = chunk.unwrap().timestamps.expect("timestamps");
        assert_eq!(timestamps.characters, ["H", "i"]);
        assert_eq!(timestamps.character_end_times_seconds, [0.1, 0.2]);
    }

    #[test]
    fn an_empty_stream_id_names_no_stream() {
        let (stream, chunk) =
            parse_frame(r#"{"stream_id":"","error_code":401,"error_message":"bad key"}"#);
        assert_eq!(stream, None, "a connection-level error must not be routed to a stream");
        assert!(chunk.is_err());
    }

    #[test]
    fn malformed_base64_is_reported_rather_than_silently_dropped() {
        let (_, chunk) = parse_frame(r#"{"stream_id":"s1","audio":"!!!not base64!!!"}"#);
        assert!(matches!(chunk, Err(Error::InvalidAudio(_))));
    }

    #[test]
    fn an_error_event_becomes_an_api_error_that_still_names_its_stream() {
        let (stream, chunk) =
            parse_frame(r#"{"stream_id":"s1","error_code":400,"error_message":"bad voice"}"#);
        assert_eq!(stream.as_deref(), Some("s1"));
        let err = chunk.unwrap_err();
        assert_eq!(err.api().map(|e| (e.code, e.message.as_str())), Some((400, "bad voice")));
    }
}
