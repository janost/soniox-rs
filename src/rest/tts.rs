use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use serde::Serialize;

use super::send;
use crate::Client;
use crate::error::{Error, Result};
use crate::realtime::tts::{MAX_TEXT_CHARS, TtsAudioFormat};
use crate::types::is_false;

/// One piece of text to synthesize over REST.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct TtsRequest {
    pub model: String,
    pub language: String,
    pub voice: String,
    pub audio_format: TtsAudioFormat,
    /// At most [`MAX_TEXT_CHARS`] characters.
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    /// Bits per second, for the compressed formats.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bitrate: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f32>,
    #[serde(skip_serializing_if = "is_false")]
    pub reduce_silence: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_reference_id: Option<String>,
}

impl TtsRequest {
    /// A request for WAV audio. Change the format with
    /// [`with_audio_format`](Self::with_audio_format).
    pub fn new(
        model: impl Into<String>,
        language: impl Into<String>,
        voice: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            model: model.into(),
            language: language.into(),
            voice: voice.into(),
            audio_format: TtsAudioFormat::Wav,
            text: text.into(),
            sample_rate: None,
            bitrate: None,
            speed: None,
            reduce_silence: false,
            client_reference_id: None,
        }
    }

    pub fn with_audio_format(mut self, format: TtsAudioFormat) -> Self {
        self.audio_format = format;
        self
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
    /// [`TtsModel`](super::TtsModel).
    pub fn with_speed(mut self, speed: f32) -> Self {
        self.speed = Some(speed);
        self
    }

    /// Shorten the pauses between words, on models that support it.
    pub fn with_reduce_silence(mut self, on: bool) -> Self {
        self.reduce_silence = on;
        self
    }

    /// A tracking id echoed in usage logs. Need not be unique.
    pub fn with_client_reference_id(mut self, id: impl Into<String>) -> Self {
        self.client_reference_id = Some(id.into());
        self
    }

    fn validate(&self) -> Result<()> {
        let count = self.text.chars().count();
        if count == 0 {
            return Err(Error::InvalidInput("there is no text to synthesize".into()));
        }
        if count > MAX_TEXT_CHARS {
            return Err(Error::InvalidInput(format!(
                "text of {count} characters exceeds the {MAX_TEXT_CHARS} character limit"
            )));
        }
        Ok(())
    }
}

/// Text-to-speech over REST. Get one from [`Client::tts`].
///
/// For text that arrives piecemeal, or audio that must start playing before
/// the text is complete, use [`Client::connect_tts`] instead.
#[derive(Debug, Clone, Copy)]
pub struct Tts<'a> {
    client: &'a Client,
}

impl<'a> Tts<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Synthesize `request` and return the whole audio.
    pub async fn generate(&self, request: &TtsRequest) -> Result<Bytes> {
        request.validate()?;
        let response = send(self.client.tts_request()?.json(request)).await?;
        response.bytes().await.map_err(Error::Http)
    }

    /// Synthesize `request` and yield the audio as it is generated, so
    /// playback or a file write can start before the last byte exists.
    pub async fn generate_stream(
        &self,
        request: &TtsRequest,
    ) -> Result<impl Stream<Item = Result<Bytes>> + Send + use<>> {
        request.validate()?;
        let response = send(self.client.tts_request()?.json(request)).await?;
        Ok(response.bytes_stream().map_err(Error::Http))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_payload_defaults_to_wav_and_omits_what_is_unset() {
        let request = TtsRequest::new("tts-rt-v2", "en", "Adrian", "Hello");
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({
                "model": "tts-rt-v2", "language": "en", "voice": "Adrian",
                "audio_format": "wav", "text": "Hello",
            })
        );
        let v = serde_json::to_value(
            request
                .with_audio_format(TtsAudioFormat::Opus)
                .with_bitrate(64_000)
                .with_sample_rate(48_000)
                .with_speed(0.9)
                .with_reduce_silence(true)
                .with_client_reference_id("r-1"),
        )
        .unwrap();
        assert_eq!(v["audio_format"], "opus");
        assert_eq!(v["bitrate"], 64_000);
        assert_eq!(v["sample_rate"], 48_000);
        assert_eq!(v["reduce_silence"], true);
        assert_eq!(v["client_reference_id"], "r-1");
    }

    #[test]
    fn empty_and_oversized_text_are_rejected_before_any_request() {
        let empty = TtsRequest::new("tts-rt-v2", "en", "Adrian", "");
        assert!(matches!(empty.validate(), Err(Error::InvalidInput(_))));
        let long = TtsRequest::new("tts-rt-v2", "en", "Adrian", "é".repeat(MAX_TEXT_CHARS + 1));
        assert!(matches!(long.validate(), Err(Error::InvalidInput(_))));
        // The limit counts characters, not bytes: 5000 two-byte characters fit.
        let exact = TtsRequest::new("tts-rt-v2", "en", "Adrian", "é".repeat(MAX_TEXT_CHARS));
        assert!(exact.validate().is_ok());
    }
}
