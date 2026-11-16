use reqwest::Method;
use serde::Deserialize;

use super::json;
use super::voices::VoiceGender;
use crate::Client;
use crate::error::Result;

/// A language a model supports, as an ISO 639-1 code and its English name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct Language {
    pub code: String,
    pub name: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TranscriptionMode {
    RealTime,
    Async,
    #[default]
    #[serde(other)]
    Unknown,
}

/// One language a model can translate into, and from which sources.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct TranslationTarget {
    pub target_language: String,
    pub source_languages: Vec<String>,
    pub exclude_source_languages: Vec<String>,
}

/// A speech-to-text model and what it supports. The `supports_*` flags say
/// which session options the model accepts; sending one it does not support
/// is rejected.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct Model {
    pub id: String,
    /// Set when `id` is an alias, e.g. `stt-rt-v6` for a dated release.
    pub aliased_model_id: Option<String>,
    pub name: String,
    pub context_version: Option<u32>,
    pub transcription_mode: TranscriptionMode,
    pub languages: Vec<Language>,
    pub supports_language_hints_strict: bool,
    pub supports_max_endpoint_delay: bool,
    pub supports_endpoint_sensitivity: bool,
    pub supports_endpoint_latency_adjustment: bool,
    /// Highest `endpoint_latency_adjustment_level` accepted. 0 when the
    /// option is unsupported.
    pub endpoint_latency_adjustment_max_level: u8,
    pub supports_agent_text_input: bool,
    /// Empty when `one_way_translation` covers every language.
    pub translation_targets: Vec<TranslationTarget>,
    /// Empty when `two_way_translation` covers every pair.
    pub two_way_translation_pairs: Vec<String>,
    /// `all_languages` when any supported language can be a target.
    pub one_way_translation: Option<String>,
    /// `all_languages` when any pair of supported languages can be used.
    pub two_way_translation: Option<String>,
}

/// A stock voice of a text-to-speech model.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct TtsVoice {
    pub id: String,
    pub description: String,
    pub gender: VoiceGender,
}

/// A text-to-speech model and what it supports.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct TtsModel {
    pub id: String,
    pub aliased_model_id: Option<String>,
    pub name: String,
    pub voices: Vec<TtsVoice>,
    pub languages: Vec<Language>,
    pub supports_timestamps: bool,
    pub supports_voice_cloning: bool,
    /// Longest reference clip accepted for a cloned voice.
    pub voice_cloning_max_audio_duration_ms: Option<u64>,
    pub supports_speed_adjustment: bool,
    pub speed_min: Option<f32>,
    pub speed_max: Option<f32>,
    pub supports_silence_reduction: bool,
}

#[derive(Deserialize)]
struct ModelList<T> {
    #[serde(default = "Vec::new")]
    models: Vec<T>,
}

/// The models a key can use. Get one from [`Client::models`].
#[derive(Debug, Clone, Copy)]
pub struct Models<'a> {
    client: &'a Client,
}

impl<'a> Models<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// The speech-to-text models, realtime and asynchronous.
    pub async fn list(&self) -> Result<Vec<Model>> {
        let list: ModelList<Model> = json(self.client.request(Method::GET, &["models"])?).await?;
        Ok(list.models)
    }

    /// The text-to-speech models and their stock voices.
    pub async fn list_tts(&self) -> Result<Vec<TtsModel>> {
        let list: ModelList<TtsModel> =
            json(self.client.request(Method::GET, &["tts-models"])?).await?;
        Ok(list.models)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_model_decodes_with_its_capability_flags() {
        let model: Model = serde_json::from_str(
            r#"{"id":"stt-rt-v6","aliased_model_id":"stt-rt-v6-2026-06","name":"Realtime v6",
                "context_version":2,"transcription_mode":"real_time",
                "languages":[{"code":"hu","name":"Hungarian"}],
                "supports_language_hints_strict":true,"supports_endpoint_sensitivity":true,
                "endpoint_latency_adjustment_max_level":3,"supports_agent_text_input":true,
                "translation_targets":[],"two_way_translation_pairs":[],
                "one_way_translation":"all_languages","two_way_translation":null,
                "a_field_added_later":1}"#,
        )
        .unwrap();
        assert_eq!(model.transcription_mode, TranscriptionMode::RealTime);
        assert_eq!(model.languages[0].code, "hu");
        assert!(model.supports_agent_text_input);
        assert!(!model.supports_max_endpoint_delay, "an absent flag is false");
        assert_eq!(model.endpoint_latency_adjustment_max_level, 3);
        assert_eq!(model.one_way_translation.as_deref(), Some("all_languages"));
    }

    #[test]
    fn a_tts_model_decodes_with_its_voices_and_speed_range() {
        let model: TtsModel = serde_json::from_str(
            r#"{"id":"tts-rt-v2","aliased_model_id":null,"name":"TTS v2",
                "voices":[{"id":"Adrian","description":"Calm","gender":"male"}],
                "supports_speed_adjustment":true,"speed_min":0.7,"speed_max":1.3,
                "supports_voice_cloning":true,"voice_cloning_max_audio_duration_ms":30000}"#,
        )
        .unwrap();
        assert_eq!(model.voices[0].gender, VoiceGender::Male);
        assert_eq!(model.speed_max, Some(1.3));
        assert_eq!(model.voice_cloning_max_audio_duration_ms, Some(30_000));
        assert!(!model.supports_timestamps);
    }
}
