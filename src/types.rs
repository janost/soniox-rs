//! Types shared by the realtime and asynchronous speech-to-text APIs.

use serde::{Deserialize, Serialize};

/// Declare a request-side enum together with its wire names. `rename_all`
/// cannot spell names like `pcm_s16le`, and a per-variant rename hides the
/// mapping from `as_str`, so the table lives in one place instead.
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident => $wire:literal,)+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        $vis enum $name {
            $($(#[$variant_meta])* $variant,)+
        }

        impl $name {
            /// The value as it appears on the wire.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }
    };
}
pub(crate) use wire_enum;

/// Declare a string newtype for one kind of server-issued id, so a file id
/// cannot be passed where a transcription id is expected.
macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(id: String) -> Self {
                Self(id)
            }
        }

        impl From<&str> for $name {
            fn from(id: &str) -> Self {
                Self(id.to_owned())
            }
        }
    };
}

id_type!(
    /// Id of an uploaded file.
    FileId
);
id_type!(
    /// Id of an asynchronous transcription job.
    TranscriptionId
);
id_type!(
    /// Id of a cloned voice.
    VoiceId
);

pub(crate) fn is_false(value: &bool) -> bool {
    !*value
}

wire_enum! {
    /// Audio encodings the realtime speech-to-text websocket accepts.
    pub enum AudioFormat {
        /// Let the server detect a container format from the stream.
        Auto => "auto",
        Aac => "aac",
        Aiff => "aiff",
        Amr => "amr",
        Asf => "asf",
        Flac => "flac",
        Mp3 => "mp3",
        Ogg => "ogg",
        Wav => "wav",
        Webm => "webm",
        PcmS8 => "pcm_s8",
        PcmS16Le => "pcm_s16le",
        PcmS16Be => "pcm_s16be",
        PcmS24Le => "pcm_s24le",
        PcmS24Be => "pcm_s24be",
        PcmS32Le => "pcm_s32le",
        PcmS32Be => "pcm_s32be",
        PcmU8 => "pcm_u8",
        PcmU16Le => "pcm_u16le",
        PcmU16Be => "pcm_u16be",
        PcmU24Le => "pcm_u24le",
        PcmU24Be => "pcm_u24be",
        PcmU32Le => "pcm_u32le",
        PcmU32Be => "pcm_u32be",
        PcmF32Le => "pcm_f32le",
        PcmF32Be => "pcm_f32be",
        PcmF64Le => "pcm_f64le",
        PcmF64Be => "pcm_f64be",
        Mulaw => "mulaw",
        Alaw => "alaw",
    }
}

impl AudioFormat {
    /// Raw formats carry no header, so the sample rate and channel count
    /// must be declared alongside them.
    pub const fn is_raw(self) -> bool {
        !matches!(
            self,
            Self::Auto
                | Self::Aac
                | Self::Aiff
                | Self::Amr
                | Self::Asf
                | Self::Flac
                | Self::Mp3
                | Self::Ogg
                | Self::Wav
                | Self::Webm
        )
    }
}

/// One `key: value` fact about the audio, e.g. `domain: Healthcare`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContextEntry {
    pub key: String,
    pub value: String,
}

/// A fixed translation for a term the model would otherwise render freely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TranslationTerm {
    pub source: String,
    pub target: String,
}

/// Recognition context sent with a request, realtime or asynchronous. An
/// empty context is not sent at all.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Context {
    /// Structured facts: domain, topic, participant names.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub general: Vec<ContextEntry>,
    /// Free-form background: prior conversation, meeting notes, documents.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Names and jargon to bias recognition toward.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub terms: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub translation_terms: Vec<TranslationTerm>,
}

impl Context {
    pub fn is_empty(&self) -> bool {
        self.general.is_empty()
            && self.text.is_none()
            && self.terms.is_empty()
            && self.translation_terms.is_empty()
    }

    pub fn with_general(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.general.push(ContextEntry { key: key.into(), value: value.into() });
        self
    }

    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    pub fn with_terms(mut self, terms: Vec<String>) -> Self {
        self.terms = terms;
        self
    }

    pub fn with_translation_term(
        mut self,
        source: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        self.translation_terms
            .push(TranslationTerm { source: source.into(), target: target.into() });
        self
    }
}

/// Soniox translation, as the `translation` block of a request. One-way turns
/// everything heard into `target_language`; two-way translates between a pair
/// in both directions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TranslationConfig {
    OneWay { target_language: String },
    TwoWay { language_a: String, language_b: String },
}

impl TranslationConfig {
    pub fn one_way(target_language: impl Into<String>) -> Self {
        Self::OneWay { target_language: target_language.into() }
    }

    pub fn two_way(language_a: impl Into<String>, language_b: impl Into<String>) -> Self {
        Self::TwoWay { language_a: language_a.into(), language_b: language_b.into() }
    }
}

/// What a token is, once translation is on. Soniox sends the spoken tokens
/// as `original` and the generated ones as `translation`; with translation
/// off, every token is `none` (or carries no status at all).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranslationStatus {
    #[default]
    None,
    Original,
    Translation,
    /// A value this crate does not know. Kept rather than dropped so a new
    /// server value shows up in a log instead of vanishing.
    #[serde(untagged)]
    Unknown(String),
}

/// Soniox injects these pseudo-tokens into the realtime stream. They are
/// control signals, never text, and must be filtered before rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marker {
    /// `<end>`: an utterance endpoint was detected.
    Endpoint,
    /// `<fin>`: a `finalize` request has drained.
    Finalized,
}

impl Marker {
    /// The token text that carries this marker.
    pub const fn as_str(self) -> &'static str {
        match self {
            Marker::Endpoint => "<end>",
            Marker::Finalized => "<fin>",
        }
    }
}

/// One recognised piece of speech. Tokens are sub-word: concatenating their
/// `text` in order, with no separator, gives the transcript.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Token {
    #[serde(default)]
    pub text: String,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub confidence: Option<f32>,
    /// Absent on tokens of an asynchronous transcript as the server sends
    /// them; [`crate::rest::Transcriptions::transcript`] fills it in.
    pub is_final: Option<bool>,
    pub speaker: Option<String>,
    pub language: Option<String>,
    /// Read `translation_status()` instead: it folds an absent value and
    /// `none` together.
    pub translation_status: Option<TranslationStatus>,
    pub source_language: Option<String>,
    /// Set on a token that names a sound rather than speech. Asynchronous
    /// transcripts only.
    pub is_audio_event: Option<bool>,
}

impl Token {
    pub fn marker(&self) -> Option<Marker> {
        match self.text.as_str() {
            "<end>" => Some(Marker::Endpoint),
            "<fin>" => Some(Marker::Finalized),
            _ => None,
        }
    }

    /// True when this token carries renderable text rather than a marker.
    pub fn is_text(&self) -> bool {
        self.marker().is_none()
    }

    pub fn is_final(&self) -> bool {
        self.is_final.unwrap_or(false)
    }

    /// The token's role under translation. Absent means `None`.
    pub fn translation_status(&self) -> TranslationStatus {
        self.translation_status.clone().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_formats_are_the_ones_without_a_container() {
        assert!(AudioFormat::PcmS16Le.is_raw());
        assert!(AudioFormat::Mulaw.is_raw());
        assert!(!AudioFormat::Auto.is_raw());
        assert!(!AudioFormat::Webm.is_raw());
        assert_eq!(serde_json::to_value(AudioFormat::PcmF32Le).unwrap(), "pcm_f32le");
    }

    #[test]
    fn an_empty_context_serializes_to_nothing_and_a_full_one_to_every_block() {
        assert_eq!(serde_json::to_value(Context::default()).unwrap(), serde_json::json!({}));
        let context = Context::default()
            .with_general("domain", "Healthcare")
            .with_text("Follow-up visit.")
            .with_terms(vec!["Kovács".into()])
            .with_translation_term("Mr. Smith", "Sr. Smith");
        assert_eq!(
            serde_json::to_value(&context).unwrap(),
            serde_json::json!({
                "general": [{"key": "domain", "value": "Healthcare"}],
                "text": "Follow-up visit.",
                "terms": ["Kovács"],
                "translation_terms": [{"source": "Mr. Smith", "target": "Sr. Smith"}],
            })
        );
    }

    #[test]
    fn translation_serializes_as_documented() {
        assert_eq!(
            serde_json::to_value(TranslationConfig::one_way("hu")).unwrap(),
            serde_json::json!({"type": "one_way", "target_language": "hu"})
        );
        assert_eq!(
            serde_json::to_value(TranslationConfig::two_way("en", "hu")).unwrap(),
            serde_json::json!({"type": "two_way", "language_a": "en", "language_b": "hu"})
        );
    }

    #[test]
    fn recognises_marker_tokens_and_excludes_them_from_text() {
        let end = Token { text: "<end>".into(), ..Default::default() };
        let fin = Token { text: "<fin>".into(), ..Default::default() };
        let word = Token { text: " szia".into(), ..Default::default() };

        assert_eq!(end.marker(), Some(Marker::Endpoint));
        assert_eq!(fin.marker(), Some(Marker::Finalized));
        assert_eq!(word.marker(), None);
        assert!(!end.is_text());
        assert!(word.is_text());
        assert!(!word.is_final(), "is_final defaults to false when absent");
    }

    #[test]
    fn translation_status_maps_the_documented_values_and_keeps_an_unknown_one() {
        let status = |json: &str| serde_json::from_str::<Token>(json).unwrap().translation_status();
        assert_eq!(status(r#"{"text":"x"}"#), TranslationStatus::None);
        assert_eq!(status(r#"{"text":"x","translation_status":null}"#), TranslationStatus::None);
        assert_eq!(status(r#"{"text":"x","translation_status":"none"}"#), TranslationStatus::None);
        assert_eq!(
            status(r#"{"text":"x","translation_status":"original"}"#),
            TranslationStatus::Original
        );
        assert_eq!(
            status(r#"{"text":"x","translation_status":"translation"}"#),
            TranslationStatus::Translation
        );
        assert_eq!(
            status(r#"{"text":"x","translation_status":"future"}"#),
            TranslationStatus::Unknown("future".into())
        );
    }

    #[test]
    fn a_translated_token_carries_its_source_language_and_no_timestamps() {
        let t: Token = serde_json::from_str(
            r#"{"text":"Szia","is_final":true,"translation_status":"translation","language":"hu","source_language":"en"}"#,
        )
        .unwrap();
        assert_eq!(t.translation_status(), TranslationStatus::Translation);
        assert_eq!(t.source_language.as_deref(), Some("en"));
        assert_eq!(t.language.as_deref(), Some("hu"));
        assert!(t.start_ms.is_none() && t.end_ms.is_none());
    }

    #[test]
    fn ids_are_transparent_strings_on_the_wire() {
        let id: FileId = serde_json::from_str("\"f-1\"").unwrap();
        assert_eq!(id, FileId::from("f-1"));
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"f-1\"");
        assert_eq!(id.to_string(), "f-1");
    }
}
