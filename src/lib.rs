//! Rust client for the [Soniox](https://soniox.com) speech APIs.
//!
//! - Realtime speech-to-text and text-to-speech over websockets:
//!   [`Client::connect_stt`], [`Client::connect_tts`],
//!   [`Client::connect_tts_multi`].
//! - The REST API, one handle per resource: [`Client::files`],
//!   [`Client::transcriptions`], [`Client::models`], [`Client::tts`],
//!   [`Client::voices`], [`Client::auth`], [`Client::usage`],
//!   [`Client::organization`].
//! - Helpers around them: [`segments`] groups tokens into segments and
//!   utterances, [`webhooks`] receives job notifications, [`languages`] lists
//!   what Soniox supports.
//!
//! The API key never appears in a config type, a request body or a URL. It
//! travels in an `Authorization` header and is held as a
//! [`secrecy::SecretString`], so `{:?}` on anything in this crate is safe to
//! log.
//!
//! TLS runs on rustls with the `ring` provider. The first [`Client`] installs
//! it as the process default unless the application installed one already.

mod client;
mod endpoint;
pub mod error;
pub mod languages;
pub mod realtime;
pub mod rest;
pub mod segments;
mod types;
pub mod webhooks;

pub use client::{Client, Timeouts};
pub use endpoint::{Endpoints, Region};
pub use error::{ApiError, ApiErrorKind, Error, Result, ValidationError};
pub use realtime::stt::{AudioSink, Event, EventStream, SttConfig, SttSession};
pub use realtime::tts::{
    MAX_TEXT_CHARS, TtsAudioFormat, TtsChunk, TtsConfig, TtsSession, TtsTimestamps,
};
pub use realtime::tts_multi::{MAX_STREAMS_PER_CONNECTION, TtsConnection, TtsStream};
pub use rest::{
    AudioSource, CreateTranscription, DEFAULT_ASYNC_MODEL, File, Page, Transcript, Transcription,
    TranscriptionStatus, TtsRequest, Upload, WaitOptions,
};
pub use types::{
    AudioFormat, Context, ContextEntry, FileId, Marker, Token, TranscriptionId, TranslationConfig,
    TranslationStatus, TranslationTerm, VoiceId,
};
