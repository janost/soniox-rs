//! Receiving the webhook Soniox calls when an asynchronous transcription
//! completes or fails.
//!
//! Soniox does not sign webhook bodies. Authenticity rests on a secret header
//! the job was created with (see
//! [`CreateTranscription::with_webhook`](crate::rest::CreateTranscription::with_webhook)),
//! which the receiver compares against what arrived.

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use crate::error::{Error, Result};
use crate::types::TranscriptionId;

/// The header name used when none is configured.
pub const DEFAULT_HEADER: &str = "X-Soniox-Webhook-Secret";

/// Environment variable naming the webhook auth header.
pub const HEADER_ENV: &str = "SONIOX_API_WEBHOOK_HEADER";
/// Environment variable holding the webhook auth secret.
pub const SECRET_ENV: &str = "SONIOX_API_WEBHOOK_SECRET";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum WebhookStatus {
    Completed,
    Error,
}

/// The body of a webhook call. It names the job and its outcome; fetch the
/// job or its transcript for the rest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub struct WebhookEvent {
    pub id: TranscriptionId,
    pub status: WebhookStatus,
}

impl WebhookEvent {
    pub fn parse(body: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(body)?)
    }
}

/// The header a webhook call must present to be believed.
#[derive(Debug, Clone)]
pub struct WebhookAuth {
    header_name: String,
    value: SecretString,
}

impl WebhookAuth {
    pub fn new(header_name: impl Into<String>, value: impl Into<SecretString>) -> Self {
        Self { header_name: header_name.into(), value: value.into() }
    }

    /// Read the secret from `SONIOX_API_WEBHOOK_SECRET` and the header name
    /// from `SONIOX_API_WEBHOOK_HEADER`, falling back to [`DEFAULT_HEADER`].
    /// `None` when no secret is set.
    pub fn from_env() -> Option<Self> {
        let value = std::env::var(SECRET_ENV).ok().filter(|value| !value.is_empty())?;
        let header_name = std::env::var(HEADER_ENV)
            .ok()
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| DEFAULT_HEADER.to_owned());
        Some(Self::new(header_name, value))
    }

    /// The header to look up on an incoming call. Header names compare
    /// case-insensitively.
    pub fn header_name(&self) -> &str {
        &self.header_name
    }

    pub(crate) fn expose_value(&self) -> &str {
        self.value.expose_secret()
    }

    /// Whether the header value an incoming call presented is the secret.
    /// Pass `None` when the header was absent. The comparison takes the same
    /// time wherever the first difference is, so response timing does not
    /// reveal how much of a guess was right.
    pub fn verify(&self, presented: Option<&str>) -> bool {
        presented.is_some_and(|presented| {
            constant_time_eq(presented.as_bytes(), self.expose_value().as_bytes())
        })
    }

    /// Check the presented header value and decode the body in one step.
    pub fn unwrap_event(&self, presented: Option<&str>, body: &[u8]) -> Result<WebhookEvent> {
        if !self.verify(presented) {
            return Err(Error::InvalidInput("the webhook secret header does not match".into()));
        }
        WebhookEvent::parse(body)
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // The length is not secret; only the content is.
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_documented_body() {
        let event = WebhookEvent::parse(br#"{"id":"548d023b-2b3d","status":"completed"}"#).unwrap();
        assert_eq!(event.id, TranscriptionId::from("548d023b-2b3d"));
        assert_eq!(event.status, WebhookStatus::Completed);
        assert!(WebhookEvent::parse(br#"{"id":"t-1","status":"queued"}"#).is_err());
    }

    #[test]
    fn only_the_exact_secret_verifies() {
        let auth = WebhookAuth::new(DEFAULT_HEADER, "s3cret");
        assert!(auth.verify(Some("s3cret")));
        assert!(!auth.verify(Some("s3cret ")));
        assert!(!auth.verify(Some("S3cret")));
        assert!(!auth.verify(Some("")));
        assert!(!auth.verify(None), "a missing header never verifies");
    }

    #[test]
    fn a_body_is_only_decoded_for_a_verified_call() {
        let auth = WebhookAuth::new(DEFAULT_HEADER, "s3cret");
        let body = br#"{"id":"t-1","status":"error"}"#;
        assert_eq!(auth.unwrap_event(Some("s3cret"), body).unwrap().status, WebhookStatus::Error);
        assert!(matches!(auth.unwrap_event(Some("wrong"), body), Err(Error::InvalidInput(_))));
    }

    #[test]
    fn debug_never_reveals_the_secret() {
        let auth = WebhookAuth::new(DEFAULT_HEADER, "s3cret-value");
        assert!(!format!("{auth:?}").contains("s3cret-value"));
    }
}
