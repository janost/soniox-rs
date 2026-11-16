use chrono::{DateTime, Utc};
use reqwest::Method;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use super::json;
use crate::Client;
use crate::error::Result;
use crate::types::wire_enum;

wire_enum! {
    /// What a temporary key may be used for.
    pub enum TemporaryApiKeyUsage {
        /// Realtime speech-to-text.
        TranscribeWebsocket => "transcribe_websocket",
        /// Realtime text-to-speech.
        TtsRealtime => "tts_rt",
    }
}

/// A request for a short-lived key to hand to an untrusted client, such as a
/// browser, so the real key never leaves the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TemporaryApiKeyRequest {
    pub usage_type: TemporaryApiKeyUsage,
    /// 1 to 3600.
    pub expires_in_seconds: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_reference_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single_use: Option<bool>,
    /// Caps how long one connection made with the key may last. 1 to 18000.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_session_duration_seconds: Option<u32>,
}

impl TemporaryApiKeyRequest {
    pub fn new(usage_type: TemporaryApiKeyUsage, expires_in_seconds: u32) -> Self {
        Self {
            usage_type,
            expires_in_seconds,
            client_reference_id: None,
            single_use: None,
            max_session_duration_seconds: None,
        }
    }

    pub fn with_client_reference_id(mut self, id: impl Into<String>) -> Self {
        self.client_reference_id = Some(id.into());
        self
    }

    /// Let the key open exactly one connection.
    pub fn with_single_use(mut self, on: bool) -> Self {
        self.single_use = Some(on);
        self
    }

    pub fn with_max_session_duration_seconds(mut self, seconds: u32) -> Self {
        self.max_session_duration_seconds = Some(seconds);
        self
    }
}

/// A temporary key. `{:?}` does not print the key itself.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct TemporaryApiKey {
    pub api_key: SecretString,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Temporary API keys. Get one from [`Client::auth`].
#[derive(Debug, Clone, Copy)]
pub struct Auth<'a> {
    client: &'a Client,
}

impl<'a> Auth<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn create_temporary_api_key(
        &self,
        request: &TemporaryApiKeyRequest,
    ) -> Result<TemporaryApiKey> {
        json(self.client.request(Method::POST, &["auth", "temporary-api-key"])?.json(request)).await
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    #[test]
    fn the_request_names_its_usage_and_omits_what_is_unset() {
        let request = TemporaryApiKeyRequest::new(TemporaryApiKeyUsage::TranscribeWebsocket, 60);
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({"usage_type": "transcribe_websocket", "expires_in_seconds": 60})
        );
        let request = TemporaryApiKeyRequest::new(TemporaryApiKeyUsage::TtsRealtime, 60)
            .with_single_use(true)
            .with_max_session_duration_seconds(300);
        let v = serde_json::to_value(&request).unwrap();
        assert_eq!(v["usage_type"], "tts_rt");
        assert_eq!(v["single_use"], true);
        assert_eq!(v["max_session_duration_seconds"], 300);
    }

    #[test]
    fn the_key_decodes_and_stays_out_of_debug_output() {
        let key: TemporaryApiKey = serde_json::from_str(
            r#"{"api_key":"temp:abc123","expires_at":"2026-11-03T10:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(key.api_key.expose_secret(), "temp:abc123");
        assert!(!format!("{key:?}").contains("abc123"));
    }
}
