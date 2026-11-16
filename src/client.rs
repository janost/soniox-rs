use std::sync::{Arc, OnceLock};
use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};
use url::Url;

use crate::endpoint::{Endpoints, Region, strip_version_segment};
use crate::error::{Error, Result};
use crate::realtime::stt::{SttConfig, SttSession};
use crate::realtime::tts::{TtsConfig, TtsSession};
use crate::realtime::tts_multi::TtsConnection;
use crate::realtime::{Connection, ensure_crypto_provider};
use crate::rest;

/// How long the client waits before giving up on the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    /// A TCP connect plus TLS and websocket handshake. Without it a
    /// black-holed endpoint parks the caller for the OS connect timeout,
    /// which is minutes.
    pub connect: Duration,
    /// One ordinary REST round trip: create, poll, fetch, delete. None of
    /// these scale with the size of the audio.
    pub request: Duration,
    /// One REST call that moves audio: an upload, or generated speech coming
    /// back. Its duration scales with the audio rather than the round trip,
    /// and a slow uplink is not a stalled one, so it gets a budget of its
    /// own. The default clears an hour of 24 kbps audio at a sustained
    /// 96 kbps.
    pub transfer: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            request: Duration::from_secs(60),
            transfer: Duration::from_secs(15 * 60),
        }
    }
}

/// The entry point: holds the API key and the endpoints, opens realtime
/// sessions, and gives access to the REST API.
///
/// Cloning is cheap, and clones share one HTTP connection pool.
#[derive(Debug, Clone)]
pub struct Client {
    api_key: SecretString,
    endpoints: Endpoints,
    timeouts: Timeouts,
    /// Built on the first REST call, so a client that only ever opens
    /// websockets does not pay for an HTTP client.
    http: Arc<OnceLock<reqwest::Client>>,
}

impl Client {
    /// Create a client pointed at the US region.
    pub fn new(api_key: impl Into<SecretString>) -> Self {
        // `reqwest` and `tokio-tungstenite` are both configured without a
        // crypto backend, so the first TLS use would panic without this.
        ensure_crypto_provider();
        Self {
            api_key: api_key.into(),
            endpoints: Endpoints::from(Region::Us),
            timeouts: Timeouts::default(),
            http: Arc::default(),
        }
    }

    /// Build a client from the environment, as the official SDKs do.
    ///
    /// `SONIOX_API_KEY` is required. `SONIOX_REGION` or `SONIOX_BASE_DOMAIN`
    /// select the host family, and `SONIOX_API_BASE_URL`, `SONIOX_WS_URL`,
    /// `SONIOX_TTS_API_URL` and `SONIOX_TTS_WS_URL` each override one
    /// endpoint.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// `from_env` with the lookup injected, so it can be tested without
    /// touching the process environment.
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let get = |name: &str| lookup(name).filter(|value| !value.trim().is_empty());
        let url = |name: &str| {
            get(name)
                .map(|raw| {
                    Url::parse(raw.trim())
                        .map_err(|e| Error::InvalidInput(format!("{name} is not a URL: {e}")))
                })
                .transpose()
        };

        let api_key = get("SONIOX_API_KEY")
            .ok_or_else(|| Error::InvalidInput("SONIOX_API_KEY is not set".into()))?;
        let mut endpoints = match (get("SONIOX_BASE_DOMAIN"), get("SONIOX_REGION")) {
            (Some(domain), _) => Endpoints::for_base_domain(domain.trim())?,
            (None, Some(region)) => region.parse::<Region>()?.into(),
            (None, None) => Region::Us.into(),
        };
        if let Some(api) = url("SONIOX_API_BASE_URL")? {
            endpoints.api = strip_version_segment(api);
        }
        if let Some(stt_ws) = url("SONIOX_WS_URL")? {
            endpoints.stt_ws = stt_ws;
        }
        if let Some(tts_api) = url("SONIOX_TTS_API_URL")? {
            endpoints.tts_api = tts_api;
        }
        if let Some(tts_ws) = url("SONIOX_TTS_WS_URL")? {
            endpoints.tts_ws = tts_ws;
        }
        Ok(Self::new(api_key).with_endpoints(endpoints))
    }

    /// Point every endpoint at a region. Call before any single-URL override.
    pub fn with_region(self, region: Region) -> Self {
        self.with_endpoints(region.into())
    }

    pub fn with_endpoints(mut self, endpoints: Endpoints) -> Self {
        self.endpoints = endpoints;
        self
    }

    /// Override the realtime speech-to-text websocket.
    pub fn with_stt_url(mut self, url: Url) -> Self {
        self.endpoints.stt_ws = url;
        self
    }

    /// Override the realtime text-to-speech websocket.
    pub fn with_tts_url(mut self, url: Url) -> Self {
        self.endpoints.tts_ws = url;
        self
    }

    /// Override the REST API base. A trailing `v1` segment is accepted and
    /// ignored.
    pub fn with_api_url(mut self, url: Url) -> Self {
        self.endpoints.api = strip_version_segment(url);
        self
    }

    /// Override the REST text-to-speech base.
    pub fn with_tts_api_url(mut self, url: Url) -> Self {
        self.endpoints.tts_api = url;
        self
    }

    pub fn with_timeouts(mut self, timeouts: Timeouts) -> Self {
        // The connect budget is baked into the HTTP client, so one built
        // under the old budgets cannot be kept.
        self.http = Arc::default();
        self.timeouts = timeouts;
        self
    }

    pub fn endpoints(&self) -> &Endpoints {
        &self.endpoints
    }

    pub fn stt_url(&self) -> &Url {
        &self.endpoints.stt_ws
    }

    pub fn tts_url(&self) -> &Url {
        &self.endpoints.tts_ws
    }

    pub fn timeouts(&self) -> Timeouts {
        self.timeouts
    }

    async fn connect(&self, url: &Url) -> Result<Connection> {
        Connection::connect(url, self.api_key.expose_secret(), self.timeouts.connect).await
    }

    /// Open a realtime transcription session. The key goes on the handshake;
    /// the config frame is sent before this returns.
    pub async fn connect_stt(&self, config: &SttConfig) -> Result<SttSession> {
        config.validate()?;
        let mut conn = self.connect(&self.endpoints.stt_ws).await?;
        conn.send_json(config).await?;
        Ok(SttSession::new(conn, config))
    }

    /// Open a realtime synthesis stream on a connection of its own.
    pub async fn connect_tts(&self, config: &TtsConfig) -> Result<TtsSession> {
        let mut conn = self.connect(&self.endpoints.tts_ws).await?;
        conn.send_json(config).await?;
        Ok(TtsSession::new(conn, config.stream_id.clone()))
    }

    /// Open a connection that carries several synthesis streams at once.
    /// Nothing is sent until a stream is opened on it.
    pub async fn connect_tts_multi(&self) -> Result<TtsConnection> {
        Ok(TtsConnection::new(self.connect(&self.endpoints.tts_ws).await?))
    }

    /// Uploaded audio files.
    pub fn files(&self) -> rest::Files<'_> {
        rest::Files::new(self)
    }

    /// Asynchronous transcription jobs.
    pub fn transcriptions(&self) -> rest::Transcriptions<'_> {
        rest::Transcriptions::new(self)
    }

    /// The speech-to-text and text-to-speech models the key can use.
    pub fn models(&self) -> rest::Models<'_> {
        rest::Models::new(self)
    }

    /// Text-to-speech over REST: one request, one audio response.
    pub fn tts(&self) -> rest::Tts<'_> {
        rest::Tts::new(self)
    }

    /// Cloned voices and the shared voice catalogue.
    pub fn voices(&self) -> rest::Voices<'_> {
        rest::Voices::new(self)
    }

    /// Temporary API keys for untrusted clients.
    pub fn auth(&self) -> rest::Auth<'_> {
        rest::Auth::new(self)
    }

    /// Usage logs, cost summaries and concurrency limits of the project.
    pub fn usage(&self) -> rest::Usage<'_> {
        rest::Usage::new(self)
    }

    /// Organization administration. Needs an organization API key rather
    /// than a project key.
    pub fn organization(&self) -> rest::Organization<'_> {
        rest::Organization::new(self)
    }

    fn http(&self) -> Result<&reqwest::Client> {
        if let Some(http) = self.http.get() {
            return Ok(http);
        }
        // Built outside `get_or_init` because the build is fallible. Two
        // racing callers both build; one client wins and the other is dropped.
        let http = reqwest::Client::builder()
            .connect_timeout(self.timeouts.connect)
            .build()
            .map_err(Error::Http)?;
        Ok(self.http.get_or_init(|| http))
    }

    /// Start an authenticated request to `url` under the given total budget.
    fn authorized(
        &self,
        method: reqwest::Method,
        url: Url,
        budget: Duration,
    ) -> Result<reqwest::RequestBuilder> {
        Ok(self
            .http()?
            .request(method, url)
            .bearer_auth(self.api_key.expose_secret())
            .timeout(budget))
    }

    /// Start a request to a `v1` REST resource.
    pub(crate) fn request(
        &self,
        method: reqwest::Method,
        segments: &[&str],
    ) -> Result<reqwest::RequestBuilder> {
        self.authorized(method, self.endpoints.api_v1(segments)?, self.timeouts.request)
    }

    /// Start a `v1` request that uploads audio, under the transfer budget.
    pub(crate) fn upload_request(&self, segments: &[&str]) -> Result<reqwest::RequestBuilder> {
        let url = self.endpoints.api_v1(segments)?;
        self.authorized(reqwest::Method::POST, url, self.timeouts.transfer)
    }

    /// Start a request to the REST text-to-speech endpoint.
    pub(crate) fn tts_request(&self) -> Result<reqwest::RequestBuilder> {
        let url = self.endpoints.tts_generate()?;
        self.authorized(reqwest::Method::POST, url, self.timeouts.transfer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn from_vars(vars: &[(&str, &str)]) -> Result<Client> {
        let vars: HashMap<String, String> =
            vars.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
        Client::from_lookup(|name| vars.get(name).cloned())
    }

    #[test]
    fn region_sets_every_endpoint_and_explicit_urls_override_one_at_a_time() {
        let c = Client::new("sk-x").with_region(Region::Eu);
        assert_eq!(c.stt_url().as_str(), "wss://stt-rt.eu.soniox.com/transcribe-websocket");
        assert_eq!(c.tts_url().as_str(), "wss://tts-rt.eu.soniox.com/tts-websocket");
        assert_eq!(c.endpoints().api.as_str(), "https://api.eu.soniox.com/");

        let c = c.with_stt_url(Url::parse("ws://127.0.0.1:9/x").unwrap());
        assert_eq!(c.stt_url().as_str(), "ws://127.0.0.1:9/x");
        assert_eq!(
            c.tts_url().as_str(),
            "wss://tts-rt.eu.soniox.com/tts-websocket",
            "overriding STT must not disturb TTS"
        );
    }

    #[test]
    fn defaults_to_the_us_region() {
        let c = Client::new("sk-x");
        assert_eq!(c.stt_url().as_str(), "wss://stt-rt.soniox.com/transcribe-websocket");
    }

    #[test]
    fn debug_never_reveals_the_key() {
        let c = Client::new("sk-supersecret");
        assert!(!format!("{c:?}").contains("sk-supersecret"));
    }

    #[test]
    fn the_environment_needs_a_key_and_nothing_else() {
        assert!(matches!(from_vars(&[]), Err(Error::InvalidInput(_))));
        assert!(matches!(from_vars(&[("SONIOX_API_KEY", "  ")]), Err(Error::InvalidInput(_))));
        let c = from_vars(&[("SONIOX_API_KEY", "sk-x")]).unwrap();
        assert_eq!(c.endpoints(), &Endpoints::from(Region::Us));
    }

    #[test]
    fn the_environment_selects_a_region_and_overrides_single_endpoints() {
        let c = from_vars(&[
            ("SONIOX_API_KEY", "sk-x"),
            ("SONIOX_REGION", "EU"),
            ("SONIOX_API_BASE_URL", "https://proxy.test/soniox/v1"),
            ("SONIOX_TTS_WS_URL", "ws://127.0.0.1:9/tts"),
        ])
        .unwrap();
        assert_eq!(c.endpoints().api.as_str(), "https://proxy.test/soniox/");
        assert_eq!(c.stt_url().as_str(), "wss://stt-rt.eu.soniox.com/transcribe-websocket");
        assert_eq!(c.tts_url().as_str(), "ws://127.0.0.1:9/tts");
        assert_eq!(c.endpoints().tts_api.as_str(), "https://tts-rt.eu.soniox.com/");
    }

    #[test]
    fn the_environment_rejects_what_it_cannot_parse() {
        let bad_region = from_vars(&[("SONIOX_API_KEY", "sk-x"), ("SONIOX_REGION", "mars")]);
        assert!(matches!(bad_region, Err(Error::InvalidInput(_))));
        let bad_url = from_vars(&[("SONIOX_API_KEY", "sk-x"), ("SONIOX_WS_URL", "not a url")]);
        assert!(matches!(bad_url, Err(Error::InvalidInput(_))));
    }

    #[test]
    fn a_base_domain_wins_over_a_region() {
        let c = from_vars(&[
            ("SONIOX_API_KEY", "sk-x"),
            ("SONIOX_REGION", "eu"),
            ("SONIOX_BASE_DOMAIN", "speech.internal.test"),
        ])
        .unwrap();
        assert_eq!(c.endpoints().api.as_str(), "https://api.speech.internal.test/");
    }
}
