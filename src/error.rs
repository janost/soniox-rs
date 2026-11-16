//! Error types shared by the realtime and REST clients.

use std::sync::Arc;

use serde::Deserialize;
use tokio_tungstenite::tungstenite;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// One field-level failure inside a `400` response.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub struct ValidationError {
    #[serde(default)]
    pub error_type: String,
    /// Where the bad value was, e.g. `body.audio_url`.
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub message: String,
}

/// An error reported by Soniox, over a websocket frame or as a non-2xx REST
/// response. Both carry the same information under different field names.
///
/// `message` is free text from the server and can echo request content (an
/// uploaded filename, a slice of submitted text). `request_id` and `code`
/// never do, so those are the fields to surface where request content must
/// stay out of logs.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ApiError {
    /// The HTTP status, or the websocket `error_code`, which uses HTTP status
    /// values. 0 when a websocket error frame carried no code.
    pub code: u16,
    /// Machine-readable category, e.g. `permission_denied`.
    pub error_type: Option<String>,
    pub message: String,
    pub request_id: Option<String>,
    pub validation_errors: Vec<ValidationError>,
    /// A documentation URL for resolving the error, when the server sends one.
    pub more_info: Option<String>,
}

/// What an [`ApiError`] means, derived from its code and type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ApiErrorKind {
    /// `400`: the request was malformed.
    InvalidRequest,
    /// `401`, or a `403` that is not a permission failure (an expired
    /// temporary key session, say).
    Authentication,
    /// `403` with type `permission_denied`: the key is valid but lacks the
    /// permission for this call.
    PermissionDenied,
    /// `402`: the balance or budget is exhausted.
    PaymentRequired,
    /// `404`.
    NotFound,
    /// `408`.
    Timeout,
    /// `409`: the resource is in the wrong state, e.g. deleting a
    /// transcription that is still processing.
    Conflict,
    /// `429`: a rate or concurrency limit was hit.
    RateLimited,
    /// `5xx`.
    Server,
    Other,
}

impl ApiError {
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            error_type: None,
            message: message.into(),
            request_id: None,
            validation_errors: Vec::new(),
            more_info: None,
        }
    }

    pub fn with_error_type(mut self, error_type: impl Into<String>) -> Self {
        self.error_type = Some(error_type.into());
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn kind(&self) -> ApiErrorKind {
        match self.code {
            400 => ApiErrorKind::InvalidRequest,
            401 => ApiErrorKind::Authentication,
            402 => ApiErrorKind::PaymentRequired,
            403 if self.error_type.as_deref() == Some("permission_denied") => {
                ApiErrorKind::PermissionDenied
            }
            403 => ApiErrorKind::Authentication,
            404 => ApiErrorKind::NotFound,
            408 => ApiErrorKind::Timeout,
            409 => ApiErrorKind::Conflict,
            429 => ApiErrorKind::RateLimited,
            500..=599 => ApiErrorKind::Server,
            _ => ApiErrorKind::Other,
        }
    }

    /// Whether the same request can succeed if sent again unchanged.
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind(), ApiErrorKind::Timeout | ApiErrorKind::Server)
    }

    /// Build the error for a non-2xx REST response.
    ///
    /// The body is normally `{status_code, error_type, message, ...}`; the TTS
    /// REST endpoint answers in the websocket shape instead. Anything else (a
    /// proxy's HTML page, an empty body) falls back to the status text, so a
    /// page of markup never becomes an error message.
    pub(crate) fn from_response(status: reqwest::StatusCode, body: &[u8]) -> Self {
        let wire: WireError = serde_json::from_slice(body).unwrap_or_default();
        let message = wire.message.or(wire.error_message).unwrap_or_else(|| {
            let text = String::from_utf8_lossy(body);
            let text = text.trim();
            let is_markup = text.starts_with('<');
            if text.is_empty() || is_markup || text.len() > MAX_FALLBACK_MESSAGE {
                status.canonical_reason().unwrap_or("request failed").to_owned()
            } else {
                text.to_owned()
            }
        });
        Self {
            code: status.as_u16(),
            error_type: wire.error_type,
            message,
            request_id: wire.request_id,
            validation_errors: wire.validation_errors,
            more_info: wire.more_info,
        }
    }

    /// Recognise a websocket error frame from its already-decoded fields.
    /// Returns `None` for an ordinary event.
    ///
    /// Only `error_message` is reliably present; `error_code` is absent on
    /// some validation failures, so it defaults to 0 rather than rejecting
    /// the frame.
    pub(crate) fn from_frame(fields: FrameError) -> Option<Self> {
        if fields.error_code.is_none() && fields.error_message.is_none() {
            return None;
        }
        Some(Self {
            code: fields.error_code.and_then(|code| u16::try_from(code).ok()).unwrap_or(0),
            error_type: fields.error_type,
            message: fields.error_message.unwrap_or_else(|| "unknown error".to_owned()),
            request_id: fields.request_id,
            validation_errors: Vec::new(),
            more_info: None,
        })
    }
}

/// A non-JSON error body longer than this is replaced by the status text.
const MAX_FALLBACK_MESSAGE: usize = 512;

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "soniox error {}: {}", self.code, self.message)?;
        for error in &self.validation_errors {
            write!(f, "; {}: {}", error.location, error.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiError {}

/// The error fields of a websocket frame. Session event types embed these
/// four fields directly, so one deserialization pass yields either the event
/// or the error.
#[derive(Debug, Default)]
pub(crate) struct FrameError {
    pub error_code: Option<u64>,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
    pub request_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WireError {
    error_type: Option<String>,
    message: Option<String>,
    error_message: Option<String>,
    request_id: Option<String>,
    #[serde(default)]
    validation_errors: Vec<ValidationError>,
    more_info: Option<String>,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The websocket handshake failed. `url` never carries credentials: the
    /// API key travels in an `Authorization` header, not the URL.
    #[error("failed to connect to {url}")]
    Connect {
        url: String,
        #[source]
        source: Box<tungstenite::Error>,
    },

    /// The handshake did not complete inside the client's connect budget.
    #[error("timed out connecting to {url}")]
    ConnectTimeout { url: String },

    /// A websocket failed mid-session. The detail is in the message as well
    /// as the source, because callers render errors with `{}`.
    #[error("websocket error: {0}")]
    WebSocket(#[source] Arc<tungstenite::Error>),

    /// An HTTP request did not produce a response: connect failure, timeout,
    /// or a body that broke off.
    #[error("HTTP request failed: {0}")]
    Http(#[source] reqwest::Error),

    #[error("could not decode server message: {0}")]
    Decode(#[from] serde_json::Error),

    #[error("audio payload is not valid base64: {0}")]
    InvalidAudio(#[from] base64::DecodeError),

    /// Boxed so that `Result<T>` stays small on the paths that move one per
    /// event.
    #[error("{0}")]
    Api(#[source] Box<ApiError>),

    /// The connection is closed, so nothing more can be sent or received.
    #[error("connection closed")]
    Closed,

    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// A polling helper ran out of time. The job itself may still finish.
    #[error("timed out waiting for {0}")]
    Timeout(String),

    #[error("could not read {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// The connection under a multiplexed TTS stream failed. Every stream on
    /// that connection receives the same cause.
    #[error(transparent)]
    Shared(Arc<Error>),
}

impl Error {
    /// The server-reported error, if that is what this is.
    pub fn api(&self) -> Option<&ApiError> {
        match self {
            Error::Api(e) => Some(e),
            Error::Shared(inner) => inner.api(),
            _ => None,
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.api().is_some_and(|e| e.kind() == ApiErrorKind::NotFound)
    }

    /// Whether reconnecting or resending can succeed: transport failures and
    /// server-side `408`/`5xx`. Rejected keys, bad requests and exhausted
    /// quotas are not retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Connect { .. }
            | Error::ConnectTimeout { .. }
            | Error::WebSocket(_)
            | Error::Http(_)
            | Error::Closed => true,
            Error::Api(e) => e.is_retryable(),
            Error::Shared(inner) => inner.is_retryable(),
            _ => false,
        }
    }
}

impl From<ApiError> for Error {
    fn from(error: ApiError) -> Self {
        Error::Api(Box::new(error))
    }
}

impl From<tungstenite::Error> for Error {
    fn from(error: tungstenite::Error) -> Self {
        match error {
            tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
                Error::Closed
            }
            other => Error::WebSocket(Arc::new(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(code: Option<u64>, message: Option<&str>) -> FrameError {
        FrameError {
            error_code: code,
            error_message: message.map(str::to_owned),
            ..Default::default()
        }
    }

    #[test]
    fn a_frame_with_only_a_message_is_an_error_with_code_zero() {
        let e = ApiError::from_frame(frame(None, Some("bad voice"))).expect("an error frame");
        assert_eq!(e.code, 0);
        assert_eq!(e.message, "bad voice");
    }

    #[test]
    fn a_frame_without_error_fields_is_not_an_error() {
        assert!(ApiError::from_frame(FrameError::default()).is_none());
    }

    #[test]
    fn an_out_of_range_code_does_not_wrap_into_a_plausible_one() {
        // 65536 + 401 truncates to 401 under an `as u16` cast.
        let e = ApiError::from_frame(frame(Some(65_937), Some("x"))).unwrap();
        assert_eq!(e.code, 0);
    }

    #[test]
    fn parses_the_rest_error_shape_with_validation_errors() {
        let body = br#"{"status_code":400,"error_type":"invalid_request","message":"Invalid request.",
            "validation_errors":[{"error_type":"missing","location":"body.model","message":"Field required"}],
            "request_id":"req-1","more_info":"https://soniox.com/docs"}"#;
        let e = ApiError::from_response(reqwest::StatusCode::BAD_REQUEST, body);
        assert_eq!(e.code, 400);
        assert_eq!(e.kind(), ApiErrorKind::InvalidRequest);
        assert_eq!(e.request_id.as_deref(), Some("req-1"));
        assert_eq!(e.validation_errors[0].location, "body.model");
        assert_eq!(e.to_string(), "soniox error 400: Invalid request.; body.model: Field required");
    }

    #[test]
    fn parses_the_websocket_shape_the_tts_rest_endpoint_answers_in() {
        let body =
            br#"{"error_code":400,"error_type":"bad_request","error_message":"unknown voice"}"#;
        let e = ApiError::from_response(reqwest::StatusCode::BAD_REQUEST, body);
        assert_eq!(e.message, "unknown voice");
        assert_eq!(e.error_type.as_deref(), Some("bad_request"));
    }

    #[test]
    fn a_markup_body_falls_back_to_the_status_text() {
        let e = ApiError::from_response(
            reqwest::StatusCode::BAD_GATEWAY,
            b"<!doctype html><html>proxy error</html>",
        );
        assert_eq!(e.message, "Bad Gateway");
        assert!(e.is_retryable());
    }

    #[test]
    fn a_403_is_a_permission_failure_only_when_typed_as_one() {
        let denied = ApiError::new(403, "no").with_error_type("permission_denied");
        assert_eq!(denied.kind(), ApiErrorKind::PermissionDenied);
        let expired = ApiError::new(403, "no").with_error_type("temp_api_key_session_expired");
        assert_eq!(expired.kind(), ApiErrorKind::Authentication);
    }

    /// thiserror does not append a source, so the detail has to be in the
    /// format string: without it every mid-session transport failure reads
    /// "websocket error" and nothing more.
    #[test]
    fn a_websocket_error_carries_its_detail() {
        let cause = tungstenite::Error::AttackAttempt;
        let rendered = Error::from(tungstenite::Error::AttackAttempt).to_string();
        assert!(rendered.starts_with("websocket error: "), "got {rendered}");
        assert!(rendered.contains(&cause.to_string()));
    }

    #[test]
    fn using_a_closed_socket_is_reported_as_closed() {
        assert!(matches!(Error::from(tungstenite::Error::ConnectionClosed), Error::Closed));
        assert!(matches!(Error::from(tungstenite::Error::AlreadyClosed), Error::Closed));
    }

    #[test]
    fn quota_and_auth_failures_are_not_retryable() {
        assert!(!Error::from(ApiError::new(401, "x")).is_retryable());
        assert!(!Error::from(ApiError::new(429, "x")).is_retryable());
        assert!(Error::from(ApiError::new(503, "x")).is_retryable());
        assert!(Error::ConnectTimeout { url: "wss://x".into() }.is_retryable());
    }
}
