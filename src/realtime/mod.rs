//! The realtime websocket APIs: speech-to-text sessions and text-to-speech
//! streams. This module holds the connect and framing plumbing they share.

pub mod stt;
pub mod tts;
pub mod tts_multi;

use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::Request;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, HeaderValue};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use url::Url;

use crate::error::{Error, Result};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(crate) type WsSink = SplitSink<Ws, Message>;
pub(crate) type WsStream = SplitStream<Ws>;

/// rustls 0.23 needs a process-wide `CryptoProvider` before the first TLS
/// handshake, and neither `tokio-tungstenite` nor `reqwest` selects a backend
/// as this crate configures them. Installing one here, rather than leaning on
/// the auto-install a single enabled backend feature would give, keeps the
/// `rustls` dependency load-bearing in code: removing it becomes a compile
/// error instead of a panic on the first TLS connect, which no plaintext test
/// would catch.
///
/// A provider the application installed first is left in place.
pub(crate) fn ensure_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Errors only when a provider is already installed, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// The handshake request: the URL as given, plus the key as a bearer token.
///
/// The key is deliberately not in the config frame, so `{:?}` on any config
/// type and the frame itself are both safe to log by construction. The header
/// is marked sensitive so `http`'s `Debug` prints it redacted too.
fn request_for(url: &Url, api_key: &str) -> Result<Request<()>> {
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|source| Error::Connect { url: url.to_string(), source: Box::new(source) })?;
    let mut value = HeaderValue::from_str(&format!("Bearer {api_key}"))
        .map_err(|_| Error::InvalidInput("the API key is not a valid header value".into()))?;
    value.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, value);
    Ok(request)
}

/// An open realtime websocket, before it is specialised into STT or TTS.
pub(crate) struct Connection {
    ws: Ws,
}

impl Connection {
    /// Open the socket, giving up after `budget`.
    ///
    /// Without the budget a black-holed endpoint (a typo'd URL, or the wrong
    /// region behind a corporate proxy) parks the caller for the OS TCP
    /// connect timeout, which is minutes.
    ///
    /// Nagle is off: audio and text go out as many small frames, and holding
    /// one back until the previous is acknowledged adds latency for nothing.
    pub(crate) async fn connect(url: &Url, api_key: &str, budget: Duration) -> Result<Self> {
        ensure_crypto_provider();
        let request = request_for(url, api_key)?;
        let handshake = connect_async_with_config(request, None, true);
        let Ok(result) = tokio::time::timeout(budget, handshake).await else {
            return Err(Error::ConnectTimeout { url: url.to_string() });
        };
        let (ws, _response) = result
            .map_err(|source| Error::Connect { url: url.to_string(), source: Box::new(source) })?;
        Ok(Self { ws })
    }

    /// Serialize `frame` and send it as one text frame.
    pub(crate) async fn send_json<T: Serialize>(&mut self, frame: &T) -> Result<()> {
        let text = serde_json::to_string(frame)?;
        self.ws.send(Message::Text(text.into())).await?;
        Ok(())
    }

    pub(crate) fn into_split(self) -> (WsSink, WsStream) {
        self.ws.split()
    }
}

/// Serialize `frame` and send it as one text frame.
pub(crate) async fn send_json<T: Serialize>(sink: &mut WsSink, frame: &T) -> Result<()> {
    let text = serde_json::to_string(frame)?;
    sink.send(Message::Text(text.into())).await?;
    Ok(())
}

/// Send a fixed control frame without allocating.
pub(crate) async fn send_static(sink: &mut WsSink, frame: &'static str) -> Result<()> {
    sink.send(Message::Text(Utf8Bytes::from_static(frame))).await?;
    Ok(())
}

/// The next text frame, or `None` once the connection closes. Ping, pong and
/// stray binary frames carry no events and are skipped.
pub(crate) fn poll_text(
    stream: &mut WsStream,
    cx: &mut Context<'_>,
) -> Poll<Option<Result<Utf8Bytes>>> {
    loop {
        return Poll::Ready(match ready!(stream.poll_next_unpin(cx)) {
            None | Some(Ok(Message::Close(_))) => None,
            Some(Err(e)) => Some(Err(e.into())),
            Some(Ok(Message::Text(text))) => Some(Ok(text)),
            Some(Ok(_)) => continue,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_request_carries_a_sensitive_bearer_header_and_a_clean_url() {
        let url = Url::parse("wss://stt-rt.soniox.com/transcribe-websocket").unwrap();
        let request = request_for(&url, "sk-secret").unwrap();
        let auth = request.headers().get(AUTHORIZATION).expect("an Authorization header");
        assert_eq!(auth.to_str().unwrap(), "Bearer sk-secret");
        assert!(auth.is_sensitive(), "a sensitive header is redacted by http's Debug");
        assert_eq!(request.uri().to_string(), url.as_str(), "the key never touches the URL");
        assert!(!format!("{request:?}").contains("sk-secret"), "{request:?}");
    }

    /// Guards the `wss://` path, which every other test misses because they all
    /// dial a plaintext local mock. Panics if no crypto backend is compiled in.
    #[test]
    fn a_crypto_provider_is_available() {
        ensure_crypto_provider();
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "no rustls crypto backend installed; a wss:// connect would panic"
        );
    }

    /// A server that completes the TCP connect and then says nothing must not
    /// hold the caller open: without the budget this hangs until the OS gives up.
    #[tokio::test]
    async fn connect_gives_up_on_a_server_that_never_completes_the_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept, then hold the socket open without ever answering.
        let _accepting = tokio::spawn(async move {
            let held = listener.accept().await;
            std::future::pending::<()>().await;
            drop(held);
        });

        let url = Url::parse(&format!("ws://{addr}/")).unwrap();
        let err = Connection::connect(&url, "sk-test", Duration::from_millis(100))
            .await
            .err()
            .expect("the handshake never completes");
        match err {
            Error::ConnectTimeout { url } => assert!(url.contains(&addr.to_string())),
            other => panic!("expected ConnectTimeout, got {other:?}"),
        }
    }

    /// A refused connection is still a connect error, not a timeout.
    #[tokio::test]
    async fn a_refused_connection_is_reported_as_connect_not_timeout() {
        // Port 9 (discard) is not listening on loopback; the connect is refused.
        let url = Url::parse("ws://127.0.0.1:9/").unwrap();
        let err = Connection::connect(&url, "sk-test", Duration::from_secs(5))
            .await
            .err()
            .expect("nothing is listening");
        assert!(matches!(err, Error::Connect { .. }), "got {err:?}");
    }
}
