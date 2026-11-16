//! A one-connection mock websocket server for exercising session lifecycles
//! without touching the network. Binds to a random loopback port and speaks
//! plain ws://, which `connect_async` handles the same as wss://.
//!
//! The harness itself records nothing: the `Vec<Message>` a `MockServer`
//! resolves to is exactly what the handler closure chooses to return. If a
//! test wants to assert on what the client sent, the handler must collect
//! those frames itself and hand them back.

#![allow(dead_code)]

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinError, JoinHandle};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderMap;

pub type ServerWs = WebSocketStream<TcpStream>;

const MOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Handle to the spawned mock server task.
///
/// This must be awaited (`server.await.unwrap()`) for its handler's
/// assertions to actually run and be observed. Dropping it without awaiting
/// it first is a bug in the test, not a benign shortcut: the server task may
/// still be mid-flight, or may have panicked, with nothing to surface that.
/// To make that failure mode impossible to miss, dropping a `MockServer`
/// that was never awaited panics.
#[must_use = "await the mock server (or its handler's assertions never provably ran)"]
pub struct MockServer {
    handle: Option<JoinHandle<Vec<Message>>>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if self.handle.is_some() && !std::thread::panicking() {
            panic!(
                "MockServer dropped without being awaited: the handler's \
                 server-side assertions may never have run"
            );
        }
    }
}

impl IntoFuture for MockServer {
    type Output = Result<Vec<Message>, JoinError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        let handle = self.handle.take().expect("MockServer polled twice");
        Box::pin(handle)
    }
}

/// Start the mock and return its ws:// URL plus a handle to the server task.
pub async fn spawn_mock<F, Fut>(handler: F) -> (String, MockServer)
where
    F: FnOnce(ServerWs) -> Fut + Send + 'static,
    Fut: Future<Output = Vec<Message>> + Send + 'static,
{
    // The headers go unread; the send into a dropped receiver is ignored.
    let (url, server, _headers) = spawn_mock_with_headers(handler).await;
    (url, server)
}

/// Receive the next text frame, panicking on anything else.
pub async fn recv_text(ws: &mut ServerWs) -> String {
    match ws.next().await {
        Some(Ok(Message::Text(t))) => t.as_str().to_owned(),
        other => panic!("expected a text frame, got {other:?}"),
    }
}

/// Receive the next frame of any kind.
pub async fn recv_any(ws: &mut ServerWs) -> Message {
    ws.next().await.expect("a frame").expect("no ws error")
}

/// Send a JSON string as a text frame.
pub async fn send_json(ws: &mut ServerWs, json: &str) {
    ws.send(Message::Text(json.into())).await.expect("send");
}

/// `spawn_mock`, plus the handshake request's headers, delivered once the
/// client has connected. For asserting on what rode on the handshake.
///
/// The handler runs under a 5-second timeout, so a protocol mismatch (the
/// client never sending a frame the handler waits for, say) fails fast
/// instead of hanging the whole test binary.
// The callback's Result type is tungstenite's, not ours.
#[allow(clippy::result_large_err)]
pub async fn spawn_mock_with_headers<F, Fut>(
    handler: F,
) -> (String, MockServer, tokio::sync::oneshot::Receiver<HeaderMap>)
where
    F: FnOnce(ServerWs) -> Fut + Send + 'static,
    Fut: Future<Output = Vec<Message>> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let (headers_tx, headers_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        tokio::time::timeout(MOCK_TIMEOUT, async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let callback =
                move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                    let _ = headers_tx.send(req.headers().clone());
                    Ok(resp)
                };
            let ws =
                tokio_tungstenite::accept_hdr_async(stream, callback).await.expect("handshake");
            handler(ws).await
        })
        .await
        .expect("mock server timed out waiting on the client")
    });
    (format!("ws://{addr}"), MockServer { handle: Some(handle) }, headers_rx)
}

pub mod http;
