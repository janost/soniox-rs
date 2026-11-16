//! Several text-to-speech streams multiplexed over one websocket.
//!
//! One background task reads the socket and routes each event to its stream
//! by `stream_id`, so streams can be consumed in any order and from different
//! tasks. Events wait in an unbounded per-stream queue: a stream nobody reads
//! holds its audio in memory rather than stalling the others.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use futures_util::{SinkExt, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use super::tts::{TtsChunk, TtsConfig, parse_frame, send_cancel, send_keepalive, send_text};
use super::{Connection, WsSink, WsStream, send_json};
use crate::error::{Error, Result};

/// The server accepts at most this many concurrent streams per connection.
pub const MAX_STREAMS_PER_CONNECTION: usize = 5;

type SharedSink = Arc<tokio::sync::Mutex<WsSink>>;
type ChunkSender = mpsc::UnboundedSender<Result<TtsChunk>>;

/// The streams currently open, keyed by id. `None` once the connection has
/// ended, after which no stream can be opened.
struct Registry(Mutex<Option<HashMap<String, ChunkSender>>>);

impl Registry {
    fn lock(&self) -> MutexGuard<'_, Option<HashMap<String, ChunkSender>>> {
        // The map is consistent after every statement that touches it, so a
        // panic elsewhere while it was held leaves nothing to repair.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hand one event to its stream. A stream is forgotten once it ends, fails,
    /// or its handle was dropped.
    fn deliver(&self, stream_id: &str, chunk: Result<TtsChunk>) {
        let mut streams = self.lock();
        let Some(map) = streams.as_mut() else { return };
        let Some(sender) = map.get(stream_id) else { return };
        let ended = match &chunk {
            Ok(chunk) => chunk.terminated,
            Err(_) => true,
        };
        if sender.send(chunk).is_err() || ended {
            map.remove(stream_id);
        }
    }

    /// End every stream, with `cause` when the connection failed rather than
    /// closed.
    fn close(&self, cause: Option<Error>) {
        let Some(map) = self.lock().take() else { return };
        if let Some(cause) = cause {
            let cause = Arc::new(cause);
            for sender in map.values() {
                let _ = sender.send(Err(Error::Shared(Arc::clone(&cause))));
            }
        }
    }
}

/// Read the socket until it closes, routing each event to its stream.
async fn route(mut socket: WsStream, registry: Arc<Registry>) {
    let cause = loop {
        match socket.next().await {
            None | Some(Ok(Message::Close(_))) => break None,
            Some(Err(e)) => break Some(Error::from(e)),
            Some(Ok(Message::Text(text))) => match parse_frame(text.as_str()) {
                (Some(stream_id), chunk) => registry.deliver(&stream_id, chunk),
                // An error that names no stream concerns the connection. A
                // frame that is neither is an acknowledgement with nothing
                // to route.
                (None, Err(e)) => break Some(e),
                (None, Ok(_)) => {}
            },
            Some(Ok(_)) => {}
        }
    };
    registry.close(cause);
}

/// A websocket that carries up to [`MAX_STREAMS_PER_CONNECTION`] synthesis
/// streams. Dropping it stops the reader, which ends every stream on it.
pub struct TtsConnection {
    sink: SharedSink,
    registry: Arc<Registry>,
    reader: JoinHandle<()>,
}

impl TtsConnection {
    pub(crate) fn new(conn: Connection) -> Self {
        let (sink, socket) = conn.into_split();
        let registry = Arc::new(Registry(Mutex::new(Some(HashMap::new()))));
        let reader = tokio::spawn(route(socket, Arc::clone(&registry)));
        Self { sink: Arc::new(tokio::sync::Mutex::new(sink)), registry, reader }
    }

    /// Start a stream. Its `stream_id` must be unique among the streams
    /// currently open on this connection.
    pub async fn open_stream(&self, config: &TtsConfig) -> Result<TtsStream> {
        let (sender, events) = mpsc::unbounded_channel();
        {
            let mut streams = self.registry.lock();
            let map = streams.as_mut().ok_or(Error::Closed)?;
            if map.contains_key(&config.stream_id) {
                return Err(Error::InvalidInput(format!(
                    "stream {:?} is already open on this connection",
                    config.stream_id
                )));
            }
            if map.len() >= MAX_STREAMS_PER_CONNECTION {
                return Err(Error::InvalidInput(format!(
                    "a connection carries at most {MAX_STREAMS_PER_CONNECTION} streams"
                )));
            }
            // Registered before the config goes out, so the first event
            // cannot arrive ahead of its queue.
            map.insert(config.stream_id.clone(), sender);
        }
        if let Err(e) = send_json(&mut *self.sink.lock().await, config).await {
            if let Some(map) = self.registry.lock().as_mut() {
                map.remove(&config.stream_id);
            }
            return Err(e);
        }
        Ok(TtsStream { stream_id: config.stream_id.clone(), sink: Arc::clone(&self.sink), events })
    }

    pub async fn keepalive(&self) -> Result<()> {
        send_keepalive(&mut *self.sink.lock().await).await
    }

    /// Close the socket. Streams still open end without a `terminated` event.
    pub async fn close(&self) -> Result<()> {
        self.sink.lock().await.close().await?;
        Ok(())
    }
}

impl Drop for TtsConnection {
    fn drop(&mut self) {
        self.reader.abort();
        self.registry.close(None);
    }
}

/// One stream on a [`TtsConnection`]. Dropping it stops delivery to this
/// handle but does not stop the server generating; call
/// [`cancel`](Self::cancel) for that.
pub struct TtsStream {
    stream_id: String,
    sink: SharedSink,
    events: mpsc::UnboundedReceiver<Result<TtsChunk>>,
}

impl TtsStream {
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Send one chunk of text. Set `text_end` on the final chunk.
    pub async fn send_text(&self, text: &str, text_end: bool) -> Result<()> {
        send_text(&mut *self.sink.lock().await, &self.stream_id, text, text_end).await
    }

    /// End the utterance without sending more text.
    pub async fn finish(&self) -> Result<()> {
        self.send_text("", true).await
    }

    /// Abandon the stream server-side so no further audio is generated.
    pub async fn cancel(&self) -> Result<()> {
        send_cancel(&mut *self.sink.lock().await, &self.stream_id).await
    }

    /// The next audio chunk, or `None` once the stream has ended: after its
    /// `terminated` chunk, after an error, or when the connection closes.
    pub async fn next_audio(&mut self) -> Option<Result<TtsChunk>> {
        self.events.recv().await
    }
}

impl Stream for TtsStream {
    type Item = Result<TtsChunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.poll_recv(cx)
    }
}
