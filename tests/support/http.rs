//! A scripted HTTP/1.1 server for exercising the REST client without the
//! network. It answers one request per connection with the next canned reply
//! and records what it was asked.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;

/// One canned response.
pub struct Reply {
    status: u16,
    body: Vec<u8>,
}

impl Reply {
    pub fn json(body: &str) -> Self {
        Self { status: 200, body: body.as_bytes().to_vec() }
    }

    pub fn status(status: u16, body: &str) -> Self {
        Self { status, body: body.as_bytes().to_vec() }
    }

    pub fn bytes(body: &[u8]) -> Self {
        Self { status: 200, body: body.to_vec() }
    }
}

/// One request as the server received it.
#[derive(Debug)]
pub struct Recorded {
    pub method: String,
    /// Path and query, exactly as on the request line.
    pub target: String,
    /// The header block, lowercased.
    pub head: String,
    pub body: Vec<u8>,
}

impl Recorded {
    /// The request line without the protocol version, e.g. `GET /v1/files`.
    pub fn line(&self) -> String {
        format!("{} {}", self.method, self.target)
    }

    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("a JSON request body")
    }
}

/// Serve `replies` in order and resolve to the requests that were made. The
/// task ends once every reply has been sent, so awaiting it also proves the
/// client made exactly that many requests.
pub async fn serve(replies: Vec<Reply>) -> (Url, JoinHandle<Vec<Recorded>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        let mut recorded = Vec::with_capacity(replies.len());
        for reply in replies {
            let (mut stream, _) = listener.accept().await.expect("accept");
            recorded.push(read_request(&mut stream).await);
            let head = format!(
                "HTTP/1.1 {} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                reply.status,
                reply.body.len()
            );
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&reply.body).await;
            let _ = stream.shutdown().await;
        }
        recorded
    });
    (Url::parse(&format!("http://{addr}/")).expect("a URL"), handle)
}

async fn read_request(stream: &mut TcpStream) -> Recorded {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    let head_end = loop {
        if let Some(at) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
        let n = stream.read(&mut chunk).await.expect("read request head");
        assert!(n > 0, "the client closed the connection mid-request");
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
    let length: usize = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map_or(0, |value| value.trim().parse().expect("a numeric content-length"));
    while buf.len() < head_end + length {
        let n = stream.read(&mut chunk).await.expect("read request body");
        assert!(n > 0, "the client closed the connection mid-body");
        buf.extend_from_slice(&chunk[..n]);
    }
    let line = String::from_utf8_lossy(&buf[..head_end]);
    let mut parts = line.lines().next().expect("a request line").split(' ');
    Recorded {
        method: parts.next().expect("a method").to_owned(),
        target: parts.next().expect("a target").to_owned(),
        head,
        body: buf[head_end..].to_vec(),
    }
}
