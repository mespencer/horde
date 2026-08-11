//! A byte-for-byte TCP tee that sits in front of the rmcp server.
//!
//! The audit's rule for this wave is "never docs-on-faith": every claim about
//! what a harness client puts on the wire has to be backed by the bytes. The
//! tee is deliberately dumb — it parses no HTTP, so it cannot launder a
//! framing detail (SSE chunking, header casing, connection reuse) into
//! something tidier than what actually crossed the socket.

use std::{
    io,
    net::SocketAddr,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::Mutex,
};

/// An append-only transcript shared by the tee and the server-side observer.
pub struct Transcript {
    file: Mutex<File>,
    start: Instant,
}

impl Transcript {
    pub async fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            file: Mutex::new(file),
            start: Instant::now(),
        })
    }

    fn stamp(&self) -> String {
        let elapsed = self.start.elapsed();
        format!("+{}.{:03}s", elapsed.as_secs(), elapsed.subsec_millis())
    }

    /// Write a framing note (connection open/close, scenario marker).
    pub async fn note(&self, text: &str) -> io::Result<()> {
        let line = format!("=== {} {} ===\n", self.stamp(), text);
        let mut file = self.file.lock().await;
        file.write_all(line.as_bytes()).await?;
        file.flush().await
    }

    /// Write one TCP read's worth of bytes, exactly as they crossed the socket.
    pub async fn chunk(&self, conn: u64, direction: &str, bytes: &[u8]) -> io::Result<()> {
        let header = format!(
            "--- conn {conn} {direction} {} {} bytes ---\n",
            self.stamp(),
            bytes.len()
        );
        let body = String::from_utf8_lossy(bytes);
        let mut file = self.file.lock().await;
        file.write_all(header.as_bytes()).await?;
        file.write_all(body.as_bytes()).await?;
        if !body.ends_with('\n') {
            file.write_all(b"\n").await?;
        }
        file.flush().await
    }
}

/// Accept on `listen`, forward to `upstream`, transcribe both directions.
pub async fn run(
    listener: TcpListener,
    upstream: SocketAddr,
    transcript: Arc<Transcript>,
) -> io::Result<()> {
    let counter = Arc::new(AtomicU64::new(0));
    loop {
        let (downstream, peer) = listener.accept().await?;
        let conn = counter.fetch_add(1, Ordering::Relaxed);
        let transcript = transcript.clone();
        tokio::spawn(async move {
            let _ = transcript
                .note(&format!("conn {conn} opened from {peer}"))
                .await;
            match TcpStream::connect(upstream).await {
                Ok(server) => {
                    let _ = downstream.set_nodelay(true);
                    let _ = server.set_nodelay(true);
                    let (client_read, client_write) = downstream.into_split();
                    let (server_read, server_write) = server.into_split();
                    let up = tokio::spawn(pump(
                        client_read,
                        server_write,
                        conn,
                        "C->S",
                        transcript.clone(),
                    ));
                    let down = tokio::spawn(pump(
                        server_read,
                        client_write,
                        conn,
                        "S->C",
                        transcript.clone(),
                    ));
                    let _ = up.await;
                    let _ = down.await;
                }
                Err(error) => {
                    let _ = transcript
                        .note(&format!("conn {conn} upstream connect failed: {error}"))
                        .await;
                }
            }
            let _ = transcript.note(&format!("conn {conn} closed")).await;
        });
    }
}

async fn pump(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    conn: u64,
    direction: &'static str,
    transcript: Arc<Transcript>,
) {
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let _ = transcript.chunk(conn, direction, &buffer[..read]).await;
        if to.write_all(&buffer[..read]).await.is_err() {
            break;
        }
        if to.flush().await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
}
