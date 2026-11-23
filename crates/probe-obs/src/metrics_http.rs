//! Loopback-only HTTP transport for pre-rendered probe status.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("invalid {field}: {detail}")]
    InvalidDomainValue { field: &'static str, detail: String },
}

const DASHBOARD_HTML: &str = include_str!("metrics_http/dashboard.html");
/// A snapshot older than this indicates the probe has stopped publishing.
const MAX_SNAPSHOT_AGE: Duration = Duration::from_secs(300);

struct PublishedMetrics {
    payload: Arc<RenderedMetricsPayload>,
    published_at: Instant,
}

impl PublishedMetrics {
    fn new(payload: RenderedMetricsPayload) -> Self {
        Self {
            payload: Arc::new(payload),
            published_at: Instant::now(),
        }
    }

    fn is_stale(&self) -> bool {
        self.published_at.elapsed() >= MAX_SNAPSHOT_AGE
    }
}

/// A coherent pair of rendered views of the same probe snapshot.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct RenderedMetricsPayload {
    pub prometheus_text: String,
    pub status_json: String,
}

impl RenderedMetricsPayload {
    /// Creates both views of a probe snapshot.
    pub fn new(prometheus_text: String, status_json: String) -> Self {
        Self {
            prometheus_text,
            status_json,
        }
    }
}

/// Serves immutable payloads on a loopback socket until dropped.
/// Status and metrics requests return HTTP 503 before the first publication or
/// after five minutes without publication.
#[non_exhaustive]
pub struct LoopbackMetricsServer {
    addr: SocketAddr,
    payload: Arc<RwLock<Option<PublishedMetrics>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl LoopbackMetricsServer {
    /// Validates and binds a loopback socket before spawning its listener.
    pub fn bind(addr: &str, initial: RenderedMetricsPayload) -> Result<Self, MetricsError> {
        Self::bind_with_snapshot(addr, Some(PublishedMetrics::new(initial)))
    }

    /// Binds a listener that returns HTTP 503 until the first snapshot is published.
    pub fn bind_unpublished(addr: &str) -> Result<Self, MetricsError> {
        Self::bind_with_snapshot(addr, None)
    }

    fn bind_with_snapshot(
        addr: &str,
        initial: Option<PublishedMetrics>,
    ) -> Result<Self, MetricsError> {
        let invalid = |detail: String| MetricsError::InvalidDomainValue {
            field: "metrics_listen_addr",
            detail,
        };
        let addr: SocketAddr = addr
            .parse()
            .map_err(|error: std::net::AddrParseError| invalid(error.to_string()))?;
        if !addr.ip().is_loopback() {
            return Err(invalid(format!("{addr} is not a loopback address")));
        }
        let listener = TcpListener::bind(addr).map_err(|error| invalid(error.to_string()))?;
        let addr = listener
            .local_addr()
            .map_err(|error| invalid(error.to_string()))?;
        let payload = Arc::new(RwLock::new(initial));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_payload = Arc::clone(&payload);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("metrics-http".to_owned())
            .spawn(move || {
                for connection in listener.incoming() {
                    if thread_stop.load(Ordering::Relaxed) {
                        break;
                    }
                    match connection {
                        Ok(stream) => helper::serve(stream, &thread_payload),
                        Err(_) => break,
                    }
                }
            })
            .map_err(|error| invalid(error.to_string()))?;
        Ok(Self {
            addr,
            payload,
            stop,
            thread: Some(thread),
        })
    }

    /// Returns the actual bound address, including an assigned ephemeral port.
    pub const fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Publishes a snapshot if the reader is not currently acquiring one.
    pub fn publish(&self, payload: RenderedMetricsPayload) {
        if let Ok(mut current) = self.payload.try_write() {
            *current = Some(PublishedMetrics::new(payload));
        }
    }
}

impl Drop for LoopbackMetricsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(500));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

mod helper {
    use super::*;

    pub(super) fn serve(mut stream: TcpStream, payload: &RwLock<Option<PublishedMetrics>>) {
        let timeout = Some(Duration::from_millis(500));
        if stream.set_read_timeout(timeout).is_err() || stream.set_write_timeout(timeout).is_err() {
            return;
        }
        let mut buffer = [0_u8; 8192];
        let mut length = 0;
        let started = std::time::Instant::now();
        while length < buffer.len() && !buffer[..length].contains(&b'\n') {
            let Some(remaining) = Duration::from_millis(500).checked_sub(started.elapsed()) else {
                return;
            };
            if remaining.is_zero() || stream.set_read_timeout(Some(remaining)).is_err() {
                return;
            }
            match stream.read(&mut buffer[length..]) {
                Ok(0) | Err(_) => return,
                Ok(count) => length += count,
            }
        }
        let Ok(line) = std::str::from_utf8(&buffer[..length]) else {
            return;
        };
        let mut parts = line.lines().next().unwrap_or_default().split_whitespace();
        let method = parts.next().unwrap_or_default();
        let path = parts
            .next()
            .unwrap_or_default()
            .split('?')
            .next()
            .unwrap_or_default();
        if method == "GET" && matches!(path, "/" | "/index.html") {
            respond(
                &mut stream,
                "200 OK",
                "text/html; charset=utf-8",
                DASHBOARD_HTML,
            );
            return;
        }
        let snapshot = {
            let current = payload
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            current
                .as_ref()
                .filter(|snapshot| !snapshot.is_stale())
                .map(|snapshot| Arc::clone(&snapshot.payload))
        };
        if method == "GET" && matches!(path, "/metrics" | "/status.json") && snapshot.is_none() {
            respond(
                &mut stream,
                "503 Service Unavailable",
                "text/plain; charset=utf-8",
                "Probe status snapshot is unavailable or stale; check the probe process.\n",
            );
            return;
        }
        let (status, content_type, body) = match (method, path, snapshot.as_deref()) {
            ("GET", "/metrics", Some(snapshot)) => (
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                snapshot.prometheus_text.as_str(),
            ),
            ("GET", "/status.json", Some(snapshot)) => (
                "200 OK",
                "application/json; charset=utf-8",
                snapshot.status_json.as_str(),
            ),
            ("GET", _, _) => ("404 Not Found", "text/plain; charset=utf-8", "Not Found\n"),
            _ => (
                "405 Method Not Allowed",
                "text/plain; charset=utf-8",
                "Method Not Allowed\n",
            ),
        };
        respond(&mut stream, status, content_type, body);
    }

    fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &str) {
        let _ = write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; font-src data:; img-src data:; connect-src 'self'\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    }
}

#[cfg(test)]
#[path = "metrics_http/tests.rs"]
mod tests;
