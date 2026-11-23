use super::{
    SubscriptionEvents, Subscriptions,
    protocol::{Protocol, Push},
};
use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};
use tungstenite::{Message, client_tls, handshake::HandshakeError};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WsError {
    #[error("invalid websocket endpoint")]
    InvalidEndpoint,
    #[error("websocket network error: {0}")]
    Network(#[from] std::io::Error),
    #[error("websocket transport error: {0}")]
    Transport(#[from] tungstenite::Error),
    #[error("websocket handshake failed")]
    Handshake,
    #[error("websocket subscription rejected")]
    SubscriptionRejected,
    #[error("invalid websocket JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("websocket heartbeat timed out")]
    Timeout,
    #[error("websocket server closed the connection")]
    Closed,
}

struct HealthGuard<'a> {
    chain: u64,
    shared: &'a dyn SubscriptionEvents,
}

impl Drop for HealthGuard<'_> {
    fn drop(&mut self) {
        self.shared.health(self.chain, false);
    }
}

pub(super) fn run(
    chain: u64,
    url: &str,
    subscriptions: &Subscriptions,
    shared: &dyn SubscriptionEvents,
) -> Result<(), WsError> {
    let updated = shared.updated_addresses(chain);
    let addresses = updated.as_deref().unwrap_or(&subscriptions.addresses);
    let filter = subscriptions.filter.as_ref();
    let _health_guard = HealthGuard { chain, shared };
    let uri: tungstenite::http::Uri = url.parse().map_err(|_| WsError::InvalidEndpoint)?;
    let scheme = uri.scheme_str().ok_or(WsError::InvalidEndpoint)?;
    let port = uri.port_u16().unwrap_or(match scheme {
        "ws" => 80,
        "wss" => 443,
        _ => return Err(WsError::InvalidEndpoint),
    });
    let host = uri.host().ok_or(WsError::InvalidEndpoint)?;
    let mut connected = None;
    for address in (host, port).to_socket_addrs()? {
        if let Ok(stream) = TcpStream::connect_timeout(&address, Duration::from_secs(5)) {
            connected = Some(stream);
            break;
        }
    }
    let stream = connected.ok_or(WsError::Timeout)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream.set_nodelay(true)?;
    let (mut socket, _) = client_tls(url, stream).map_err(|error| match error {
        HandshakeError::Failure(e) => WsError::Transport(e),
        HandshakeError::Interrupted(_) => WsError::Handshake,
    })?;
    match socket.get_mut() {
        tungstenite::stream::MaybeTlsStream::Plain(s) => {
            s.set_read_timeout(Some(Duration::from_secs(1)))?
        }
        tungstenite::stream::MaybeTlsStream::Rustls(s) => {
            s.sock.set_read_timeout(Some(Duration::from_secs(1)))?
        }
        _ => return Err(WsError::InvalidEndpoint),
    }
    for request in Protocol::requests(addresses, subscriptions.heads, filter) {
        socket.send(Message::Text(request.into()))?;
    }
    let mut protocol = Protocol::new(subscriptions.heads, !addresses.is_empty(), filter.is_some());
    if protocol.ready() {
        shared.health(chain, true);
    }
    let subscribed_at = Instant::now();
    let mut last_received = Instant::now();
    let mut last_ping = Instant::now();
    let mut awaiting_pong = None;
    while !shared.stopped() {
        if shared.updated_addresses(chain) != updated {
            return Ok(());
        }
        if (!protocol.settled() && subscribed_at.elapsed() >= Duration::from_secs(10))
            || last_received.elapsed() >= Duration::from_secs(45)
            || awaiting_pong.is_some_and(|t: Instant| t.elapsed() >= Duration::from_secs(15))
        {
            return Err(WsError::Timeout);
        }
        if awaiting_pong.is_none() && last_ping.elapsed() >= Duration::from_secs(15) {
            socket.send(Message::Ping(Vec::new().into()))?;
            last_ping = Instant::now();
            awaiting_pong = Some(last_ping);
        }
        match socket.read() {
            Ok(message) => {
                last_received = Instant::now();
                match message {
                    Message::Text(text) => {
                        let push = protocol.receive(&text)?;
                        if protocol.ready() {
                            shared.health(chain, true);
                        }
                        if let Some(push) = push {
                            match push {
                                Push::Head(head) => {
                                    let _ = head;
                                    shared.push(chain, None);
                                }
                                Push::Log(log) => {
                                    let _ = log.address;
                                    shared.push(chain, Some(&log.topics));
                                }
                            }
                        }
                    }
                    Message::Ping(_) => socket.flush()?,
                    Message::Pong(_) => awaiting_pong = None,
                    Message::Close(_) => return Err(WsError::Closed),
                    _ => {}
                }
            }
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
