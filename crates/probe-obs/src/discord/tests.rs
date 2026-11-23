use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;
#[test]
fn discord_delivers_json_and_throttles_duplicate_notice() -> Result<(), Box<dyn std::error::Error>>
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let url = format!("http://{}/secret-token", listener.local_addr()?);
    let server = thread::spawn(move || -> std::io::Result<Vec<Vec<u8>>> {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut bodies = Vec::new();
        while bodies.len() < 2 && std::time::Instant::now() < deadline {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    socket.set_nonblocking(false)?;
                    let _ = socket.set_read_timeout(Some(Duration::from_secs(2)));
                    let mut data = Vec::new();
                    let mut buf = [0_u8; 4096];
                    while let Ok(n) = socket.read(&mut buf) {
                        if n == 0 {
                            break;
                        }
                        data.extend_from_slice(&buf[..n]);
                        if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&data[..pos]);
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|s| s.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if data.len() >= pos + 4 + length {
                                bodies.push(data[pos + 4..pos + 4 + length].to_vec());
                                break;
                            }
                        }
                    }
                    let _ = socket.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(_) => break,
            }
        }
        Ok(bodies)
    });
    let mut alerter = DiscordAlerter::with_cooldown_for_tests(url, 60);
    alerter.send(DiscordPriority::Notice, "rpc".into(), "quota".into(), false);
    alerter.send(
        DiscordPriority::Notice,
        "rpc".into(),
        "duplicate".into(),
        false,
    );
    alerter.send(
        DiscordPriority::Critical,
        "settlement".into(),
        "settled".into(),
        false,
    );
    alerter.flush_and_close();
    let bodies = server.join().map_err(|_| "mock server failed")??;
    let messages = bodies
        .iter()
        .map(|body| serde_json::from_slice::<serde_json::Value>(body))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(messages.iter().any(|v| v["content"] == "settled"));
    assert!(
        messages.iter().any(|v| v["content"] == "quota"),
        "{messages:?}; sent={}; errors={}",
        alerter.counters.sent_total.load(Ordering::Relaxed),
        alerter.counters.send_errors_total.load(Ordering::Relaxed)
    );
    assert_eq!(alerter.counters.throttled_total.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn discord_sheds_full_notice_queue_before_critical() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let url = format!("http://{}/test-token", listener.local_addr()?);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        let (mut first, _) = listener.accept()?;
        read_request(&mut first)?;
        let _ = started_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(10));
        first.write_all(
            b"HTTP/1.1 204 No Content\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        )?;
        let (mut second, _) = listener.accept()?;
        let body = read_request(&mut second)?;
        second.write_all(
            b"HTTP/1.1 204 No Content\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        )?;
        let (mut third, _) = listener.accept()?;
        let second_body = read_request(&mut third)?;
        third.write_all(
            b"HTTP/1.1 204 No Content\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        )?;
        Ok((body, second_body))
    });
    let mut alerter = DiscordAlerter::with_cooldown_for_tests(url, 60);
    alerter.send(
        DiscordPriority::Notice,
        "noise".into(),
        "first".into(),
        false,
    );
    started_rx.recv_timeout(Duration::from_secs(5))?;
    let started = std::time::Instant::now();
    for _ in 0..80 {
        alerter.send(
            DiscordPriority::Notice,
            "noise".into(),
            "noise".into(),
            false,
        );
    }
    alerter.send(
        DiscordPriority::Critical,
        "settlement".into(),
        "critical-settlement".into(),
        false,
    );
    alerter.send_halt("critical-halt".into());
    assert!(started.elapsed() < Duration::from_secs(1));
    release_tx.send(())?;
    alerter.flush_and_close();
    let (halt, settlement) = server.join().map_err(|_| "mock server failed")??;
    assert!(String::from_utf8_lossy(&halt).contains("critical-halt"));
    assert!(String::from_utf8_lossy(&settlement).contains("critical-settlement"));
    assert!(alerter.counters.dropped_total.load(Ordering::Relaxed) >= 16);
    assert_eq!(alerter.counters.sent_total.load(Ordering::Relaxed), 3);
    Ok(())
}

fn read_request(socket: &mut std::net::TcpStream) -> std::io::Result<Vec<u8>> {
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut data = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let n = socket.read(&mut buffer)?;
        if n == 0 {
            return Err(std::io::Error::other("incomplete request"));
        }
        data.extend_from_slice(&buffer[..n]);
        if let Some(pos) = data.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&data[..pos]);
            let length = headers.lines().find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|s| s.trim().parse::<usize>().ok())
            });
            if length.is_some_and(|length| data.len() >= pos + 4 + length) {
                return Ok(data);
            }
        }
    }
}

#[test]
fn discord_failure_logs_redact_webhook_token() -> Result<(), Box<dyn std::error::Error>> {
    const MARKER: &str = "ARB_TEST_DISCORD_FAILURE_CHILD";
    const TOKEN: &str = "test-secret-webhook-token";
    if std::env::var_os(MARKER).is_some() {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("http://{}/{TOKEN}", listener.local_addr()?);
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut stream, _) = listener.accept()?;
            read_request(&mut stream)?;
            stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
        });
        let mut alerter = DiscordAlerter::with_cooldown_for_tests(url, 0);
        alerter.send(
            DiscordPriority::Critical,
            "halt".into(),
            "halt".into(),
            false,
        );
        alerter.flush_and_close();
        server.join().map_err(|_| "mock server failed")??;
        assert_eq!(
            alerter.counters.send_errors_total.load(Ordering::Relaxed),
            1
        );
        return Ok(());
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("discord::tests::discord_failure_logs_redact_webhook_token")
        .arg("--nocapture")
        .env(MARKER, "1")
        .output()?;
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("HTTP 500"));
    assert!(!stderr.contains(TOKEN));
    Ok(())
}
