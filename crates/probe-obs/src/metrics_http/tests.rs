use super::*;

fn request(addr: SocketAddr, line: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(addr)?;
    write!(stream, "{line}\r\nHost: localhost\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

#[test]
fn rejects_non_loopback_addresses() {
    for addr in ["0.0.0.0:9100", "192.168.1.10:9100", "[::]:9100"] {
        assert!(matches!(
            LoopbackMetricsServer::bind(addr, RenderedMetricsPayload::default()),
            Err(MetricsError::InvalidDomainValue {
                field: "metrics_listen_addr",
                ..
            })
        ));
    }
}

#[test]
fn serves_both_payloads_and_http_errors() -> Result<(), Box<dyn std::error::Error>> {
    let payload = RenderedMetricsPayload::new("# TYPE arb_rpc_provider_requests_total counter\narb_rpc_provider_requests_total{provider=\"example.com\"} 1\n".to_owned(), "{\"ok\":true}".to_owned());
    let server = LoopbackMetricsServer::bind("127.0.0.1:0", payload)?;
    let addr = server.local_addr();
    for path in ["/", "/?view=1", "/index.html"] {
        let response = request(addr, &format!("GET {path} HTTP/1.1"))?;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("Content-Type: text/html; charset=utf-8"));
        assert!(response.contains("Content-Security-Policy: default-src 'none'"));
        assert!(response.contains("font-src data:"));
        assert!(response.contains("Cache-Control: no-store"));
        assert!(response.contains("X-Content-Type-Options: nosniff"));
        let body = response.split_once("\r\n\r\n").ok_or("missing body")?.1;
        assert!(body.contains("/status.json"));
        // The bundled page must load nothing beyond its own origin.
        for external in [
            "src=\"http",
            "href=\"http",
            "url(http",
            "src=\"//",
            "href=\"//",
        ] {
            assert!(!body.contains(external), "external resource: {external}");
        }
        assert!(!body.contains("src=\"/_astro/") && !body.contains("href=\"/_astro/"));
    }
    let metrics = request(addr, "GET /metrics?test=1 HTTP/1.1")?;
    assert!(metrics.starts_with("HTTP/1.1 200 OK"));
    assert!(metrics.contains("provider=\"example.com\""));
    let status = request(addr, "GET /status.json HTTP/1.1")?;
    let body = status.split_once("\r\n\r\n").ok_or("missing body")?.1;
    assert_eq!(serde_json::from_str::<serde_json::Value>(body)?["ok"], true);
    assert!(request(addr, "GET /unknown HTTP/1.1")?.starts_with("HTTP/1.1 404"));
    assert!(request(addr, "POST /metrics HTTP/1.1")?.starts_with("HTTP/1.1 405"));
    drop(server);
    assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err());
    Ok(())
}

#[test]
fn stale_snapshot_returns_503_until_a_new_snapshot_is_published()
-> Result<(), Box<dyn std::error::Error>> {
    let server = LoopbackMetricsServer::bind(
        "127.0.0.1:0",
        RenderedMetricsPayload::new("old metric".to_owned(), "{\"old\":true}".to_owned()),
    )?;
    let addr = server.local_addr();
    {
        let mut snapshot = server
            .payload
            .write()
            .map_err(|_| "snapshot lock poisoned")?;
        let snapshot = snapshot.as_mut().ok_or("missing published snapshot")?;
        snapshot.published_at = Instant::now()
            .checked_sub(MAX_SNAPSHOT_AGE)
            .ok_or("monotonic clock does not support test age")?;
    }

    for path in ["/metrics", "/status.json"] {
        let response = request(addr, &format!("GET {path} HTTP/1.1"))?;
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("snapshot is unavailable or stale"));
        assert!(!response.contains("old metric") && !response.contains("old\":true"));
    }
    assert!(request(addr, "GET / HTTP/1.1")?.starts_with("HTTP/1.1 200 OK"));

    server.publish(RenderedMetricsPayload::new(
        "new metric".to_owned(),
        "{\"new\":true}".to_owned(),
    ));
    let metrics = request(addr, "GET /metrics HTTP/1.1")?;
    let status = request(addr, "GET /status.json HTTP/1.1")?;
    assert!(metrics.starts_with("HTTP/1.1 200 OK") && metrics.contains("new metric"));
    assert!(status.starts_with("HTTP/1.1 200 OK") && status.contains("\"new\":true"));
    Ok(())
}

#[test]
fn unpublished_snapshot_returns_503_until_first_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let server = LoopbackMetricsServer::bind_unpublished("127.0.0.1:0")?;
    let addr = server.local_addr();
    for path in ["/metrics", "/status.json"] {
        assert!(
            request(addr, &format!("GET {path} HTTP/1.1"))?
                .starts_with("HTTP/1.1 503 Service Unavailable")
        );
    }
    server.publish(RenderedMetricsPayload::new(
        "ready".to_owned(),
        "{}".to_owned(),
    ));
    assert!(request(addr, "GET /metrics HTTP/1.1")?.starts_with("HTTP/1.1 200 OK"));
    assert!(request(addr, "GET /status.json HTTP/1.1")?.starts_with("HTTP/1.1 200 OK"));
    Ok(())
}
