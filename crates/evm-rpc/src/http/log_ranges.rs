use super::{HttpRpcClient, ProfileResult};
use crate::{RpcError, health};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

const FALLBACK_SPAN: u64 = 10;

#[derive(Clone, Copy)]
pub(super) struct LogRangeLimit {
    pub(super) span: u64,
    pub(super) expires_at: Instant,
}

pub(crate) fn limit_snapshot(
    endpoint_id: alloy_primitives::B256,
) -> Option<crate::telemetry::LogRangeLimitSnapshot> {
    let limits = super::LOG_RANGE_LIMITS.get()?;
    let mut limits = limits.lock().unwrap_or_else(|e| e.into_inner());
    let now = Instant::now();
    limits.retain(|_, limit| limit.expires_at > now);
    limits
        .get(&endpoint_id)
        .map(|limit| crate::telemetry::LogRangeLimitSnapshot {
            span: limit.span,
            expires_in_secs: limit
                .expires_at
                .duration_since(now)
                .as_secs()
                .saturating_add(1),
        })
}

pub(super) struct LogRange {
    from: u64,
    to: u64,
}

impl LogRange {
    pub(super) fn parse(params: &Value) -> Option<Self> {
        let filter = params.get(0)?;
        let quantity =
            |key| u64::from_str_radix(filter.get(key)?.as_str()?.strip_prefix("0x")?, 16).ok();
        let range = Self {
            from: quantity("fromBlock")?,
            to: quantity("toBlock")?,
        };
        (range.from <= range.to).then_some(range)
    }

    fn span(&self) -> u64 {
        self.to.saturating_sub(self.from).saturating_add(1)
    }

    fn params(&self, original: &Value) -> Value {
        let mut params = original.clone();
        params[0]["fromBlock"] = json!(format!("0x{:x}", self.from));
        params[0]["toBlock"] = json!(format!("0x{:x}", self.to));
        params
    }
}

impl HttpRpcClient {
    pub(super) fn call_logs_with_fallback(
        &self,
        params: &Value,
        profile: &mut ProfileResult,
    ) -> Result<Value, RpcError> {
        let Some(range) = LogRange::parse(params) else {
            return Err(RpcError::InvalidDomainValue {
                field: "eth_getLogs",
                detail: "invalid block range".to_owned(),
            });
        };
        if self.offline_miss.is_some() {
            return self
                .call_single_inner("eth_getLogs", params, (false, None), profile)
                .map(|result: Option<Value>| result.unwrap_or(Value::Null));
        }
        let mut archive =
            self.archive_request(Self::historical_block("eth_getLogs", params).into_iter());
        let mut last_error = None;
        for attempt in 0..self.max_attempts {
            let now = Instant::now();
            let start = health::preferred_start_index(
                self.endpoints.len(),
                self.rotation_now_secs(now),
                self.rotation_interval_secs,
            );
            let mut indices: std::collections::VecDeque<_> =
                self.available_indices(now, start, archive).into();
            if indices.is_empty()
                && archive
                && let Some(index) = self.provider_counters.iter().position(|provider| {
                    provider
                        .auth
                        .archive_unsupported
                        .load(std::sync::atomic::Ordering::Relaxed)
                })
            {
                last_error = Some(self.archive_error(index, "eth_getLogs"));
            }
            let mut visited = vec![false; self.endpoints.len()];
            while let Some(index) = indices.pop_front() {
                if std::mem::replace(&mut visited[index], true) {
                    continue;
                }
                let endpoint = &self.endpoints[index];
                let endpoint_id = alloy_primitives::keccak256(endpoint);
                let limit = {
                    let mut limits = self
                        .log_range_limits
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    limits.retain(|_, limit| limit.expires_at > now);
                    limits.get(&endpoint_id).map(|limit| limit.span)
                };
                let result = if let Some(limit) = limit.filter(|limit| range.span() > *limit) {
                    self.call_log_chunks(index, params, (&range, limit), profile)
                } else {
                    match self.call_single_inner::<Vec<Value>>(
                        "eth_getLogs",
                        params,
                        (false, Some(index)),
                        profile,
                    ) {
                        Ok(Some(logs)) => Ok(logs),
                        Ok(None) => Ok(Vec::new()),
                        Err(original) if range.span() > FALLBACK_SPAN => {
                            match Self::log_range_error_kind(&original) {
                                Some("span_limit") => {
                                    self.probe_log_span(index, (&range, params), original, profile)
                                }
                                Some(reason) => {
                                    eprintln!(
                                        "[warn] rpc_log_range_fallback provider={} original_span={} fallback_span={} reason={}",
                                        crate::telemetry::redact_endpoint_host(endpoint),
                                        range.span(),
                                        FALLBACK_SPAN,
                                        reason
                                    );
                                    match self.call_log_chunks(
                                        index,
                                        params,
                                        (&range, FALLBACK_SPAN),
                                        profile,
                                    ) {
                                        Ok(logs) => Ok(logs),
                                        Err(_) => Err(original),
                                    }
                                }
                                None => Err(original),
                            }
                        }
                        Err(error) => Err(error),
                    }
                };
                match result {
                    Ok(logs) => return Ok(Value::Array(logs)),
                    Err(error) => {
                        if matches!(error, RpcError::RpcArchiveUnavailable { .. }) {
                            archive = true;
                            indices.extend(self.available_indices(Instant::now(), start, true));
                        }
                        last_error = Some(error);
                    }
                }
            }
            if let Some(error) = self.auth_exhausted("eth_getLogs") {
                return Err(error);
            }
            if self.retry_backoff_ms > 0 && attempt + 1 < self.max_attempts {
                std::thread::sleep(Duration::from_millis(self.retry_backoff_ms * (attempt + 1)));
            }
        }
        Err(last_error.unwrap_or_else(|| RpcError::RpcProtocol {
            endpoint: String::new(),
            method: "eth_getLogs".to_owned(),
            code: -1,
            message: "no RPC endpoints configured".to_owned(),
        }))
    }

    fn call_log_chunks(
        &self,
        index: usize,
        params: &Value,
        range_limit: (&LogRange, u64),
        profile: &mut ProfileResult,
    ) -> Result<Vec<Value>, RpcError> {
        let (range, limit) = range_limit;
        let mut result = Vec::new();
        let mut from = range.from;
        loop {
            let to = from.saturating_add(limit - 1).min(range.to);
            let chunk = LogRange { from, to }.params(params);
            if let Some(logs) = self.call_single_inner::<Vec<Value>>(
                "eth_getLogs",
                &chunk,
                (false, Some(index)),
                profile,
            )? {
                result.extend(logs);
            }
            if to == range.to {
                break;
            }
            from = to + 1;
        }
        Ok(result)
    }

    // Providers reject wide ranges intermittently, so halve the span instead of
    // pinning the endpoint to the smallest chunk for the whole cache TTL.
    fn probe_log_span(
        &self,
        index: usize,
        request: (&LogRange, &Value),
        original: RpcError,
        profile: &mut ProfileResult,
    ) -> Result<Vec<Value>, RpcError> {
        let (range, params) = request;
        let endpoint = &self.endpoints[index];
        let mut candidate = (range.span() / 2).max(FALLBACK_SPAN);
        loop {
            match self.call_log_chunks(index, params, (range, candidate), profile) {
                Ok(logs) => {
                    self.log_range_limits
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(
                            alloy_primitives::keccak256(endpoint),
                            LogRangeLimit {
                                span: candidate,
                                expires_at: Instant::now() + self.log_range_limit_ttl,
                            },
                        );
                    eprintln!(
                        "[warn] rpc_log_range_fallback provider={} original_span={} fallback_span={} reason=span_limit",
                        crate::telemetry::redact_endpoint_host(endpoint),
                        range.span(),
                        candidate
                    );
                    return Ok(logs);
                }
                Err(error) if Self::log_range_error_kind(&error) == Some("span_limit") => {
                    if candidate == FALLBACK_SPAN {
                        return Err(original);
                    }
                    candidate = (candidate / 2).max(FALLBACK_SPAN);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn log_range_error_kind(error: &RpcError) -> Option<&'static str> {
        match error {
            RpcError::RpcProtocol { code, message, .. }
                if !crate::classify::is_restricted_rpc_error(*code, message)
                    && crate::classify::is_range_limit_error(*code, message) =>
            {
                Some("span_limit")
            }
            RpcError::RpcNetwork { source, .. } => {
                let message = source.to_string();
                if crate::classify::is_range_limit_error(0, &message) {
                    Some("span_limit")
                } else if !message.starts_with("HTTP ") || message == "HTTP 400" {
                    Some("transient_network")
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    fn accepted() -> (u16, String) {
        (200, r#"{"jsonrpc":"2.0","id":1,"result":[]}"#.to_owned())
    }

    fn rejected() -> (u16, String) {
        (
            200,
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"block range too large"}})
                .to_string(),
        )
    }

    fn log_params(span: u64) -> Value {
        json!([{"fromBlock": "0x1", "toBlock": format!("0x{:x}", span)}])
    }

    // Serves `count` sequential JSON-RPC requests and records each `(fromBlock, toBlock)`.
    fn run_server<T>(
        count: usize,
        reply: impl Fn(u64) -> (u16, String) + Send,
        call: impl FnOnce(HttpRpcClient) -> T,
    ) -> std::io::Result<(T, Vec<(u64, u64)>)> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        std::thread::scope(|scope| {
            let server = scope.spawn(move || -> std::io::Result<Vec<(u64, u64)>> {
                let mut ranges = Vec::new();
                for _ in 0..count {
                    let (mut stream, _) = listener.accept()?;
                    let mut reader = BufReader::new(&stream);
                    let mut length = 0;
                    for line in reader.by_ref().lines() {
                        let line = line?;
                        if line.is_empty() {
                            break;
                        }
                        if let Some((key, value)) = line.split_once(':')
                            && key.eq_ignore_ascii_case("content-length")
                        {
                            length = value.trim().parse().map_err(std::io::Error::other)?;
                        }
                    }
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body)?;
                    let request: Value = serde_json::from_slice(&body)?;
                    let range = LogRange::parse(&request["params"])
                        .ok_or_else(|| std::io::Error::other("request lacks a block range"))?;
                    ranges.push((range.from, range.to));
                    let (status, body) = reply(range.span());
                    write!(
                        stream,
                        "HTTP/1.1 {status} test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )?;
                }
                Ok(ranges)
            });
            let result = call(HttpRpcClient::new_live(vec![endpoint]));
            let ranges = server
                .join()
                .map_err(|_| std::io::Error::other("server thread panicked"))??;
            Ok((result, ranges))
        })
    }

    #[test]
    fn expired_limit_retries_the_full_range() -> Result<(), Box<dyn std::error::Error>> {
        let (result, ranges) = run_server(
            1,
            |_| accepted(),
            |client| {
                let id = alloy_primitives::keccak256(&client.endpoints[0]);
                client
                    .log_range_limits
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(
                        id,
                        LogRangeLimit {
                            span: 10,
                            expires_at: Instant::now() - Duration::from_secs(1),
                        },
                    );
                let logs: Vec<Value> = client.call_single("eth_getLogs", log_params(100))?;
                Ok::<_, RpcError>((logs, limit_snapshot(id)))
            },
        )?;
        let (logs, limit) = result?;
        assert!(logs.is_empty());
        assert_eq!(ranges, [(0x1, 0x64)]);
        assert!(limit.is_none());
        Ok(())
    }

    #[test]
    fn probes_half_span_when_full_range_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let (result, ranges) = run_server(
            3,
            |span| if span > 50 { rejected() } else { accepted() },
            |client| {
                let id = alloy_primitives::keccak256(&client.endpoints[0]);
                let logs: Vec<Value> = client.call_single("eth_getLogs", log_params(100))?;
                Ok::<_, RpcError>((logs, limit_snapshot(id)))
            },
        )?;
        let (logs, limit) = result?;
        assert!(logs.is_empty());
        // 100 blocks rejected, then two accepted 50-block chunks.
        assert_eq!(ranges, [(0x1, 0x64), (0x1, 0x32), (0x33, 0x64)]);
        assert_eq!(limit.map(|limit| limit.span), Some(50));
        Ok(())
    }

    #[test]
    fn probes_down_to_minimum_span() -> Result<(), Box<dyn std::error::Error>> {
        let (result, ranges) = run_server(
            6,
            |span| if span > 25 { rejected() } else { accepted() },
            |client| {
                let id = alloy_primitives::keccak256(&client.endpoints[0]);
                let logs: Vec<Value> = client.call_single("eth_getLogs", log_params(100))?;
                Ok::<_, RpcError>((logs, limit_snapshot(id)))
            },
        )?;
        let (logs, limit) = result?;
        assert!(logs.is_empty());
        // 100 and 50 rejected, then four accepted 25-block chunks.
        assert_eq!(
            ranges,
            [
                (0x1, 0x64),
                (0x1, 0x32),
                (0x1, 0x19),
                (0x1a, 0x32),
                (0x33, 0x4b),
                (0x4c, 0x64),
            ]
        );
        assert_eq!(limit.map(|limit| limit.span), Some(25));
        Ok(())
    }

    #[test]
    fn cached_span_skips_the_probe() -> Result<(), Box<dyn std::error::Error>> {
        let (result, ranges) = run_server(
            4,
            |_| accepted(),
            |client| {
                let id = alloy_primitives::keccak256(&client.endpoints[0]);
                client
                    .log_range_limits
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(
                        id,
                        LogRangeLimit {
                            span: 25,
                            expires_at: Instant::now() + Duration::from_secs(60),
                        },
                    );
                let logs: Vec<Value> = client.call_single("eth_getLogs", log_params(100))?;
                Ok::<_, RpcError>(logs)
            },
        )?;
        let logs = result?;
        assert!(logs.is_empty());
        assert_eq!(
            ranges,
            [(0x1, 0x19), (0x1a, 0x32), (0x33, 0x4b), (0x4c, 0x64)]
        );
        Ok(())
    }

    #[test]
    fn transient_http_400_uses_fixed_fallback_without_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let (result, ranges) = run_server(
            11,
            |span| {
                if span == 100 {
                    (400, String::new())
                } else {
                    accepted()
                }
            },
            |client| {
                let id = alloy_primitives::keccak256(&client.endpoints[0]);
                let logs: Vec<Value> = client.call_single("eth_getLogs", log_params(100))?;
                Ok::<_, RpcError>((logs, limit_snapshot(id)))
            },
        )?;
        let (logs, limit) = result?;
        assert!(logs.is_empty());
        // The 100-block request fails with HTTP 400, then ten fixed 10-block chunks follow.
        assert_eq!(
            ranges,
            [
                (0x1, 0x64),
                (0x1, 0xa),
                (0xb, 0x14),
                (0x15, 0x1e),
                (0x1f, 0x28),
                (0x29, 0x32),
                (0x33, 0x3c),
                (0x3d, 0x46),
                (0x47, 0x50),
                (0x51, 0x5a),
                (0x5b, 0x64),
            ]
        );
        assert!(limit.is_none());
        Ok(())
    }
}
