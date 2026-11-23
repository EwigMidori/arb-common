use crate::{RpcError, health, telemetry};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use telemetry::{RpcClientTelemetry, RpcTelemetrySnapshot};
use ureq::Agent;
mod batch;
mod log_ranges;
pub(crate) use log_ranges::limit_snapshot as log_range_limit_snapshot;
type LogRangeLimits =
    Arc<Mutex<std::collections::HashMap<alloy_primitives::B256, log_ranges::LogRangeLimit>>>;
static LOG_RANGE_LIMITS: OnceLock<LogRangeLimits> = OnceLock::new();
pub use batch::{BatchAttemptError, BatchItemDto};
#[derive(Debug, serde::Deserialize)]
struct JsonRpcEnvelope<T> {
    pub result: Option<T>,
    pub error: Option<JsonRpcErrorDto>,
}

#[derive(Debug, serde::Deserialize)]
struct JsonRpcErrorDto {
    pub code: i64,
    pub message: String,
}

#[derive(Default)]
struct ProfileResult {
    sent: bool,
    result_count: Option<&'static str>,
}

/// Default live RPC timeout in milliseconds (`1.8s` for low-latency live probing).
pub const DEFAULT_LIVE_RPC_TIMEOUT_MS: u64 = 1_800;

/// Single-request JSON-RPC client with endpoint failover and timeout classification.
#[non_exhaustive]
#[derive(Clone)]
pub struct HttpRpcClient {
    chain_ids: Vec<Arc<Mutex<Option<String>>>>,
    endpoints: Vec<String>,
    agent: Agent,
    offline_miss: Option<Arc<OnceLock<String>>>,
    telemetry: Arc<RpcClientTelemetry>,
    provider_counters: Vec<Arc<telemetry::ProviderCounters>>,
    slow_threshold_ms: f64,
    max_attempts: u64,
    retry_backoff_ms: u64,
    health: Arc<Mutex<std::collections::HashMap<String, health::EndpointHealth>>>,
    rotation_interval_secs: u64,
    rotation_anchor: Instant,
    rotation_epoch_secs: u64,
    archive_heads: Arc<Mutex<std::collections::HashMap<String, (u64, Instant)>>>,
    archive_head_cache_ttl: Duration,
    log_range_limits: LogRangeLimits,
    log_range_limit_ttl: Duration,
}

impl std::fmt::Debug for HttpRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let providers: Vec<_> = self
            .endpoints
            .iter()
            .map(|url| telemetry::redact_endpoint_host(url))
            .collect();
        f.debug_struct("HttpRpcClient")
            .field("providers", &providers)
            .finish_non_exhaustive()
    }
}

impl HttpRpcClient {
    /// Creates an `HttpRpcClient` with explicit RPC endpoints and 20s timeout.
    pub fn new(endpoints: Vec<String>) -> Self {
        Self::with_timing(endpoints, Duration::from_secs(20), 3, 200)
    }

    /// Creates a low-latency live `HttpRpcClient` with `1.8s` timeout and single-pass failover.
    pub fn new_live(endpoints: Vec<String>) -> Self {
        Self::with_timing(
            endpoints,
            Duration::from_millis(DEFAULT_LIVE_RPC_TIMEOUT_MS),
            1,
            0,
        )
    }

    fn with_timing(
        endpoints: Vec<String>,
        timeout: Duration,
        max_attempts: u64,
        retry_backoff_ms: u64,
    ) -> Self {
        let config = Agent::config_builder()
            .timeout_global(Some(timeout))
            .http_status_as_error(false)
            .build();
        let health = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let telemetry = Arc::new(RpcClientTelemetry::new());
        let provider_counters = endpoints
            .iter()
            .map(|endpoint| telemetry.provider(endpoint))
            .collect();
        Self {
            chain_ids: endpoints.iter().map(|_| Arc::default()).collect(),
            endpoints,
            agent: config.into(),
            offline_miss: None,
            telemetry,
            provider_counters,
            slow_threshold_ms: telemetry::DEFAULT_SLOW_RPC_THRESHOLD_MS,
            max_attempts: max_attempts.max(1),
            retry_backoff_ms,
            health,
            rotation_interval_secs: 0,
            archive_heads: Arc::default(),
            archive_head_cache_ttl: Duration::from_secs(5),
            log_range_limits: Arc::clone(LOG_RANGE_LIMITS.get_or_init(Arc::default)),
            log_range_limit_ttl: Duration::from_secs(900),
            rotation_anchor: Instant::now(),
            rotation_epoch_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Attaches a shared `RpcClientTelemetry` recorder to aggregate metrics across clones/sessions.
    pub fn with_shared_telemetry(mut self, telemetry: Arc<RpcClientTelemetry>) -> Self {
        self.provider_counters = self
            .endpoints
            .iter()
            .map(|endpoint| telemetry.provider(endpoint))
            .collect();
        self.telemetry = telemetry;
        self
    }

    /// Shares endpoint cooldowns with another client, keyed by endpoint URL.
    pub fn with_shared_health(mut self, other: &Self) -> Self {
        for (url, slot) in self.endpoints.iter().zip(&mut self.chain_ids) {
            if let Some(index) = other.endpoints.iter().position(|endpoint| endpoint == url) {
                *slot = Arc::clone(&other.chain_ids[index]);
            }
        }
        self.health = Arc::clone(&other.health);
        self.archive_heads = Arc::clone(&other.archive_heads);
        self.log_range_limits = Arc::clone(&other.log_range_limits);
        self.rotation_anchor = other.rotation_anchor;
        self.rotation_epoch_secs = other.rotation_epoch_secs;
        self
    }

    /// Sets the log range limit TTL, clamped to 60–3600 seconds.
    pub fn with_log_range_limit_ttl_secs(mut self, seconds: u64) -> Self {
        self.log_range_limit_ttl = Duration::from_secs(seconds.clamp(60, 3600));
        self
    }

    /// Sets the archive classification head cache TTL, clamped to 1–60 seconds.
    pub fn with_archive_head_cache_ttl_secs(mut self, seconds: u64) -> Self {
        self.archive_head_cache_ttl = Duration::from_secs(seconds.clamp(1, 60));
        self
    }

    /// Overrides the single-attempt duration threshold in milliseconds classified as a slow RPC call.
    pub fn with_slow_threshold_ms(mut self, slow_threshold_ms: f64) -> Self {
        if slow_threshold_ms.is_finite() && slow_threshold_ms >= 0.0 {
            self.slow_threshold_ms = slow_threshold_ms;
        }
        self
    }

    /// Returns the shared `RpcClientTelemetry` handle.
    pub fn shared_telemetry(&self) -> Arc<RpcClientTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// Captures a point-in-time RPC rate-limit and failover latency snapshot.
    pub fn rpc_telemetry_snapshot(&self) -> RpcTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    pub fn offline() -> Self {
        Self {
            offline_miss: Some(Arc::default()),
            ..Self::new(Vec::new())
        }
    }

    // A failed lookup can be caught by an EVM call or a candidate preview. Reject
    // the entire offline result if any path depended on missing archive data.
    pub fn check_cache_coverage(&self) -> Result<(), RpcError> {
        if let Some(method) = self.offline_miss.as_ref().and_then(|miss| miss.get()) {
            return Err(RpcError::OfflineCacheMiss {
                method: method.clone(),
            });
        }
        Ok(())
    }

    pub fn take_unauthorized_notices(&self) -> Vec<String> {
        self.endpoints
            .iter()
            .zip(&self.provider_counters)
            .filter(|(_, counters)| counters.auth.take_notice())
            .map(|(url, _)| telemetry::redact_endpoint_host(url))
            .collect()
    }

    fn available_indices(&self, now: Instant, start: usize, archive_request: bool) -> Vec<usize> {
        let health = self.health.lock().unwrap_or_else(|e| e.into_inner());
        health::available_where(&self.endpoints, &health, now, start, |idx| {
            self.provider_counters[idx].auth.code().is_none()
                && (!archive_request
                    || !self.provider_counters[idx]
                        .auth
                        .archive_unsupported
                        .load(std::sync::atomic::Ordering::Relaxed))
        })
    }

    fn unauthorized(&self, idx: usize, method: &str, code: i64) -> RpcError {
        self.provider_counters[idx].record_unauthorized(code);
        self.unauthorized_error(idx, method, code)
    }

    fn archive_error(&self, idx: usize, method: &str) -> RpcError {
        self.provider_counters[idx]
            .auth
            .archive_unsupported
            .store(true, std::sync::atomic::Ordering::Relaxed);
        RpcError::RpcArchiveUnavailable {
            provider: telemetry::redact_endpoint_host(&self.endpoints[idx]),
            method: method.to_owned(),
        }
    }

    fn status_error(&self, idx: usize, method: &str, code: u16, body: &str) -> RpcError {
        let body = body.to_ascii_lowercase();
        if [
            "-32002",
            "archive",
            "historical state",
            "not available",
            "pruned",
        ]
        .iter()
        .any(|term| body.contains(term))
        {
            self.archive_error(idx, method)
        } else {
            self.unauthorized(idx, method, i64::from(code))
        }
    }

    fn historical_block(method: &str, params: &serde_json::Value) -> Option<u64> {
        let block = match method {
            "eth_getLogs" => params.get(0)?.get("fromBlock")?,
            "eth_call" => params.get(1)?,
            "eth_getStorageAt" => params.get(2)?,
            _ => return None,
        };
        let hex = block.as_str()?.strip_prefix("0x")?;
        u64::from_str_radix(hex, 16).ok()
    }

    fn archive_request(&self, blocks: impl Iterator<Item = u64>) -> bool {
        let Some(block) = blocks.min() else {
            return false;
        };
        if !self.provider_counters.iter().any(|p| {
            p.auth
                .archive_unsupported
                .load(std::sync::atomic::Ordering::Relaxed)
        }) {
            return false;
        }
        let now = Instant::now();
        let start = health::preferred_start_index(
            self.endpoints.len(),
            self.rotation_now_secs(now),
            self.rotation_interval_secs,
        );
        let indices = self.available_indices(now, start, false);
        let mut heads = self.archive_heads.lock().unwrap_or_else(|e| e.into_inner());
        for idx in indices {
            let endpoint = &self.endpoints[idx];
            if let Some((head, fetched_at)) = heads.get(endpoint)
                && fetched_at.elapsed() < self.archive_head_cache_ttl
            {
                return block < head.saturating_sub(128);
            }
            let mut probe = self.clone();
            probe.endpoints = vec![endpoint.clone()];
            probe.provider_counters = vec![Arc::clone(&self.provider_counters[idx])];
            probe.chain_ids = vec![Arc::clone(&self.chain_ids[idx])];
            if let Ok(head) = probe.latest_block_number() {
                heads.insert(endpoint.clone(), (head, Instant::now()));
                return block < head.saturating_sub(128);
            }
        }
        false
    }

    fn unauthorized_error(&self, idx: usize, method: &str, code: i64) -> RpcError {
        RpcError::RpcUnauthorized {
            provider: telemetry::redact_endpoint_host(&self.endpoints[idx]),
            method: method.to_owned(),
            code,
        }
    }

    fn auth_exhausted(&self, method: &str) -> Option<RpcError> {
        let first = self.provider_counters.first()?.auth.code()?;
        if self.endpoints.len() == 1 {
            return Some(self.unauthorized_error(0, method, first));
        }
        self.provider_counters
            .iter()
            .all(|c| c.auth.code().is_some())
            .then(|| RpcError::AllRpcUnauthorized {
                method: method.to_owned(),
            })
    }

    pub fn call_single<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, RpcError> {
        match self.call_single_with_policy::<T>(method, params, false)? {
            Some(value) => Ok(value),
            None => Err(RpcError::RpcProtocol {
                endpoint: self
                    .endpoints
                    .first()
                    .map(|url| crate::telemetry::redact_endpoint_host(url))
                    .unwrap_or_default(),
                method: method.to_owned(),
                code: -32_000,
                message: "null JSON-RPC result".to_owned(),
            }),
        }
    }

    pub fn call_single_nullable<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<Option<T>, RpcError> {
        self.call_single_with_policy(method, params, true)
    }

    fn call_single_with_policy<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
        allow_null: bool,
    ) -> Result<Option<T>, RpcError> {
        let started = Instant::now();
        let mut profile_result = ProfileResult::default();
        let result = if !allow_null
            && method == "eth_getLogs"
            && log_ranges::LogRange::parse(&params).is_some()
        {
            self.call_logs_with_fallback(&params, &mut profile_result)
                .and_then(|value| {
                    serde_json::from_value(value).map(Some).map_err(|_| {
                        RpcError::InvalidDomainValue {
                            field: "eth_getLogs",
                            detail: "invalid JSON-RPC result shape".to_owned(),
                        }
                    })
                })
        } else {
            self.call_single_inner(method, &params, (allow_null, None), &mut profile_result)
        };
        if profile_result.sent
            && let Some((profile, chain)) = self.telemetry.profile()
        {
            profile.record(crate::profile::Observation {
                chain,
                method,
                params: &params,
                result_count: profile_result.result_count.unwrap_or("unknown"),
                error: result.is_err(),
                latency: started.elapsed(),
                batch_size: 1,
            });
        }
        result
    }

    fn call_single_inner<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: &serde_json::Value,
        policy: (bool, Option<usize>),
        profile_result: &mut ProfileResult,
    ) -> Result<Option<T>, RpcError> {
        let (allow_null, forced_endpoint) = policy;
        if let Some(miss) = &self.offline_miss {
            let method = miss.get_or_init(|| method.to_owned()).clone();
            return Err(RpcError::OfflineCacheMiss { method });
        }
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let cache_chain_id =
            method == "eth_chainId" && payload["params"].as_array().is_some_and(Vec::is_empty);
        let mut recorded = !cache_chain_id;
        if recorded {
            self.telemetry.record_request();
        }
        let call_started = Instant::now();
        let mut last_err: Option<RpcError> = None;
        let mut switched_endpoint = false;
        let mut archive_request =
            self.archive_request(Self::historical_block(method, params).into_iter());

        for attempt in 0..self.max_attempts {
            let now = Instant::now();
            let start_idx = health::preferred_start_index(
                self.endpoints.len(),
                self.rotation_now_secs(now),
                self.rotation_interval_secs,
            );
            let mut indices: std::collections::VecDeque<_> = forced_endpoint
                .map_or_else(
                    || self.available_indices(now, start_idx, archive_request),
                    |index| vec![index],
                )
                .into();
            let mut visited = vec![false; self.endpoints.len()];
            if indices.is_empty()
                && archive_request
                && let Some(idx) = self.provider_counters.iter().position(|p| {
                    p.auth
                        .archive_unsupported
                        .load(std::sync::atomic::Ordering::Relaxed)
                })
            {
                last_err = Some(self.archive_error(idx, method));
            }
            while let Some(ep_idx) = indices.pop_front() {
                if std::mem::replace(&mut visited[ep_idx], true) {
                    continue;
                }
                if self.provider_counters[ep_idx].auth.code().is_some() {
                    continue;
                }
                if archive_request
                    && self.provider_counters[ep_idx]
                        .auth
                        .archive_unsupported
                        .load(std::sync::atomic::Ordering::Relaxed)
                {
                    last_err = Some(self.archive_error(ep_idx, method));
                    continue;
                }
                let endpoint = &self.endpoints[ep_idx];
                if ep_idx != start_idx || attempt > 0 {
                    switched_endpoint = true;
                }
                let mut chain_id = cache_chain_id.then(|| {
                    self.chain_ids[ep_idx]
                        .lock()
                        .unwrap_or_else(|err| err.into_inner())
                });
                if let Some(hex) = chain_id.as_deref().and_then(Option::as_ref) {
                    return serde_json::from_value(serde_json::Value::String(hex.clone()))
                        .map(Some)
                        .map_err(|_| RpcError::InvalidDomainValue {
                            field: "eth_chainId",
                            detail: "expected quantity string".into(),
                        });
                }
                if !recorded {
                    self.telemetry.record_request();
                    recorded = true;
                }
                let attempt_started = Instant::now();
                self.telemetry.record_method(method);
                RpcClientTelemetry::provider_request(&self.provider_counters[ep_idx]);
                profile_result.sent = true;
                let response = self
                    .agent
                    .post(endpoint)
                    .header("content-type", "application/json")
                    .send_json(&payload);
                let response = match response {
                    Ok(mut resp) if matches!(resp.status().as_u16(), 401 | 403) => {
                        let code = resp.status().as_u16();
                        let body = resp.body_mut().read_to_string().unwrap_or_default();
                        self.telemetry.record_attempt_latency(
                            attempt_started.elapsed().as_secs_f64() * 1_000.0,
                            self.slow_threshold_ms,
                        );
                        let error = self.status_error(ep_idx, method, code, &body);
                        archive_request |= matches!(error, RpcError::RpcArchiveUnavailable { .. });
                        if archive_request && forced_endpoint.is_none() {
                            indices.extend(self.available_indices(Instant::now(), start_idx, true));
                        }
                        last_err = Some(error);
                        continue;
                    }
                    Ok(mut resp) if method == "eth_getLogs" && resp.status().as_u16() == 400 => {
                        let body = resp.body_mut().read_to_string().unwrap_or_default();
                        if crate::classify::is_rate_limit_rpc_error(400, &body) {
                            Err(ureq::Error::StatusCode(429))
                        } else if crate::classify::is_restricted_rpc_error(400, &body) {
                            self.telemetry.record_attempt_latency(
                                attempt_started.elapsed().as_secs_f64() * 1_000.0,
                                self.slow_threshold_ms,
                            );
                            last_err = Some(self.status_error(ep_idx, method, 400, &body));
                            continue;
                        } else if crate::classify::is_range_limit_error(400, &body) {
                            self.telemetry.record_attempt_latency(
                                attempt_started.elapsed().as_secs_f64() * 1_000.0,
                                self.slow_threshold_ms,
                            );
                            last_err = Some(RpcError::RpcProtocol {
                                endpoint: telemetry::redact_endpoint_host(endpoint),
                                method: method.to_owned(),
                                code: 400,
                                message: "block range exceeds the maximum".to_owned(),
                            });
                            continue;
                        } else {
                            Err(ureq::Error::StatusCode(400))
                        }
                    }
                    Ok(resp) if resp.status().as_u16() >= 400 => {
                        Err(ureq::Error::StatusCode(resp.status().as_u16()))
                    }
                    other => other,
                };
                let attempt_ms = attempt_started.elapsed().as_secs_f64() * 1_000.0;

                match response {
                    Ok(mut resp) => {
                        self.telemetry
                            .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                        let envelope: JsonRpcEnvelope<serde_json::Value> =
                            match resp.body_mut().read_json() {
                                Ok(val) => val,
                                Err(err) => {
                                    self.mark_other_failure(endpoint);
                                    last_err = Some(RpcError::network(endpoint, method, err));
                                    continue;
                                }
                            };

                        if let Some(err_obj) = envelope.error {
                            if crate::classify::is_unauthorized(err_obj.code, &err_obj.message) {
                                last_err = Some(self.unauthorized(ep_idx, method, err_obj.code));
                                continue;
                            }
                            let protocol_error = RpcError::RpcProtocol {
                                endpoint: telemetry::redact_endpoint_host(endpoint),
                                method: method.to_owned(),
                                code: err_obj.code,
                                message: if method == "eth_getLogs"
                                    && crate::classify::is_restricted_rpc_error(
                                        err_obj.code,
                                        &err_obj.message,
                                    ) {
                                    "access denied"
                                } else {
                                    crate::classify::upstream_summary(
                                        err_obj.code,
                                        &err_obj.message,
                                    )
                                }
                                .to_owned(),
                            };
                            if crate::classify::is_quota_exhausted_rpc_error(&err_obj.message)
                                && crate::classify::is_restricted_rpc_error(
                                    err_obj.code,
                                    &err_obj.message,
                                )
                            {
                                self.mark_provider_rejected(endpoint, 402);
                            } else if crate::classify::is_rate_limit_rpc_error(
                                err_obj.code,
                                &err_obj.message,
                            ) {
                                self.telemetry.record_rate_limit();
                                self.mark_provider_rejected(endpoint, 429);
                            } else if crate::classify::is_restricted_rpc_error(
                                err_obj.code,
                                &err_obj.message,
                            ) {
                                self.mark_provider_rejected(endpoint, 403);
                            } else if matches!(method, "eth_call" | "eth_estimateGas")
                                && crate::classify::is_deterministic_evm_error(
                                    err_obj.code,
                                    &err_obj.message,
                                )
                            {
                                self.health
                                    .lock()
                                    .unwrap_or_else(|err| err.into_inner())
                                    .entry(endpoint.clone())
                                    .or_default()
                                    .success();
                                RpcClientTelemetry::provider_success(
                                    &self.provider_counters[ep_idx],
                                );
                                if switched_endpoint {
                                    self.telemetry.record_failover_switch(
                                        call_started.elapsed().as_secs_f64() * 1_000.0,
                                    );
                                }
                                return Err(protocol_error);
                            } else {
                                self.mark_other_failure(endpoint);
                            }
                            last_err = Some(protocol_error);
                            continue;
                        }

                        if method == "eth_getLogs" {
                            profile_result.result_count =
                                Some(crate::profile::CallShape::results(envelope.result.as_ref()));
                        }

                        if let Some(slot) = chain_id.as_deref_mut()
                            && let Some(hex) =
                                envelope.result.as_ref().and_then(serde_json::Value::as_str)
                        {
                            crate::parse::parse_u64_hex(hex, "eth_chainId")?;
                            *slot = Some(hex.to_owned());
                        }
                        let parsed = match envelope.result {
                            None | Some(serde_json::Value::Null) if allow_null => None,
                            None | Some(serde_json::Value::Null) => {
                                self.mark_other_failure(endpoint);
                                switched_endpoint = true;
                                last_err = Some(RpcError::RpcProtocol {
                                    endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                                    method: method.to_owned(),
                                    code: -32_000,
                                    message: "null JSON-RPC result".to_owned(),
                                });
                                continue;
                            }
                            Some(raw_val) => match serde_json::from_value::<T>(raw_val) {
                                Ok(value) => Some(value),
                                Err(_) => {
                                    self.mark_other_failure(endpoint);
                                    switched_endpoint = true;
                                    last_err = Some(RpcError::RpcProtocol {
                                        endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                                        method: method.to_owned(),
                                        code: -32_000,
                                        message: "invalid JSON-RPC result shape".to_owned(),
                                    });
                                    continue;
                                }
                            },
                        };
                        self.health
                            .lock()
                            .unwrap_or_else(|err| err.into_inner())
                            .entry(endpoint.clone())
                            .or_default()
                            .success();
                        RpcClientTelemetry::provider_success(&self.provider_counters[ep_idx]);
                        if switched_endpoint {
                            self.telemetry.record_failover_switch(
                                call_started.elapsed().as_secs_f64() * 1_000.0,
                            );
                        }
                        return Ok(parsed);
                    }
                    Err(ureq::Error::Timeout(kind)) => {
                        self.telemetry.record_timeout();
                        self.mark_timeout(endpoint);
                        last_err = Some(RpcError::RpcTimeout {
                            endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                            method: method.to_owned(),
                            source: std::sync::Arc::new(ureq::Error::Timeout(kind)),
                        });
                    }
                    Err(ureq::Error::StatusCode(code @ (401 | 403))) => {
                        self.telemetry
                            .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                        last_err = Some(self.unauthorized(ep_idx, method, i64::from(code)));
                    }
                    Err(ureq::Error::StatusCode(code @ 402)) => {
                        self.telemetry
                            .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                        self.mark_provider_rejected(endpoint, 402);
                        last_err = Some(RpcError::RpcProtocol {
                            endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                            method: method.to_owned(),
                            code: i64::from(code),
                            message: format!("HTTP {code} Restricted"),
                        });
                    }
                    Err(ureq::Error::StatusCode(429)) => {
                        self.telemetry
                            .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                        self.telemetry.record_rate_limit();
                        self.mark_provider_rejected(endpoint, 429);
                        last_err = Some(RpcError::RpcProtocol {
                            endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                            method: method.to_owned(),
                            code: 429,
                            message: "HTTP 429 Too Many Requests".to_owned(),
                        });
                    }
                    Err(err) => {
                        self.health
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .entry(endpoint.clone())
                            .or_default()
                            .other_failure();
                        self.telemetry
                            .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                        last_err = Some(RpcError::network(endpoint, method, err));
                    }
                }
            }
            if let Some(err) = self.auth_exhausted(method) {
                return Err(err);
            }
            if forced_endpoint.is_some() {
                break;
            }
            if self.retry_backoff_ms > 0 && attempt + 1 < self.max_attempts {
                std::thread::sleep(Duration::from_millis(self.retry_backoff_ms * (attempt + 1)));
            }
        }

        if switched_endpoint {
            self.telemetry
                .record_failover_switch(call_started.elapsed().as_secs_f64() * 1_000.0);
        }

        Err(last_err.unwrap_or_else(|| RpcError::RpcProtocol {
            endpoint: self
                .endpoints
                .first()
                .map(|url| crate::telemetry::redact_endpoint_host(url))
                .unwrap_or_default(),
            method: method.to_owned(),
            code: -1,
            message: "no RPC endpoints configured".to_owned(),
        }))
    }
}

impl HttpRpcClient {
    /// Sets the sticky primary-endpoint rotation interval; zero preserves list order.
    pub fn with_rotation_interval_secs(mut self, interval_secs: u64) -> Self {
        self.rotation_interval_secs = interval_secs;
        self
    }

    /// Returns the sticky rotation interval in seconds.
    pub const fn rotation_interval_secs(&self) -> u64 {
        self.rotation_interval_secs
    }

    fn rotation_now_secs(&self, now: Instant) -> u64 {
        self.rotation_epoch_secs.saturating_add(
            now.saturating_duration_since(self.rotation_anchor)
                .as_secs(),
        )
    }

    fn mark_other_failure(&self, endpoint: &str) {
        self.health
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .entry(endpoint.to_owned())
            .or_default()
            .other_failure();
    }

    fn mark_provider_rejected(&self, endpoint: &str, code: u16) {
        let mut health = self.health.lock().unwrap_or_else(|err| err.into_inner());
        let state = health.entry(endpoint.to_owned()).or_default();
        let now = Instant::now();
        if code == 429 {
            state.rate_limited(now);
        } else {
            state.restricted(now);
        }
        if let Some(until) = state.cooldown_until() {
            self.telemetry.provider_rejected(endpoint, code, until);
        }
    }

    pub fn mark_timeout(&self, endpoint: &str) {
        let mut health = self.health.lock().unwrap_or_else(|err| err.into_inner());
        let state = health.entry(endpoint.to_owned()).or_default();
        state.timeout(Instant::now());
        if let Some(until) = state.cooldown_until() {
            self.telemetry.provider_timeout_cooldown(endpoint, until);
        }
    }
}

impl HttpRpcClient {
    pub fn set_rotation_epoch(&mut self, epoch_secs: u64) {
        self.rotation_anchor = Instant::now();
        self.rotation_epoch_secs = epoch_secs;
    }
    pub fn rotation_epoch_secs(&self) -> u64 {
        self.rotation_epoch_secs
    }

    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    pub fn require_online(&self, method: &str) -> Result<(), RpcError> {
        if let Some(miss) = &self.offline_miss {
            return Err(RpcError::OfflineCacheMiss {
                method: miss.get_or_init(|| method.to_owned()).clone(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cooldown_is_shared_by_endpoint_across_rpc_roles() {
        let first = "http://first".to_owned();
        let second = "http://second".to_owned();
        let read = HttpRpcClient::new(vec![first.clone(), second.clone()]);
        let broadcast =
            HttpRpcClient::new(vec![second.clone(), first.clone()]).with_shared_health(&read);
        let logs = HttpRpcClient::new(vec![first.clone(), second]).with_shared_health(&read);
        let now = Instant::now();
        read.health
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(first)
            .or_default()
            .rate_limited(now);
        for client in [broadcast, logs] {
            let health = client.health.lock().unwrap_or_else(|e| e.into_inner());
            let available = health::available_from(&client.endpoints, &health, now, 0);
            assert_eq!(available.len(), 1);
            assert_eq!(client.endpoints[available[0]], "http://second");
        }
    }
}
