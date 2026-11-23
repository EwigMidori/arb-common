use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Returns only the endpoint authority host, removing credentials and URL components.
pub fn redact_endpoint_host(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    let lowered = endpoint.to_ascii_lowercase();
    let endpoint = ["http://", "https://", "ws://", "wss://"]
        .iter()
        .find_map(|scheme| {
            lowered
                .starts_with(scheme)
                .then(|| &endpoint[scheme.len()..])
        })
        .unwrap_or(endpoint);
    let authority = endpoint
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .rsplit('@')
        .next()
        .unwrap_or_default();
    let authority = authority.to_ascii_lowercase();
    let host = authority
        .strip_suffix(":80")
        .or_else(|| authority.strip_suffix(":443"))
        .unwrap_or(&authority);
    if host.is_empty() || host.chars().any(char::is_control) {
        "unknown".to_owned()
    } else {
        host.to_owned()
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::{RpcClientTelemetry, redact_endpoint_host};
    use std::time::{Duration, Instant};

    #[test]
    fn strips_secrets_and_normalizes_authority() {
        for (url, expected) in [
            (
                "HTTPS://user:pass@Base.Example:443/v2/SECRET?api_key=SECRET#SECRET",
                "base.example",
            ),
            (
                "WSS://user:pass@Host.Example:8443/path?api_key=SECRET#SECRET",
                "host.example:8443",
            ),
            ("http://[::1]:80/SECRET", "[::1]"),
            ("http://127.0.0.1:1234/?secret=SECRET", "127.0.0.1:1234"),
            ("host/path?redirect=https://SECRET", "host"),
            ("https:///SECRET", "unknown"),
            ("", "unknown"),
        ] {
            assert_eq!(redact_endpoint_host(url), expected);
        }
    }

    #[test]
    fn success_clears_rejection_cooldown() {
        let telemetry = RpcClientTelemetry::new();
        let endpoint = "https://example.com/SECRET";
        let provider = telemetry.provider(endpoint);
        RpcClientTelemetry::provider_request(&provider);
        telemetry.provider_rejected(endpoint, 429, Instant::now() + Duration::from_secs(30));
        assert!(telemetry.snapshot().providers[0].cooldown_active);
        RpcClientTelemetry::provider_success(&provider);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.providers[0].ok_count, 1);
        assert!(!snapshot.providers[0].cooldown_active);
        assert_eq!(snapshot.providers[0].cooldown_remaining_secs, 0);
    }
}

pub const DEFAULT_SLOW_RPC_THRESHOLD_MS: f64 = 500.0;

const MAX_FAILOVER_SAMPLES: usize = 128;

const METHODS: [&str; 15] = [
    "eth_call",
    "eth_getLogs",
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_getTransactionReceipt",
    "eth_getBalance",
    "eth_getTransactionCount",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_blockNumber",
    "eth_chainId",
    "eth_feeHistory",
    "eth_maxPriorityFeePerGas",
    "eth_getTransactionByHash",
    "other",
];

/// Active endpoint log range limit and remaining lifetime.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRangeLimitSnapshot {
    pub span: u64,
    pub expires_in_secs: u64,
}

/// Point-in-time RPC rate-limit, slow-call, and endpoint failover telemetry snapshot.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcProviderSnapshot {
    pub provider: String,
    pub req_count: u64,
    pub ok_count: u64,
    pub rate_limit_429_count: u64,
    pub restricted_403_count: u64,
    pub quota_402_count: u64,
    #[serde(default)]
    pub auth_failed: bool,
    #[serde(default)]
    pub archive_unsupported: bool,
    #[serde(skip)]
    endpoint_id: alloy_primitives::B256,
    pub cooldown_active: bool,
    pub cooldown_remaining_secs: u64,
    #[serde(default)]
    pub log_range_limit: Option<LogRangeLimitSnapshot>,
}

#[non_exhaustive]
#[derive(Debug, Default)]
pub struct ProviderCounters {
    pub(crate) auth: Arc<crate::auth::EndpointAuth>,
    host: String,
    req: AtomicU64,
    ok: AtomicU64,
    rate: AtomicU64,
    restricted: AtomicU64,
    quota: AtomicU64,
    cooldown_until_nanos: AtomicU64,
}

impl ProviderCounters {
    pub(crate) fn record_unauthorized(&self, code: i64) {
        self.auth.reject(code);
        if matches!(code, 401 | 403) {
            self.restricted.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Point-in-time RPC rate-limit, slow-call, and endpoint failover telemetry snapshot.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RpcTelemetrySnapshot {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rpc_methods: BTreeMap<String, u64>,
    pub total_requests: u64,
    pub rate_limit_429s: u64,
    pub timeout_errors: u64,
    pub slow_calls: u64,
    pub failover_switches: u64,
    pub failover_p50_ms: Option<f64>,
    pub failover_p95_ms: Option<f64>,
    pub last_failover_ms: Option<f64>,
    #[serde(default)]
    pub providers: Vec<RpcProviderSnapshot>,
}

/// Thread-safe rolling RPC rate-limit and failover latency recorder shared across client clones.
#[non_exhaustive]
#[derive(Debug)]
pub struct RpcClientTelemetry {
    profile: Mutex<Option<(Arc<crate::profile::RpcProfile>, &'static str)>>,
    methods: [AtomicU64; METHODS.len()],
    total_requests: AtomicU64,
    rate_limit_429s: AtomicU64,
    timeout_errors: AtomicU64,
    slow_calls: AtomicU64,
    failover_switches: AtomicU64,
    failover_latencies_ms: Mutex<VecDeque<f64>>,
    providers: Mutex<BTreeMap<alloy_primitives::B256, Arc<ProviderCounters>>>,
    telemetry_anchor: Instant,
}

impl Default for RpcClientTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcClientTelemetry {
    /// Creates a zeroed `RpcClientTelemetry` recorder.
    pub fn new() -> Self {
        Self {
            profile: Mutex::new(None),
            methods: Default::default(),
            total_requests: AtomicU64::new(0),
            rate_limit_429s: AtomicU64::new(0),
            timeout_errors: AtomicU64::new(0),
            slow_calls: AtomicU64::new(0),
            failover_switches: AtomicU64::new(0),
            failover_latencies_ms: Mutex::new(VecDeque::with_capacity(MAX_FAILOVER_SAMPLES)),
            providers: Mutex::new(BTreeMap::new()),
            telemetry_anchor: Instant::now(),
        }
    }

    /// Shares an RPC profiler across telemetry handles for a chain.
    pub fn attach_profile(&self, profile: Arc<crate::profile::RpcProfile>, chain: &'static str) {
        *self
            .profile
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((profile, chain));
    }

    pub(crate) fn profile(&self) -> Option<(Arc<crate::profile::RpcProfile>, &'static str)> {
        self.profile
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Increments total logical JSON-RPC request count.
    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_method(&self, method: &str) {
        let index = METHODS
            .iter()
            .position(|name| *name == method)
            .unwrap_or(METHODS.len() - 1);
        self.methods[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn provider(&self, endpoint: &str) -> Arc<ProviderCounters> {
        let id = alloy_primitives::keccak256(endpoint);
        let mut providers = self
            .providers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(providers.entry(id).or_insert_with(|| {
            Arc::new(ProviderCounters {
                auth: crate::auth::EndpointAuth::for_id(id),
                host: redact_endpoint_host(endpoint),
                ..Default::default()
            })
        }))
    }

    pub fn provider_request(provider: &ProviderCounters) {
        provider.req.fetch_add(1, Ordering::Relaxed);
    }

    pub fn provider_success(provider: &ProviderCounters) {
        provider.ok.fetch_add(1, Ordering::Relaxed);
        provider.cooldown_until_nanos.store(0, Ordering::Relaxed);
    }

    pub fn provider_rejected(&self, endpoint: &str, code: u16, until: Instant) {
        let provider = self.provider(endpoint);
        let counter = match code {
            429 => &provider.rate,
            402 => &provider.quota,
            _ => &provider.restricted,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.set_provider_cooldown(&provider, until);
    }

    pub fn provider_timeout_cooldown(&self, endpoint: &str, until: Instant) {
        self.set_provider_cooldown(&self.provider(endpoint), until);
    }

    fn set_provider_cooldown(&self, provider: &ProviderCounters, until: Instant) {
        let nanos = u64::try_from(
            until
                .saturating_duration_since(self.telemetry_anchor)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);
        provider
            .cooldown_until_nanos
            .store(nanos, Ordering::Relaxed);
    }

    /// Records a single HTTP/JSON-RPC endpoint attempt latency against `slow_threshold_ms`.
    pub fn record_attempt_latency(&self, elapsed_ms: f64, slow_threshold_ms: f64) {
        if elapsed_ms.is_finite() && elapsed_ms >= slow_threshold_ms {
            self.slow_calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records an HTTP `429` or JSON-RPC rate-limit rejection.
    pub fn record_rate_limit(&self) {
        self.rate_limit_429s.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a request timeout (`RpcTimeout`) and counts it as a slow call.
    pub fn record_timeout(&self) {
        self.timeout_errors.fetch_add(1, Ordering::Relaxed);
        self.slow_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a failover switch from a failed/rate-limited/slow primary endpoint to a backup endpoint.
    pub fn record_failover_switch(&self, switch_latency_ms: f64) {
        self.failover_switches.fetch_add(1, Ordering::Relaxed);
        if !switch_latency_ms.is_finite() || switch_latency_ms < 0.0 {
            return;
        }
        let mut guard = self
            .failover_latencies_ms
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.len() >= MAX_FAILOVER_SAMPLES {
            guard.pop_front();
        }
        guard.push_back(switch_latency_ms);
    }

    /// Captures a consistent telemetry snapshot including P50/P95 failover switch latencies.
    pub fn snapshot(&self) -> RpcTelemetrySnapshot {
        let (failover_p50_ms, failover_p95_ms, last_failover_ms) = {
            let guard = self
                .failover_latencies_ms
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let last = guard.back().copied();
            let (p50, p95) = quantiles_p50_p95(&guard);
            (p50, p95, last)
        };

        RpcTelemetrySnapshot {
            rpc_methods: METHODS
                .iter()
                .zip(&self.methods)
                .map(|(method, count)| ((*method).to_owned(), count.load(Ordering::Relaxed)))
                .collect(),
            total_requests: self.total_requests.load(Ordering::Relaxed),
            rate_limit_429s: self.rate_limit_429s.load(Ordering::Relaxed),
            timeout_errors: self.timeout_errors.load(Ordering::Relaxed),
            slow_calls: self.slow_calls.load(Ordering::Relaxed),
            failover_switches: self.failover_switches.load(Ordering::Relaxed),
            failover_p50_ms,
            failover_p95_ms,
            last_failover_ms,
            providers: self
                .providers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|(provider, counters)| {
                    let now = u64::try_from(self.telemetry_anchor.elapsed().as_nanos())
                        .unwrap_or(u64::MAX);
                    let remaining = counters
                        .cooldown_until_nanos
                        .load(Ordering::Relaxed)
                        .saturating_sub(now)
                        .div_ceil(1_000_000_000);
                    RpcProviderSnapshot {
                        provider: counters.host.clone(),
                        endpoint_id: *provider,
                        auth_failed: counters.auth.code().is_some(),
                        archive_unsupported: counters
                            .auth
                            .archive_unsupported
                            .load(Ordering::Relaxed),
                        req_count: counters.req.load(Ordering::Relaxed),
                        ok_count: counters.ok.load(Ordering::Relaxed),
                        rate_limit_429_count: counters.rate.load(Ordering::Relaxed),
                        restricted_403_count: counters.restricted.load(Ordering::Relaxed),
                        quota_402_count: counters.quota.load(Ordering::Relaxed),
                        cooldown_active: remaining > 0,
                        cooldown_remaining_secs: remaining,
                        log_range_limit: crate::http::log_range_limit_snapshot(*provider),
                    }
                })
                .collect(),
        }
    }
}

pub fn quantiles_p50_p95(samples: &VecDeque<f64>) -> (Option<f64>, Option<f64>) {
    if samples.is_empty() {
        return (None, None);
    }
    let mut sorted: Vec<f64> = samples
        .iter()
        .copied()
        .filter(|v| v.is_finite() && *v >= 0.0)
        .collect();
    if sorted.is_empty() {
        return (None, None);
    }
    sorted.sort_by(f64::total_cmp);
    let len = sorted.len();
    let p50_idx = percentile_index(len, 50);
    let p95_idx = percentile_index(len, 95);
    (sorted.get(p50_idx).copied(), sorted.get(p95_idx).copied())
}

fn percentile_index(len: usize, pct: usize) -> usize {
    let rank = len.saturating_mul(pct).div_ceil(100).max(1);
    rank.saturating_sub(1).min(len.saturating_sub(1))
}
impl RpcProviderSnapshot {
    fn combine(&mut self, provider: &Self) {
        self.auth_failed |= provider.auth_failed;
        self.archive_unsupported |= provider.archive_unsupported;
        self.req_count = self.req_count.saturating_add(provider.req_count);
        self.ok_count = self.ok_count.saturating_add(provider.ok_count);
        self.rate_limit_429_count = self
            .rate_limit_429_count
            .saturating_add(provider.rate_limit_429_count);
        self.restricted_403_count = self
            .restricted_403_count
            .saturating_add(provider.restricted_403_count);
        self.quota_402_count = self
            .quota_402_count
            .saturating_add(provider.quota_402_count);
        self.cooldown_remaining_secs = self
            .cooldown_remaining_secs
            .max(provider.cooldown_remaining_secs);
        self.cooldown_active = self.cooldown_remaining_secs > 0;
        if let Some(limit) = &provider.log_range_limit {
            match &mut self.log_range_limit {
                Some(current) if current.span <= limit.span => {
                    if current.span == limit.span {
                        current.expires_in_secs =
                            current.expires_in_secs.max(limit.expires_in_secs);
                    }
                }
                _ => self.log_range_limit = Some(limit.clone()),
            }
        }
    }
}

impl RpcTelemetrySnapshot {
    /// Prometheus series are keyed by host even when multiple credentials share it.
    pub fn totals_by_host(&self) -> Vec<RpcProviderSnapshot> {
        let mut hosts = BTreeMap::new();
        for provider in &self.providers {
            hosts
                .entry(&provider.provider)
                .or_insert_with(|| RpcProviderSnapshot {
                    provider: provider.provider.clone(),
                    ..Default::default()
                })
                .combine(provider);
        }
        hosts.into_values().collect()
    }
}

pub fn merge_rpc_snapshots(
    primary: Option<RpcTelemetrySnapshot>,
    secondary: Option<RpcTelemetrySnapshot>,
) -> RpcTelemetrySnapshot {
    match (primary, secondary) {
        (Some(a), Some(b)) => {
            let mut rpc_methods = a.rpc_methods;
            for (method, count) in b.rpc_methods {
                let total = rpc_methods.entry(method).or_default();
                *total = total.saturating_add(count);
            }
            let mut providers = BTreeMap::new();
            for provider in a.providers.into_iter().chain(b.providers) {
                let entry = providers
                    .entry((provider.provider.clone(), provider.endpoint_id))
                    .or_insert_with(|| RpcProviderSnapshot {
                        provider: provider.provider.clone(),
                        endpoint_id: provider.endpoint_id,
                        ..Default::default()
                    });
                entry.combine(&provider);
            }
            RpcTelemetrySnapshot {
                rpc_methods,
                total_requests: a.total_requests.saturating_add(b.total_requests),
                rate_limit_429s: a.rate_limit_429s.saturating_add(b.rate_limit_429s),
                timeout_errors: a.timeout_errors.saturating_add(b.timeout_errors),
                slow_calls: a.slow_calls.saturating_add(b.slow_calls),
                failover_switches: a.failover_switches.saturating_add(b.failover_switches),
                failover_p50_ms: max_opt(a.failover_p50_ms, b.failover_p50_ms),
                failover_p95_ms: max_opt(a.failover_p95_ms, b.failover_p95_ms),
                last_failover_ms: b.last_failover_ms.or(a.last_failover_ms),
                providers: providers.into_values().collect(),
            }
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => RpcTelemetrySnapshot::default(),
    }
}

fn max_opt(lhs: Option<f64>, rhs: Option<f64>) -> Option<f64> {
    match (lhs, rhs) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}
