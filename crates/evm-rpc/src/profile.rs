//! Bounded, sanitized information extracted at the JSON-RPC boundary.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const ZERO_SELECTOR: &str = "0x00000000";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CallShape {
    pub(crate) target: String,
    pub(crate) selector: String,
    pub(crate) addresses: Option<&'static str>,
    pub(crate) span: Option<&'static str>,
}

impl CallShape {
    pub(crate) fn from_params(method: &str, params: &Value) -> Self {
        let mut shape = Self {
            target: ZERO_ADDRESS.to_owned(),
            selector: ZERO_SELECTOR.to_owned(),
            addresses: None,
            span: None,
        };
        if method == "eth_call" {
            let call = &params[0];
            if let Some(target) = call.get("to").and_then(Value::as_str)
                && let Some(address) = Self::hex_prefix(target, 40)
                && target.len() == 42
            {
                shape.target = address;
            }
            if let Some(data) = call
                .get("data")
                .or_else(|| call.get("input"))
                .and_then(Value::as_str)
                && let Some(selector) = Self::hex_prefix(data, 8)
            {
                shape.selector = selector;
            }
        }
        if method == "eth_getLogs" {
            let filter = &params[0];
            let count = match filter.get("address") {
                Some(Value::String(_)) => 1,
                Some(Value::Array(items)) => items.len(),
                _ => 0,
            };
            shape.addresses = Some(match count {
                0 => "0",
                1 => "1",
                2..=10 => "2-10",
                11..=50 => "11-50",
                _ => "51+",
            });
            let number = |key: &str| {
                filter
                    .get(key)
                    .and_then(Value::as_str)
                    .and_then(|s| s.strip_prefix("0x"))
                    .and_then(|s| u64::from_str_radix(s, 16).ok())
            };
            shape.span = Some(if filter.get("blockHash").is_some() {
                "1"
            } else if let Some(span) = number("fromBlock")
                .zip(number("toBlock"))
                .and_then(|(from, to)| to.checked_sub(from))
                .and_then(|difference| difference.checked_add(1))
            {
                match span {
                    1 => "1",
                    2..=10 => "2-10",
                    11..=100 => "11-100",
                    101..=1000 => "101-1000",
                    _ => "1001+",
                }
            } else {
                "unknown"
            });
        }
        shape
    }

    fn hex_prefix(value: &str, digits: usize) -> Option<String> {
        let prefix = value.get(..digits + 2)?;
        (prefix.starts_with("0x") && prefix[2..].bytes().all(|b| b.is_ascii_hexdigit()))
            .then(|| prefix.to_ascii_lowercase())
    }

    pub(crate) fn results(result: Option<&Value>) -> &'static str {
        match result.and_then(Value::as_array).map(Vec::len) {
            Some(0) => "0",
            Some(1..=10) => "1-10",
            Some(11..=100) => "11-100",
            Some(_) => "101+",
            None => "unknown",
        }
    }
}

/// Bounds for one latency observation, in milliseconds. The last bucket is +Inf.
const LATENCY_BOUNDS_MS: [u64; 7] = [10, 50, 100, 250, 500, 1_000, 5_000];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[non_exhaustive]
pub struct ProfileKey {
    pub chain: String,
    pub method: String,
    pub target: String,
    pub selector: String,
}

#[derive(Debug, Clone, Default, Serialize)]
#[non_exhaustive]
pub struct Counters {
    pub requests: u64,
    pub errors: u64,
    pub total_latency_ms: u64,
    pub max_latency_ms: u64,
    pub last_seen: u64,
    pub latency_buckets: [u64; 8],
}

impl Counters {
    fn record(&mut self, latency_ms: u64, error: bool, timestamp: u64) {
        self.requests = self.requests.saturating_add(1);
        self.errors = self.errors.saturating_add(u64::from(error));
        self.total_latency_ms = self.total_latency_ms.saturating_add(latency_ms);
        self.max_latency_ms = self.max_latency_ms.max(latency_ms);
        self.last_seen = self.last_seen.max(timestamp);
        let index = LATENCY_BOUNDS_MS
            .iter()
            .position(|bound| latency_ms <= *bound)
            .unwrap_or(LATENCY_BOUNDS_MS.len());
        self.latency_buckets[index] = self.latency_buckets[index].saturating_add(1);
    }

    fn merge(&mut self, other: &Self) {
        self.requests = self.requests.saturating_add(other.requests);
        self.errors = self.errors.saturating_add(other.errors);
        self.total_latency_ms = self.total_latency_ms.saturating_add(other.total_latency_ms);
        self.max_latency_ms = self.max_latency_ms.max(other.max_latency_ms);
        self.last_seen = self.last_seen.max(other.last_seen);
        for (bucket, add) in self.latency_buckets.iter_mut().zip(other.latency_buckets) {
            *bucket = bucket.saturating_add(add);
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct ProfileEntry {
    #[serde(flatten)]
    pub key: ProfileKey,
    #[serde(flatten)]
    pub counters: Counters,
    pub window_requests: u64,
}

#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct TraceEntry {
    #[serde(flatten)]
    key: ProfileKey,
    latency_ms: u64,
    error: bool,
    batch_size: usize,
}

#[derive(Debug, Default)]
struct TrackedKey {
    counters: Counters,
    recent: VecDeque<(u64, u64)>,
}

#[derive(Debug, Default)]
struct ProfileState {
    entries: BTreeMap<ProfileKey, TrackedKey>,
    other: Counters,
    dropped_keys: u64,
    recent: VecDeque<(u64, u64)>,
    log_shapes: BTreeMap<(&'static str, &'static str, &'static str, &'static str), u64>,
    batch_sizes: BTreeMap<usize, u64>,
    traces: Vec<TraceEntry>,
}

/// An admission-bounded RPC recorder. Unadmitted observations accumulate in `other`.
#[derive(Debug)]
#[non_exhaustive]
pub struct RpcProfile {
    top_n: usize,
    window_secs: u64,
    trace_limit: usize,
    started: Instant,
    state: Mutex<ProfileState>,
}

#[derive(Debug, Serialize)]
#[non_exhaustive]
pub struct ProfileSnapshot {
    pub window_secs: u64,
    pub window_requests: u64,
    pub requests_per_second: f64,
    pub total: u64,
    pub dropped_keys: u64,
    pub entries: Vec<ProfileEntry>,
    pub top_methods: Vec<LabelCount>,
    pub top_targets: Vec<LabelCount>,
    pub top_selectors: Vec<LabelCount>,
    pub other: Counters,
    pub log_filter_shapes: Vec<LogShapeCount>,
    pub batch_sizes: BTreeMap<usize, u64>,
    pub traces: Vec<TraceEntry>,
}

#[derive(Debug, Serialize)]
#[non_exhaustive]
pub struct LogShapeCount {
    pub chain: &'static str,
    pub address_count: &'static str,
    pub span_blocks: &'static str,
    pub result_count: &'static str,
    pub requests: u64,
}

#[derive(Debug, Serialize)]
#[non_exhaustive]
pub struct LabelCount {
    pub label: String,
    pub requests: u64,
}

pub(crate) struct Observation<'a> {
    pub(crate) chain: &'a str,
    pub(crate) method: &'a str,
    pub(crate) params: &'a Value,
    pub(crate) result_count: &'static str,
    pub(crate) error: bool,
    pub(crate) latency: Duration,
    pub(crate) batch_size: usize,
}

impl RpcProfile {
    pub fn new(top_n: usize, window_secs: u64, trace_limit: usize) -> Self {
        Self {
            top_n: top_n.clamp(10, 200),
            window_secs: window_secs.clamp(10, 3_600),
            trace_limit: trace_limit.min(10_000),
            started: Instant::now(),
            state: Mutex::new(ProfileState::default()),
        }
    }

    pub(crate) fn record(&self, observation: Observation<'_>) {
        self.record_at(observation, self.started.elapsed().as_secs());
    }

    fn record_at(&self, observation: Observation<'_>, elapsed_secs: u64) {
        let shape = CallShape::from_params(observation.method, observation.params);
        // These labels come from fixed lists, never from arbitrary request text.
        let chain = match observation.chain {
            "ethereum" => "ethereum",
            "base" => "base",
            "avalanche" => "avalanche",
            "arbitrum" => "arbitrum",
            _ => "other",
        };
        let method = match observation.method {
            "eth_call"
            | "eth_getLogs"
            | "eth_getBlockByNumber"
            | "eth_getBlockByHash"
            | "eth_getTransactionReceipt"
            | "eth_getBalance"
            | "eth_getTransactionCount"
            | "eth_getCode"
            | "eth_getStorageAt"
            | "eth_blockNumber"
            | "eth_chainId"
            | "eth_estimateGas"
            | "eth_gasPrice"
            | "eth_sendRawTransaction"
            | "eth_feeHistory"
            | "eth_maxPriorityFeePerGas"
            | "eth_getTransactionByHash" => observation.method,
            _ => "other",
        };
        let key = ProfileKey {
            chain: chain.to_owned(),
            method: method.to_owned(),
            target: shape.target,
            selector: shape.selector,
        };
        let latency_ms = u64::try_from(observation.latency.as_millis()).unwrap_or(u64::MAX);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let elapsed_secs = state
            .recent
            .back()
            .map_or(elapsed_secs, |(last, _)| elapsed_secs.max(*last));
        if !state.entries.contains_key(&key) && state.entries.len() >= self.top_n.saturating_mul(4)
        {
            let expired = state.entries.iter_mut().find_map(|(key, tracked)| {
                Self::expire(&mut tracked.recent, elapsed_secs, self.window_secs);
                tracked.recent.is_empty().then(|| key.clone())
            });
            if let Some(expired) = expired
                && let Some(tracked) = state.entries.remove(&expired)
            {
                state.other.merge(&tracked.counters);
                state.dropped_keys = state.dropped_keys.saturating_add(1);
            }
        }
        if state.traces.len() < self.trace_limit {
            state.traces.push(TraceEntry {
                key: key.clone(),
                latency_ms,
                error: observation.error,
                batch_size: observation.batch_size,
            });
        }
        if state.entries.contains_key(&key) || state.entries.len() < self.top_n.saturating_mul(4) {
            let tracked = state.entries.entry(key).or_default();
            tracked
                .counters
                .record(latency_ms, observation.error, timestamp);
            Self::record_second(&mut tracked.recent, elapsed_secs, self.window_secs);
        } else {
            state.other.record(latency_ms, observation.error, timestamp);
            state.dropped_keys = state.dropped_keys.saturating_add(1);
        }
        if let (Some(addresses), Some(span)) = (shape.addresses, shape.span) {
            let count = state
                .log_shapes
                .entry((chain, addresses, span, observation.result_count))
                .or_default();
            *count = count.saturating_add(1);
        }
        Self::record_second(&mut state.recent, elapsed_secs, self.window_secs);
    }

    fn record_second(recent: &mut VecDeque<(u64, u64)>, now: u64, window_secs: u64) {
        if let Some((_, count)) = recent.back_mut().filter(|(second, _)| *second == now) {
            *count = count.saturating_add(1);
        } else {
            recent.push_back((now, 1));
        }
        Self::expire(recent, now, window_secs);
    }

    pub(crate) fn record_batch(&self, size: usize) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = state.batch_sizes.entry(size.min(1_000)).or_default();
        *count = count.saturating_add(1);
    }

    fn expire(recent: &mut VecDeque<(u64, u64)>, now: u64, window_secs: u64) {
        while recent
            .front()
            .is_some_and(|(second, _)| now.saturating_sub(*second) >= window_secs)
        {
            recent.pop_front();
        }
    }

    pub fn snapshot(&self) -> ProfileSnapshot {
        self.snapshot_at(self.started.elapsed().as_secs())
    }

    fn snapshot_at(&self, now: u64) -> ProfileSnapshot {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::expire(&mut state.recent, now, self.window_secs);
        let mut entries: Vec<_> = state
            .entries
            .iter_mut()
            .map(|(key, tracked)| {
                Self::expire(&mut tracked.recent, now, self.window_secs);
                ProfileEntry {
                    key: key.clone(),
                    counters: tracked.counters.clone(),
                    window_requests: tracked.recent.iter().map(|(_, count)| count).sum(),
                }
            })
            .collect();
        entries.sort_by(|a, b| {
            b.window_requests
                .cmp(&a.window_requests)
                .then_with(|| a.key.cmp(&b.key))
        });
        let mut other = state.other.clone();
        let omitted = entries.len().saturating_sub(self.top_n);
        for entry in entries.iter().skip(self.top_n) {
            other.merge(&entry.counters);
        }
        let window_requests: u64 = state.recent.iter().map(|(_, count)| count).sum();
        let top = |field: fn(&ProfileKey) -> &str| {
            let mut counts = BTreeMap::<String, u64>::new();
            for entry in &entries {
                let count = counts.entry(field(&entry.key).to_owned()).or_default();
                *count = count.saturating_add(entry.window_requests);
            }
            let mut labels: Vec<_> = counts
                .into_iter()
                .map(|(label, requests)| LabelCount { label, requests })
                .collect();
            labels.sort_by(|a, b| {
                b.requests
                    .cmp(&a.requests)
                    .then_with(|| a.label.cmp(&b.label))
            });
            labels.truncate(self.top_n);
            labels
        };
        let top_methods = top(|key| &key.method);
        let top_targets = top(|key| &key.target);
        let top_selectors = top(|key| &key.selector);
        entries.truncate(self.top_n);
        ProfileSnapshot {
            window_secs: self.window_secs,
            window_requests,
            requests_per_second: f64::from(u32::try_from(window_requests).unwrap_or(u32::MAX))
                / f64::from(u32::try_from(self.window_secs).unwrap_or(u32::MAX)),
            total: entries
                .iter()
                .map(|entry| entry.counters.requests)
                .sum::<u64>()
                .saturating_add(other.requests),
            dropped_keys: state
                .dropped_keys
                .saturating_add(u64::try_from(omitted).unwrap_or(u64::MAX)),
            top_methods,
            top_targets,
            top_selectors,
            entries,
            other,
            log_filter_shapes: state
                .log_shapes
                .iter()
                .map(
                    |(&(chain, address_count, span_blocks, result_count), &requests)| {
                        LogShapeCount {
                            chain,
                            address_count,
                            span_blocks,
                            result_count,
                            requests,
                        }
                    },
                )
                .collect(),
            batch_sizes: state.batch_sizes.clone(),
            traces: state.traces.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CallShape, Observation, RpcProfile};
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn extracts_only_valid_call_target_and_selector() {
        let address = "0x1234567890abcdef1234567890abcdef12345678";
        let call = json!([{ "to": address, "data": "0xabcdef010203" }]);
        let shape = CallShape::from_params("eth_call", &call);
        assert_eq!(shape.target, address);
        assert_eq!(shape.selector, "0xabcdef01");
        let other = CallShape::from_params("eth_getBalance", &call);
        assert_eq!(other.target, "0x0000000000000000000000000000000000000000");
        assert_eq!(other.selector, "0x00000000");
    }

    #[test]
    fn log_shapes_handle_inclusive_spans_and_unknown_values() {
        let shape = CallShape::from_params(
            "eth_getLogs",
            &json!([{ "address": ["a", "b"], "fromBlock": "0x10", "toBlock": "0x19" }]),
        );
        assert_eq!(shape.addresses, Some("2-10"));
        assert_eq!(shape.span, Some("2-10"));
        assert_eq!(CallShape::results(Some(&json!([1, 2]))), "1-10");
        let unknown = CallShape::from_params("eth_getLogs", &json!([{ "fromBlock": "latest" }]));
        assert_eq!(unknown.span, Some("unknown"));
        assert_eq!(CallShape::results(None), "unknown");
    }

    #[test]
    fn profile_bounds_keys_and_traces_and_tracks_window() -> Result<(), serde_json::Error> {
        let profile = RpcProfile::new(10, 10, 2);
        profile.record_batch(2);
        for index in 0..12 {
            let params = json!([{ "to": format!("0x{index:040x}"), "data": "0xabcdef0100SECRET" }]);
            profile.record_at(
                Observation {
                    chain: "base",
                    method: "eth_call",
                    params: &params,
                    result_count: "unknown",
                    error: index == 0,
                    latency: Duration::from_millis(55),
                    batch_size: 2,
                },
                index,
            );
        }
        let snapshot = profile.snapshot_at(12);
        assert_eq!(snapshot.total, 12);
        assert_eq!(snapshot.window_requests, 9);
        assert_eq!(snapshot.entries.len(), 10);
        assert_eq!(snapshot.other.requests, 2);
        assert_eq!(snapshot.dropped_keys, 2);
        assert_eq!(snapshot.traces.len(), 2);
        assert_eq!(snapshot.entries[0].counters.latency_buckets[2], 1);
        assert_eq!(snapshot.batch_sizes[&2], 1);
        let serialized = serde_json::to_string(&snapshot)?;
        assert!(!serialized.contains("SECRET"));
        Ok(())
    }

    #[test]
    fn profile_records_log_shapes_and_clears_expired_window() {
        let profile = RpcProfile::new(10, 10, 0);
        let params = json!([{ "address": ["a", "b"], "fromBlock": "0x10", "toBlock": "0x19" }]);
        let result = json!([1, 2]);
        profile.record_at(
            Observation {
                chain: "base",
                method: "eth_getLogs",
                params: &params,
                result_count: CallShape::results(Some(&result)),
                error: false,
                latency: Duration::from_millis(1),
                batch_size: 1,
            },
            1,
        );
        let snapshot = profile.snapshot_at(11);
        assert_eq!(snapshot.window_requests, 0);
        assert_eq!(snapshot.log_filter_shapes[0].address_count, "2-10");
        assert_eq!(snapshot.log_filter_shapes[0].span_blocks, "2-10");
        assert_eq!(snapshot.log_filter_shapes[0].result_count, "1-10");
        assert!(snapshot.traces.is_empty());
    }

    #[test]
    fn recent_hot_key_enters_top_n_and_expired_keys_free_capacity() {
        let profile = RpcProfile::new(10, 10, 0);
        for index in 0..40 {
            let params = json!([{ "to": format!("0x{index:040x}") }]);
            profile.record_at(
                Observation {
                    chain: "base",
                    method: "eth_call",
                    params: &params,
                    result_count: "unknown",
                    error: false,
                    latency: Duration::from_millis(5),
                    batch_size: 1,
                },
                0,
            );
        }
        let hot = json!([{ "to": format!("0x{:040x}", 39) }]);
        for _ in 0..3 {
            profile.record_at(
                Observation {
                    chain: "base",
                    method: "eth_call",
                    params: &hot,
                    result_count: "unknown",
                    error: false,
                    latency: Duration::from_millis(5),
                    batch_size: 1,
                },
                2,
            );
        }
        assert_eq!(
            profile.snapshot_at(2).entries[0].key.target,
            format!("0x{:040x}", 39)
        );
        let later = json!([{ "to": format!("0x{:040x}", 41) }]);
        profile.record_at(
            Observation {
                chain: "base",
                method: "eth_call",
                params: &later,
                result_count: "unknown",
                error: false,
                latency: Duration::from_millis(5),
                batch_size: 1,
            },
            11,
        );
        let snapshot = profile.snapshot_at(11);
        assert!(
            snapshot
                .entries
                .iter()
                .any(|entry| entry.key.target == format!("0x{:040x}", 41))
        );
        assert_eq!(snapshot.total, 44);
        assert_eq!(snapshot.window_requests, 4);
    }
}
