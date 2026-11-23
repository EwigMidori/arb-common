use super::{HttpRpcClient, ProfileResult};
use crate::{RpcError, health};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

const FALLBACK_SPAN: u64 = 10;

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
                let limit = self
                    .log_range_limits
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(endpoint)
                    .copied();
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
                        Err(original)
                            if range.span() > FALLBACK_SPAN
                                && Self::is_log_range_error(&original) =>
                        {
                            match self.call_log_chunks(
                                index,
                                params,
                                (&range, FALLBACK_SPAN),
                                profile,
                            ) {
                                Ok(logs) => {
                                    self.log_range_limits
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .insert(endpoint.clone(), FALLBACK_SPAN);
                                    Ok(logs)
                                }
                                Err(_) => return Err(original),
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

    fn is_log_range_error(error: &RpcError) -> bool {
        match error {
            RpcError::RpcProtocol { code, message, .. } => {
                crate::classify::is_block_span_limit_error(*code, message)
            }
            RpcError::RpcNetwork { source, .. } => {
                let message = source.to_string();
                message == "HTTP 400" || crate::classify::is_block_span_limit_error(0, &message)
            }
            _ => false,
        }
    }
}
