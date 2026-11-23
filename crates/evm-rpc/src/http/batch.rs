use super::{HttpRpcClient, RpcError};
use serde::Deserialize;
use std::time::Instant;
#[derive(Debug, Deserialize)]
struct BatchErrorDto {
    code: i64,
    message: String,
}

#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct BatchItemDto {
    pub id: u64,
    pub result: Option<serde_json::Value>,
    error: Option<BatchErrorDto>,
}

#[derive(Debug)]
pub enum BatchAttemptError {
    BatchRejected(RpcError),
    ExecutionError(RpcError),
    InvalidResponse(RpcError),
    Transport(RpcError),
}

#[derive(Default)]
struct BatchProfile {
    sent: bool,
    items: Option<Vec<(bool, &'static str)>>,
}

struct BatchOptions<'a> {
    method_label: &'a str,
    propagate_revert: bool,
}

impl HttpRpcClient {
    /// Returns exactly one response per request in request order. Invalid response IDs
    /// terminate the batch before error handling or sequential fallback.
    pub fn execute_batch(
        &self,
        method_label: &str,
        payload: &serde_json::Value,
        propagate_revert: bool,
    ) -> Result<Vec<BatchItemDto>, BatchAttemptError> {
        let started = Instant::now();
        let mut observed = BatchProfile::default();
        let result =
            self.execute_batch_inner(method_label, payload, propagate_revert, &mut observed);
        if observed.sent
            && let Some(calls) = payload.as_array()
            && let Some((profile, chain)) = self.telemetry.profile()
        {
            profile.record_batch(calls.len());
            let latency = started.elapsed();
            for (index, call) in calls.iter().enumerate() {
                let (error, result_count) = observed
                    .items
                    .as_ref()
                    .and_then(|items| items.get(index))
                    .copied()
                    .unwrap_or((true, "unknown"));
                profile.record(crate::profile::Observation {
                    chain,
                    method: call["method"].as_str().unwrap_or("other"),
                    params: &call["params"],
                    result_count,
                    error,
                    latency,
                    batch_size: calls.len(),
                });
            }
        }
        result
    }

    fn execute_batch_inner(
        &self,
        method_label: &str,
        payload: &serde_json::Value,
        propagate_revert: bool,
        observed: &mut BatchProfile,
    ) -> Result<Vec<BatchItemDto>, BatchAttemptError> {
        self.telemetry.record_request();
        let call_started = Instant::now();
        let mut last_err: Option<RpcError> = None;
        let mut batch_rejected = false;
        let mut switched_endpoint = false;
        let mut archive_request =
            self.archive_request(payload.as_array().into_iter().flatten().filter_map(|call| {
                Self::historical_block(call["method"].as_str()?, &call["params"])
            }));

        let now = Instant::now();
        let start_idx = super::health::preferred_start_index(
            self.endpoints.len(),
            self.rotation_now_secs(now),
            self.rotation_interval_secs,
        );
        let mut indices: std::collections::VecDeque<_> = self
            .available_indices(now, start_idx, archive_request)
            .into();
        let mut visited = vec![false; self.endpoints.len()];
        if indices.is_empty() && archive_request
            && let Some(idx) = self.provider_counters.iter().position(|p| {
                p.auth
                    .archive_unsupported
                    .load(std::sync::atomic::Ordering::Relaxed)
            }) {
            last_err = Some(self.archive_error(idx, method_label));
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
                last_err = Some(self.archive_error(ep_idx, method_label));
                continue;
            }
            let endpoint = &self.endpoints[ep_idx];
            if ep_idx != start_idx {
                switched_endpoint = true;
            }
            if let Some(calls) = payload.as_array() {
                for call in calls {
                    self.telemetry
                        .record_method(call["method"].as_str().unwrap_or("other"));
                }
            }
            super::RpcClientTelemetry::provider_request(&self.provider_counters[ep_idx]);
            observed.sent = true;
            observed.items = None;
            match self.try_batch_endpoint(
                ep_idx,
                payload,
                BatchOptions {
                    method_label,
                    propagate_revert,
                },
                observed,
            ) {
                Ok(items) => {
                    self.health
                        .lock()
                        .unwrap_or_else(|err| err.into_inner())
                        .entry(endpoint.clone())
                        .or_default()
                        .success();
                    super::RpcClientTelemetry::provider_success(&self.provider_counters[ep_idx]);
                    if switched_endpoint {
                        self.telemetry
                            .record_failover_switch(call_started.elapsed().as_secs_f64() * 1_000.0);
                    }
                    return Ok(items);
                }
                Err(
                    err @ (BatchAttemptError::InvalidResponse(_)
                    | BatchAttemptError::ExecutionError(_)),
                ) => {
                    self.health
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .entry(endpoint.clone())
                        .or_default()
                        .other_failure();
                    return Err(err);
                }
                Err(BatchAttemptError::BatchRejected(err)) => {
                    self.health
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .entry(endpoint.clone())
                        .or_default()
                        .other_failure();
                    batch_rejected = true;
                    last_err = Some(err);
                }
                Err(BatchAttemptError::Transport(err)) => {
                    if matches!(err, RpcError::RpcArchiveUnavailable { .. }) {
                        archive_request = true;
                        indices.extend(self.available_indices(Instant::now(), start_idx, true));
                    }
                    if matches!(
                        err,
                        RpcError::RpcUnauthorized { .. } | RpcError::RpcArchiveUnavailable { .. }
                    ) {
                        last_err = Some(err);
                        continue;
                    }
                    if let RpcError::RpcProtocol { code, message, .. } = &err
                        && let Some(category) = provider_rejection(*code, message)
                    {
                        self.mark_provider_rejected(endpoint, category);
                        last_err = Some(err);
                        continue;
                    }
                    match &err {
                        RpcError::RpcTimeout { .. } => self.mark_timeout(endpoint),
                        _ => self.mark_other_failure(endpoint),
                    }
                    last_err = Some(err);
                }
            }
        }

        if switched_endpoint {
            self.telemetry
                .record_failover_switch(call_started.elapsed().as_secs_f64() * 1_000.0);
        }

        if let Some(err) = self.auth_exhausted(method_label) {
            return Err(BatchAttemptError::Transport(err));
        }
        let err = last_err.unwrap_or_else(|| RpcError::RpcProtocol {
            endpoint: self
                .endpoints
                .first()
                .map(|url| crate::telemetry::redact_endpoint_host(url))
                .unwrap_or_default(),
            method: method_label.to_owned(),
            code: -1,
            message: "no RPC endpoints configured".to_owned(),
        });
        if batch_rejected {
            Err(BatchAttemptError::BatchRejected(err))
        } else {
            Err(BatchAttemptError::Transport(err))
        }
    }

    fn try_batch_endpoint(
        &self,
        ep_idx: usize,
        payload: &serde_json::Value,
        options: BatchOptions<'_>,
        observed: &mut BatchProfile,
    ) -> Result<Vec<BatchItemDto>, BatchAttemptError> {
        let BatchOptions {
            method_label,
            propagate_revert,
        } = options;
        let endpoint = &self.endpoints[ep_idx];
        let attempt_started = Instant::now();
        let response = self
            .agent
            .post(endpoint)
            .header("content-type", "application/json")
            .send_json(payload);
        let response = match response {
            Ok(mut resp) if matches!(resp.status().as_u16(), 401 | 403) => {
                let code = resp.status().as_u16();
                let body = resp.body_mut().read_to_string().unwrap_or_default();
                self.telemetry.record_attempt_latency(
                    attempt_started.elapsed().as_secs_f64() * 1_000.0,
                    self.slow_threshold_ms,
                );
                return Err(BatchAttemptError::Transport(self.status_error(
                    ep_idx,
                    method_label,
                    code,
                    &body,
                )));
            }
            Ok(resp) if resp.status().as_u16() >= 400 => {
                Err(ureq::Error::StatusCode(resp.status().as_u16()))
            }
            other => other,
        };
        let attempt_ms = attempt_started.elapsed().as_secs_f64() * 1_000.0;

        let mut resp = match response {
            Ok(resp) => {
                self.telemetry
                    .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                resp
            }
            Err(ureq::Error::Timeout(kind)) => {
                self.telemetry.record_timeout();
                return Err(BatchAttemptError::Transport(RpcError::RpcTimeout {
                    endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                    method: method_label.to_owned(),
                    source: std::sync::Arc::new(ureq::Error::Timeout(kind)),
                }));
            }
            Err(ureq::Error::StatusCode(code @ (401 | 403))) => {
                self.telemetry
                    .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                return Err(BatchAttemptError::Transport(self.unauthorized(
                    ep_idx,
                    method_label,
                    i64::from(code),
                )));
            }
            Err(ureq::Error::StatusCode(code @ 402)) => {
                self.telemetry
                    .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                return Err(BatchAttemptError::Transport(RpcError::RpcProtocol {
                    endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                    method: method_label.to_owned(),
                    code: i64::from(code),
                    message: format!("HTTP {code} Restricted"),
                }));
            }
            Err(ureq::Error::StatusCode(429)) => {
                self.telemetry
                    .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                self.telemetry.record_rate_limit();
                return Err(BatchAttemptError::Transport(RpcError::RpcProtocol {
                    endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                    method: method_label.to_owned(),
                    code: 429,
                    message: "HTTP 429 Too Many Requests".to_owned(),
                }));
            }
            Err(ureq::Error::StatusCode(code)) if code == 400 || code == 415 => {
                self.telemetry
                    .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                return Err(BatchAttemptError::BatchRejected(RpcError::RpcProtocol {
                    endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                    method: method_label.to_owned(),
                    code: i64::from(code),
                    message: "endpoint rejected JSON-RPC batch HTTP status".to_owned(),
                }));
            }
            Err(err) => {
                self.telemetry
                    .record_attempt_latency(attempt_ms, self.slow_threshold_ms);
                return Err(BatchAttemptError::Transport(RpcError::network(
                    endpoint,
                    method_label,
                    err,
                )));
            }
        };

        let raw_json: serde_json::Value = resp.body_mut().read_json().map_err(|err| {
            BatchAttemptError::Transport(RpcError::network(endpoint, method_label, err))
        })?;

        // Providers can report a batch-wide quota or access error as a single object.
        let batch_error = raw_json
            .get("error")
            .and_then(|error| serde_json::from_value::<BatchErrorDto>(error.clone()).ok());
        if let Some(error) = &batch_error
            && crate::classify::is_unauthorized(error.code, &error.message)
        {
            return Err(BatchAttemptError::Transport(self.unauthorized(
                ep_idx,
                method_label,
                error.code,
            )));
        }
        let items = align_response(payload, raw_json).map_err(|mut message| {
            let mut code = -32_600;
            if let Some(error) = batch_error {
                code = error.code;
                if let Some(category) = provider_rejection(code, &error.message) {
                    if category == 429 {
                        self.telemetry.record_rate_limit();
                    }
                    self.mark_provider_rejected(endpoint, category);
                }
                message.push_str(": ");
                message.push_str(crate::classify::upstream_summary(code, &error.message));
            }
            BatchAttemptError::InvalidResponse(RpcError::RpcProtocol {
                endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                method: method_label.to_owned(),
                code,
                message,
            })
        })?;
        observed.items = Some(
            items
                .iter()
                .map(|item| {
                    (
                        item.error.is_some(),
                        crate::profile::CallShape::results(item.result.as_ref()),
                    )
                })
                .collect(),
        );

        for (index, item) in items.iter().enumerate() {
            if let Some(error) = &item.error
                && crate::classify::is_unauthorized(error.code, &error.message)
            {
                let method = payload[index]["method"].as_str().unwrap_or(method_label);
                return Err(BatchAttemptError::Transport(
                    self.unauthorized(ep_idx, method, error.code),
                ));
            }
        }
        for (index, item) in items.iter().enumerate() {
            if let Some(ref err) = item.error {
                let method = payload[index]["method"].as_str().unwrap_or(method_label);
                let protocol_err = RpcError::RpcProtocol {
                    endpoint: crate::telemetry::redact_endpoint_host(endpoint),
                    method: method.to_owned(),
                    code: err.code,
                    message: format!(
                        "batch id {}: {}",
                        item.id,
                        crate::classify::upstream_summary(err.code, &err.message)
                    ),
                };
                if crate::classify::is_restricted_rpc_error(err.code, &err.message)
                    && (!crate::classify::is_rate_limit_rpc_error(err.code, &err.message)
                        || crate::classify::is_quota_exhausted_rpc_error(&err.message))
                {
                    return Err(BatchAttemptError::Transport(protocol_err));
                }
                if crate::classify::is_rate_limit_rpc_error(err.code, &err.message) {
                    self.telemetry.record_rate_limit();
                    return Err(BatchAttemptError::Transport(protocol_err));
                }
                if propagate_revert
                    && crate::classify::is_deterministic_evm_error(err.code, &err.message)
                {
                    return Err(BatchAttemptError::ExecutionError(protocol_err));
                }
                if err.code == -32_600 || err.code == -32_601 {
                    return Err(BatchAttemptError::BatchRejected(protocol_err));
                }
                return Err(BatchAttemptError::Transport(protocol_err));
            }
        }

        Ok(items)
    }
}

/// Validates the complete response before any caller interprets individual results or errors.
fn align_response(
    payload: &serde_json::Value,
    response: serde_json::Value,
) -> Result<Vec<BatchItemDto>, String> {
    let requests = payload.as_array().ok_or("expected batch request array")?;
    let expected = || {
        requests
            .iter()
            .map(|r| format!("{} id {}", r["method"].as_str().unwrap_or("?"), r["id"]))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let serde_json::Value::Array(rows) = response else {
        return Err(format!("non-array batch response for {}", expected()));
    };
    let count = rows.len();
    let invalid = |reason: String| {
        format!(
            "{reason}; received {count} responses for {} requests ({})",
            requests.len(),
            expected()
        )
    };
    let mut slots = std::collections::HashMap::with_capacity(requests.len());
    for (index, request) in requests.iter().enumerate() {
        let id = request["id"]
            .as_u64()
            .ok_or_else(|| invalid("invalid request id".into()))?;
        if slots.insert(id, index).is_some() {
            return Err(invalid(format!("duplicate request id {id}")));
        }
    }
    let mut ordered: Vec<Option<BatchItemDto>> = (0..requests.len()).map(|_| None).collect();
    for row in rows {
        let id = row["id"]
            .as_u64()
            .ok_or_else(|| invalid("missing or invalid response id".to_owned()))?;
        let index = *slots
            .get(&id)
            .ok_or_else(|| invalid(format!("unknown response id {id}")))?;
        if ordered[index].is_some() {
            return Err(invalid(format!("duplicate response id {id}")));
        }
        ordered[index] = Some(
            serde_json::from_value(row)
                .map_err(|_| invalid(format!("invalid response for id {id}")))?,
        );
    }
    ordered
        .into_iter()
        .zip(requests)
        .map(|(item, request)| {
            item.ok_or_else(|| invalid(format!("missing response id {}", request["id"])))
        })
        .collect()
}

impl BatchItemDto {
    pub(crate) fn hex_result(&self, method: &'static str) -> Result<&str, RpcError> {
        self.result
            .as_ref()
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| RpcError::InvalidDomainValue {
                field: method,
                detail: format!("expected hex string for batch id {}", self.id),
            })
    }
}

fn provider_rejection(code: i64, message: &str) -> Option<u16> {
    if code == 402 || crate::classify::is_quota_exhausted_rpc_error(message) {
        Some(402)
    } else if crate::classify::is_rate_limit_rpc_error(code, message) {
        Some(429)
    } else if matches!(code, 401 | 403) || crate::classify::is_restricted_rpc_error(code, message) {
        Some(403)
    } else {
        None
    }
}
