use alloy_primitives::{Address, B256};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Head {
    pub number: Option<String>,
    pub hash: Option<String>,
    pub base_fee_per_gas: Option<String>,
    pub timestamp: Option<String>,
    #[serde(default, deserialize_with = "crate::deserialize_optional_quantity")]
    pub timestamp_ms: Option<u64>,
}

#[cfg(test)]
mod head_tests {
    use super::Head;

    #[test]
    fn timestamp_ms_accepts_hex_numeric_and_missing() -> Result<(), serde_json::Error> {
        for (value, expected) in [
            (serde_json::json!("0x123"), Some(291)),
            (serde_json::json!(291), Some(291)),
            (serde_json::Value::Null, None),
        ] {
            let head: Head = serde_json::from_value(serde_json::json!({"timestampMs": value}))?;
            assert_eq!(head.timestamp_ms, expected);
        }
        let head: Head = serde_json::from_value(serde_json::json!({}))?;
        assert_eq!(head.timestamp_ms, None);
        assert!(
            serde_json::from_value::<Head>(serde_json::json!({"timestampMs": "0xzz"})).is_err()
        );
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct Log {
    pub address: Address,
    pub topics: Vec<B256>,
}

pub(super) enum Push {
    Head(Head),
    Log(Log),
}

pub(super) struct Protocol {
    expect_heads: bool,
    expect_logs: bool,
    heads: Option<String>,
    logs: Option<String>,
    extra_logs: Option<String>,
    extra_logs_expected: bool,
    extra_logs_disabled: bool,
}

impl Protocol {
    pub(super) fn new(expect_heads: bool, expect_logs: bool, extra_logs_expected: bool) -> Self {
        Self {
            expect_heads,
            expect_logs,
            heads: None,
            logs: None,
            extra_logs: None,
            extra_logs_expected,
            extra_logs_disabled: false,
        }
    }

    pub(super) fn ready(&self) -> bool {
        self.settled() && (!self.extra_logs_disabled || self.expect_heads || self.expect_logs)
    }

    pub(super) fn settled(&self) -> bool {
        (!self.expect_heads || self.heads.is_some())
            && (!self.expect_logs || self.logs.is_some())
            && (!self.extra_logs_expected || self.extra_logs.is_some() || self.extra_logs_disabled)
    }

    pub(super) fn requests(
        addresses: &[Address],
        subscribe_heads: bool,
        filter: Option<&super::WsTopicLogFilter>,
    ) -> Vec<String> {
        let mut requests = Vec::new();
        if subscribe_heads {
            requests.push(
                json!({"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]})
                    .to_string(),
            );
        }
        if !addresses.is_empty() {
            requests.push(json!({"jsonrpc":"2.0","id":2,"method":"eth_subscribe","params":["logs",{"address":addresses}]}).to_string());
        }
        if let Some(filter) = filter {
            requests.push(json!({"jsonrpc":"2.0","id":3,"method":"eth_subscribe","params":["logs",{"address":[filter.address],"topics":[filter.topic0,filter.topic1]}]}).to_string());
        }
        requests
    }

    pub(super) fn receive(&mut self, text: &str) -> Result<Option<Push>, super::client::WsError> {
        let value: Value = serde_json::from_str(text)?;
        if let Some(id) = value.get("id").and_then(Value::as_u64) {
            if id == 3 && self.extra_logs_expected {
                if let Some(subscription) = value.get("result").and_then(Value::as_str) {
                    self.extra_logs = Some(subscription.to_owned());
                } else {
                    self.extra_logs_disabled = true;
                }
                return Ok(None);
            }
            if (id == 1 && self.expect_heads) || (id == 2 && self.expect_logs) {
                let subscription = value
                    .get("result")
                    .and_then(Value::as_str)
                    .ok_or(super::client::WsError::SubscriptionRejected)?
                    .to_owned();
                if id == 1 {
                    self.heads = Some(subscription);
                } else {
                    self.logs = Some(subscription);
                }
            } else {
                return Err(super::client::WsError::SubscriptionRejected);
            }
            return Ok(None);
        }
        if value.get("method").and_then(Value::as_str) != Some("eth_subscription") {
            return Ok(None);
        }
        let Some(params) = value.get("params") else {
            return Ok(None);
        };
        let Some(id) = params.get("subscription").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(result) = params.get("result") else {
            return Ok(None);
        };
        if self.heads.as_deref() == Some(id) {
            let head: Head = serde_json::from_value(result.clone())?;
            // Read every field at the boundary so malformed field types fail closed.
            let _ = (
                &head.number,
                &head.hash,
                &head.base_fee_per_gas,
                &head.timestamp,
                &head.timestamp_ms,
            );
            return Ok(Some(Push::Head(head)));
        }
        if self.logs.as_deref() == Some(id) || self.extra_logs.as_deref() == Some(id) {
            let log: Log = serde_json::from_value(result.clone())?;
            return Ok(Some(Push::Log(log)));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws::WsTopicLogFilter;
    type TestResult = Result<(), Box<dyn std::error::Error>>;
    #[test]
    fn rejected_extra_filter_keeps_primary_subscription_active() -> TestResult {
        use alloy_primitives::B256;
        let filter = WsTopicLogFilter::new(
            Address::ZERO,
            vec![B256::repeat_byte(1)],
            vec![B256::repeat_byte(2)],
        );
        let requests = Protocol::requests(&[Address::ZERO], false, Some(&filter));
        let extra: Value = serde_json::from_str(&requests[1])?;
        assert_eq!(extra["id"], 3);
        assert_eq!(
            extra["params"][1]["topics"],
            json!([filter.topic0, filter.topic1])
        );
        let mut protocol = Protocol::new(false, true, true);
        protocol.receive(r#"{"jsonrpc":"2.0","id":2,"result":"primary"}"#)?;
        protocol.receive(
            r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32602,"message":"unsupported"}}"#,
        )?;
        assert!(protocol.ready());
        let frame = json!({"method":"eth_subscription","params":{"subscription":"primary","result":{"address":Address::ZERO,"topics":[]}}}).to_string();
        assert!(matches!(protocol.receive(&frame)?, Some(Push::Log(_))));
        Ok(())
    }

    #[test]
    fn protocol_readiness_matches_requested_subscriptions() -> TestResult {
        assert!(Protocol::requests(&[], false, None).is_empty());
        let mut logs = Protocol::new(false, true, false);
        assert!(!logs.ready());
        assert!(logs.receive(r#"{"id":1,"result":"unexpected"}"#).is_err());
        logs.receive(r#"{"id":2,"result":"logs"}"#)?;
        assert!(logs.ready());
        let mut heads = Protocol::new(true, false, false);
        heads.receive(r#"{"id":1,"result":"heads"}"#)?;
        assert!(heads.ready());
        Ok(())
    }
}
