//! Domain-independent EVM RPC transport and subscriptions.
#![forbid(unsafe_code)]

pub mod archive;
mod auth;
mod batch;
pub mod classify;
mod dto;
pub mod health;
pub mod http;
mod live;
pub mod models;
pub mod parse;
pub mod presign;
pub mod profile;
pub mod telemetry;
pub mod wake;

#[non_exhaustive]
#[derive(Clone, Debug, thiserror::Error)]
pub enum RpcError {
    #[error("ABI codec error in {context}: {detail}")]
    AbiCodec {
        context: &'static str,
        detail: String,
    },
    #[error("invalid domain value for `{field}`: {detail}")]
    InvalidDomainValue { field: &'static str, detail: String },
    /// A cache-only replay requested missing historical state.
    #[error("offline replay requires cached data for {method}")]
    OfflineCacheMiss { method: String },

    #[error("RPC unauthorized calling `{method}` on {provider} (code {code})")]
    RpcUnauthorized {
        provider: String,
        method: String,
        code: i64,
    },
    #[error("all configured RPC endpoints failed authentication for `{method}`")]
    AllRpcUnauthorized { method: String },

    #[error("RPC archive data unavailable calling `{method}` on {provider}")]
    RpcArchiveUnavailable { provider: String, method: String },

    /// JSON-RPC call timed out waiting for the upstream archive node.
    #[error("RPC timeout calling `{method}` on {endpoint}: {source}")]
    RpcTimeout {
        endpoint: String,
        method: String,
        #[source]
        source: std::sync::Arc<dyn std::error::Error + Send + Sync>,
    },

    /// JSON-RPC call failed due to transport, DNS, TLS, or HTTP error.
    #[error("RPC network error calling `{method}` on {endpoint}: {source}")]
    RpcNetwork {
        endpoint: String,
        method: String,
        #[source]
        source: std::sync::Arc<dyn std::error::Error + Send + Sync>,
    },

    /// JSON-RPC server returned a structured error payload or malformed response.
    #[error("RPC protocol error calling `{method}` on {endpoint} (code {code}): {message}")]
    RpcProtocol {
        endpoint: String,
        method: String,
        code: i64,
        message: String,
    },
}
impl RpcError {
    // Transport sources can carry the full request URI. Retain only trusted diagnostics.
    fn network(endpoint: &str, method: &str, error: ureq::Error) -> Self {
        let source: std::io::Error = match error {
            ureq::Error::Io(error) => error.kind().into(),
            ureq::Error::StatusCode(status) => std::io::Error::other(format!("HTTP {status}")),
            _ => std::io::Error::other("transport or response decoding failed"),
        };
        Self::RpcNetwork {
            endpoint: telemetry::redact_endpoint_host(endpoint),
            method: method.to_owned(),
            source: std::sync::Arc::new(source),
        }
    }
}

pub mod ws;

pub fn deserialize_optional_quantity<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum Quantity {
        Number(u64),
        Text(String),
    }
    <Option<Quantity> as serde::Deserialize>::deserialize(deserializer)?
        .map(|value| match value {
            Quantity::Number(number) => Ok(number),
            Quantity::Text(text) => if let Some(hex) = text.strip_prefix("0x") {
                u64::from_str_radix(hex, 16)
            } else {
                text.parse::<u64>()
            }
            .map_err(serde::de::Error::custom),
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::RpcError;
    use std::error::Error;

    #[test]
    fn transport_sources_cannot_reintroduce_uri_credentials() -> Result<(), &'static str> {
        let url = "https://user-marker:pass-marker@example.org/path-marker?key=query-marker";
        let error = RpcError::network(url, "eth_call", ureq::Error::Io(std::io::Error::other(url)));
        let source = error.source().ok_or("missing transport cause")?;
        let client = crate::http::HttpRpcClient::new_live(vec![url.to_owned()]);
        let report = format!("{error} {error:?} {source} {source:?} {client:?}");
        assert!(report.contains("example.org"));
        assert!(!report.contains("-marker"));
        assert!(source.source().is_none());
        Ok(())
    }
}
