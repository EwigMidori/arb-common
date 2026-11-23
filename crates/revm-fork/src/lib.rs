//! Fork state, disk caches and local EVM execution.
#![forbid(unsafe_code)]
use std::path::PathBuf;
pub mod b20_precompile;
pub mod execution;
pub mod fork_db;
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ForkDbError {
    #[error(transparent)]
    Rpc(#[from] evm_rpc::RpcError),
    #[error("disk cache I/O failed at {path}: {source}")]
    CacheIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("disk cache JSON error at {path}: {source}")]
    CacheDecode {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid hex in B20 cache {path}, key {key}: {source}")]
    CacheHex {
        path: PathBuf,
        key: String,
        #[source]
        source: hex::FromHexError,
    },
    #[error("invalid domain value for `{field}`: {detail}")]
    InvalidDomainValue { field: &'static str, detail: String },
    #[error("ABI codec error in {context}: {detail}")]
    AbiCodec {
        context: &'static str,
        detail: String,
    },
    #[error("EVM execution error in {context}: {detail}")]
    EvmExecution { context: String, detail: String },
    #[error("EVM reverted in {context} (gas {gas_used}): {reason}")]
    EvmReverted {
        context: String,
        reason: String,
        gas_used: u64,
    },
    #[error("EVM halted in {context} (gas {gas_used}): {reason}")]
    EvmHalted {
        context: String,
        reason: String,
        gas_used: u64,
    },
}
