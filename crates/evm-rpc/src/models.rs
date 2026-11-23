use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use serde::{Deserialize, Serialize};
/// On-chain block header and transaction payload fetched from Archive JSON-RPC.
#[non_exhaustive]
#[derive(Debug, Clone, typed_builder::TypedBuilder, Serialize, Deserialize)]
pub struct RpcBlockData {
    pub number: u64,
    pub hash: B256,
    pub timestamp: u64,
    #[serde(
        default,
        alias = "timestampMs",
        deserialize_with = "deserialize_optional_quantity",
        skip_serializing_if = "Option::is_none"
    )]
    pub timestamp_ms: Option<u64>,
    pub gas_limit: u64,
    pub base_fee_per_gas: u64,
    pub beneficiary: Address,
    pub prevrandao: Option<B256>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excess_blob_gas: Option<u64>,
    pub transactions: Vec<RpcTransactionData>,
}

use crate::deserialize_optional_quantity;

/// On-chain transaction envelope needed for intra-block `revm` replay.
#[non_exhaustive]
#[derive(Debug, Clone, typed_builder::TypedBuilder, Serialize, Deserialize)]
pub struct RpcTransactionData {
    pub hash: B256,
    pub transaction_index: u64,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
    pub gas: u64,
    pub gas_price: u128,
    pub max_priority_fee_per_gas: Option<u128>,
    /// Fee cap for type-2 transactions with a captured access list.
    #[serde(
        default,
        alias = "eip1559_empty_access_list_max_fee",
        skip_serializing_if = "Option::is_none"
    )]
    pub eip1559_max_fee: Option<u128>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub access_list: Vec<RpcAccessListItem>,
    pub nonce: u64,
}

/// One address and its ordered storage keys from an EIP-2930 access list.
#[non_exhaustive]
#[derive(Debug, Clone, typed_builder::TypedBuilder, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcAccessListItem {
    pub address: Address,
    pub storage_keys: Vec<B256>,
}

impl RpcTransactionData {
    /// Converts `to` into `revm::primitives::TxKind`.
    pub fn tx_kind(&self) -> TxKind {
        match self.to {
            Some(target) => TxKind::Call(target),
            None => TxKind::Create,
        }
    }
}

/// On-chain transaction receipt with emitted logs.
#[non_exhaustive]
#[derive(Debug, Clone, typed_builder::TypedBuilder, Serialize, Deserialize)]
pub struct RpcReceiptData {
    // Older offline backtest caches predate block hashes. Live ReceiptDto requires blockHash.
    #[serde(default)]
    pub block_hash: B256,
    pub transaction_hash: B256,
    pub block_number: u64,
    pub transaction_index: u64,
    pub status: bool,
    pub gas_used: u64,
    pub effective_gas_price: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub l1_fee_wei: Option<u128>,
    pub logs: Vec<RpcLogData>,
}

/// Raw EVM log entry from a transaction receipt.
#[non_exhaustive]
#[derive(Debug, Clone, typed_builder::TypedBuilder, Serialize, Deserialize)]
pub struct RpcLogData {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
    pub log_index: u64,
}

/// Parameters for `eth_getLogs`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RpcLogFilter {
    pub from_block: u64,
    pub to_block: u64,
    pub addresses: Vec<Address>,
    pub topic0: Vec<B256>,
    pub topic1: Vec<B256>,
}

impl RpcLogFilter {
    /// Creates a log filter across `[from_block, to_block]` for target contracts and `topic0` hashes.
    pub fn new(from_block: u64, to_block: u64, addresses: Vec<Address>, topic0: Vec<B256>) -> Self {
        Self {
            from_block,
            to_block,
            addresses,
            topic0,
            topic1: Vec::new(),
        }
    }
}

/// Raw EVM log returned by `eth_getLogs`, including block and transaction context.
#[non_exhaustive]
#[derive(Debug, Clone, typed_builder::TypedBuilder)]
pub struct RpcRawLog {
    #[builder(default)]
    pub block_hash: Option<B256>,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
    pub block_number: u64,
    pub transaction_hash: B256,
    pub transaction_index: u64,
    pub log_index: u64,
}
