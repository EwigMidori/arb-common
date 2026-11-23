use crate::{
    RpcError,
    http::{BatchAttemptError, HttpRpcClient},
    parse as helper,
};
use alloy_primitives::{Address, Bytes, U256};

impl HttpRpcClient {
    /// Executes a historical read-only `eth_call` at `block_number`.
    pub fn eth_call(
        &self,
        to: Address,
        calldata: &[u8],
        block_number: u64,
    ) -> Result<Bytes, RpcError> {
        let params = serde_json::json!([
            { "to": format!("{to:#x}"), "data": format!("0x{}", hex::encode(calldata)) },
            format!("0x{block_number:x}")
        ]);
        let hex_val: String = self.call_single("eth_call", params)?;
        helper::parse_bytes_hex(&hex_val, "eth_call")
    }

    /// Reads the exact EIP-1898 block; a stale canonical height cannot populate another hash.
    pub fn eth_call_at_hash(
        &self,
        to: Address,
        calldata: &[u8],
        hash: alloy_primitives::B256,
    ) -> Result<Bytes, RpcError> {
        let params = serde_json::json!([
            { "to": to, "data": format!("0x{}", hex::encode(calldata)) },
            { "blockHash": hash, "requireCanonical": true }
        ]);
        let raw: String = self.call_single("eth_call", params)?;
        helper::parse_bytes_hex(&raw, "eth_call")
    }

    /// Fetches account balance (`eth_getBalance`) at `block_number`.
    pub fn get_balance(&self, address: Address, block_number: u64) -> Result<U256, RpcError> {
        let params = serde_json::json!([format!("{address:#x}"), format!("0x{block_number:x}")]);
        let hex_val: String = self.call_single("eth_getBalance", params)?;
        helper::parse_u256_hex(&hex_val, "eth_getBalance")
    }

    /// Fetches account nonce (`eth_getTransactionCount`) at `block_number`.
    pub fn get_transaction_count(
        &self,
        address: Address,
        block_number: u64,
    ) -> Result<u64, RpcError> {
        let params = serde_json::json!([format!("{address:#x}"), format!("0x{block_number:x}")]);
        let hex_val: String = self.call_single("eth_getTransactionCount", params)?;
        helper::parse_u64_hex(&hex_val, "eth_getTransactionCount")
    }

    /// Fetches deployed bytecode (`eth_getCode`) at `block_number`.
    pub fn get_code(&self, address: Address, block_number: u64) -> Result<Bytes, RpcError> {
        let params = serde_json::json!([format!("{address:#x}"), format!("0x{block_number:x}")]);
        let hex_val: String = self.call_single("eth_getCode", params)?;
        helper::parse_bytes_hex(&hex_val, "eth_getCode")
    }

    /// Fetches 32-byte storage slot (`eth_getStorageAt`) at `block_number`.
    pub fn get_storage_at(
        &self,
        address: Address,
        slot: U256,
        block_number: u64,
    ) -> Result<U256, RpcError> {
        let params = serde_json::json!([
            format!("{address:#x}"),
            format!("0x{slot:064x}"),
            format!("0x{block_number:x}")
        ]);
        let hex_val: String = self.call_single("eth_getStorageAt", params)?;
        helper::parse_u256_hex(&hex_val, "eth_getStorageAt")
    }

    /// Fetches balance, nonce, and code in a single 3-item JSON-RPC batch HTTP call,
    /// falling back sequentially only if an endpoint rejects JSON-RPC batching.
    pub fn get_account_basic_batch(
        &self,
        address: Address,
        block_number: u64,
    ) -> Result<(U256, u64, Bytes), RpcError> {
        self.require_online("eth_getBalance")?;

        let addr_hex = format!("{address:#x}");
        let block_hex = format!("0x{block_number:x}");
        let payload = serde_json::json!([
            { "jsonrpc": "2.0", "id": 1, "method": "eth_getBalance", "params": [&addr_hex, &block_hex] },
            { "jsonrpc": "2.0", "id": 2, "method": "eth_getTransactionCount", "params": [&addr_hex, &block_hex] },
            { "jsonrpc": "2.0", "id": 3, "method": "eth_getCode", "params": [&addr_hex, &block_hex] }
        ]);

        let items = match self.execute_batch("eth_batch_account_basic", &payload, false) {
            Ok(items) => items,
            Err(BatchAttemptError::BatchRejected(_)) => {
                let balance = self.get_balance(address, block_number)?;
                let nonce = self.get_transaction_count(address, block_number)?;
                let code = self.get_code(address, block_number)?;
                return Ok((balance, nonce, code));
            }
            Err(
                BatchAttemptError::InvalidResponse(err)
                | BatchAttemptError::ExecutionError(err)
                | BatchAttemptError::Transport(err),
            ) => {
                return Err(err);
            }
        };

        let balance =
            helper::parse_u256_hex(items[0].hex_result("eth_getBalance")?, "eth_getBalance")?;
        let nonce = helper::parse_u64_hex(
            items[1].hex_result("eth_getTransactionCount")?,
            "eth_getTransactionCount",
        )?;
        let code = helper::parse_bytes_hex(items[2].hex_result("eth_getCode")?, "eth_getCode")?;
        Ok((balance, nonce, code))
    }
}
