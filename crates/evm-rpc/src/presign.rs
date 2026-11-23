use crate::{RpcError, models::RpcBlockData};
use alloy_primitives::{Address, Bytes, U256, address};
use alloy_sol_types::{SolCall, sol};
use derive_builder::Builder;
/// Canonical OP-Stack `GasPriceOracle` predeploy on Base Mainnet (`8453`).
pub const OP_STACK_GAS_PRICE_ORACLE: Address = address!("420000000000000000000000000000000000000F");

sol! {
    function getL1Fee(bytes memory data) external view returns (uint256);
}

/// Coalesced pre-sign RPC read parameters (`pending` nonce + `estimate_gas` + `latest` header + optional Base `getL1Fee`).
#[non_exhaustive]
#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned", setter(into))]
pub struct PreSignReadRequest {
    pub signer: Address,
    pub to: Address,
    pub calldata: Bytes,
    #[builder(default = "U256::ZERO")]
    pub value: U256,
    #[builder(default)]
    pub op_l1_fee_tx_envelope: Option<Bytes>,
}

/// Coalesced pre-sign RPC read result returned by a single JSON-RPC batch round-trip.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct PreSignReadSnapshot {
    pub pending_nonce: u64,
    pub estimated_gas: u64,
    pub latest_header: RpcBlockData,
    pub l1_fee_wei: U256,
    pub signer_balance_wei: U256,
}

impl PreSignReadSnapshot {
    /// Creates a `PreSignReadSnapshot`.
    pub fn new(
        pending_nonce: u64,
        estimated_gas: u64,
        latest_header: RpcBlockData,
        l1_fee_wei: U256,
        signer_balance_wei: U256,
    ) -> Self {
        Self {
            pending_nonce,
            estimated_gas,
            latest_header,
            l1_fee_wei,
            signer_balance_wei,
        }
    }
}

pub fn encode_get_l1_fee_call(tx_envelope: &[u8]) -> Vec<u8> {
    getL1FeeCall {
        data: Bytes::copy_from_slice(tx_envelope),
    }
    .abi_encode()
}

pub fn decode_get_l1_fee_return(raw: &[u8]) -> Result<U256, RpcError> {
    getL1FeeCall::abi_decode_returns_validate(raw).map_err(|err| RpcError::AbiCodec {
        context: "GasPriceOracle.getL1Fee",
        detail: err.to_string(),
    })
}
