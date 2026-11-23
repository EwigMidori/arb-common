use crate::{
    RpcError, dto,
    http::{BatchAttemptError, HttpRpcClient},
    models::RpcBlockData,
    parse as helper,
    presign::{self, *},
};
use alloy_primitives::U256;
impl HttpRpcClient {
    pub(super) fn execute_presign_batch(
        &self,
        req: &PreSignReadRequest,
    ) -> Result<PreSignReadSnapshot, RpcError> {
        self.require_online("eth_getTransactionCount")?;

        let mut tx_obj = serde_json::Map::new();
        tx_obj.insert(
            "from".to_owned(),
            serde_json::json!(format!("{:#x}", req.signer)),
        );
        tx_obj.insert("to".to_owned(), serde_json::json!(format!("{:#x}", req.to)));
        tx_obj.insert(
            "data".to_owned(),
            serde_json::json!(format!("0x{}", hex::encode(&req.calldata))),
        );
        if !req.value.is_zero() {
            tx_obj.insert(
                "value".to_owned(),
                serde_json::json!(format!("{:#x}", req.value)),
            );
        }

        let mut batch_calls = vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getTransactionCount",
                "params": [format!("{:#x}", req.signer), "pending"]
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_estimateGas",
                "params": [serde_json::Value::Object(tx_obj)]
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "eth_getBlockByNumber",
                "params": ["latest", false]
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "eth_getBalance",
                "params": [format!("{:#x}", req.signer), "latest"]
            }),
        ];

        if let Some(ref envelope) = req.op_l1_fee_tx_envelope {
            let l1_calldata = presign::encode_get_l1_fee_call(envelope);
            batch_calls.push(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "eth_call",
                "params": [
                    {
                        "to": format!("{:#x}", OP_STACK_GAS_PRICE_ORACLE),
                        "data": format!("0x{}", hex::encode(l1_calldata))
                    },
                    "latest"
                ]
            }));
        }

        let payload = serde_json::Value::Array(batch_calls);
        let items = match self.execute_batch("eth_batch_pre_sign", &payload, true) {
            Ok(items) => items,
            Err(BatchAttemptError::BatchRejected(_)) => {
                return self.fallback_presign_sequential(req);
            }
            Err(
                BatchAttemptError::InvalidResponse(err)
                | BatchAttemptError::ExecutionError(err)
                | BatchAttemptError::Transport(err),
            ) => {
                return Err(err);
            }
        };

        let pending_nonce = helper::parse_u64_hex(
            items[0].hex_result("eth_getTransactionCount")?,
            "eth_getTransactionCount",
        )?;
        let estimated_gas =
            helper::parse_u64_hex(items[1].hex_result("eth_estimateGas")?, "eth_estimateGas")?;
        let block_dto: dto::BlockDto = serde::Deserialize::deserialize(
            items[2].result.as_ref().unwrap_or(&serde_json::Value::Null),
        )
        .map_err(|_| RpcError::InvalidDomainValue {
            field: "eth_getBlockByNumber",
            detail: format!("invalid block header for batch id {}", items[2].id),
        })?;
        let balance =
            helper::parse_u256_hex(items[3].hex_result("eth_getBalance")?, "eth_getBalance")?;
        let latest_header: RpcBlockData = block_dto.try_into()?;
        let l1_fee_wei = match items.get(4) {
            Some(item) => {
                let raw_bytes = helper::parse_bytes_hex(
                    item.hex_result("eth_call")?,
                    "GasPriceOracle.getL1Fee",
                )?;
                presign::decode_get_l1_fee_return(&raw_bytes)?
            }
            None => U256::ZERO,
        };

        Ok(PreSignReadSnapshot::new(
            pending_nonce,
            estimated_gas,
            latest_header,
            l1_fee_wei,
            balance,
        ))
    }

    fn fallback_presign_sequential(
        &self,
        req: &PreSignReadRequest,
    ) -> Result<PreSignReadSnapshot, RpcError> {
        let pending_nonce = self.get_pending_nonce(req.signer)?;
        let estimated_gas =
            self.estimate_gas(req.signer, Some(req.to), &req.calldata, req.value)?;
        let signer_balance_wei = self.get_latest_balance(req.signer)?;
        let l1_fee_wei = match req.op_l1_fee_tx_envelope.as_deref() {
            Some(envelope) => self.get_op_l1_fee(envelope)?,
            None => U256::ZERO,
        };
        let latest_header = self.get_latest_block_header()?;
        Ok(PreSignReadSnapshot::new(
            pending_nonce,
            estimated_gas,
            latest_header,
            l1_fee_wei,
            signer_balance_wei,
        ))
    }
}
