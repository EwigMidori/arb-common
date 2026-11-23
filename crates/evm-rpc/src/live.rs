use crate::{
    RpcError, dto,
    http::HttpRpcClient,
    models::*,
    parse::{self as helper, parse_eth_address},
    presign::{self, *},
};
use alloy_primitives::{Address, B256, Bytes, U256};
use serde::Deserialize;
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawLogDto {
    block_hash: Option<B256>,
    address: String,
    topics: Vec<String>,
    data: String,
    block_number: String,
    transaction_hash: String,
    transaction_index: String,
    log_index: String,
}

impl TryFrom<RawLogDto> for RpcRawLog {
    type Error = RpcError;

    fn try_from(dto: RawLogDto) -> Result<Self, Self::Error> {
        let mut topics = Vec::with_capacity(dto.topics.len());
        for topic in &dto.topics {
            topics.push(helper::parse_b256_hex(topic, "eth_getLogs.topic")?);
        }
        Ok(Self {
            block_hash: dto.block_hash,
            address: parse_eth_address(&dto.address)?,
            topics,
            data: helper::parse_bytes_hex(&dto.data, "eth_getLogs.data")?,
            block_number: helper::parse_u64_hex(&dto.block_number, "eth_getLogs.blockNumber")?,
            transaction_hash: helper::parse_b256_hex(
                &dto.transaction_hash,
                "eth_getLogs.transactionHash",
            )?,
            transaction_index: helper::parse_u64_hex(
                &dto.transaction_index,
                "eth_getLogs.transactionIndex",
            )?,
            log_index: helper::parse_u64_hex(&dto.log_index, "eth_getLogs.logIndex")?,
        })
    }
}

impl HttpRpcClient {
    pub fn chain_id(&self) -> Result<u64, RpcError> {
        let hex_val: String = self.call_single("eth_chainId", serde_json::json!([]))?;
        helper::parse_u64_hex(&hex_val, "eth_chainId")
    }

    pub fn latest_block_number(&self) -> Result<u64, RpcError> {
        let hex_val: String = self.call_single("eth_blockNumber", serde_json::json!([]))?;
        helper::parse_u64_hex(&hex_val, "eth_blockNumber")
    }

    pub fn get_latest_block_header(&self) -> Result<RpcBlockData, RpcError> {
        let params = serde_json::json!(["latest", false]);
        let dto: dto::BlockDto = self.call_single("eth_getBlockByNumber", params)?;
        dto.try_into()
    }

    pub fn get_latest_balance(&self, address: Address) -> Result<U256, RpcError> {
        let params = serde_json::json!([format!("{address:#x}"), "latest"]);
        let hex_val: String = self.call_single("eth_getBalance", params)?;
        helper::parse_u256_hex(&hex_val, "eth_getBalance")
    }

    pub fn get_pending_nonce(&self, address: Address) -> Result<u64, RpcError> {
        let params = serde_json::json!([format!("{address:#x}"), "pending"]);
        let hex_val: String = self.call_single("eth_getTransactionCount", params)?;
        helper::parse_u64_hex(&hex_val, "eth_getTransactionCount")
    }

    pub fn gas_price(&self) -> Result<u128, RpcError> {
        let hex_val: String = self.call_single("eth_gasPrice", serde_json::json!([]))?;
        helper::parse_u128_hex(&hex_val, "eth_gasPrice")
    }

    pub fn max_priority_fee_per_gas(&self) -> Result<u128, RpcError> {
        let hex_val: String =
            self.call_single("eth_maxPriorityFeePerGas", serde_json::json!([]))?;
        helper::parse_u128_hex(&hex_val, "eth_maxPriorityFeePerGas")
    }

    pub fn estimate_gas(
        &self,
        from: Address,
        to: Option<Address>,
        data: &[u8],
        value: U256,
    ) -> Result<u64, RpcError> {
        let mut tx_obj = serde_json::Map::new();
        tx_obj.insert("from".to_owned(), serde_json::json!(format!("{from:#x}")));
        if let Some(target) = to {
            tx_obj.insert("to".to_owned(), serde_json::json!(format!("{target:#x}")));
        }
        tx_obj.insert(
            "data".to_owned(),
            serde_json::json!(format!("0x{}", hex::encode(data))),
        );
        if !value.is_zero() {
            tx_obj.insert("value".to_owned(), serde_json::json!(format!("{value:#x}")));
        }

        let hex_val: String = self.call_single(
            "eth_estimateGas",
            serde_json::Value::Array(vec![tx_obj.into()]),
        )?;
        helper::parse_u64_hex(&hex_val, "eth_estimateGas")
    }

    pub fn eth_call_at_block(
        &self,
        to: Address,
        calldata: &[u8],
        block_number: u64,
    ) -> Result<Bytes, RpcError> {
        self.eth_call(to, calldata, block_number)
    }

    pub fn eth_call_latest(&self, to: Address, calldata: &[u8]) -> Result<Bytes, RpcError> {
        let params = serde_json::json!([
            { "to": format!("{to:#x}"), "data": format!("0x{}", hex::encode(calldata)) },
            "latest"
        ]);
        let hex_val: String = self.call_single("eth_call", params)?;
        helper::parse_bytes_hex(&hex_val, "eth_call")
    }

    pub fn get_latest_code(&self, address: Address) -> Result<Bytes, RpcError> {
        let params = serde_json::json!([format!("{address:#x}"), "latest"]);
        let hex_val: String = self.call_single("eth_getCode", params)?;
        helper::parse_bytes_hex(&hex_val, "eth_getCode")
    }

    pub fn send_raw_transaction(&self, raw_tx: &[u8]) -> Result<B256, RpcError> {
        let params = serde_json::json!([format!("0x{}", hex::encode(raw_tx))]);
        let hex_val: String = self.call_single("eth_sendRawTransaction", params)?;
        helper::parse_b256_hex(&hex_val, "eth_sendRawTransaction")
    }

    pub fn try_get_transaction_receipt(
        &self,
        tx_hash: B256,
    ) -> Result<Option<RpcReceiptData>, RpcError> {
        let params = serde_json::json!([format!("{tx_hash:#x}")]);
        let maybe_dto: Option<dto::ReceiptDto> =
            self.call_single_nullable("eth_getTransactionReceipt", params)?;
        maybe_dto
            .map(|dto| dto.for_transaction(tx_hash))
            .transpose()
    }

    pub fn try_get_transaction_by_hash(
        &self,
        tx_hash: B256,
    ) -> Result<Option<RpcTransactionData>, RpcError> {
        let params = serde_json::json!([format!("{tx_hash:#x}")]);
        let maybe_dto: Option<dto::TxDto> =
            self.call_single_nullable("eth_getTransactionByHash", params)?;
        maybe_dto.map(TryInto::try_into).transpose()
    }

    pub fn read_pre_sign_snapshot(
        &self,
        req: &PreSignReadRequest,
    ) -> Result<PreSignReadSnapshot, RpcError> {
        self.execute_presign_batch(req)
    }

    pub fn get_logs(&self, filter: &RpcLogFilter) -> Result<Vec<RpcRawLog>, RpcError> {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "fromBlock".to_owned(),
            serde_json::json!(format!("0x{:x}", filter.from_block)),
        );
        obj.insert(
            "toBlock".to_owned(),
            serde_json::json!(format!("0x{:x}", filter.to_block)),
        );
        if !filter.addresses.is_empty() {
            let addrs: Vec<String> = filter.addresses.iter().map(|a| format!("{a:#x}")).collect();
            obj.insert("address".to_owned(), serde_json::json!(addrs));
        }
        if !filter.topic0.is_empty() {
            let topics: Vec<String> = filter.topic0.iter().map(|t| format!("{t:#x}")).collect();
            if filter.topic1.is_empty() {
                obj.insert("topics".to_owned(), serde_json::json!([topics]));
            } else {
                obj.insert(
                    "topics".to_owned(),
                    serde_json::json!([topics, filter.topic1]),
                );
            }
        }

        let raw_logs: Vec<RawLogDto> =
            self.call_single("eth_getLogs", serde_json::Value::Array(vec![obj.into()]))?;
        raw_logs.into_iter().map(TryInto::try_into).collect()
    }

    pub fn get_finalized_block_header(&self) -> Result<Option<RpcBlockData>, RpcError> {
        for tag in ["finalized", "safe"] {
            let params = serde_json::json!([tag, false]);
            match self.call_single_nullable::<dto::BlockDto>("eth_getBlockByNumber", params) {
                Ok(Some(dto)) => return dto.try_into().map(Some),
                Ok(None) | Err(RpcError::RpcProtocol { .. }) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(None)
    }

    pub fn is_block_executed(
        &self,
        chain: u64,
        block: &RpcBlockData,
        probe_address: Address,
    ) -> Result<bool, RpcError> {
        if chain != 43114 || block.hash.is_zero() {
            return Ok(true);
        }

        let params = serde_json::json!([{
            "blockHash": format!("{:#x}", block.hash),
            "address": format!("{probe_address:#x}"),
        }]);
        match self.call_single_nullable::<Vec<RawLogDto>>("eth_getLogs", params) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(RpcError::RpcProtocol {
                endpoint,
                method,
                code,
                message,
            }) => {
                let lower = message.to_ascii_lowercase();
                if lower.contains("unknown block")
                    || lower.contains("not executed")
                    || lower.contains("header not found")
                    || lower.contains("block not found")
                    || lower.contains("beyond")
                    || lower.contains("unexecuted")
                {
                    Ok(false)
                } else {
                    Err(RpcError::RpcProtocol {
                        endpoint,
                        method,
                        code,
                        message,
                    })
                }
            }
            Err(err) => Err(err),
        }
    }

    pub fn get_logs_by_block_hash(
        &self,
        block_hash: B256,
        addresses: &[Address],
    ) -> Result<Option<Vec<RpcRawLog>>, RpcError> {
        if block_hash.is_zero() {
            return Ok(None);
        }

        let mut obj = serde_json::Map::new();
        obj.insert(
            "blockHash".to_owned(),
            serde_json::json!(format!("{block_hash:#x}")),
        );
        if !addresses.is_empty() {
            let addrs: Vec<String> = addresses.iter().map(|a| format!("{a:#x}")).collect();
            obj.insert("address".to_owned(), serde_json::json!(addrs));
        }

        let raw_logs: Vec<RawLogDto> =
            self.call_single("eth_getLogs", serde_json::Value::Array(vec![obj.into()]))?;
        let parsed: Result<Vec<RpcRawLog>, RpcError> =
            raw_logs.into_iter().map(TryInto::try_into).collect();
        parsed.map(Some)
    }
    pub fn get_op_l1_fee(&self, tx_envelope: &[u8]) -> Result<U256, RpcError> {
        let calldata = presign::encode_get_l1_fee_call(tx_envelope);
        let raw = self.eth_call_latest(OP_STACK_GAS_PRICE_ORACLE, &calldata)?;
        presign::decode_get_l1_fee_return(&raw)
    }
    /// Fetches block header and optionally full transactions (`eth_getBlockByNumber`).
    pub fn get_block_by_number(
        &self,
        block_number: u64,
        full_txs: bool,
    ) -> Result<RpcBlockData, RpcError> {
        let params = serde_json::json!([format!("0x{block_number:x}"), full_txs]);
        let dto: dto::BlockDto = self.call_single("eth_getBlockByNumber", params)?;
        dto.try_into()
    }

    /// Fetches a transaction receipt (`eth_getTransactionReceipt`).
    pub fn get_transaction_receipt(&self, tx_hash: &str) -> Result<RpcReceiptData, RpcError> {
        let tx_hash = helper::parse_b256_hex(tx_hash, "receipt_transaction_hash")?;
        let params = serde_json::json!([tx_hash]);
        let dto: dto::ReceiptDto = self.call_single("eth_getTransactionReceipt", params)?;
        dto.for_transaction(tx_hash)
    }
}
