use serde::Deserialize;

use crate::models::{RpcBlockData, RpcLogData, RpcReceiptData, RpcTransactionData};
use crate::{RpcError, parse as helper};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BlockDto {
    pub number: String,
    pub hash: String,
    pub timestamp: String,
    #[serde(default, deserialize_with = "crate::deserialize_optional_quantity")]
    pub timestamp_ms: Option<u64>,
    pub gas_limit: String,
    pub base_fee_per_gas: Option<String>,
    pub miner: String,
    pub mix_hash: Option<String>,
    pub excess_blob_gas: Option<String>,
    pub transactions: Vec<serde_json::Value>,
}

#[cfg(test)]
mod block_timestamp_tests {
    use super::BlockDto;
    use crate::models::RpcBlockData;

    #[test]
    fn timestamp_ms_accepts_hex_number_and_legacy_missing() -> Result<(), Box<dyn std::error::Error>>
    {
        let block = serde_json::json!({"number":"0x1", "hash":format!("{:#x}", alloy_primitives::B256::ZERO), "timestamp":"0x1", "gasLimit":"0x1", "miner":format!("{:#x}", alloy_primitives::Address::ZERO), "transactions":[]});
        for (value, expected) in [
            (serde_json::json!("0x123"), Some(291)),
            (serde_json::json!("0x0"), Some(0)),
            (serde_json::json!(291), Some(291)),
            (serde_json::Value::Null, None),
        ] {
            let mut input = block.clone();
            input["timestampMs"] = value;
            let dto: BlockDto = serde_json::from_value(input)?;
            let header: RpcBlockData = dto.try_into()?;
            assert_eq!(header.timestamp_ms, expected);
        }
        let mut empty_quantity = block.clone();
        empty_quantity["timestampMs"] = serde_json::json!("0x");
        assert!(serde_json::from_value::<BlockDto>(empty_quantity).is_err());
        let dto: BlockDto = serde_json::from_value(block)?;
        assert_eq!(RpcBlockData::try_from(dto)?.timestamp_ms, None);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TxDto {
    pub hash: String,
    pub transaction_index: String,
    pub from: String,
    pub to: Option<String>,
    pub value: String,
    pub input: String,
    pub gas: String,
    pub gas_price: Option<String>,
    pub max_priority_fee_per_gas: Option<String>,
    pub max_fee_per_gas: Option<String>,
    #[serde(rename = "type")]
    pub transaction_type: Option<String>,
    pub access_list: Option<Vec<crate::models::RpcAccessListItem>>,
    pub nonce: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReceiptDto {
    pub block_hash: String,
    pub transaction_hash: String,
    pub block_number: String,
    pub transaction_index: String,
    pub status: Option<String>,
    pub gas_used: String,
    pub effective_gas_price: Option<String>,
    #[serde(default)]
    pub l1_fee: Option<String>,
    pub logs: Vec<LogDto>,
}

impl ReceiptDto {
    pub(super) fn for_transaction(
        self,
        expected: alloy_primitives::B256,
    ) -> Result<RpcReceiptData, RpcError> {
        let receipt = RpcReceiptData::try_from(self)?;
        if receipt.transaction_hash != expected {
            return Err(RpcError::InvalidDomainValue {
                field: "receipt.transactionHash",
                detail: "does not match requested transaction".to_owned(),
            });
        }
        Ok(receipt)
    }

    fn valid_quantity(value: &str) -> bool {
        value.strip_prefix("0x").is_some_and(|digits| {
            !digits.is_empty() && digits.bytes().all(|digit| digit.is_ascii_hexdigit())
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct LogDto {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub log_index: String,
}

impl TryFrom<BlockDto> for RpcBlockData {
    type Error = RpcError;

    fn try_from(dto: BlockDto) -> Result<Self, Self::Error> {
        let mut transactions = Vec::with_capacity(dto.transactions.len());
        for item in dto.transactions {
            if item.is_object() {
                let tx: TxDto =
                    serde_json::from_value(item).map_err(|_| RpcError::RpcProtocol {
                        endpoint: "rpc".to_owned(),
                        method: "eth_getBlockByNumber".to_owned(),
                        code: -32_000,
                        message: "invalid transaction object".to_owned(),
                    })?;
                transactions.push(tx.try_into()?);
            }
        }
        Ok(Self {
            number: helper::parse_u64_hex(&dto.number, "block.number")?,
            hash: helper::parse_b256_hex(&dto.hash, "block.hash")?,
            timestamp: helper::parse_u64_hex(&dto.timestamp, "block.timestamp")?,
            timestamp_ms: dto.timestamp_ms,
            gas_limit: helper::parse_u64_hex(&dto.gas_limit, "block.gasLimit")?,
            base_fee_per_gas: match dto.base_fee_per_gas {
                Some(ref v) => helper::parse_u64_hex(v, "block.baseFeePerGas")?,
                None => 0,
            },
            beneficiary: helper::parse_eth_address(&dto.miner)?,
            prevrandao: match dto.mix_hash {
                Some(ref h) => Some(helper::parse_b256_hex(h, "block.mixHash")?),
                None => None,
            },
            excess_blob_gas: dto
                .excess_blob_gas
                .as_deref()
                .map(|value| helper::parse_u64_hex(value, "block.excessBlobGas"))
                .transpose()?,
            transactions,
        })
    }
}

impl TryFrom<TxDto> for RpcTransactionData {
    type Error = RpcError;

    fn try_from(dto: TxDto) -> Result<Self, Self::Error> {
        let to = match dto.to {
            Some(ref addr) if !addr.trim().is_empty() => Some(helper::parse_eth_address(addr)?),
            _ => None,
        };
        let gas_price = match dto.gas_price {
            Some(ref gp) => helper::parse_u128_hex(gp, "tx.gasPrice")?,
            None => 0,
        };
        let max_priority_fee_per_gas = match dto.max_priority_fee_per_gas {
            Some(ref pf) => Some(helper::parse_u128_hex(pf, "tx.maxPriorityFeePerGas")?),
            None => None,
        };
        let eip1559_max_fee =
            if dto.transaction_type.as_deref() == Some("0x2") && dto.access_list.is_some() {
                dto.max_fee_per_gas
                    .as_deref()
                    .map(|value| helper::parse_u128_hex(value, "tx.maxFeePerGas"))
                    .transpose()?
            } else {
                None
            };
        Ok(Self {
            hash: helper::parse_b256_hex(&dto.hash, "tx.hash")?,
            transaction_index: helper::parse_u64_hex(
                &dto.transaction_index,
                "tx.transactionIndex",
            )?,
            from: helper::parse_eth_address(&dto.from)?,
            to,
            value: helper::parse_u256_hex(&dto.value, "tx.value")?,
            input: helper::parse_bytes_hex(&dto.input, "tx.input")?,
            gas: helper::parse_u64_hex(&dto.gas, "tx.gas")?,
            gas_price,
            max_priority_fee_per_gas,
            eip1559_max_fee,
            access_list: dto.access_list.unwrap_or_default(),
            nonce: helper::parse_u64_hex(&dto.nonce, "tx.nonce")?,
        })
    }
}

impl TryFrom<ReceiptDto> for RpcReceiptData {
    type Error = RpcError;

    fn try_from(dto: ReceiptDto) -> Result<Self, Self::Error> {
        let mut logs = Vec::with_capacity(dto.logs.len());
        for log in dto.logs {
            let mut topics = Vec::with_capacity(log.topics.len());
            for t in &log.topics {
                topics.push(helper::parse_b256_hex(t, "receipt.log.topic")?);
            }
            logs.push(RpcLogData {
                address: helper::parse_eth_address(&log.address)?,
                topics,
                data: helper::parse_bytes_hex(&log.data, "receipt.log.data")?,
                log_index: helper::parse_u64_hex(&log.log_index, "receipt.log.logIndex")?,
            });
        }
        let status = match dto.status.as_deref() {
            Some("0x0") => false,
            Some("0x1") => true,
            _ => {
                return Err(RpcError::InvalidDomainValue {
                    field: "receipt.status",
                    detail: "expected 0x0 or 0x1".to_owned(),
                });
            }
        };
        let effective_gas_price = match dto.effective_gas_price.as_deref() {
            Some(value) if ReceiptDto::valid_quantity(value) => {
                helper::parse_u128_hex(value, "receipt.effectiveGasPrice")?
            }
            _ => {
                return Err(RpcError::InvalidDomainValue {
                    field: "receipt.effectiveGasPrice",
                    detail: "expected a hexadecimal quantity".to_owned(),
                });
            }
        };
        let gas_used = if ReceiptDto::valid_quantity(&dto.gas_used) {
            helper::parse_u64_hex(&dto.gas_used, "receipt.gasUsed")?
        } else {
            return Err(RpcError::InvalidDomainValue {
                field: "receipt.gasUsed",
                detail: "expected a hexadecimal quantity".to_owned(),
            });
        };
        let l1_fee_wei = match dto.l1_fee.as_deref() {
            Some(fee) if ReceiptDto::valid_quantity(fee) => {
                Some(helper::parse_u128_hex(fee, "receipt.l1Fee")?)
            }
            Some(_) => {
                return Err(RpcError::InvalidDomainValue {
                    field: "receipt.l1Fee",
                    detail: "expected a hexadecimal quantity".to_owned(),
                });
            }
            None => None,
        };
        Ok(Self {
            transaction_hash: helper::parse_b256_hex(
                &dto.transaction_hash,
                "receipt.transactionHash",
            )?,
            block_hash: helper::parse_b256_hex(&dto.block_hash, "receipt.blockHash")?,
            block_number: helper::parse_u64_hex(&dto.block_number, "receipt.blockNumber")?,
            transaction_index: helper::parse_u64_hex(
                &dto.transaction_index,
                "receipt.transactionIndex",
            )?,
            status,
            gas_used,
            effective_gas_price,
            l1_fee_wei,
            logs,
        })
    }
}

#[cfg(test)]
mod receipt_tests {
    use super::ReceiptDto;
    use crate::{RpcError, models::RpcReceiptData};
    use serde_json::{Value, json};

    fn receipt() -> Value {
        json!({
            "blockHash": format!("{:#x}", alloy_primitives::B256::ZERO),
            "transactionHash": format!("{:#x}", alloy_primitives::B256::ZERO),
            "blockNumber": "0x1",
            "transactionIndex": "0x0",
            "status": "0x1",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x3b9aca00",
            "logs": []
        })
    }

    fn decode(value: Value) -> Result<RpcReceiptData, Box<dyn std::error::Error>> {
        let dto: ReceiptDto = serde_json::from_value(value)?;
        Ok(dto.try_into()?)
    }

    #[test]
    fn receipt_retains_success_revert_and_actual_gas_price()
    -> Result<(), Box<dyn std::error::Error>> {
        for (status, expected) in [("0x0", false), ("0x1", true)] {
            let mut input = receipt();
            input["status"] = json!(status);
            let parsed = decode(input)?;
            assert_eq!(parsed.status, expected);
            assert_eq!(parsed.gas_used, 21_000);
            assert_eq!(parsed.effective_gas_price, 1_000_000_000);
        }
        Ok(())
    }

    #[test]
    fn receipt_requires_a_block_hash() -> Result<(), Box<dyn std::error::Error>> {
        let mut input = receipt();
        input
            .as_object_mut()
            .ok_or("expected object")?
            .remove("blockHash");
        assert!(decode(input.clone()).is_err());
        input["blockHash"] = json!("0x01");
        assert!(decode(input.clone()).is_err());
        let hash = alloy_primitives::B256::repeat_byte(42);
        input["blockHash"] = json!(hash);
        assert_eq!(decode(input)?.block_hash, hash);
        Ok(())
    }

    #[test]
    fn incomplete_or_invalid_status_cannot_finalize_a_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        for status in [
            Value::Null,
            json!(""),
            json!("0x"),
            json!("0x2"),
            json!("garbage"),
        ] {
            let mut input = receipt();
            input["status"] = status;
            let dto: ReceiptDto = serde_json::from_value(input)?;
            assert!(matches!(
                RpcReceiptData::try_from(dto),
                Err(RpcError::InvalidDomainValue {
                    field: "receipt.status",
                    ..
                })
            ));
        }
        let mut input = receipt();
        input
            .as_object_mut()
            .ok_or("expected object")?
            .remove("status");
        assert!(decode(input).is_err());
        Ok(())
    }

    #[test]
    fn incomplete_or_invalid_gas_price_cannot_finalize_a_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        for price in [
            Value::Null,
            json!(""),
            json!("0x"),
            json!("0xzz"),
            json!("0x0x1"),
            json!("123"),
        ] {
            let mut input = receipt();
            input["effectiveGasPrice"] = price;
            let dto: ReceiptDto = serde_json::from_value(input)?;
            assert!(matches!(
                RpcReceiptData::try_from(dto),
                Err(RpcError::InvalidDomainValue {
                    field: "receipt.effectiveGasPrice",
                    ..
                })
            ));
        }
        let mut input = receipt();
        input
            .as_object_mut()
            .ok_or("expected object")?
            .remove("effectiveGasPrice");
        assert!(decode(input).is_err());
        let mut zero_price = receipt();
        zero_price["effectiveGasPrice"] = json!("0x0");
        assert_eq!(decode(zero_price)?.effective_gas_price, 0);
        Ok(())
    }

    #[test]
    fn invalid_gas_used_cannot_finalize_a_receipt() -> Result<(), Box<dyn std::error::Error>> {
        for gas in ["", "0x", "0xzz", "0x0x1", "21000"] {
            let mut input = receipt();
            input["gasUsed"] = json!(gas);
            let dto: ReceiptDto = serde_json::from_value(input)?;
            assert!(matches!(
                RpcReceiptData::try_from(dto),
                Err(RpcError::InvalidDomainValue {
                    field: "receipt.gasUsed",
                    ..
                })
            ));
        }
        Ok(())
    }

    #[test]
    fn malformed_l1_fee_cannot_finalize_a_receipt() -> Result<(), Box<dyn std::error::Error>> {
        for fee in ["", "0x", "0x0x1", "0xzz"] {
            let mut input = receipt();
            input["l1Fee"] = json!(fee);
            let dto: ReceiptDto = serde_json::from_value(input)?;
            assert!(matches!(
                RpcReceiptData::try_from(dto),
                Err(RpcError::InvalidDomainValue {
                    field: "receipt.l1Fee",
                    ..
                })
            ));
        }
        Ok(())
    }
}
