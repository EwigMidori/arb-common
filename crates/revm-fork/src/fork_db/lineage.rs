use super::*;
use alloy_primitives::U64;
use evm_rpc::http::BatchAttemptError;
use serde_json::{Value, json};

pub(super) fn invalid(detail: impl Into<String>) -> ForkDbError {
    ForkDbError::InvalidDomainValue {
        field: "fork_lineage",
        detail: detail.into(),
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct Header {
    number: U64,
    pub hash: B256,
    #[serde(rename = "parentHash")]
    parent: B256,
}

impl Header {
    pub(super) fn height(self) -> u64 {
        self.number.to()
    }

    pub fn canonical(rpc: &HttpRpcClient, height: u64) -> Result<Self, ForkDbError> {
        let h: Self = rpc.call_single(
            "eth_getBlockByNumber",
            json!([format!("0x{height:x}"), false]),
        )?;
        if h.height() != height {
            return Err(invalid(format!(
                "expected height {height}, received {}",
                h.height()
            )));
        }
        Ok(h)
    }

    fn parent(self, rpc: &HttpRpcClient, canonical: bool) -> Result<Self, ForkDbError> {
        let height = self
            .height()
            .checked_sub(1)
            .ok_or_else(|| invalid("no common ancestor at genesis"))?;
        let parent: Self = if canonical {
            Self::canonical(rpc, height)?
        } else {
            rpc.call_single("eth_getBlockByHash", json!([self.parent, false]))?
        };
        if parent.height() != height || parent.hash != self.parent {
            return Err(invalid(format!(
                "invalid parent of {} at {height}",
                self.hash
            )));
        }
        Ok(parent)
    }

    pub fn ancestor(self, rpc: &HttpRpcClient, mut old: Self) -> Result<u64, ForkDbError> {
        let mut new = self;
        loop {
            if old == new {
                return Ok(old.height());
            }
            let old_height = old.height();
            let new_height = new.height();
            if new_height.checked_sub(1) == Some(old_height) && new.parent == old.hash {
                return Ok(old_height);
            }
            if old_height == new_height && old.parent == new.parent {
                return Ok(new.parent(rpc, true)?.height());
            }
            if old_height >= new_height {
                old = old.parent(rpc, false)?;
            }
            if new_height >= old_height {
                new = new.parent(rpc, true)?;
            }
        }
    }

    pub fn historical_hash(self, rpc: &HttpRpcClient, height: u64) -> Result<B256, ForkDbError> {
        if height > self.height() {
            return Err(invalid("block above pinned fork"));
        }
        let mut cursor = self;
        while cursor.height() > height {
            if cursor.height() - height == 1 {
                return Ok(cursor.parent);
            }
            cursor = cursor.parent(rpc, false)?;
        }
        Ok(cursor.hash)
    }

    pub fn selector(self) -> Value {
        json!({"blockHash":self.hash,"requireCanonical":true})
    }

    pub fn account(
        self,
        rpc: &HttpRpcClient,
        address: Address,
    ) -> Result<(U256, u64, Bytes), ForkDbError> {
        rpc.require_online("eth_getBalance")?;
        let params = json!([address, self.selector()]);
        let methods = ["eth_getBalance", "eth_getTransactionCount", "eth_getCode"];
        let payload: Vec<_> = methods
            .iter()
            .enumerate()
            .map(|(i, method)| json!({"jsonrpc":"2.0","id":i+1,"method":method,"params":params}))
            .collect();
        let results: Vec<String> =
            match rpc.execute_batch("eth_batch_account_basic", &json!(payload), false) {
                Ok(items) => items
                    .into_iter()
                    .map(|item| {
                        item.result
                            .and_then(|v| v.as_str().map(str::to_owned))
                            .ok_or_else(|| invalid("missing account batch result"))
                    })
                    .collect::<Result<_, _>>()?,
                Err(BatchAttemptError::BatchRejected(_)) => methods
                    .iter()
                    .map(|method| rpc.call_single(method, params.clone()))
                    .collect::<Result<_, _>>()?,
                Err(
                    BatchAttemptError::Transport(e)
                    | BatchAttemptError::InvalidResponse(e)
                    | BatchAttemptError::ExecutionError(e),
                ) => return Err(e.into()),
            };
        let [balance, nonce, code] = results.as_slice() else {
            return Err(invalid("incomplete account batch"));
        };
        Ok((
            rpc_helper::parse_u256_hex(balance, methods[0])?,
            rpc_helper::parse_u64_hex(nonce, methods[1])?,
            rpc_helper::parse_bytes_hex(code, methods[2])?,
        ))
    }
}
