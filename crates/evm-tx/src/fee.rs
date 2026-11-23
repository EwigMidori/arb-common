use crate::TxError;
use alloy_primitives::U256;
use derive_builder::Builder;

/// Native-token budget after the caller converts its pricing units into wei.
#[non_exhaustive]
#[derive(Debug, Builder)]
#[builder(pattern = "owned")]
pub struct FeeBudget {
    budget_wei: U256,
    gas_limit: u64,
    base_fee_wei: u128,
    l1_fee_wei: U256,
}

impl FeeBudget {
    pub fn new(budget_wei: U256, gas_limit: u64, base_fee_wei: u128, l1_fee_wei: U256) -> Self {
        Self {
            budget_wei,
            gas_limit,
            base_fee_wei,
            l1_fee_wei,
        }
    }

    /// Reserves twice the base fee plus the L1 fee before flooring the priority bid.
    pub fn clamp(&self, proposed: u128) -> Result<u128, TxError> {
        if self.gas_limit == 0 {
            return Err(TxError::DivisionByZero {
                operation: "fee cap gas limit",
            });
        }
        let reserved = U256::from(self.base_fee_wei)
            .checked_mul(U256::from(2_u8))
            .and_then(|value| value.checked_mul(U256::from(self.gas_limit)))
            .and_then(|value| value.checked_add(self.l1_fee_wei))
            .ok_or(TxError::MathOverflow {
                operation: "fee cap base reserve",
            })?;
        let available = self.budget_wei.saturating_sub(reserved) / U256::from(self.gas_limit);
        u128::try_from(available.min(U256::from(proposed))).map_err(|_| TxError::MathOverflow {
            operation: "fee cap priority conversion",
        })
    }
}
