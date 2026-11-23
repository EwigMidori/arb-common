use crate::RpcError;
use alloy_primitives::{Address, B256, Bytes, U256};

fn quantity_digits<'a>(raw: &'a str, field: &'static str) -> Result<&'a str, RpcError> {
    let stripped = raw.trim().trim_start_matches("0x");
    if stripped.is_empty() {
        return Err(RpcError::InvalidDomainValue {
            field,
            detail: "empty quantity".to_owned(),
        });
    }
    Ok(stripped)
}

pub fn parse_u64_hex(raw: &str, field: &'static str) -> Result<u64, RpcError> {
    let stripped = quantity_digits(raw, field)?;
    u64::from_str_radix(stripped, 16).map_err(|_| RpcError::InvalidDomainValue {
        field,
        detail: "invalid hex u64".to_owned(),
    })
}

pub fn parse_u128_hex(raw: &str, field: &'static str) -> Result<u128, RpcError> {
    let stripped = quantity_digits(raw, field)?;
    u128::from_str_radix(stripped, 16).map_err(|_| RpcError::InvalidDomainValue {
        field,
        detail: "invalid hex u128".to_owned(),
    })
}

pub fn parse_u256_hex(raw: &str, field: &'static str) -> Result<U256, RpcError> {
    let stripped = quantity_digits(raw, field)?;
    U256::from_str_radix(stripped, 16).map_err(|_| RpcError::InvalidDomainValue {
        field,
        detail: "invalid hex U256".to_owned(),
    })
}

pub fn parse_b256_hex(raw: &str, field: &'static str) -> Result<B256, RpcError> {
    raw.trim()
        .parse::<B256>()
        .map_err(|_| RpcError::InvalidDomainValue {
            field,
            detail: "invalid hex B256".to_owned(),
        })
}

pub fn parse_bytes_hex(raw: &str, field: &'static str) -> Result<Bytes, RpcError> {
    let stripped = raw.trim().trim_start_matches("0x");
    if stripped.is_empty() {
        return Ok(Bytes::new());
    }
    let decoded = hex::decode(stripped).map_err(|_| RpcError::InvalidDomainValue {
        field,
        detail: "invalid hex bytes".to_owned(),
    })?;
    Ok(Bytes::from(decoded))
}
pub fn parse_eth_address(input: &str) -> Result<Address, RpcError> {
    input
        .trim()
        .parse::<Address>()
        .map_err(|_| RpcError::InvalidDomainValue {
            field: "ethereum_address",
            detail: "invalid address".to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_quantities_report_the_field() {
        for raw in ["", "0x", " 0x "] {
            for error in [
                parse_u64_hex(raw, "nonce").err(),
                parse_u128_hex(raw, "nonce").err(),
                parse_u256_hex(raw, "nonce").err(),
            ] {
                assert!(matches!(
                    error,
                    Some(RpcError::InvalidDomainValue { field: "nonce", detail })
                        if detail == "empty quantity"
                ));
            }
        }
    }

    #[test]
    fn quantities_accept_zero_and_surrounding_whitespace() -> Result<(), RpcError> {
        for (raw, expected) in [("0x0", 0), ("0x00", 0), (" 0x0 ", 0), (" 0x1a ", 26)] {
            assert_eq!(parse_u64_hex(raw, "value")?, expected);
            assert_eq!(parse_u128_hex(raw, "value")?, u128::from(expected));
            assert_eq!(parse_u256_hex(raw, "value")?, U256::from(expected));
        }
        Ok(())
    }

    #[test]
    fn empty_calldata_is_valid() -> Result<(), RpcError> {
        assert_eq!(parse_bytes_hex("0x", "tx.input")?, Bytes::new());
        Ok(())
    }
}
