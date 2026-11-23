//! Pure EIP-1559 signing, encoding, and sender recovery.
use crate::TxError;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, keccak256};
use alloy_rlp::{Encodable, Header};
use derive_builder::Builder;
use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
use std::fmt;
/// EIP-2930 / EIP-1559 access list entry `[address, [storage_keys...]]`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessListEntry {
    pub address: Address,
    pub storage_keys: Vec<B256>,
}

impl AccessListEntry {
    /// Creates an access list entry for `address` and `storage_keys`.
    pub fn new(address: Address, storage_keys: Vec<B256>) -> Self {
        Self {
            address,
            storage_keys,
        }
    }
}

/// Unsigned EIP-1559 Type-2 (`0x02`) transaction request.
#[non_exhaustive]
#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned", setter(into))]
pub struct Eip1559TxRequest {
    pub chain_id: u64,
    pub nonce: u64,
    pub max_priority_fee_per_gas: u128,
    pub max_fee_per_gas: u128,
    pub gas_limit: u64,
    pub to: TxKind,
    #[builder(default = "U256::ZERO")]
    pub value: U256,
    #[builder(default)]
    pub data: Bytes,
    #[builder(default)]
    pub access_list: Vec<AccessListEntry>,
}

/// Signed EIP-1559 Type-2 (`0x02`) transaction envelope ready for `eth_sendRawTransaction`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedEip1559Tx {
    pub tx_hash: B256,
    pub signing_hash: B256,
    pub raw_tx: Bytes,
    pub from: Address,
    pub y_parity: u8,
    pub r: U256,
    pub s: U256,
}

/// Owns a signing key supplied explicitly by the composition layer.
#[non_exhaustive]
pub struct Eip1559Signer {
    signing_key: SigningKey,
    address: Address,
}
impl fmt::Debug for Eip1559Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Eip1559Signer")
            .field("address", &self.address_checksum())
            .field("secret", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}
impl Eip1559Signer {
    /// Validates the scalar and derives the EOA without reading external state.
    pub fn from_secret_bytes(secret: &[u8]) -> Result<Self, TxError> {
        let signing_key = SigningKey::from_slice(secret).map_err(|error| TxError::Crypto {
            context: "private_key_scalar",
            detail: error.to_string(),
        })?;
        let address = helper::address_from_verifying_key(signing_key.verifying_key())?;
        Ok(Self {
            signing_key,
            address,
        })
    }
    /// Returns the derived EOA address.
    pub const fn address(&self) -> Address {
        self.address
    }

    /// Returns the EIP-55 checksummed EOA address string.
    pub fn address_checksum(&self) -> String {
        self.address.to_checksum(None)
    }

    /// Computes the deterministic `CREATE` contract address `keccak256(rlp([deployer, nonce]))[12..32]`.
    pub fn compute_create_address(deployer: Address, nonce: u64) -> Address {
        let payload_len = deployer.length() + nonce.length();
        let mut encoded = Vec::with_capacity(payload_len + 3);
        Header {
            list: true,
            payload_length: payload_len,
        }
        .encode(&mut encoded);
        deployer.encode(&mut encoded);
        nonce.encode(&mut encoded);
        let hash = keccak256(&encoded);
        Address::from_slice(&hash[12..32])
    }

    /// Computes the EIP-1559 unsigned signing digest `keccak256(0x02 || rlp([chain_id, nonce, ...]))`.
    pub fn signing_hash(tx: &Eip1559TxRequest) -> B256 {
        let payload_len = helper::unsigned_payload_length(tx);
        let mut buf = Vec::with_capacity(1 + payload_len + 4);
        buf.push(0x02);
        Header {
            list: true,
            payload_length: payload_len,
        }
        .encode(&mut buf);
        helper::encode_unsigned_fields(tx, &mut buf);
        keccak256(&buf)
    }

    /// Encodes a Type-2 (`0x02`) EIP-1559 RLP envelope with deterministic full-entropy placeholder
    /// signature scalars so OP-Stack `GasPriceOracle.getL1Fee(bytes)` can be queried before signing.
    pub fn encode_l1_fee_envelope(tx: &Eip1559TxRequest) -> Bytes {
        let y_parity: u8 = 1;
        let r = U256::from_be_bytes(*keccak256(b"arb_engine_op_l1_fee_placeholder_r"));
        let s = U256::from_be_bytes(*keccak256(b"arb_engine_op_l1_fee_placeholder_s"));

        let list_payload_len =
            helper::unsigned_payload_length(tx) + y_parity.length() + r.length() + s.length();
        let mut raw = Vec::with_capacity(1 + list_payload_len + 4);
        raw.push(0x02);
        Header {
            list: true,
            payload_length: list_payload_len,
        }
        .encode(&mut raw);
        helper::encode_unsigned_fields(tx, &mut raw);
        y_parity.encode(&mut raw);
        r.encode(&mut raw);
        s.encode(&mut raw);

        Bytes::from(raw)
    }

    /// Signs `tx` using deterministic RFC 6979 ECDSA, enforces EIP-2 low-S, and encodes `0x02 || rlp(...)`.
    pub fn sign_eip1559(&self, tx: &Eip1559TxRequest) -> Result<SignedEip1559Tx, TxError> {
        let signing_hash = Self::signing_hash(tx);
        let (mut sig, rec_id) = self
            .signing_key
            .sign_prehash_recoverable(signing_hash.as_slice())
            .map_err(|err| TxError::Crypto {
                context: "sign_prehash_recoverable",
                detail: err.to_string(),
            })?;

        let mut y_parity = u8::from(rec_id.is_y_odd());
        if let Some(normalized) = sig.normalize_s() {
            sig = normalized;
            y_parity ^= 1;
        }

        let r = U256::from_be_slice(&sig.r().to_bytes());
        let s = U256::from_be_slice(&sig.s().to_bytes());

        let recovered = Self::recover_sender(signing_hash, y_parity, r, s)?;
        if recovered != self.address {
            return Err(TxError::Crypto {
                context: "sign_eip1559_self_verify",
                detail: format!(
                    "recovered signer {recovered} did not match expected EOA {}",
                    self.address
                ),
            });
        }

        let list_payload_len =
            helper::unsigned_payload_length(tx) + y_parity.length() + r.length() + s.length();
        let mut raw = Vec::with_capacity(1 + list_payload_len + 4);
        raw.push(0x02);
        Header {
            list: true,
            payload_length: list_payload_len,
        }
        .encode(&mut raw);
        helper::encode_unsigned_fields(tx, &mut raw);
        y_parity.encode(&mut raw);
        r.encode(&mut raw);
        s.encode(&mut raw);

        let tx_hash = keccak256(&raw);
        Ok(SignedEip1559Tx {
            tx_hash,
            signing_hash,
            raw_tx: Bytes::from(raw),
            from: self.address,
            y_parity,
            r,
            s,
        })
    }

    /// Recovers the EOA sender address from an EIP-1559 signature, enforcing EIP-2 low-S.
    pub fn recover_sender(
        signing_hash: B256,
        y_parity: u8,
        r: U256,
        s: U256,
    ) -> Result<Address, TxError> {
        if y_parity > 1 {
            return Err(TxError::Crypto {
                context: "recover_sender",
                detail: format!("invalid y_parity {y_parity}; expected 0 or 1"),
            });
        }
        let mut sig_bytes = [0_u8; 64];
        sig_bytes[..32].copy_from_slice(&r.to_be_bytes::<32>());
        sig_bytes[32..].copy_from_slice(&s.to_be_bytes::<32>());

        let sig = Signature::from_slice(&sig_bytes).map_err(|err| TxError::Crypto {
            context: "signature_from_slice",
            detail: err.to_string(),
        })?;
        if sig.normalize_s().is_some() {
            return Err(TxError::Crypto {
                context: "eip2_low_s",
                detail: "signature failed EIP-2 low-S normalization check".to_owned(),
            });
        }

        let rec_id = RecoveryId::try_from(y_parity).map_err(|err| TxError::Crypto {
            context: "recovery_id",
            detail: err.to_string(),
        })?;
        let verifying_key =
            VerifyingKey::recover_from_prehash(signing_hash.as_slice(), &sig, rec_id).map_err(
                |err| TxError::Crypto {
                    context: "recover_from_prehash",
                    detail: err.to_string(),
                },
            )?;
        helper::address_from_verifying_key(&verifying_key)
    }
}

mod helper {
    use super::*;
    pub(super) fn address_from_verifying_key(
        verifying_key: &VerifyingKey,
    ) -> Result<Address, TxError> {
        let point = verifying_key.to_encoded_point(false);
        let bytes = point.as_bytes();
        if bytes.len() != 65 || bytes[0] != 0x04 {
            return Err(TxError::Crypto {
                context: "uncompressed_pubkey",
                detail: format!(
                    "expected 65-byte 0x04-prefixed uncompressed pubkey, got {} bytes",
                    bytes.len()
                ),
            });
        }
        let hash = keccak256(&bytes[1..65]);
        Ok(Address::from_slice(&hash[12..32]))
    }

    pub(super) fn unsigned_payload_length(tx: &Eip1559TxRequest) -> usize {
        let to_len = match tx.to {
            TxKind::Call(addr) => addr.length(),
            TxKind::Create => b"".as_slice().length(),
        };
        let access_list_payload_len = access_list_payload_length(&tx.access_list);
        let access_list_len = Header {
            list: true,
            payload_length: access_list_payload_len,
        }
        .length()
            + access_list_payload_len;

        tx.chain_id.length()
            + tx.nonce.length()
            + tx.max_priority_fee_per_gas.length()
            + tx.max_fee_per_gas.length()
            + tx.gas_limit.length()
            + to_len
            + tx.value.length()
            + tx.data.as_ref().length()
            + access_list_len
    }

    pub(super) fn encode_unsigned_fields(tx: &Eip1559TxRequest, out: &mut Vec<u8>) {
        tx.chain_id.encode(out);
        tx.nonce.encode(out);
        tx.max_priority_fee_per_gas.encode(out);
        tx.max_fee_per_gas.encode(out);
        tx.gas_limit.encode(out);
        match tx.to {
            TxKind::Call(addr) => addr.encode(out),
            TxKind::Create => b"".as_slice().encode(out),
        }
        tx.value.encode(out);
        tx.data.as_ref().encode(out);
        encode_access_list(&tx.access_list, out);
    }

    fn access_list_payload_length(entries: &[AccessListEntry]) -> usize {
        let mut total = 0_usize;
        for entry in entries {
            let keys_payload: usize = entry.storage_keys.iter().map(Encodable::length).sum();
            let keys_header = Header {
                list: true,
                payload_length: keys_payload,
            }
            .length();
            let item_payload = entry.address.length() + keys_header + keys_payload;
            let item_header = Header {
                list: true,
                payload_length: item_payload,
            }
            .length();
            total += item_header + item_payload;
        }
        total
    }

    fn encode_access_list(entries: &[AccessListEntry], out: &mut Vec<u8>) {
        let payload_length = access_list_payload_length(entries);
        Header {
            list: true,
            payload_length,
        }
        .encode(out);

        for entry in entries {
            let keys_payload: usize = entry.storage_keys.iter().map(Encodable::length).sum();
            let keys_header = Header {
                list: true,
                payload_length: keys_payload,
            };
            let item_payload = entry.address.length() + keys_header.length() + keys_payload;
            Header {
                list: true,
                payload_length: item_payload,
            }
            .encode(out);
            entry.address.encode(out);
            keys_header.encode(out);
            for key in &entry.storage_keys {
                key.encode(out);
            }
        }
    }
}
