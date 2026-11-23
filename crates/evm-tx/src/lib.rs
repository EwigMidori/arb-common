//! Pure transaction encoding, signing, sender recovery, and fee arithmetic.

#![forbid(unsafe_code)]

mod error;
mod fee;
mod signer;
pub use error::TxError;
pub use fee::{FeeBudget, FeeBudgetBuilder};
pub use signer::{
    AccessListEntry, Eip1559Signer, Eip1559TxRequest, Eip1559TxRequestBuilder, SignedEip1559Tx,
};
