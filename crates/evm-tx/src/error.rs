/// Failures in pure transaction encoding, signing, and fee calculations.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum TxError {
    #[error("cryptographic operation `{context}` failed: {detail}")]
    Crypto {
        context: &'static str,
        detail: String,
    },
    #[error("arithmetic overflow during {operation}")]
    MathOverflow { operation: &'static str },
    #[error("division by zero during {operation}")]
    DivisionByZero { operation: &'static str },
}
