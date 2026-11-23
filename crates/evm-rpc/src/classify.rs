/// Stable diagnostic categories for provider-controlled text that may echo credentials.
pub(crate) fn upstream_summary(code: i64, message: &str) -> &'static str {
    if is_unauthorized(code, message) {
        "unauthorized"
    } else if is_quota_exhausted_rpc_error(message) {
        "quota exceeded"
    } else if is_range_limit_error(code, message) && !is_block_span_limit_error(code, message) {
        "max results exceeded"
    } else if is_range_limit_error(code, message) {
        "block range exceeds the maximum"
    } else if is_rate_limit_rpc_error(code, message) {
        "rate limit exceeded"
    } else if is_restricted_rpc_error(code, message) {
        "access denied"
    } else if is_deterministic_evm_error(code, message) {
        let lower = message.to_ascii_lowercase();
        if lower.contains("insufficient funds") {
            "insufficient funds"
        } else if lower.contains("out of gas") {
            "out of gas"
        } else {
            "execution reverted"
        }
    } else {
        "upstream RPC error"
    }
}

pub(crate) fn is_unauthorized(code: i64, message: &str) -> bool {
    if !matches!(code, -32_000 | -32_600) {
        return false;
    }
    let lower = message.to_ascii_lowercase();
    [
        "unauthorized",
        "authenticate your request",
        "api key",
        "forbidden",
    ]
    .iter()
    .any(|phrase| lower.contains(phrase))
}

pub fn is_quota_exhausted_rpc_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    [
        "payment required",
        "quota exceeded",
        "daily request count exceeded",
        "insufficient credits",
    ]
    .iter()
    .any(|text| lower.contains(text))
}

pub fn is_restricted_rpc_error(code: i64, message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    if [
        "revert",
        "vm exception",
        "insufficient funds",
        "gas required exceeds",
        "nonce too low",
    ]
    .iter()
    .any(|text| lower.contains(text))
    {
        return false;
    }
    let explicit = [
        "archive",
        "historical state",
        "not available on your",
        "upgrade your plan",
        "payment required",
        "quota exceeded",
        "daily request count exceeded",
        "insufficient credits",
        "unauthorized",
        "forbidden",
        "access denied",
        "missing trie node",
    ]
    .iter()
    .any(|text| lower.contains(text));
    if ["block range", "max block", "query returned more than"]
        .iter()
        .any(|text| lower.contains(text))
        && !explicit
    {
        return false;
    }
    code == -32_002 || explicit
}

pub fn is_rate_limit_rpc_error(code: i64, message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    if code == 429
        || lower.contains("rate limit")
        || lower.contains("rate-limit")
        || lower.contains("too many requests")
        || lower.contains("429")
    {
        return true;
    }
    if lower.contains("block range")
        || lower.contains("range too large")
        || lower.contains("too many blocks")
        || lower.contains("maximum block")
        || lower.contains("exceeds the maximum")
        || lower.contains("max results")
        || lower.contains("query returned more than")
    {
        return false;
    }
    if code == -32_005 || code == -32_016 {
        return true;
    }
    lower.contains("rate limit")
        || lower.contains("rate-limit")
        || lower.contains("too many requests")
        || lower.contains("429")
}

pub fn is_deterministic_evm_error(code: i64, message: &str) -> bool {
    if code == 3 {
        return true;
    }
    let lower = message.to_ascii_lowercase();
    lower.contains("revert") || lower.contains("insufficient funds") || lower.contains("out of gas")
}

pub fn is_range_limit_error(code: i64, message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    if code == 429
        || lower.contains("rate limit")
        || lower.contains("rate-limit")
        || lower.contains("too many requests")
        || lower.contains("429")
    {
        return false;
    }
    lower.contains("block range")
        || lower.contains("range too large")
        || lower.contains("too many blocks")
        || lower.contains("maximum block")
        || (lower.contains("exceeds the maximum")
            && (lower.contains("block") || lower.contains("range")))
        || lower.contains("max results")
        || lower.contains("query returned more than")
}

pub(crate) fn is_block_span_limit_error(code: i64, message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    is_range_limit_error(code, message)
        && (lower.contains("block range")
            || lower.contains("range too large")
            || lower.contains("too many blocks")
            || lower.contains("maximum block")
            || (lower.contains("exceeds the maximum") && lower.contains("block")))
}

#[cfg(test)]
mod tests {
    use super::is_range_limit_error;

    #[test]
    fn upstream_errors_keep_only_the_diagnostic_category() {
        for (code, message, expected) in [
            (-32000, "UNAUTHORIZED", "unauthorized"),
            (-32600, "authenticate your request", "unauthorized"),
            (-32600, "invalid API key", "unauthorized"),
            (-32000, "API key required", "unauthorized"),
            (-32000, "Forbidden", "unauthorized"),
            (-32005, "API key throttled", "rate limit exceeded"),
            (-32000, "insufficient credits", "quota exceeded"),
            (-32005, "range too large", "block range exceeds the maximum"),
            (429, "too many requests", "rate limit exceeded"),
            (-32002, "forbidden", "access denied"),
            (3, "VM Exception", "execution reverted"),
            (-32000, "insufficient funds", "insufficient funds"),
            (-32000, "out of gas", "out of gas"),
            (-32603, "internal error", "upstream RPC error"),
        ] {
            let echoed = format!("{message} at /private?key=secret");
            assert_eq!(super::upstream_summary(code, &echoed), expected);
        }
    }

    #[test]
    fn rate_limit_does_not_trigger_range_retries() {
        assert!(!is_range_limit_error(-32_005, "rate limit exceeded"));
        assert!(!is_range_limit_error(
            429,
            "block range exceeds the maximum"
        ));
        assert!(!is_range_limit_error(-32_005, "limit exceeded"));
        assert!(!is_range_limit_error(-32_602, "address count exceeds the maximum"));
        assert!(is_range_limit_error(
            -32_005,
            "block range exceeds the maximum"
        ));
    }
}
