use alloy_primitives::{Address, B256, Bytes};
use evm_rpc::{
    RpcError,
    http::{BatchAttemptError, HttpRpcClient},
    presign::PreSignReadRequestBuilder,
};
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::TcpListener,
    sync::Arc,
};

use evm_rpc::profile::RpcProfile;

// Serve exactly one response. An accidental sequential fallback gets a connection error.
fn with_response<T>(response: Value, call: impl FnOnce(HttpRpcClient) -> T) -> io::Result<T> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let endpoint = format!(
        "http://{}/path-marker?key=query-marker",
        listener.local_addr()?
    );
    std::thread::scope(|scope| {
        let server = scope.spawn(move || -> io::Result<()> {
            let (mut stream, _) = listener.accept()?;
            drop(listener);
            let mut reader = BufReader::new(&stream);
            let mut length = 0;
            for line in reader.by_ref().lines() {
                let line = line?;
                if line.is_empty() { break; }
                if let Some((key, value)) = line.split_once(':')
                    && key.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().map_err(io::Error::other)?;
                }
            }
            reader.read_exact(&mut vec![0; length])?;
            let body = response.to_string();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
        });
        let result = call(HttpRpcClient::new_live(vec![endpoint]));
        server
            .join()
            .map_err(|_| io::Error::other("HTTP fixture thread failed"))??;
        Ok(result)
    })
}

fn row(id: u64, value: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": value})
}

#[test]
fn profile_records_individual_batch_results_and_redacts_inputs() -> io::Result<()> {
    let profile = Arc::new(RpcProfile::new(10, 60, 2));
    let request = json!([
        {"id":1,"method":"eth_call","params":[{"to":"0x1234567890abcdef1234567890abcdef12345678","data":"0xabcdef01deadbeef"}]},
        {"id":2,"method":"eth_getLogs","params":[{"address":["secret-a","secret-b"],"fromBlock":"0x10","toBlock":"0x19"}]}
    ]);
    let response = json!([
        {"id":2,"result":[1,2]},
        {"id":1,"error":{"code":-32000,"message":"failure"}}
    ]);
    with_response(response, |rpc| {
        rpc.shared_telemetry()
            .attach_profile(Arc::clone(&profile), "base");
        assert!(rpc.execute_batch("batch", &request, false).is_err());
    })?;
    let snap = profile.snapshot();
    assert_eq!(snap.total, 2);
    assert_eq!(snap.batch_sizes[&2], 1);
    let call = snap
        .entries
        .iter()
        .find(|entry| entry.key.method == "eth_call")
        .ok_or_else(|| io::Error::other("missing eth_call"))?;
    assert_eq!(
        call.key.target,
        "0x1234567890abcdef1234567890abcdef12345678"
    );
    assert_eq!(call.key.selector, "0xabcdef01");
    assert_eq!(call.counters.errors, 1);
    let logs = snap
        .entries
        .iter()
        .find(|entry| entry.key.method == "eth_getLogs")
        .ok_or_else(|| io::Error::other("missing eth_getLogs"))?;
    assert_eq!(logs.counters.errors, 0);
    assert_eq!(snap.log_filter_shapes[0].result_count, "1-10");
    let json = serde_json::to_string(&snap)?;
    for secret in [
        "secret-a",
        "secret-b",
        "deadbeef",
        "query-marker",
        "path-marker",
    ] {
        assert!(!json.contains(secret));
    }
    Ok(())
}

#[test]
fn profile_records_single_requests() -> io::Result<()> {
    let profile = Arc::new(RpcProfile::new(10, 60, 0));
    with_response(json!({"id":1,"result":[1,2]}), |rpc| {
        rpc.shared_telemetry()
            .attach_profile(Arc::clone(&profile), "base");
        rpc.call_single::<Value>(
            "eth_getLogs",
            json!([{"address":"0x123", "fromBlock":"0x1", "toBlock":"0x1"}]),
        )
    })?
    .map_err(io::Error::other)?;
    let snap = profile.snapshot();
    assert_eq!(snap.total, 1);
    assert_eq!(snap.log_filter_shapes[0].result_count, "1-10");
    Ok(())
}

#[test]
fn invalid_batches_report_the_provider_method_and_ids() -> io::Result<()> {
    let request = json!([{"method":"eth_getLogs","id":7},{"method":"eth_getLogs","id":2}]);
    for (response, reason) in [
        (json!([row(7, "a"), row(7, "b")]), "duplicate response id 7"),
        (json!([row(7, "a")]), "missing response id 2"),
        (json!([row(7, "a"), row(9, "b")]), "unknown response id 9"),
        (
            json!([row(7, "a"), row(2, "b"), row(7, "c")]),
            "received 3 responses for 2 requests",
        ),
        (
            json!([row(7,"a"),{"result":[]}]),
            "missing or invalid response id",
        ),
        (row(7, "a"), "non-array"),
    ] {
        let result = with_response(response, |rpc| rpc.execute_batch("logs", &request, false))?;
        let Err(BatchAttemptError::InvalidResponse(RpcError::RpcProtocol {
            endpoint,
            message,
            ..
        })) = result
        else {
            return Err(io::Error::other(format!(
                "expected protocol error, got {result:?}"
            )));
        };
        assert!(endpoint.starts_with("127.0.0.1:"));
        assert!(
            message.contains("eth_getLogs id 7") && message.contains(reason),
            "{message}"
        );
    }
    Ok(())
}

#[test]
fn shuffled_and_single_item_batches_follow_request_order() -> io::Result<()> {
    for ids in [vec![8, 3, 5], vec![8]] {
        let request: Value = ids
            .iter()
            .map(|id| json!({"id":id,"method":"eth_getLogs"}))
            .collect();
        let response: Value = ids
            .iter()
            .rev()
            .map(|&id| row(id, &id.to_string()))
            .collect();
        let items = with_response(response, |rpc| rpc.execute_batch("logs", &request, false))?
            .map_err(|err| io::Error::other(format!("{err:?}")))?;
        assert_eq!(items.len(), ids.len());
        for (item, id) in items.iter().zip(ids) {
            assert_eq!((item.id, &item.result), (id, &Some(json!(id.to_string()))));
        }
    }
    Ok(())
}

#[test]
fn duplicate_nonce_or_balance_rejects_presign_snapshot() -> io::Result<()> {
    let request = PreSignReadRequestBuilder::default()
        .signer(Address::ZERO)
        .to(Address::ZERO)
        .calldata(Bytes::new())
        .build()
        .map_err(io::Error::other)?;
    for (id, method) in [(1, "eth_getTransactionCount"), (5, "eth_getBalance")] {
        let response = json!([
            row(1, "0x1"),
            row(2, "0x2"),
            json!({"id":3, "result":{"number":"0x1","hash":B256::repeat_byte(1),
                "timestamp":"0x1","gasLimit":"0xf000","baseFeePerGas":"0x1",
                "miner":Address::ZERO,"transactions":[]}}),
            row(5, "0x5"),
            row(id, "0x99")
        ]);
        let err = with_response(response, |rpc| rpc.read_pre_sign_snapshot(&request))?
            .err()
            .ok_or_else(|| io::Error::other("accepted ambiguous snapshot"))?;
        assert!(matches!(err, RpcError::RpcProtocol { .. }));
        assert!(err.to_string().contains(&format!("{method} id {id}")));
    }
    Ok(())
}

#[test]
fn duplicate_balance_rejects_historical_account() -> io::Result<()> {
    let response = json!([row(1, "0x1"), row(2, "0x2"), row(3, "0x"), row(1, "0x99")]);
    let err = with_response(response, |rpc| {
        rpc.get_account_basic_batch(Address::ZERO, 1)
    })?
    .err()
    .ok_or_else(|| io::Error::other("accepted ambiguous account"))?;
    assert!(matches!(err, RpcError::RpcProtocol { .. }));
    assert!(err.to_string().contains("duplicate response id 1"));
    Ok(())
}

#[test]
fn malformed_second_log_rejects_whole_log_read() -> io::Result<()> {
    let log = json!({"address":Address::ZERO,"topics":[],"data":"0x","blockNumber":"0x1",
        "transactionHash":B256::ZERO,"transactionIndex":"0x0","logIndex":"0x0"});
    let mut invalid = log.clone();
    invalid["logIndex"] = json!("xyz");
    let response = json!({"jsonrpc":"2.0","id":1,"result":[log,invalid]});
    let err = with_response(response, |rpc| {
        rpc.get_logs_by_block_hash(B256::repeat_byte(1), &[])
    })?
    .err()
    .ok_or_else(|| io::Error::other("accepted invalid second log"))?;
    assert!(err.to_string().contains("eth_getLogs.logIndex"));
    Ok(())
}

#[test]
fn errors_expose_only_the_provider_host_and_safe_diagnostics() -> io::Result<()> {
    let echo = "https://user-marker:pass-marker@provider.example/path-marker?key=query-marker";
    let failure =
        json!({"id":1,"error":{"code":3,"message":format!("execution reverted at {echo}")}});
    let request = json!([{"method":"eth_call","id":1}]);
    for response in [failure.clone(), json!([failure]), json!([{"id":echo}])] {
        let result = with_response(response, |rpc| {
            rpc.execute_batch("eth_call", &request, true)
        })?;
        let err = match result {
            Err(BatchAttemptError::InvalidResponse(e) | BatchAttemptError::ExecutionError(e)) => e,
            other => {
                return Err(io::Error::other(format!(
                    "unexpected batch outcome {other:?}"
                )));
            }
        };
        assert_safe_error(err);
    }
    for response in [
        json!({"error":{"code":3,"message":echo}}),
        json!({"result":echo}),
        json!({"error":echo}),
    ] {
        let Err(err) = with_response(response, |rpc| {
            rpc.call_single::<u64>("eth_call", json!([]))
        })?
        else {
            return Err(io::Error::other("accepted malformed or rejected call"));
        };
        assert_safe_error(err);
    }
    Ok(())
}

fn assert_safe_error(error: RpcError) {
    let mut report = format!("{error} {error:?}");
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        report.push_str(&format!(" {cause} {cause:?}"));
        source = cause.source();
    }
    assert!(report.contains("127.0.0.1:"));
    for marker in ["user-marker", "pass-marker", "path-marker", "query-marker"] {
        assert!(!report.contains(marker));
    }
}

#[test]
fn method_counts_match_single_and_batch_attempts() -> io::Result<()> {
    for (method, bucket) in [("eth_call", "eth_call"), ("private_name", "other")] {
        let snap = with_response(row(1, "ok"), |rpc| {
            assert!(rpc.call_single::<String>(method, json!([])).is_ok());
            rpc.rpc_telemetry_snapshot()
        })?;
        assert_eq!(snap.rpc_methods[bucket], 1);
        assert_eq!(
            snap.rpc_methods.values().sum::<u64>(),
            snap.providers[0].req_count
        );
    }

    let methods = ["eth_call", "eth_getLogs", "eth_blockNumber"];
    let calls: Vec<_> = methods
        .iter()
        .enumerate()
        .map(|(id, method)| json!({"id": id, "method": method}))
        .collect();
    let snap = with_response(json!([row(0, "a"), row(1, "b"), row(2, "c")]), |rpc| {
        assert!(rpc.execute_batch("batch", &json!(calls), false).is_ok());
        rpc.rpc_telemetry_snapshot()
    })?;
    for method in methods {
        assert_eq!(snap.rpc_methods[method], 1);
    }
    assert_eq!(snap.providers[0].req_count, 1);
    assert_eq!(snap.rpc_methods.values().sum::<u64>(), 3);
    Ok(())
}
