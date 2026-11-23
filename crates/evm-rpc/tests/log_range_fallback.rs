use evm_rpc::http::HttpRpcClient;
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::TcpListener,
};

fn run_server<T>(
    count: usize,
    reply: impl Fn(usize, &Value) -> (u16, Value) + Send,
    call: impl FnOnce(HttpRpcClient) -> T,
) -> io::Result<(T, Vec<Value>)> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    std::thread::scope(|scope| {
        let server = scope.spawn(move || -> io::Result<Vec<Value>> {
            let mut requests = Vec::new();
            for index in 0..count {
                let (mut stream, _) = listener.accept()?;
                let mut reader = BufReader::new(&stream);
                let mut length = 0;
                for line in reader.by_ref().lines() {
                    let line = line?;
                    if line.is_empty() { break; }
                    if let Some((key, value)) = line.split_once(':') && key.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().map_err(io::Error::other)?;
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body)?;
                let request: Value = serde_json::from_slice(&body)?;
                let (status, response) = reply(index, &request);
                requests.push(request);
                let body = response.to_string();
                write!(stream, "HTTP/1.1 {status} test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())?;
            }
            Ok(requests)
        });
        let result = call(HttpRpcClient::new_live(vec![endpoint]));
        Ok((
            result,
            server
                .join()
                .map_err(|_| io::Error::other("fixture failed"))??,
        ))
    })
}

#[test]
fn range_fallback_merges_and_remembers_endpoint_limit() -> io::Result<()> {
    let (result, requests) = run_server(
        21,
        |index, request| {
            if index == 0 {
                (
                    400,
                    json!({"error":{"message":"block range exceeds the maximum"}}),
                )
            } else {
                (
                    200,
                    json!({"jsonrpc":"2.0", "id":1, "result":[request["params"][0]["fromBlock"]]}),
                )
            }
        },
        |rpc| -> Result<_, evm_rpc::RpcError> {
            let first: Vec<String> = rpc.call_single(
                "eth_getLogs",
                json!([{"fromBlock":"0x1", "toBlock":"0x64"}]),
            )?;
            let second: Vec<String> = rpc.call_single(
                "eth_getLogs",
                json!([{"fromBlock":"0x65", "toBlock":"0xc8"}]),
            )?;
            Ok((first, second))
        },
    )?;
    let (first, second) = result.map_err(io::Error::other)?;
    assert_eq!(first.len(), 10);
    assert_eq!(second.len(), 10);
    assert_eq!(first[0], "0x1");
    assert_eq!(first[9], "0x5b");
    for request in &requests[1..] {
        let filter = &request["params"][0];
        let quantity = |key| {
            u64::from_str_radix(
                filter[key].as_str().unwrap_or("").trim_start_matches("0x"),
                16,
            )
        };
        assert_eq!(
            quantity("toBlock").map_err(io::Error::other)?
                - quantity("fromBlock").map_err(io::Error::other)?,
            9
        );
    }
    Ok(())
}

#[test]
fn auth_and_rate_limit_errors_do_not_split() -> io::Result<()> {
    for status in [403, 429] {
        let (result, requests) = run_server(
            1,
            |_, _| (status, json!({})),
            |rpc| {
                let result = rpc.call_single::<Vec<Value>>(
                    "eth_getLogs",
                    json!([{"fromBlock":"0x1", "toBlock":"0x64"}]),
                );
                (result, rpc.rpc_telemetry_snapshot())
            },
        )?;
        assert!(result.0.is_err());
        assert!(result.1.providers[0].log_range_limit.is_none());
        assert_eq!(requests.len(), 1);
    }
    Ok(())
}

#[test]
fn archive_error_with_range_text_does_not_cache() -> io::Result<()> {
    let (result, requests) = run_server(
        1,
        |_, _| {
            (
                200,
                json!({"id":1,"error":{"code":-32002,"message":"archive block range unavailable"}}),
            )
        },
        |rpc| {
            let result = rpc.call_single::<Vec<Value>>(
                "eth_getLogs",
                json!([{"fromBlock":"0x1","toBlock":"0x64"}]),
            );
            (result, rpc.rpc_telemetry_snapshot())
        },
    )?;
    assert!(result.0.is_err());
    assert!(result.1.providers[0].log_range_limit.is_none());
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[test]
fn invalid_params_range_error_splits_and_failed_retry_returns_original() -> io::Result<()> {
    let (result, requests) = run_server(
        2,
        |index, _| {
            if index == 0 {
                (
                    200,
                    json!({"id":1,"error":{"code":-32602,"message":"block range too large"}}),
                )
            } else {
                (429, json!({}))
            }
        },
        |rpc| {
            rpc.call_single::<Vec<Value>>(
                "eth_getLogs",
                json!([{"fromBlock":"0x1", "toBlock":"0x64"}]),
            )
        },
    )?;
    assert!(matches!(
        result,
        Err(evm_rpc::RpcError::RpcProtocol { code: -32602, .. })
    ));
    assert_eq!(requests.len(), 2);
    Ok(())
}

#[test]
fn unrelated_bad_request_falls_back_and_returns_original_error() -> io::Result<()> {
    let (result, requests) = run_server(
        2,
        |_, _| (400, json!({"error":{"message":"invalid address"}})),
        |rpc| {
            rpc.call_single::<Vec<Value>>(
                "eth_getLogs",
                json!([{"fromBlock":"0x1", "toBlock":"0x64"}]),
            )
        },
    )?;
    assert!(result.is_err());
    assert_eq!(requests.len(), 2);
    Ok(())
}

#[test]
fn result_count_limit_triggers_fallback() -> io::Result<()> {
    let (result, requests) = run_server(
        11,
        |index, _| {
            if index == 0 {
                (
                    200,
                    json!({"id":1,"error":{"code":-32602,"message":"max results exceeded"}}),
                )
            } else {
                (200, json!({"id":1,"result":[]}))
            }
        },
        |rpc| {
            let result = rpc.call_single::<Vec<Value>>(
                "eth_getLogs",
                json!([{"fromBlock":"0x1", "toBlock":"0x64"}]),
            );
            (result, rpc.rpc_telemetry_snapshot())
        },
    )?;
    assert!(result.0.is_ok());
    assert_eq!(requests.len(), 11);
    let status = serde_json::to_value(result.1)?;
    assert_eq!(status["providers"][0]["log_range_limit"]["span"], 10);
    assert!(
        (1..=900).contains(
            &status["providers"][0]["log_range_limit"]["expires_in_secs"]
                .as_u64()
                .unwrap_or_default()
        )
    );
    Ok(())
}

#[test]
fn transient_400_only_splits_current_request() -> io::Result<()> {
    let (result, requests) = run_server(
        12,
        |index, _| {
            if index == 0 {
                (400, json!({}))
            } else {
                (200, json!({"id":1,"result":[]}))
            }
        },
        |rpc| -> Result<_, evm_rpc::RpcError> {
            let params = json!([{"fromBlock":"0x1", "toBlock":"0x64"}]);
            rpc.call_single::<Vec<Value>>("eth_getLogs", params.clone())?;
            let status = rpc.rpc_telemetry_snapshot();
            rpc.call_single::<Vec<Value>>("eth_getLogs", params)?;
            Ok(status)
        },
    )?;
    let status = result.map_err(io::Error::other)?;
    assert!(status.providers[0].log_range_limit.is_none());
    assert_eq!(requests[11]["params"][0]["fromBlock"], "0x1");
    assert_eq!(requests[11]["params"][0]["toBlock"], "0x64");
    Ok(())
}

#[test]
fn nullable_log_request_keeps_null_result() -> io::Result<()> {
    let (result, requests) = run_server(
        1,
        |_, _| (200, json!({"id":1,"result":null})),
        |rpc| {
            rpc.call_single_nullable::<Vec<Value>>(
                "eth_getLogs",
                json!([{"fromBlock":"0x1", "toBlock":"0x64"}]),
            )
        },
    )?;
    assert!(result.map_err(io::Error::other)?.is_none());
    assert_eq!(requests.len(), 1);
    Ok(())
}
