#![allow(clippy::expect_used, clippy::panic)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use alloy_primitives::address;

use super::*;

fn read_http_request(stream: &mut std::net::TcpStream) -> Result<String, std::io::Error> {
    let mut reader = BufReader::new(stream);
    let mut content_length = 0_usize;
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("content-length:") {
            content_length = rest.trim().parse::<usize>().unwrap_or(0);
        }
    }
    let mut body = vec![0_u8; content_length];
    reader.read_exact(&mut body)?;
    Ok(String::from_utf8_lossy(&body).into_owned())
}

use crate::execution::{ForkExecution, StateChanges, output};
use revm::{
    context::{BlockEnv, CfgEnv, TxEnv},
    database::CacheDB,
};
use serde_json::{Value, json};
use std::sync::atomic::AtomicBool;

const CONTRACT: Address = address!("1000000000000000000000000000000000000001");
type TestResult = Result<(), Box<dyn std::error::Error>>;

struct ChainRpc {
    endpoint: std::net::SocketAddr,
    branch: Arc<AtomicUsize>,
    batches: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

fn hash(id: u64) -> B256 {
    B256::from(U256::from(id))
}

fn block_id(n: u64, branch: usize) -> u64 {
    if branch != 0 && n >= 2 { n + 20 } else { n + 1 }
}

fn response(req: &Value, branch: usize) -> Value {
    let params = &req["params"];
    let result = match req["method"].as_str() {
        Some("eth_getBlockByNumber" | "eth_getBlockByHash") => {
            let raw = params[0].as_str().expect("block selector");
            let value = u64::from_str_radix(raw.trim_start_matches("0x"), 16)
                .expect("block selector quantity");
            let (n, side) = if req["method"] == "eth_getBlockByNumber" {
                (value, branch)
            } else if value >= 22 {
                (value - 20, 1)
            } else {
                (value - 1, 0)
            };
            let parent = if side == 2 && n == 3 {
                99
            } else {
                block_id(n.saturating_sub(1), side)
            };
            json!({"number":format!("0x{n:x}"),"hash":hash(block_id(n,side)),"parentHash":hash(parent), "timestamp":"0x1","gasLimit":"0xffffff","miner":Address::ZERO,"transactions":[]})
        }
        Some("eth_getTransactionReceipt") => json!({
            "transactionHash": if branch == 3 { json!(hash(777)) } else { params[0].clone() },
            "blockHash": hash(if branch == 4 { 999 } else { block_id(2,branch) }),
            "blockNumber":"0x2","transactionIndex":"0x0","status":"0x1",
            "gasUsed":"0x5208","effectiveGasPrice":"0x1","logs":[]
        }),
        Some(method) => {
            let selector = params
                .as_array()
                .and_then(|v| v.last())
                .expect("block selector");
            assert_eq!(selector["requireCanonical"], true);
            let id = selector["blockHash"].as_str().expect("block hash");
            match method {
                "eth_getBalance" => json!("0xde0b6b3a7640000"),
                "eth_getTransactionCount" => json!("0x0"),
                "eth_getCode" if params[0] == json!(CONTRACT) => {
                    json!("0x600054600052600143034060205260406000f3")
                }
                "eth_getCode" => json!("0x"),
                "eth_getStorageAt" | "eth_call" => json!(id),
                _ => panic!("unexpected method {method}"),
            }
        }
        None => panic!("request without method"),
    };
    json!({"jsonrpc":"2.0","id":req["id"],"result":result})
}

impl ChainRpc {
    fn new() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = listener.local_addr()?;
        let branch = Arc::new(AtomicUsize::new(0));
        let batches = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (side, count, done) = (branch.clone(), batches.clone(), stop.clone());
        let handle = thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                if done.load(Ordering::SeqCst) {
                    break;
                }
                let req: Value =
                    serde_json::from_str(&read_http_request(&mut stream).expect("HTTP request"))
                        .expect("RPC request");
                let branch = side.load(Ordering::SeqCst);
                let reply = if let Some(items) = req.as_array() {
                    count.fetch_add(1, Ordering::SeqCst);
                    Value::Array(items.iter().map(|it| response(it, branch)).collect())
                } else {
                    response(&req, branch)
                }
                .to_string();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                )
                .expect("HTTP response");
            }
        });
        Ok(Self {
            endpoint,
            branch,
            batches,
            stop,
            handle: Some(handle),
        })
    }
    fn client(&self) -> HttpRpcClient {
        HttpRpcClient::new_live(vec![format!("http://{}", self.endpoint)])
    }
}
impl Drop for ChainRpc {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(self.endpoint);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[test]
fn same_height_reorg_rebuilds_accounts_storage_and_hashes() -> TestResult {
    let rpc = ChainRpc::new()?;
    let db = ForkDatabase::open_live(1, 2, rpc.client());
    let b20 = db.b20_backing()?;
    let account = db.basic_ref(CONTRACT)?.ok_or("missing test account")?;
    assert!(account.code.is_none());
    assert_eq!(account.nonce, 0);
    assert_eq!(
        db.code_by_hash_ref(account.code_hash)?
            .original_bytes()
            .as_ref(),
        hex::decode("600054600052600143034060205260406000f3")?
    );
    assert_eq!(db.block_hash_ref(2)?, hash(3));
    assert_eq!(db.storage_ref(CONTRACT, U256::ZERO)?, U256::from(3));
    assert_eq!(
        b20.eth_call_cached(CONTRACT, &[])?.as_ref(),
        hash(3).as_slice()
    );
    db.advance_to_block(2)?;
    db.basic_ref(CONTRACT)?;
    assert_eq!(rpc.batches.load(Ordering::SeqCst), 1);
    rpc.branch.store(1, Ordering::SeqCst);
    db.reset(2)?;
    db.basic_ref(CONTRACT)?;
    assert_eq!(rpc.batches.load(Ordering::SeqCst), 2);
    assert_eq!(db.storage_ref(CONTRACT, U256::ZERO)?, U256::from(22));
    assert_eq!(db.block_hash_ref(2)?, hash(22));
    assert_eq!(
        b20.eth_call_cached(CONTRACT, &[])?.as_ref(),
        hash(22).as_slice()
    );
    Ok(())
}

#[test]
fn broken_parent_blocks_reads_until_verified_retry() -> TestResult {
    let rpc = ChainRpc::new()?;
    let db = ForkDatabase::open_live(1, 2, rpc.client());
    db.basic_ref(CONTRACT)?;
    db.block_hash_ref(2)?;
    rpc.branch.store(2, Ordering::SeqCst);
    assert!(db.advance_to_block(3).is_err());
    assert_eq!(db.fork_block(), 2);
    assert!(db.block_hash_ref(2).is_err());
    assert!(db.basic_ref(CONTRACT).is_err());
    rpc.branch.store(1, Ordering::SeqCst);
    db.advance_to_block(3)?;
    assert_eq!(db.block_hash_ref(2)?, hash(22));
    Ok(())
}

fn simulate(db: &ForkDatabase) -> Result<(U256, B256), Box<dyn std::error::Error>> {
    let mut cache = CacheDB::new(db);
    let tx = TxEnv {
        caller: Address::with_last_byte(99),
        kind: alloy_primitives::TxKind::Call(CONTRACT),
        gas_limit: 100_000,
        ..Default::default()
    };
    let block = BlockEnv {
        number: U256::from(db.fork_block() + 1),
        ..Default::default()
    };
    let outcome = ForkExecution::new(CfgEnv::default(), block, None).transact(
        &mut cache,
        tx,
        StateChanges::Discard,
    )?;
    let bytes = output(outcome, "read slot and parent hash")?;
    assert_eq!(bytes.len(), 64);
    Ok((
        U256::from_be_slice(&bytes[..32]),
        B256::from_slice(&bytes[32..]),
    ))
}

#[test]
fn historical_and_current_forks_execute_with_their_own_state() -> TestResult {
    let rpc = ChainRpc::new()?;
    let db = ForkDatabase::open_live(1, 2, rpc.client());
    assert_eq!(simulate(&db)?, (U256::from(3), hash(3)));
    rpc.branch.store(1, Ordering::SeqCst);
    db.advance_to_block(3)?;
    assert_eq!(simulate(&db)?, (U256::from(23), hash(23)));
    db.reset(1)?;
    assert_eq!(simulate(&db)?, (U256::from(2), hash(2)));
    db.advance_to_block(3)?;
    assert_eq!(simulate(&db)?, (U256::from(23), hash(23)));
    Ok(())
}

#[test]
fn disk_snapshot_reopens_only_for_its_branch_and_height() -> TestResult {
    let rpc = ChainRpc::new()?;
    let dir = std::env::temp_dir().join(format!(
        "fork-reorg-{}-{}",
        std::process::id(),
        rpc.endpoint.port()
    ));
    std::fs::create_dir(&dir)?;
    let dir = TempDir(dir);
    assert_eq!(
        ForkDatabase::load_or_fetch_block(&dir.0, 1, 2, &rpc.client())?.hash,
        hash(3)
    );
    {
        let db = ForkDatabase::open(1, 2, &dir.0, rpc.client())?;
        db.basic_ref(CONTRACT)?;
        db.block_hash_ref(2)?;
    }
    {
        let db = ForkDatabase::open(1, 2, &dir.0, rpc.client())?;
        db.basic_ref(CONTRACT)?;
        assert_eq!(rpc.batches.load(Ordering::SeqCst), 1);
    }
    rpc.branch.store(1, Ordering::SeqCst);
    assert_eq!(
        ForkDatabase::load_or_fetch_block(&dir.0, 1, 2, &rpc.client())?.hash,
        hash(22)
    );
    {
        let db = ForkDatabase::open(1, 2, &dir.0, rpc.client())?;
        db.basic_ref(CONTRACT)?;
        assert_eq!(rpc.batches.load(Ordering::SeqCst), 2);
        assert_eq!(db.block_hash_ref(2)?, hash(22));
        db.advance_to_block(3)?;
        assert_eq!(db.block_hash_ref(3)?, hash(23));
        db.b20_backing()?.eth_call_cached(CONTRACT, &[])?;
    }
    for (height, id) in [(2, 22), (3, 23)] {
        let rpc = HttpRpcClient::offline();
        let db = ForkDatabase::open(1, height, &dir.0, rpc.clone())?;
        assert_eq!(db.block_hash_ref(height)?, hash(id));
        if height == 3 {
            assert_eq!(
                db.b20_backing()?.eth_call_cached(CONTRACT, &[])?.as_ref(),
                hash(id).as_slice()
            );
        }
        rpc.check_cache_coverage()?;
    }
    Ok(())
}

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn receipts_follow_reorgs_and_reject_mismatched_identity() -> TestResult {
    let rpc = ChainRpc::new()?;
    let dir = TempDir(std::env::temp_dir().join(format!(
        "receipt-test-{}-{}",
        std::process::id(),
        rpc.endpoint.port()
    )));
    std::fs::create_dir(&dir.0)?;
    let tx = hash(123);
    let path = dir.0.join(format!("receipt_1_{tx:x}.json"));
    let get = |client: &HttpRpcClient| {
        ForkDatabase::load_or_fetch_receipt(&dir.0, 1, &format!("{tx:#x}"), client)
    };
    let offline = HttpRpcClient::offline();
    let first = get(&rpc.client())?;
    assert_eq!(first.block_hash, hash(3));
    assert!(matches!(
        get(&offline),
        Err(ForkDbError::InvalidDomainValue { .. })
    ));
    ForkDatabase::load_or_fetch_block(&dir.0, 1, 2, &rpc.client())?;
    assert_eq!(get(&offline)?.transaction_hash, tx);

    for omit_hash in [true, false] {
        let mut legacy = serde_json::to_value(&first)?;
        if omit_hash {
            legacy
                .as_object_mut()
                .ok_or("expected receipt object")?
                .remove("block_hash");
        } else {
            legacy["block_hash"] = json!(B256::ZERO);
        }
        std::fs::write(&path, serde_json::to_vec(&legacy)?)?;
        assert!(
            matches!(get(&offline), Err(ForkDbError::InvalidDomainValue { detail, .. })
            if detail == "cached receipt block hash is missing or zero")
        );
        assert_eq!(get(&rpc.client())?.block_hash, first.block_hash);
        assert_eq!(get(&offline)?.block_hash, first.block_hash);
    }

    rpc.branch.store(1, Ordering::SeqCst);
    assert_eq!(get(&rpc.client())?.block_hash, hash(22));
    assert!(matches!(
        get(&offline),
        Err(ForkDbError::InvalidDomainValue { .. })
    ));
    ForkDatabase::load_or_fetch_block(&dir.0, 1, 2, &rpc.client())?;
    let mut cached = get(&offline)?;
    assert_eq!(cached.block_hash, hash(22));

    cached.transaction_hash = hash(456);
    std::fs::write(&path, serde_json::to_vec(&cached)?)?;
    assert!(matches!(
        get(&offline),
        Err(ForkDbError::InvalidDomainValue { .. })
    ));
    assert_eq!(get(&rpc.client())?.transaction_hash, tx);
    std::fs::remove_file(&path)?;
    rpc.branch.store(3, Ordering::SeqCst);
    assert!(matches!(
        rpc.client().try_get_transaction_receipt(tx),
        Err(evm_rpc::RpcError::InvalidDomainValue {
            field: "receipt.transactionHash",
            ..
        })
    ));
    assert!(matches!(
        get(&rpc.client()),
        Err(ForkDbError::Rpc(evm_rpc::RpcError::InvalidDomainValue {
            field: "receipt.transactionHash",
            ..
        }))
    ));
    assert!(!path.exists());
    rpc.branch.store(4, Ordering::SeqCst);
    assert!(get(&rpc.client()).is_err());
    assert!(!path.exists());
    Ok(())
}
