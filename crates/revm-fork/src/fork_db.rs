use std::collections::HashMap;
use std::fs;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use flate2::read::GzDecoder;
use revm::bytecode::Bytecode;
use revm::database_interface::{DBErrorMarker, DatabaseRef};
use revm::primitives::KECCAK_EMPTY;
use revm::state::AccountInfo;
use serde::{Deserialize, Serialize};

mod lineage;
use crate::ForkDbError;
use evm_rpc::models::{RpcBlockData, RpcReceiptData};
use evm_rpc::{http::HttpRpcClient, parse as rpc_helper};
use lineage::{Header, invalid};

/// Maximum historical block hashes retained across `advance_to_block` resets.
pub const MAX_CACHED_BLOCK_HASHES: usize = 256;

/// Storage mode distinguishing persistent backtest disk caching from pure in-memory live execution.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkCacheMode {
    /// Persists fetched accounts, slots, and block hashes to `engine/cache/` JSON snapshots.
    PersistentDisk,
    /// Pure in-memory live execution mode with zero disk reads or JSON serialization.
    InMemory,
}

impl DBErrorMarker for ForkDbError {}

/// Persistent disk-backed state cache for reproducible `revm` block fork replays.
#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedForkState {
    chain_id: u64,
    fork_block: u64,
    #[serde(default)]
    header: Option<Header>,
    accounts: HashMap<String, PersistedAccount>,
    storage: HashMap<String, HashMap<String, String>>,
    block_hashes: HashMap<u64, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAccount {
    balance_hex: String,
    nonce: u64,
    code_hex: String,
}

/// `revm::DatabaseRef` supporting persistent backtest snapshots and zero-disk live in-memory mode.
#[non_exhaustive]
#[derive(Debug)]
pub struct ForkDatabase {
    chain: u64,
    fork_block: AtomicU64,
    mode: ForkCacheMode,
    cache_dir: PathBuf,
    rpc: HttpRpcClient,
    state: Arc<RwLock<InMemoryForkCache>>,
}

#[derive(Debug, Default)]
pub(crate) struct InMemoryForkCache {
    head: Option<Header>,
    blocked: Option<String>,
    accounts: HashMap<Address, AccountInfo>,
    contracts: HashMap<B256, Arc<Bytecode>>,
    storage: HashMap<(Address, U256), U256>,
    block_hashes: HashMap<u64, B256>,
    dirty_writes: usize,
}

impl InMemoryForkCache {
    pub(crate) fn identity(&self) -> Result<Option<(u64, B256)>, ForkDbError> {
        if let Some(detail) = &self.blocked {
            return Err(invalid(detail.clone()));
        }
        Ok(self.head.map(|h| (h.height(), h.hash)))
    }
}

impl ForkDatabase {
    /// Loads a block header + transactions from disk cache (`engine/cache/`) or Archive RPC.
    pub fn load_or_fetch_block(
        cache_dir: &Path,
        chain: u64,
        block_number: u64,
        rpc: &HttpRpcClient,
    ) -> Result<RpcBlockData, ForkDbError> {
        fs::create_dir_all(cache_dir).map_err(|source| ForkDbError::CacheIo {
            path: cache_dir.to_path_buf(),
            source,
        })?;
        let path = cache_dir.join(format!("block_{}_num_{block_number}.json", chain));
        let canonical = if rpc.endpoints().is_empty() {
            None
        } else {
            Some(Header::canonical(rpc, block_number)?.hash)
        };
        if let Some(block) = helper::read_cache::<RpcBlockData>(&path)?
            && block.number == block_number
            && canonical.is_none_or(|hash| hash == block.hash)
        {
            return Ok(block);
        }

        let block = rpc.get_block_by_number(block_number, true)?;
        if block.number != block_number || canonical.is_some_and(|hash| hash != block.hash) {
            return Err(invalid("block changed during transaction fetch"));
        }
        let json =
            serde_json::to_string_pretty(&block).map_err(|source| ForkDbError::CacheDecode {
                path: path.clone(),
                source,
            })?;
        fs::write(&path, json).map_err(|source| ForkDbError::CacheIo { path, source })?;
        Ok(block)
    }

    /// Loads a transaction receipt from disk cache (`engine/cache/`) or Archive RPC.
    pub fn load_or_fetch_receipt(
        cache_dir: &Path,
        chain: u64,
        tx_hash: &str,
        rpc: &HttpRpcClient,
    ) -> Result<RpcReceiptData, ForkDbError> {
        let tx_hash =
            tx_hash
                .trim()
                .parse::<B256>()
                .map_err(|_| ForkDbError::InvalidDomainValue {
                    field: "receipt_transaction_hash",
                    detail: "expected a 32-byte transaction hash".to_owned(),
                })?;
        fs::create_dir_all(cache_dir).map_err(|source| ForkDbError::CacheIo {
            path: cache_dir.to_path_buf(),
            source,
        })?;
        let path = cache_dir.join(format!("receipt_{}_{tx_hash:x}.json", chain));
        if let Some(receipt) = helper::read_cache::<RpcReceiptData>(&path)? {
            if rpc.endpoints().is_empty() {
                if receipt.transaction_hash != tx_hash {
                    return Err(invalid("cached receipt transaction hash mismatch"));
                }
                if receipt.block_hash.is_zero() {
                    return Err(invalid("cached receipt block hash is missing or zero"));
                }
                let block_path =
                    cache_dir.join(format!("block_{chain}_num_{}.json", receipt.block_number));
                let block = helper::read_cache::<RpcBlockData>(&block_path)?
                    .ok_or_else(|| invalid("missing cached block for receipt"))?;
                if block.number != receipt.block_number || block.hash != receipt.block_hash {
                    return Err(invalid("cached receipt does not match cached block"));
                }
                return Ok(receipt);
            }
            if receipt.transaction_hash == tx_hash
                && !receipt.block_hash.is_zero()
                && Header::canonical(rpc, receipt.block_number)?.hash == receipt.block_hash
            {
                return Ok(receipt);
            }
        }

        let receipt = rpc.get_transaction_receipt(&format!("{tx_hash:#x}"))?;
        if receipt.block_hash.is_zero()
            || Header::canonical(rpc, receipt.block_number)?.hash != receipt.block_hash
        {
            return Err(invalid("fetched receipt does not match canonical block"));
        }
        let json =
            serde_json::to_string_pretty(&receipt).map_err(|source| ForkDbError::CacheDecode {
                path: path.clone(),
                source,
            })?;
        fs::write(&path, json).map_err(|source| ForkDbError::CacheIo { path, source })?;
        Ok(receipt)
    }

    /// Opens or initializes a disk-cached `ForkDatabase` (`PersistentDisk`) pinned at `fork_block`.
    pub fn open(
        chain: u64,
        fork_block: u64,
        cache_dir: &Path,
        rpc: HttpRpcClient,
    ) -> Result<Self, ForkDbError> {
        fs::create_dir_all(cache_dir).map_err(|source| ForkDbError::CacheIo {
            path: cache_dir.to_path_buf(),
            source,
        })?;

        let cache_path = cache_dir.join(format!("state_{}_block_{fork_block}.json", chain));
        let mut in_mem = InMemoryForkCache::default();
        helper::seed_empty_contracts(&mut in_mem);

        if !rpc.endpoints().is_empty() {
            in_mem.head = Some(Header::canonical(&rpc, fork_block)?);
            in_mem.dirty_writes = 1;
        }
        if let Some(persisted) = helper::read_cache::<PersistedForkState>(&cache_path)? {
            if persisted.chain_id != chain || persisted.fork_block != fork_block {
                return Err(ForkDbError::InvalidDomainValue {
                    field: "fork_state_cache",
                    detail: format!(
                        "{} contains chain {} at block {}, expected chain {} at block {}",
                        cache_path.display(),
                        persisted.chain_id,
                        persisted.fork_block,
                        chain,
                        fork_block
                    ),
                });
            }
            if in_mem.head.is_none_or(|h| Some(h) == persisted.header) {
                if persisted.header.is_some_and(|h| h.height() != fork_block) {
                    return Err(invalid("snapshot header height differs from fork height"));
                }
                in_mem.head = persisted.header;
                in_mem.dirty_writes = 0;
                helper::load_in_memory(&persisted, &mut in_mem)?;
            }
        }

        Ok(Self {
            chain,
            fork_block: AtomicU64::new(fork_block),
            mode: ForkCacheMode::PersistentDisk,
            cache_dir: cache_dir.to_owned(),
            rpc,
            state: Arc::new(RwLock::new(in_mem)),
        })
    }

    /// Initializes a zero-disk `InMemory` `ForkDatabase` for low-latency live simulation.
    pub fn open_live(chain: u64, fork_block: u64, rpc: HttpRpcClient) -> Self {
        let mut in_mem = InMemoryForkCache::default();
        helper::seed_empty_contracts(&mut in_mem);
        if !rpc.endpoints().is_empty() {
            match Header::canonical(&rpc, fork_block) {
                Ok(head) => in_mem.head = Some(head),
                Err(error) => in_mem.blocked = Some(error.to_string()),
            }
        }
        Self {
            chain,
            fork_block: AtomicU64::new(fork_block),
            mode: ForkCacheMode::InMemory,
            cache_dir: PathBuf::new(),
            rpc,
            state: Arc::new(RwLock::new(in_mem)),
        }
    }

    /// Returns the active cache mode (`PersistentDisk` or `InMemory`).
    pub const fn mode(&self) -> ForkCacheMode {
        self.mode
    }

    /// Returns the currently pinned fork block number.
    pub fn fork_block(&self) -> u64 {
        self.fork_block.load(Ordering::Relaxed)
    }

    /// Pre-warms immutable `Arc<Bytecode>` entries (Spoke, Morpho, Pools, Liquidator) into `contracts`.
    pub fn prewarm_contracts(&self, addresses: &[Address]) -> Result<usize, ForkDbError> {
        let mut warmed = 0_usize;
        for &address in addresses {
            if address.is_zero() {
                continue;
            }
            let info = self.basic_ref(address)?;
            if info.is_some_and(|acct| acct.code_hash != KECCAK_EMPTY) {
                warmed = warmed.saturating_add(1);
            }
        }
        Ok(warmed)
    }

    fn locked(&self) -> Result<std::sync::RwLockWriteGuard<'_, InMemoryForkCache>, ForkDbError> {
        self.state
            .write()
            .map_err(|_| invalid("fork state lock poisoned"))
    }

    fn active(&self) -> Result<std::sync::RwLockWriteGuard<'_, InMemoryForkCache>, ForkDbError> {
        let state = self.locked()?;
        state.identity()?;
        Ok(state)
    }

    /// Verifies ancestry before publishing a new fork height.
    pub fn advance_to_block(&self, new_block: u64) -> Result<(), ForkDbError> {
        self.reset(new_block)
    }

    /// Switches to a canonical fork, preserving only history through the common ancestor.
    pub fn reset(&self, new_block: u64) -> Result<(), ForkDbError> {
        let mut state = self.locked()?;
        state.blocked = Some("fork transition failed; reset required".to_owned());
        let old = match state.head {
            Some(h) => h,
            None => Header::canonical(&self.rpc, self.fork_block())?,
        };
        state.head = Some(old);
        let new = Header::canonical(&self.rpc, new_block)?;
        if old != new {
            let ancestor = new.ancestor(&self.rpc, old)?;
            if self.mode == ForkCacheMode::PersistentDisk
                && state.dirty_writes > 0
                && new_block != self.fork_block()
            {
                helper::write_cache_snapshot(
                    self.chain,
                    self.fork_block(),
                    &self.snapshot_path(),
                    &state,
                )?;
            }
            state.block_hashes.retain(|n, _| {
                *n <= ancestor && new_block.saturating_sub(*n) < MAX_CACHED_BLOCK_HASHES as u64
            });
            state.accounts.clear();
            state.storage.clear();
            state.dirty_writes = usize::from(self.mode == ForkCacheMode::PersistentDisk);
        }
        state.head = Some(new);
        self.fork_block.store(new_block, Ordering::Relaxed);
        state.blocked = None;
        Ok(())
    }

    fn snapshot_path(&self) -> PathBuf {
        self.cache_dir.join(format!(
            "state_{}_block_{}.json",
            self.chain,
            self.fork_block()
        ))
    }

    /// Opens the B20 call cache matching this database's storage mode and block.
    pub fn b20_backing(
        &self,
    ) -> Result<Arc<crate::b20_precompile::B20ArchiveBacking>, ForkDbError> {
        use crate::b20_precompile::B20ArchiveBacking;
        B20ArchiveBacking::for_fork(
            self.chain,
            self.fork_block(),
            (self.mode == ForkCacheMode::PersistentDisk).then_some(self.cache_dir.as_path()),
            self.rpc.clone(),
            Some(self.state.clone()),
        )
    }
    /// Persists newly fetched state to disk when running in `PersistentDisk` mode.
    pub fn flush_to_disk(&self) -> Result<(), ForkDbError> {
        if self.mode == ForkCacheMode::InMemory {
            return Ok(());
        }
        let mut guard = self.state.write().map_err(|_| ForkDbError::EvmExecution {
            context: "fork_db::flush_to_disk".to_owned(),
            detail: "state RwLock poisoned".to_owned(),
        })?;
        if guard.blocked.is_some() || guard.dirty_writes == 0 {
            return Ok(());
        }

        helper::write_cache_snapshot(self.chain, self.fork_block(), &self.snapshot_path(), &guard)?;
        guard.dirty_writes = 0;
        Ok(())
    }

    fn maybe_periodic_flush(&self, guard: &mut InMemoryForkCache) {
        if self.mode != ForkCacheMode::PersistentDisk {
            return;
        }
        if guard.dirty_writes >= 25
            && helper::write_cache_snapshot(
                self.chain,
                self.fork_block(),
                &self.snapshot_path(),
                guard,
            )
            .is_ok()
        {
            guard.dirty_writes = 0;
        }
    }
}

impl Drop for ForkDatabase {
    fn drop(&mut self) {
        if self.mode == ForkCacheMode::PersistentDisk {
            let _ = self.flush_to_disk();
        }
    }
}

impl DatabaseRef for ForkDatabase {
    type Error = ForkDbError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let mut guard = self.active()?;
        if let Some(info) = guard.accounts.get(&address) {
            return Ok(Some(info.clone()));
        }
        let (balance, nonce, code_bytes) = match guard.head {
            Some(head) => head.account(&self.rpc, address)?,
            None => self
                .rpc
                .get_account_basic_batch(address, self.fork_block())?,
        };

        let (code_hash, maybe_bytecode) = if code_bytes.is_empty() {
            (KECCAK_EMPTY, None)
        } else {
            let hash = keccak256(&code_bytes);
            (hash, Some(code_bytes))
        };

        // Store `code: None` so `basic_ref` never clones multi-KB `Bytecode`; `code_by_hash_ref` serves it once.
        let info = AccountInfo {
            balance,
            nonce,
            code_hash,
            account_id: None,
            code: None,
        };

        if let Some(raw_code) = maybe_bytecode {
            guard
                .contracts
                .entry(code_hash)
                .or_insert_with(|| Arc::new(Bytecode::new_raw(raw_code)));
        }
        guard.accounts.insert(address, info.clone());
        if self.mode == ForkCacheMode::PersistentDisk {
            guard.dirty_writes += 1;
            self.maybe_periodic_flush(&mut guard);
        }
        Ok(Some(info))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let guard = self.active()?;
        Ok(guard
            .contracts
            .get(&code_hash)
            .map(|bc| (**bc).clone())
            .unwrap_or_default())
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let mut guard = self.active()?;
        if let Some(&val) = guard.storage.get(&(address, index)) {
            return Ok(val);
        }
        let val = match guard.head {
            Some(head) => {
                let raw: String = self.rpc.call_single(
                    "eth_getStorageAt",
                    serde_json::json!([address, format!("0x{index:064x}"), head.selector()]),
                )?;
                rpc_helper::parse_u256_hex(&raw, "eth_getStorageAt")?
            }
            None => self.rpc.get_storage_at(address, index, self.fork_block())?,
        };
        guard.storage.insert((address, index), val);
        if self.mode == ForkCacheMode::PersistentDisk {
            guard.dirty_writes += 1;
            self.maybe_periodic_flush(&mut guard);
        }
        Ok(val)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let mut guard = self.active()?;
        if let Some(&hash) = guard.block_hashes.get(&number) {
            return Ok(hash);
        }
        let hash = match guard.head {
            Some(head) => head.historical_hash(&self.rpc, number)?,
            None => self.rpc.get_block_by_number(number, false)?.hash,
        };
        if guard.block_hashes.len() >= MAX_CACHED_BLOCK_HASHES
            && let Some(oldest) = guard.block_hashes.keys().min().copied()
        {
            guard.block_hashes.remove(&oldest);
        }
        guard.block_hashes.insert(number, hash);
        if self.mode == ForkCacheMode::PersistentDisk {
            guard.dirty_writes += 1;
            self.maybe_periodic_flush(&mut guard);
        }
        Ok(hash)
    }
}

mod helper {
    use super::*;

    pub(super) fn read_cache<T: serde::de::DeserializeOwned>(
        path: &Path,
    ) -> Result<Option<T>, ForkDbError> {
        let io_error = |path: &Path, source| ForkDbError::CacheIo {
            path: path.to_owned(),
            source,
        };
        let (bytes, source_path) = match fs::read(path) {
            Ok(bytes) => (bytes, path.to_owned()),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let compressed_path = path.with_extension("json.gz");
                let file = match fs::File::open(&compressed_path) {
                    Ok(file) => file,
                    Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
                    Err(err) => return Err(io_error(&compressed_path, err)),
                };
                let mut bytes = Vec::new();
                GzDecoder::new(file)
                    .read_to_end(&mut bytes)
                    .map_err(|err| io_error(&compressed_path, err))?;
                (bytes, compressed_path)
            }
            Err(err) => return Err(io_error(path, err)),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| ForkDbError::CacheDecode {
                path: source_path,
                source,
            })
    }

    pub(super) fn seed_empty_contracts(in_mem: &mut InMemoryForkCache) {
        let empty = Arc::new(Bytecode::default());
        in_mem.contracts.insert(KECCAK_EMPTY, Arc::clone(&empty));
        in_mem.contracts.insert(B256::ZERO, empty);
    }

    pub(super) fn write_cache_snapshot(
        chain: u64,
        fork_block: u64,
        cache_path: &Path,
        guard: &InMemoryForkCache,
    ) -> Result<(), ForkDbError> {
        let mut persisted = PersistedForkState {
            chain_id: chain,
            header: guard.head,
            fork_block,
            accounts: HashMap::with_capacity(guard.accounts.len()),
            storage: HashMap::new(),
            block_hashes: HashMap::with_capacity(guard.block_hashes.len()),
        };

        for (addr, info) in &guard.accounts {
            let code_bytes = guard
                .contracts
                .get(&info.code_hash)
                .map_or_else(Bytes::new, |bc| bc.original_bytes());
            persisted.accounts.insert(
                format!("{addr:#x}"),
                PersistedAccount {
                    balance_hex: format!("0x{:x}", info.balance),
                    nonce: info.nonce,
                    code_hex: format!("0x{}", hex::encode(code_bytes)),
                },
            );
        }

        for ((addr, slot), val) in &guard.storage {
            persisted
                .storage
                .entry(format!("{addr:#x}"))
                .or_default()
                .insert(format!("0x{slot:064x}"), format!("0x{val:064x}"));
        }

        for (num, hash) in &guard.block_hashes {
            persisted.block_hashes.insert(*num, format!("{hash:#x}"));
        }

        let encoded =
            serde_json::to_string(&persisted).map_err(|source| ForkDbError::CacheDecode {
                path: cache_path.to_path_buf(),
                source,
            })?;
        fs::write(cache_path, encoded).map_err(|source| ForkDbError::CacheIo {
            path: cache_path.to_path_buf(),
            source,
        })?;
        Ok(())
    }

    pub(super) fn load_in_memory(
        persisted: &PersistedForkState,
        in_mem: &mut InMemoryForkCache,
    ) -> Result<(), ForkDbError> {
        for (addr_str, acct) in &persisted.accounts {
            let addr = rpc_helper::parse_eth_address(addr_str)?;
            let balance = rpc_helper::parse_u256_hex(&acct.balance_hex, "cache.balance")?;
            let code_bytes = rpc_helper::parse_bytes_hex(&acct.code_hex, "cache.code")?;
            let code_hash = if code_bytes.is_empty() {
                KECCAK_EMPTY
            } else {
                let hash = keccak256(&code_bytes);
                in_mem
                    .contracts
                    .entry(hash)
                    .or_insert_with(|| Arc::new(Bytecode::new_raw(code_bytes)));
                hash
            };
            in_mem.accounts.insert(
                addr,
                AccountInfo {
                    balance,
                    nonce: acct.nonce,
                    code_hash,
                    account_id: None,
                    code: None,
                },
            );
        }

        for (addr_str, slots) in &persisted.storage {
            let addr = rpc_helper::parse_eth_address(addr_str)?;
            for (slot_str, val_str) in slots {
                let slot = rpc_helper::parse_u256_hex(slot_str, "cache.slot")?;
                let val = rpc_helper::parse_u256_hex(val_str, "cache.val")?;
                in_mem.storage.insert((addr, slot), val);
            }
        }

        for (num, hash_str) in &persisted.block_hashes {
            let hash = rpc_helper::parse_b256_hex(hash_str, "cache.block_hash")?;
            in_mem.block_hashes.insert(*num, hash);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
