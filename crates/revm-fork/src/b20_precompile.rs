use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use alloy_primitives::{Address, Bytes, Log, LogData, U256, keccak256};
use alloy_sol_types::{SolCall, SolEvent, SolValue, sol};
use revm::context::{Cfg, ContextTr};
use revm::context_interface::JournalTr;
use revm::handler::{EthPrecompiles, PrecompileProvider};
use revm::interpreter::{CallInputs, Gas, InstructionResult, InterpreterResult};
use revm::primitives::{AddressSet, hardfork::SpecId};

use crate::ForkDbError;
use evm_rpc::http::HttpRpcClient;

/// 11-byte address prefix identifying Base native B20 token precompiles (`0xb200000000000000000000...`).
const B20_PREFIX: [u8; 11] = [0xb2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

sol! {
    interface IB20Methods {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function transfer(address to, uint256 amount) external returns (bool);
        function transferFrom(address from, address to, uint256 amount) external returns (bool);
        function decimals() external view returns (uint8);
        function totalSupply() external view returns (uint256);
        function name() external view returns (string memory);
        function symbol() external view returns (string memory);
    }

    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);
}

/// Persistent disk-cached `eth_call` backing for Base native B20 token precompiles (`0xb200...`).
#[non_exhaustive]
#[derive(Debug)]
pub struct B20ArchiveBacking {
    fork_state: Option<Arc<RwLock<crate::fork_db::InMemoryForkCache>>>,
    fork_block: AtomicU64,
    cache_path: PathBuf,
    chain: u64,
    rpc: HttpRpcClient,
    cache: RwLock<B20CallCache>,
}

#[derive(Debug)]
struct B20CallCache {
    path: PathBuf,
    hash: Option<alloy_primitives::B256>,
    calls: HashMap<String, String>,
    dirty: bool,
}

impl B20ArchiveBacking {
    /// Opens or initializes cached B20 precompile `eth_call` responses for `fork_block`.
    pub fn open(
        chain: u64,
        fork_block: u64,
        cache_dir: &Path,
        rpc: HttpRpcClient,
    ) -> Result<Arc<Self>, ForkDbError> {
        Self::for_fork(chain, fork_block, Some(cache_dir), rpc, None)
    }

    pub(crate) fn for_fork(
        chain: u64,
        height: u64,
        dir: Option<&Path>,
        rpc: HttpRpcClient,
        state: Option<Arc<RwLock<crate::fork_db::InMemoryForkCache>>>,
    ) -> Result<Arc<Self>, ForkDbError> {
        let mut backing = Self::in_memory(height, rpc);
        backing.chain = chain;
        backing.fork_state = state;
        if let Some(dir) = dir {
            fs::create_dir_all(dir).map_err(|source| ForkDbError::CacheIo {
                path: dir.to_owned(),
                source,
            })?;
            backing.cache_path = dir.join(format!("b20_{chain}_block_{height}.json"));
            let identity = backing
                .fork_state
                .as_ref()
                .map(|s| s.read().map_err(|_| backing.poisoned_cache())?.identity())
                .transpose()?
                .flatten();
            if identity.is_none() {
                backing.cache = RwLock::new(B20CallCache::open(backing.cache_path.clone(), None)?);
            }
        }
        Ok(Arc::new(backing))
    }

    /// Initializes a pure in-memory B20 precompile backing for live simulation with zero disk I/O.
    pub fn open_live(fork_block: u64, rpc: HttpRpcClient) -> Arc<Self> {
        Arc::new(Self::in_memory(fork_block, rpc))
    }

    fn in_memory(fork_block: u64, rpc: HttpRpcClient) -> Self {
        Self {
            fork_state: None,
            fork_block: AtomicU64::new(fork_block),
            cache_path: PathBuf::new(),
            chain: 0,
            rpc,
            cache: RwLock::new(B20CallCache {
                path: PathBuf::new(),
                hash: None,
                calls: HashMap::new(),
                dirty: false,
            }),
        }
    }

    /// Advances the live B20 precompile backing to `new_block` and clears per-block cached calls.
    pub fn advance_to_block(&self, new_block: u64) -> Result<(), ForkDbError> {
        if let Some(state) = &self.fork_state
            && let Some((height, _)) = state
                .read()
                .map_err(|_| self.poisoned_cache())?
                .identity()?
        {
            if height != new_block {
                return Err(ForkDbError::InvalidDomainValue {
                    field: "b20_fork_height",
                    detail: format!("expected {height}, got {new_block}"),
                });
            }
            // Bound backings switch when the next call observes the new fork hash.
            return Ok(());
        }
        let mut cache = self.cache.write().map_err(|_| self.poisoned_cache())?;
        cache.flush()?;
        self.fork_block.store(new_block, Ordering::Relaxed);
        cache.calls.clear();
        cache.dirty = false;
        Ok(())
    }

    /// Queries an immutable historical B20 read call (`balanceOf`, `allowance`, `decimals`, etc.)
    /// from disk cache or Base Archive `eth_call`.
    pub fn eth_call_cached(&self, token: Address, calldata: &[u8]) -> Result<Bytes, ForkDbError> {
        let state = self
            .fork_state
            .as_ref()
            .map(|state| state.read().map_err(|_| self.poisoned_cache()))
            .transpose()?;
        let identity = state.as_ref().map(|s| s.identity()).transpose()?.flatten();
        let hash = identity.map(|(_, hash)| hash);
        let key = format!("{token:#x}:0x{}", hex::encode(calldata));
        let mut cache = self.cache.write().map_err(|_| self.poisoned_cache())?;
        if let Some((height, hash)) = identity
            && cache.hash != Some(hash)
        {
            cache.flush()?;
            let path = if self.cache_path.as_os_str().is_empty() {
                PathBuf::new()
            } else {
                self.cache_path
                    .with_file_name(format!("b20_{}_block_{height}_{hash:x}.json", self.chain))
            };
            *cache = B20CallCache::open(path, Some(hash))?;
        }
        if let Some(raw) = cache.calls.get(&key) {
            return hex::decode(raw.trim().trim_start_matches("0x"))
                .map(Bytes::from)
                .map_err(|source| ForkDbError::CacheHex {
                    path: cache.path.clone(),
                    key,
                    source,
                });
        }
        let res_bytes = match hash {
            Some(hash) => self.rpc.call_single(
                "eth_call",
                serde_json::json!([
                    {"to":token,"data":format!("0x{}", hex::encode(calldata))},
                    {"blockHash":hash,"requireCanonical":true}
                ]),
            )?,
            None => self
                .rpc
                .eth_call(token, calldata, self.fork_block.load(Ordering::Relaxed))?,
        };
        let val_hex = format!("0x{}", hex::encode(&res_bytes));
        cache.calls.insert(key, val_hex);
        cache.dirty = true;
        Ok(res_bytes)
    }

    /// Flushes cached B20 responses to `engine/cache/`.
    pub fn flush(&self) -> Result<(), ForkDbError> {
        self.cache
            .write()
            .map_err(|_| self.poisoned_cache())?
            .flush()
    }

    fn poisoned_cache(&self) -> ForkDbError {
        ForkDbError::CacheIo {
            path: self.cache_path.clone(),
            source: std::io::Error::other("B20 cache lock poisoned"),
        }
    }
}

impl B20CallCache {
    fn open(path: PathBuf, hash: Option<alloy_primitives::B256>) -> Result<Self, ForkDbError> {
        let calls = if path.as_os_str().is_empty() {
            HashMap::new()
        } else {
            match fs::read(&path) {
                Ok(bytes) => {
                    serde_json::from_slice(&bytes).map_err(|source| ForkDbError::CacheDecode {
                        path: path.clone(),
                        source,
                    })?
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
                Err(source) => return Err(ForkDbError::CacheIo { path, source }),
            }
        };
        Ok(Self {
            path,
            hash,
            calls,
            dirty: false,
        })
    }

    fn flush(&mut self) -> Result<(), ForkDbError> {
        if self.path.as_os_str().is_empty() || !self.dirty {
            return Ok(());
        }
        let encoded =
            serde_json::to_vec(&self.calls).map_err(|source| ForkDbError::CacheDecode {
                path: self.path.clone(),
                source,
            })?;
        fs::write(&self.path, encoded).map_err(|source| ForkDbError::CacheIo {
            path: self.path.clone(),
            source,
        })?;
        self.dirty = false;
        Ok(())
    }
}

impl Drop for B20ArchiveBacking {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_cached_call_identifies_the_entry_without_rpc() {
        let token = Address::ZERO;
        let entry = format!("{token:#x}:0xab");
        let path = PathBuf::from("b20.json");
        let backing = B20ArchiveBacking {
            fork_state: None,
            fork_block: AtomicU64::new(1),
            cache_path: path.clone(),
            chain: 1,
            rpc: HttpRpcClient::new(Vec::new()),
            cache: RwLock::new(B20CallCache {
                path: path.clone(),
                hash: None,
                calls: HashMap::from([(entry.clone(), "0xgg".to_owned())]),
                dirty: false,
            }),
        };
        assert!(matches!(
            backing.eth_call_cached(token, &[0xab]),
            Err(ForkDbError::CacheHex { path: actual, key, .. }) if actual == path && key == entry
        ));
    }
}

/// Custom `PrecompileProvider` extending Ethereum `EthPrecompiles` with Base B20 native precompiles.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct BaseB20Precompiles {
    inner: EthPrecompiles,
    backing: Option<Arc<B20ArchiveBacking>>,
}

impl BaseB20Precompiles {
    /// Creates a provider supporting standard Ethereum precompiles plus optional Base B20 precompiles.
    pub fn new(spec: SpecId, backing: Option<Arc<B20ArchiveBacking>>) -> Self {
        Self {
            inner: EthPrecompiles::new(spec),
            backing,
        }
    }

    /// Returns `true` when `addr` lies in Base's `0xb200...` B20 native token namespace.
    pub fn is_b20_address(addr: Address) -> bool {
        addr.as_slice()[..11] == B20_PREFIX
    }
}

impl<CTX> PrecompileProvider<CTX> for BaseB20Precompiles
where
    CTX: ContextTr<Cfg: Cfg<Spec = SpecId>>,
{
    type Output = InterpreterResult;

    fn set_spec(&mut self, spec: <CTX::Cfg as Cfg>::Spec) -> bool {
        <EthPrecompiles as PrecompileProvider<CTX>>::set_spec(&mut self.inner, spec)
    }

    fn run(
        &mut self,
        context: &mut CTX,
        inputs: &CallInputs,
    ) -> Result<Option<Self::Output>, String> {
        if let Some(ref backing) = self.backing
            && Self::is_b20_address(inputs.bytecode_address)
        {
            return helper::execute_b20_call(context, inputs, backing)
                .map(Some)
                .map_err(|err| err.to_string());
        }
        self.inner.run(context, inputs)
    }

    fn warm_addresses(&self) -> &AddressSet {
        self.inner.warm_addresses()
    }

    fn contains(&self, address: &Address) -> bool {
        self.inner.contains(address) || (self.backing.is_some() && Self::is_b20_address(*address))
    }
}

mod helper {
    use super::*;

    const B20_GAS_COST: u64 = 3_000;

    fn success_result(bytes: Bytes, gas_limit: u64) -> InterpreterResult {
        let mut gas = Gas::new(gas_limit);
        let _ = gas.record_regular_cost(B20_GAS_COST.min(gas_limit));
        InterpreterResult::new(InstructionResult::Return, bytes, gas)
    }

    fn revert_result(gas_limit: u64) -> InterpreterResult {
        let mut gas = Gas::new(gas_limit);
        let _ = gas.record_regular_cost(B20_GAS_COST.min(gas_limit));
        InterpreterResult::new(InstructionResult::Revert, Bytes::new(), gas)
    }

    fn bal_slots(owner: Address) -> (U256, U256) {
        let mut buf = [0_u8; 28];
        buf[..8].copy_from_slice(b"b20_ibal");
        buf[8..28].copy_from_slice(owner.as_slice());
        let init_slot = U256::from_be_bytes(keccak256(buf).0);

        buf[..8].copy_from_slice(b"b20_vbal");
        let val_slot = U256::from_be_bytes(keccak256(buf).0);
        (init_slot, val_slot)
    }

    fn allow_slots(owner: Address, spender: Address) -> (U256, U256) {
        let mut buf = [0_u8; 48];
        buf[..8].copy_from_slice(b"b20_iall");
        buf[8..28].copy_from_slice(owner.as_slice());
        buf[28..48].copy_from_slice(spender.as_slice());
        let init_slot = U256::from_be_bytes(keccak256(buf).0);

        buf[..8].copy_from_slice(b"b20_vall");
        let val_slot = U256::from_be_bytes(keccak256(buf).0);
        (init_slot, val_slot)
    }

    fn read_balance<CTX: ContextTr>(
        context: &mut CTX,
        token: Address,
        owner: Address,
        backing: &B20ArchiveBacking,
    ) -> Result<U256, ForkDbError> {
        let (init_slot, val_slot) = bal_slots(owner);
        let init = context
            .journal_mut()
            .sload(token, init_slot)
            .map_err(journal_error)?;
        if !init.data.is_zero() {
            let val = context
                .journal_mut()
                .sload(token, val_slot)
                .map_err(journal_error)?;
            return Ok(val.data);
        }
        let calldata = IB20Methods::balanceOfCall { account: owner }.abi_encode();
        let ret = backing.eth_call_cached(token, &calldata)?;
        U256::abi_decode(&ret).map_err(abi_error)
    }

    fn ensure_balance_initialized<CTX: ContextTr>(
        context: &mut CTX,
        token: Address,
        owner: Address,
        backing: &B20ArchiveBacking,
    ) -> Result<U256, ForkDbError> {
        let (init_slot, val_slot) = bal_slots(owner);
        let init = context
            .journal_mut()
            .sload(token, init_slot)
            .map_err(journal_error)?;
        if !init.data.is_zero() {
            let val = context
                .journal_mut()
                .sload(token, val_slot)
                .map_err(journal_error)?;
            return Ok(val.data);
        }
        let bal = read_balance(context, token, owner, backing)?;
        context
            .journal_mut()
            .sstore(token, val_slot, bal)
            .map_err(journal_error)?;
        context
            .journal_mut()
            .sstore(token, init_slot, U256::from(1_u64))
            .map_err(journal_error)?;
        Ok(bal)
    }

    fn read_allowance<CTX: ContextTr>(
        context: &mut CTX,
        token: Address,
        owner: Address,
        spender: Address,
        backing: &B20ArchiveBacking,
    ) -> Result<U256, ForkDbError> {
        let (init_slot, val_slot) = allow_slots(owner, spender);
        let init = context
            .journal_mut()
            .sload(token, init_slot)
            .map_err(journal_error)?;
        if !init.data.is_zero() {
            let val = context
                .journal_mut()
                .sload(token, val_slot)
                .map_err(journal_error)?;
            return Ok(val.data);
        }
        let calldata = IB20Methods::allowanceCall { owner, spender }.abi_encode();
        let ret = backing.eth_call_cached(token, &calldata)?;
        U256::abi_decode(&ret).map_err(abi_error)
    }

    fn ensure_allowance_initialized<CTX: ContextTr>(
        context: &mut CTX,
        token: Address,
        owner: Address,
        spender: Address,
        backing: &B20ArchiveBacking,
    ) -> Result<U256, ForkDbError> {
        let (init_slot, val_slot) = allow_slots(owner, spender);
        let init = context
            .journal_mut()
            .sload(token, init_slot)
            .map_err(journal_error)?;
        if !init.data.is_zero() {
            let val = context
                .journal_mut()
                .sload(token, val_slot)
                .map_err(journal_error)?;
            return Ok(val.data);
        }
        let allow = read_allowance(context, token, owner, spender, backing)?;
        context
            .journal_mut()
            .sstore(token, val_slot, allow)
            .map_err(journal_error)?;
        context
            .journal_mut()
            .sstore(token, init_slot, U256::from(1_u64))
            .map_err(journal_error)?;
        Ok(allow)
    }

    fn emit_transfer<CTX: ContextTr>(
        context: &mut CTX,
        token: Address,
        from: Address,
        to: Address,
        value: U256,
    ) {
        let ev = Transfer { from, to, value };
        let ld = ev.encode_log_data();
        context.journal_mut().log(Log {
            address: token,
            data: LogData::new_unchecked(ld.topics().to_vec(), ld.data),
        });
    }

    pub(super) fn execute_b20_call<CTX: ContextTr>(
        context: &mut CTX,
        inputs: &CallInputs,
        backing: &B20ArchiveBacking,
    ) -> Result<InterpreterResult, ForkDbError> {
        if inputs.gas_limit < B20_GAS_COST {
            return Ok(InterpreterResult::new(
                InstructionResult::PrecompileOOG,
                Bytes::new(),
                Gas::new(inputs.gas_limit),
            ));
        }

        let token = inputs.target_address;
        let input_bytes: Vec<u8> = inputs.input.as_bytes(context).to_vec();
        if input_bytes.len() < 4 {
            return Ok(revert_result(inputs.gas_limit));
        }

        let sel: [u8; 4] = [
            input_bytes[0],
            input_bytes[1],
            input_bytes[2],
            input_bytes[3],
        ];

        if sel == IB20Methods::balanceOfCall::SELECTOR {
            let call = IB20Methods::balanceOfCall::abi_decode(&input_bytes).map_err(abi_error)?;
            let bal = read_balance(context, token, call.account, backing)?;
            return Ok(success_result(
                Bytes::from(bal.abi_encode()),
                inputs.gas_limit,
            ));
        }

        if sel == IB20Methods::allowanceCall::SELECTOR {
            let call = IB20Methods::allowanceCall::abi_decode(&input_bytes).map_err(abi_error)?;
            let allow = read_allowance(context, token, call.owner, call.spender, backing)?;
            return Ok(success_result(
                Bytes::from(allow.abi_encode()),
                inputs.gas_limit,
            ));
        }

        if sel == IB20Methods::decimalsCall::SELECTOR
            || sel == IB20Methods::totalSupplyCall::SELECTOR
            || sel == IB20Methods::nameCall::SELECTOR
            || sel == IB20Methods::symbolCall::SELECTOR
        {
            let ret = backing.eth_call_cached(token, &input_bytes)?;
            return Ok(success_result(ret, inputs.gas_limit));
        }

        if inputs.is_static {
            return Ok(revert_result(inputs.gas_limit));
        }

        if sel == IB20Methods::approveCall::SELECTOR {
            let call = IB20Methods::approveCall::abi_decode(&input_bytes).map_err(abi_error)?;
            let (init_slot, val_slot) = allow_slots(inputs.caller, call.spender);
            context
                .journal_mut()
                .sstore(token, val_slot, call.amount)
                .map_err(journal_error)?;
            context
                .journal_mut()
                .sstore(token, init_slot, U256::from(1_u64))
                .map_err(journal_error)?;
            let ev = Approval {
                owner: inputs.caller,
                spender: call.spender,
                value: call.amount,
            };
            let ld = ev.encode_log_data();
            context.journal_mut().log(Log {
                address: token,
                data: LogData::new_unchecked(ld.topics().to_vec(), ld.data),
            });
            return Ok(success_result(
                Bytes::from(true.abi_encode()),
                inputs.gas_limit,
            ));
        }

        if sel == IB20Methods::transferCall::SELECTOR {
            let call = IB20Methods::transferCall::abi_decode(&input_bytes).map_err(abi_error)?;
            let from_bal = ensure_balance_initialized(context, token, inputs.caller, backing)?;
            if from_bal < call.amount {
                return Ok(revert_result(inputs.gas_limit));
            }
            let (_, from_val_slot) = bal_slots(inputs.caller);
            context
                .journal_mut()
                .sstore(token, from_val_slot, from_bal - call.amount)
                .map_err(journal_error)?;

            let to_bal = ensure_balance_initialized(context, token, call.to, backing)?;
            let (_, to_val_slot) = bal_slots(call.to);
            context
                .journal_mut()
                .sstore(token, to_val_slot, to_bal.saturating_add(call.amount))
                .map_err(journal_error)?;

            emit_transfer(context, token, inputs.caller, call.to, call.amount);
            return Ok(success_result(
                Bytes::from(true.abi_encode()),
                inputs.gas_limit,
            ));
        }

        if sel == IB20Methods::transferFromCall::SELECTOR {
            let call =
                IB20Methods::transferFromCall::abi_decode(&input_bytes).map_err(abi_error)?;
            let allow =
                ensure_allowance_initialized(context, token, call.from, inputs.caller, backing)?;
            if allow < call.amount {
                return Ok(revert_result(inputs.gas_limit));
            }
            if allow != U256::MAX {
                let (_, allow_val_slot) = allow_slots(call.from, inputs.caller);
                context
                    .journal_mut()
                    .sstore(token, allow_val_slot, allow - call.amount)
                    .map_err(journal_error)?;
            }

            let from_bal = ensure_balance_initialized(context, token, call.from, backing)?;
            if from_bal < call.amount {
                return Ok(revert_result(inputs.gas_limit));
            }
            let (_, from_val_slot) = bal_slots(call.from);
            context
                .journal_mut()
                .sstore(token, from_val_slot, from_bal - call.amount)
                .map_err(journal_error)?;

            let to_bal = ensure_balance_initialized(context, token, call.to, backing)?;
            let (_, to_val_slot) = bal_slots(call.to);
            context
                .journal_mut()
                .sstore(token, to_val_slot, to_bal.saturating_add(call.amount))
                .map_err(journal_error)?;

            emit_transfer(context, token, call.from, call.to, call.amount);
            return Ok(success_result(
                Bytes::from(true.abi_encode()),
                inputs.gas_limit,
            ));
        }

        // Fallback: forward unknown read-only B20 queries to Base Archive `eth_call`.
        let ret = backing.eth_call_cached(token, &input_bytes)?;
        Ok(success_result(ret, inputs.gas_limit))
    }

    fn abi_error(source: alloy_sol_types::Error) -> ForkDbError {
        ForkDbError::AbiCodec {
            context: "B20 precompile",
            detail: source.to_string(),
        }
    }

    fn journal_error(source: impl std::fmt::Debug) -> ForkDbError {
        ForkDbError::EvmExecution {
            context: "B20 storage access".to_owned(),
            detail: format!("{source:?}"),
        }
    }
}
