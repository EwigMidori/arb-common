//! Execute a transaction against the fork and optionally commit its state changes.
use crate::{
    ForkDbError,
    b20_precompile::{B20ArchiveBacking, BaseB20Precompiles},
};
use alloy_primitives::Bytes;
use revm::{
    context::{BlockEnv, CfgEnv, Context, Evm, TxEnv},
    context_interface::result::{EVMError, ExecutionResult},
    database_interface::{Database, DatabaseCommit},
    handler::{ExecuteCommitEvm, ExecuteEvm, MainContext, instructions::EthInstructions},
};
use std::sync::Arc;

#[non_exhaustive]
pub enum StateChanges {
    Discard,
    Commit,
}

#[non_exhaustive]
pub struct ForkExecution {
    cfg: CfgEnv,
    block: BlockEnv,
    b20: Option<Arc<B20ArchiveBacking>>,
}
impl ForkExecution {
    pub fn new(cfg: CfgEnv, block: BlockEnv, b20: Option<Arc<B20ArchiveBacking>>) -> Self {
        Self { cfg, block, b20 }
    }
    pub fn transact<DB: Database + DatabaseCommit>(
        self,
        db: &mut DB,
        tx: TxEnv,
        changes: StateChanges,
    ) -> Result<ExecutionResult, EVMError<DB::Error>> {
        let spec = self.cfg.spec;
        let ctx = Context::mainnet()
            .with_cfg(self.cfg)
            .with_block(self.block)
            .with_db(db);
        let mut evm = Evm::new(
            ctx,
            EthInstructions::new_mainnet_with_spec(spec),
            BaseB20Precompiles::new(spec, self.b20),
        );
        match changes {
            StateChanges::Discard => Ok(evm.transact(tx)?.result),
            StateChanges::Commit => evm.transact_commit(tx),
        }
    }
}

pub fn output(result: ExecutionResult, context: &str) -> Result<Bytes, ForkDbError> {
    match result {
        ExecutionResult::Success { output, .. } => Ok(output.into_data()),
        ExecutionResult::Revert { gas, output, .. } => Err(ForkDbError::EvmReverted {
            context: context.to_owned(),
            reason: format!("0x{}", hex::encode(output)),
            gas_used: gas.tx_gas_used(),
        }),
        ExecutionResult::Halt { reason, gas, .. } => Err(ForkDbError::EvmHalted {
            context: context.to_owned(),
            reason: format!("{reason:?}"),
            gas_used: gas.tx_gas_used(),
        }),
    }
}

/// Decode the standard Solidity Error(string) payload.
pub fn revert_reason(output: &[u8]) -> Option<String> {
    use alloy_sol_types::{Revert, SolError};
    Revert::abi_decode(output).ok().map(|error| error.reason)
}
