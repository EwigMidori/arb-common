//! Authentication failures persist across clients using the same endpoint in this process.
use alloy_primitives::B256;
use std::collections::HashMap;
use std::sync::{
    Arc, LazyLock, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Default)]
pub(crate) struct EndpointAuth {
    code: OnceLock<i64>,
    reported: AtomicBool,
    pub(crate) archive_unsupported: AtomicBool,
}

impl EndpointAuth {
    pub(crate) fn for_id(id: B256) -> Arc<Self> {
        static STATES: LazyLock<Mutex<HashMap<B256, Arc<EndpointAuth>>>> =
            LazyLock::new(Mutex::default);
        let mut states = STATES.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(states.entry(id).or_default())
    }

    pub(crate) fn reject(&self, code: i64) {
        self.code.get_or_init(|| code);
    }

    pub(crate) fn code(&self) -> Option<i64> {
        self.code.get().copied()
    }

    pub(crate) fn take_notice(&self) -> bool {
        self.code().is_some() && !self.reported.swap(true, Ordering::AcqRel)
    }
}
