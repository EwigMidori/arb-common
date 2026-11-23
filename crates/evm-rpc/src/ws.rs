//! JSON-RPC subscriptions with heartbeat monitoring and endpoint failover.
mod client;
mod failover;
mod protocol;
use alloy_primitives::{Address, B256};
pub use client::WsError;
pub use failover::run;
pub trait SubscriptionEvents: Send + Sync {
    fn updated_addresses(&self, _chain: u64) -> Option<std::sync::Arc<Vec<Address>>> {
        None
    }
    fn stopped(&self) -> bool;
    fn health(&self, chain_id: u64, healthy: bool);
    fn push(&self, chain_id: u64, topics: Option<&[B256]>);
    fn reconnect(&self, switched: bool);
}
#[non_exhaustive]
pub struct Subscriptions {
    heads: bool,
    addresses: Vec<Address>,
    filter: Option<WsTopicLogFilter>,
}
impl Subscriptions {
    pub fn new(heads: bool, addresses: Vec<Address>, filter: Option<WsTopicLogFilter>) -> Self {
        Self {
            heads,
            addresses,
            filter,
        }
    }
}
/// Address and topics for an additional filtered log subscription.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct WsTopicLogFilter {
    pub address: Address,
    pub topic0: Vec<B256>,
    pub topic1: Vec<B256>,
}

impl WsTopicLogFilter {
    pub fn new(address: Address, topic0: Vec<B256>, topic1: Vec<B256>) -> Self {
        Self {
            address,
            topic0,
            topic1,
        }
    }
}
