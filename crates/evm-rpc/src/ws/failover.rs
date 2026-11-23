use super::{SubscriptionEvents, Subscriptions, client};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub fn run(
    chain: u64,
    urls: Vec<String>,
    subscriptions: Subscriptions,
    shared: Arc<dyn SubscriptionEvents>,
) {
    if urls.is_empty() {
        return;
    }
    let mut index = 0_usize;
    let mut attempts = 0_u32;
    while !shared.stopped() {
        let started = Instant::now();
        let result = client::run(chain, &urls[index], &subscriptions, shared.as_ref());
        if shared.stopped() {
            break;
        }
        if let Err(error) = result {
            eprintln!("[rpc_ws] {chain} stream disconnected: {error}");
        } else {
            continue;
        }
        let switched = urls.len() > 1;
        shared.reconnect(switched);
        if switched {
            index = (index + 1) % urls.len();
        }
        attempts = if started.elapsed() > Duration::from_secs(60) {
            0
        } else {
            attempts.saturating_add(1).min(6)
        };
        let base = 250_u64.saturating_mul(1_u64 << attempts);
        let jitter = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::from(d.subsec_nanos()) % 251);
        let deadline = Instant::now() + Duration::from_millis(base + jitter);
        while Instant::now() < deadline && !shared.stopped() {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
