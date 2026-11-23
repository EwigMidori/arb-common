//! Coalesces pending chain wakeups and their notification under one mutex.
use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

#[non_exhaustive]
#[derive(Default)]
pub struct WakeQueue {
    pending: Mutex<HashMap<u64, Instant>>,
    changed: Condvar,
}

impl WakeQueue {
    pub fn push(&self, chain: u64) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.entry(chain).or_insert_with(Instant::now);
            self.changed.notify_one();
        }
    }

    pub fn wait(&self, timeout: Duration) -> Vec<(u64, Instant)> {
        let Ok(pending) = self.pending.lock() else {
            return Vec::new();
        };
        let Ok((mut pending, _)) = self
            .changed
            .wait_timeout_while(pending, timeout, |p| p.is_empty())
        else {
            return Vec::new();
        };
        pending.drain().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::WakeQueue;
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn coalesces_per_chain_and_preserves_first_arrival() {
        let queue = WakeQueue::default();
        queue.push(8453);
        let first = queue
            .pending
            .lock()
            .ok()
            .and_then(|p| p.get(&8453).copied());
        queue.push(8453);
        queue.push(42161);

        let wakeups = queue.wait(Duration::ZERO);
        assert_eq!(wakeups.len(), 2);
        assert_eq!(
            wakeups
                .iter()
                .find(|(chain, _)| *chain == 8453)
                .map(|(_, t)| *t),
            first
        );
        assert!(wakeups.iter().any(|(chain, _)| *chain == 42161));

        let start = Instant::now();
        assert!(queue.wait(Duration::from_millis(30)).is_empty());
        assert!(start.elapsed() >= Duration::from_millis(30));
    }

    #[test]
    fn concurrent_push_after_drain_remains_available() -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(WakeQueue::default());
        queue.push(8453);
        let mut pending = queue.pending.lock().map_err(|_| "poisoned queue")?;
        let (started_tx, started_rx) = mpsc::channel();
        let worker_queue = Arc::clone(&queue);
        let worker = thread::spawn(move || {
            let _ = started_tx.send(());
            worker_queue.push(42161);
        });
        started_rx.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(pending.drain().count(), 1);
        drop(pending);
        worker.join().map_err(|_| "worker panicked")?;

        let wakeups = queue.wait(Duration::ZERO);
        assert_eq!(wakeups.len(), 1);
        assert_eq!(wakeups[0].0, 42161);
        Ok(())
    }
}
