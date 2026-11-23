use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Webhook transport settings supplied by the composition root.
#[non_exhaustive]
#[derive(derive_builder::Builder)]
pub struct DiscordWebhookConfig {
    pub url: Option<String>,
    pub cooldown_secs: u64,
}
impl DiscordWebhookConfig {
    pub fn new(url: Option<String>, cooldown_secs: u64) -> Self {
        Self { url, cooldown_secs }
    }
}

pub const DEFAULT_DISCORD_QUEUE_CAPACITY: usize = 64;

#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum DiscordPriority {
    Critical,
    Notice,
}

struct DiscordMessage {
    key: String,
    content: String,
    throttle: bool,
}

#[non_exhaustive]
#[derive(Default)]
pub struct DiscordCounters {
    pub queued_total: AtomicU64,
    pub sent_total: AtomicU64,
    pub dropped_total: AtomicU64,
    pub throttled_total: AtomicU64,
    pub send_errors_total: AtomicU64,
}

#[non_exhaustive]
pub struct DiscordAlerter {
    halt: Option<mpsc::SyncSender<DiscordMessage>>,
    critical: Option<mpsc::SyncSender<DiscordMessage>>,
    notice: Option<mpsc::SyncSender<DiscordMessage>>,
    signal: Option<mpsc::SyncSender<()>>,
    worker: Option<JoinHandle<()>>,
    pub counters: Arc<DiscordCounters>,
}

impl fmt::Debug for DiscordAlerter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiscordAlerter")
            .field("webhook", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl DiscordAlerter {
    pub fn is_enabled(&self) -> bool {
        self.critical.is_some()
    }

    pub fn disabled() -> Self {
        Self {
            halt: None,
            critical: None,
            notice: None,
            signal: None,
            worker: None,
            counters: Arc::default(),
        }
    }

    pub fn from_config(config: &DiscordWebhookConfig) -> Self {
        let url = config.url.clone();
        let Some(url) = url.filter(|s| !s.trim().is_empty() && !s.trim().starts_with('#')) else {
            return Self::disabled();
        };
        Self::start(url.trim().to_owned(), config.cooldown_secs.clamp(60, 300))
    }

    fn start(url: String, cooldown_secs: u64) -> Self {
        let (halt, halt_rx) = mpsc::sync_channel::<DiscordMessage>(1);
        let (critical, critical_rx) = mpsc::sync_channel::<DiscordMessage>(32);
        let (notice, notice_rx) =
            mpsc::sync_channel::<DiscordMessage>(DEFAULT_DISCORD_QUEUE_CAPACITY);
        let (signal, signal_rx) = mpsc::sync_channel::<()>(97);
        let counters = Arc::new(DiscordCounters::default());
        let stats = Arc::clone(&counters);
        let worker = thread::spawn(move || {
            let agent: ureq::Agent = ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(3)))
                .build()
                .into();
            let mut cooldown = HashMap::<String, Instant>::new();
            while signal_rx.recv().is_ok() {
                let message = halt_rx
                    .try_recv()
                    .or_else(|_| critical_rx.try_recv())
                    .or_else(|_| notice_rx.try_recv());
                let Ok(message) = message else {
                    continue;
                };
                if message.throttle
                    && cooldown
                        .get(&message.key)
                        .is_some_and(|last| last.elapsed() < Duration::from_secs(cooldown_secs))
                {
                    stats.throttled_total.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if message.throttle {
                    cooldown.insert(message.key, Instant::now());
                }
                let result = agent.post(&url).send_json(Content {
                    content: &message.content,
                });
                match result {
                    Ok(_) => {
                        stats.sent_total.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        stats.send_errors_total.fetch_add(1, Ordering::Relaxed);
                        match error {
                            ureq::Error::StatusCode(code) => {
                                eprintln!("Discord webhook failed with HTTP {code} [REDACTED]")
                            }
                            _ => eprintln!("Discord webhook transport failed [REDACTED]"),
                        }
                    }
                }
            }
        });
        Self {
            halt: Some(halt),
            critical: Some(critical),
            notice: Some(notice),
            signal: Some(signal),
            worker: Some(worker),
            counters,
        }
    }

    #[cfg(test)]
    pub(super) fn with_cooldown_for_tests(url: String, cooldown_secs: u64) -> Self {
        Self::start(url, cooldown_secs)
    }

    pub fn send(
        &self,
        priority: DiscordPriority,
        key: String,
        content: String,
        throttle_critical: bool,
    ) {
        let sender = match priority {
            DiscordPriority::Critical => &self.critical,
            DiscordPriority::Notice => &self.notice,
        };
        let Some(sender) = sender else {
            return;
        };
        let throttle = matches!(priority, DiscordPriority::Notice) || throttle_critical;
        self.enqueue(
            sender,
            DiscordMessage {
                key,
                content,
                throttle,
            },
        );
    }

    pub fn send_halt(&self, content: String) {
        if let Some(sender) = &self.halt {
            self.enqueue(
                sender,
                DiscordMessage {
                    key: "halt".to_owned(),
                    content,
                    throttle: false,
                },
            );
        }
    }

    fn enqueue(&self, sender: &mpsc::SyncSender<DiscordMessage>, message: DiscordMessage) {
        match sender.try_send(message) {
            Ok(()) => {
                self.counters.queued_total.fetch_add(1, Ordering::Relaxed);
                if let Some(signal) = &self.signal {
                    let _ = signal.try_send(());
                }
            }
            Err(_) => {
                self.counters.dropped_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn flush_and_close(&mut self) {
        self.halt.take();
        self.critical.take();
        self.notice.take();
        self.signal.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for DiscordAlerter {
    fn drop(&mut self) {
        self.flush_and_close();
    }
}

#[derive(Serialize)]
struct Content<'a> {
    content: &'a str,
}

#[cfg(test)]
mod tests;
