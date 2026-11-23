use std::collections::HashMap;
use std::time::{Duration, Instant};

#[non_exhaustive]
#[derive(Debug, Default)]
pub struct EndpointHealth {
    cooldown_until: Option<Instant>,
    cooldowns: u32,
    timeouts: u32,
}

impl EndpointHealth {
    pub fn cooldown_until(&self) -> Option<Instant> {
        self.cooldown_until
    }

    pub fn rate_limited(&mut self, now: Instant) {
        let seconds = 30_u64 * (1_u64 << self.cooldowns.min(5));
        self.cooldowns = self.cooldowns.saturating_add(1);
        self.timeouts = 0;
        self.cooldown_until = Some(now + Duration::from_secs(seconds));
    }

    pub fn restricted(&mut self, now: Instant) {
        let seconds = 30_u64 * (1_u64 << self.cooldowns.min(3));
        self.cooldowns = self.cooldowns.saturating_add(1);
        self.timeouts = 0;
        self.cooldown_until = Some(now + Duration::from_secs(seconds));
    }

    pub fn timeout(&mut self, now: Instant) {
        self.timeouts = self.timeouts.saturating_add(1);
        if self.timeouts >= 2 {
            self.rate_limited(now);
        }
    }

    pub fn success(&mut self) {
        *self = Self::default();
    }

    pub fn other_failure(&mut self) {
        self.timeouts = 0;
    }
}

pub fn preferred_start_index(endpoint_count: usize, now_secs: u64, interval_secs: u64) -> usize {
    if endpoint_count <= 1 || interval_secs == 0 {
        return 0;
    }
    let count = u64::try_from(endpoint_count).unwrap_or(1);
    usize::try_from((now_secs / interval_secs) % count).unwrap_or(0)
}

pub fn available_from(
    endpoints: &[String],
    health: &HashMap<String, EndpointHealth>,
    now: Instant,
    start_index: usize,
) -> Vec<usize> {
    available_where(endpoints, health, now, start_index, |_| true)
}

pub(crate) fn available_where(
    endpoints: &[String],
    health: &HashMap<String, EndpointHealth>,
    now: Instant,
    start_index: usize,
    eligible: impl Fn(usize) -> bool,
) -> Vec<usize> {
    let indices = || {
        (0..endpoints.len())
            .map(|offset| (start_index + offset) % endpoints.len())
            .filter(|&index| eligible(index))
    };
    let available: Vec<_> = indices()
        .filter(|&index| {
            health
                .get(&endpoints[index])
                .is_none_or(|item| item.cooldown_until.is_none_or(|until| until <= now))
        })
        .collect();
    if !available.is_empty() {
        return available;
    }
    indices()
        .min_by_key(|&index| {
            health
                .get(&endpoints[index])
                .and_then(|item| item.cooldown_until)
        })
        .map(|index| vec![index])
        .unwrap_or_default()
}
