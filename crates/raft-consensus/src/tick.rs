use rand::Rng;
use std::time::Duration;

/// Timer management for Raft election timeouts and heartbeat intervals.
#[derive(Debug, Clone)]
pub struct TickConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
}

impl TickConfig {
    pub fn new(
        election_timeout_min_ms: u64,
        election_timeout_max_ms: u64,
        heartbeat_interval_ms: u64,
    ) -> Self {
        Self {
            election_timeout_min: Duration::from_millis(election_timeout_min_ms),
            election_timeout_max: Duration::from_millis(election_timeout_max_ms),
            heartbeat_interval: Duration::from_millis(heartbeat_interval_ms),
        }
    }

    /// Get the max election timeout in milliseconds (used for transfer timeout).
    pub fn election_timeout_max_ms(&self) -> u64 {
        self.election_timeout_max.as_millis() as u64
    }

    /// Generate a randomized election timeout to prevent split votes.
    pub fn random_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let min = self.election_timeout_min.as_millis() as u64;
        let max = self.election_timeout_max.as_millis() as u64;
        Duration::from_millis(rng.gen_range(min..=max))
    }
}

impl Default for TickConfig {
    fn default() -> Self {
        Self::new(150, 300, 50)
    }
}
