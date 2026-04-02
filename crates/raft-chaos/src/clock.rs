use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

/// Simulated clock with skew injection for testing time-dependent behavior.
///
/// Used to test:
/// - **Clock skew**: nodes with drifted clocks (election timeouts fire at wrong times)
/// - **Clock jump**: sudden forward/backward time jumps
/// - **Frozen clock**: time stops advancing (simulates a suspended VM)
pub struct ClockSim {
    /// Clock offset in milliseconds (positive = ahead, negative = behind).
    offset_ms: AtomicI64,
    /// Whether the clock is frozen (time stops advancing).
    frozen: std::sync::atomic::AtomicBool,
    /// The instant when the clock was frozen.
    frozen_at: parking_lot::RwLock<Option<Instant>>,
}

impl ClockSim {
    pub fn new() -> Self {
        Self {
            offset_ms: AtomicI64::new(0),
            frozen: std::sync::atomic::AtomicBool::new(false),
            frozen_at: parking_lot::RwLock::new(None),
        }
    }

    /// Get the current simulated time.
    pub fn now(&self) -> Instant {
        if self.frozen.load(Ordering::SeqCst) {
            if let Some(frozen_at) = *self.frozen_at.read() {
                return frozen_at;
            }
        }

        let real_now = Instant::now();
        let offset = self.offset_ms.load(Ordering::SeqCst);

        if offset >= 0 {
            real_now + Duration::from_millis(offset as u64)
        } else {
            real_now
                .checked_sub(Duration::from_millis((-offset) as u64))
                .unwrap_or(real_now)
        }
    }

    /// Set clock skew in milliseconds.
    /// Positive = clock is ahead, negative = clock is behind.
    pub fn set_offset(&self, ms: i64) {
        self.offset_ms.store(ms, Ordering::SeqCst);
    }

    /// Get current offset in milliseconds.
    pub fn offset(&self) -> i64 {
        self.offset_ms.load(Ordering::SeqCst)
    }

    /// Jump the clock forward by the given duration.
    pub fn jump_forward(&self, ms: u64) {
        self.offset_ms.fetch_add(ms as i64, Ordering::SeqCst);
    }

    /// Jump the clock backward by the given duration.
    pub fn jump_backward(&self, ms: u64) {
        self.offset_ms.fetch_sub(ms as i64, Ordering::SeqCst);
    }

    /// Freeze the clock (time stops advancing).
    pub fn freeze(&self) {
        *self.frozen_at.write() = Some(self.now());
        self.frozen.store(true, Ordering::SeqCst);
    }

    /// Unfreeze the clock (time resumes).
    pub fn unfreeze(&self) {
        self.frozen.store(false, Ordering::SeqCst);
        *self.frozen_at.write() = None;
    }

    /// Whether the clock is currently frozen.
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    /// Check if a duration has elapsed since `since`, accounting for skew.
    pub fn elapsed_since(&self, since: Instant) -> Duration {
        let now = self.now();
        now.saturating_duration_since(since)
    }

    /// Reset clock to normal (no offset, not frozen).
    pub fn reset(&self) {
        self.offset_ms.store(0, Ordering::SeqCst);
        self.frozen.store(false, Ordering::SeqCst);
        *self.frozen_at.write() = None;
    }
}

impl Default for ClockSim {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_skew() {
        let clock = ClockSim::new();
        let before = Instant::now();
        let sim_now = clock.now();
        let after = Instant::now();

        // Simulated time should be very close to real time
        assert!(sim_now >= before);
        assert!(sim_now <= after + Duration::from_millis(1));
    }

    #[test]
    fn positive_offset() {
        let clock = ClockSim::new();
        clock.set_offset(1000); // 1 second ahead

        let real_now = Instant::now();
        let sim_now = clock.now();

        // Simulated time should be ~1s ahead
        let diff = sim_now.duration_since(real_now);
        assert!(diff >= Duration::from_millis(900));
        assert!(diff <= Duration::from_millis(1100));
    }

    #[test]
    fn jump_forward() {
        let clock = ClockSim::new();
        clock.jump_forward(500);
        assert_eq!(clock.offset(), 500);

        clock.jump_forward(300);
        assert_eq!(clock.offset(), 800);
    }

    #[test]
    fn jump_backward() {
        let clock = ClockSim::new();
        clock.set_offset(1000);
        clock.jump_backward(500);
        assert_eq!(clock.offset(), 500);
    }

    #[test]
    fn freeze_and_unfreeze() {
        let clock = ClockSim::new();

        clock.freeze();
        assert!(clock.is_frozen());

        let t1 = clock.now();
        std::thread::sleep(Duration::from_millis(50));
        let t2 = clock.now();

        // Time should not advance while frozen
        assert_eq!(t1, t2);

        clock.unfreeze();
        assert!(!clock.is_frozen());

        std::thread::sleep(Duration::from_millis(10));
        let t3 = clock.now();
        assert!(t3 > t2);
    }

    #[test]
    fn reset() {
        let clock = ClockSim::new();
        clock.set_offset(5000);
        clock.freeze();

        clock.reset();

        assert_eq!(clock.offset(), 0);
        assert!(!clock.is_frozen());
    }
}
