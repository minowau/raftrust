use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Simulated disk failures for chaos testing.
///
/// Injects:
/// - **Write errors**: force all writes to fail
/// - **Disk full**: simulate ENOSPC after a byte threshold
/// - **Slow I/O**: track simulated disk latency
/// - **Corruption**: mark specific byte ranges as corrupted
pub struct DiskSim {
    /// Whether writes should fail.
    writes_failing: AtomicBool,
    /// Whether reads should fail.
    reads_failing: AtomicBool,
    /// Simulated disk capacity in bytes (0 = unlimited).
    capacity: AtomicU64,
    /// Simulated bytes written.
    bytes_written: AtomicU64,
    /// Simulated I/O latency in microseconds.
    io_latency_us: AtomicU64,
    /// Corruption ranges: (offset, length) pairs.
    corruptions: RwLock<Vec<(u64, u64)>>,
}

impl DiskSim {
    pub fn new() -> Self {
        Self {
            writes_failing: AtomicBool::new(false),
            reads_failing: AtomicBool::new(false),
            capacity: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            io_latency_us: AtomicU64::new(0),
            corruptions: RwLock::new(Vec::new()),
        }
    }

    /// Force all writes to fail.
    pub fn fail_writes(&self) {
        self.writes_failing.store(true, Ordering::SeqCst);
    }

    /// Restore write capability.
    pub fn restore_writes(&self) {
        self.writes_failing.store(false, Ordering::SeqCst);
    }

    /// Force all reads to fail.
    pub fn fail_reads(&self) {
        self.reads_failing.store(true, Ordering::SeqCst);
    }

    /// Restore read capability.
    pub fn restore_reads(&self) {
        self.reads_failing.store(false, Ordering::SeqCst);
    }

    /// Set disk capacity for ENOSPC simulation. 0 = unlimited.
    pub fn set_capacity(&self, bytes: u64) {
        self.capacity.store(bytes, Ordering::SeqCst);
    }

    /// Set simulated I/O latency.
    pub fn set_io_latency_us(&self, us: u64) {
        self.io_latency_us.store(us, Ordering::SeqCst);
    }

    /// Check if a write of `size` bytes should succeed.
    pub fn check_write(&self, size: u64) -> Result<(), DiskError> {
        if self.writes_failing.load(Ordering::SeqCst) {
            return Err(DiskError::WriteFailed);
        }

        let capacity = self.capacity.load(Ordering::SeqCst);
        if capacity > 0 {
            let current = self.bytes_written.load(Ordering::SeqCst);
            if current + size > capacity {
                return Err(DiskError::DiskFull {
                    capacity,
                    used: current,
                    requested: size,
                });
            }
        }

        self.bytes_written.fetch_add(size, Ordering::SeqCst);
        Ok(())
    }

    /// Check if a read should succeed.
    pub fn check_read(&self) -> Result<(), DiskError> {
        if self.reads_failing.load(Ordering::SeqCst) {
            return Err(DiskError::ReadFailed);
        }
        Ok(())
    }

    /// Record a corruption at a specific offset.
    pub fn inject_corruption(&self, offset: u64, length: u64) {
        self.corruptions.write().push((offset, length));
    }

    /// Check if a range is corrupted.
    pub fn is_corrupted(&self, offset: u64, length: u64) -> bool {
        let corruptions = self.corruptions.read();
        corruptions.iter().any(|&(c_off, c_len)| {
            let c_end = c_off + c_len;
            let r_end = offset + length;
            offset < c_end && r_end > c_off
        })
    }

    /// Get simulated I/O latency.
    pub fn io_latency(&self) -> std::time::Duration {
        let us = self.io_latency_us.load(Ordering::SeqCst);
        std::time::Duration::from_micros(us)
    }

    /// Reset all simulated failures.
    pub fn reset(&self) {
        self.writes_failing.store(false, Ordering::SeqCst);
        self.reads_failing.store(false, Ordering::SeqCst);
        self.capacity.store(0, Ordering::SeqCst);
        self.bytes_written.store(0, Ordering::SeqCst);
        self.io_latency_us.store(0, Ordering::SeqCst);
        self.corruptions.write().clear();
    }

    /// Total bytes written through this simulator.
    pub fn total_bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::SeqCst)
    }
}

impl Default for DiskSim {
    fn default() -> Self {
        Self::new()
    }
}

/// Disk simulation error types.
#[derive(Debug, PartialEq, Eq)]
pub enum DiskError {
    WriteFailed,
    ReadFailed,
    DiskFull {
        capacity: u64,
        used: u64,
        requested: u64,
    },
    Corrupted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_operations() {
        let disk = DiskSim::new();
        assert!(disk.check_write(100).is_ok());
        assert!(disk.check_read().is_ok());
    }

    #[test]
    fn write_failure() {
        let disk = DiskSim::new();
        disk.fail_writes();
        assert_eq!(disk.check_write(1), Err(DiskError::WriteFailed));

        disk.restore_writes();
        assert!(disk.check_write(1).is_ok());
    }

    #[test]
    fn read_failure() {
        let disk = DiskSim::new();
        disk.fail_reads();
        assert_eq!(disk.check_read(), Err(DiskError::ReadFailed));

        disk.restore_reads();
        assert!(disk.check_read().is_ok());
    }

    #[test]
    fn disk_full() {
        let disk = DiskSim::new();
        disk.set_capacity(100);

        assert!(disk.check_write(50).is_ok());
        assert!(disk.check_write(50).is_ok());
        assert!(disk.check_write(1).is_err()); // Over capacity
    }

    #[test]
    fn corruption_detection() {
        let disk = DiskSim::new();
        disk.inject_corruption(100, 50); // bytes 100-149 corrupted

        assert!(disk.is_corrupted(100, 10)); // Within corrupt range
        assert!(disk.is_corrupted(120, 50)); // Overlaps
        assert!(disk.is_corrupted(90, 20)); // Overlaps start
        assert!(!disk.is_corrupted(0, 50)); // Before
        assert!(!disk.is_corrupted(200, 50)); // After
    }

    #[test]
    fn io_latency() {
        let disk = DiskSim::new();
        assert_eq!(disk.io_latency(), std::time::Duration::ZERO);

        disk.set_io_latency_us(5000);
        assert_eq!(disk.io_latency(), std::time::Duration::from_millis(5));
    }

    #[test]
    fn reset_clears_everything() {
        let disk = DiskSim::new();
        disk.fail_writes();
        disk.fail_reads();
        disk.set_capacity(100);
        disk.inject_corruption(0, 50);

        disk.reset();

        assert!(disk.check_write(1).is_ok());
        assert!(disk.check_read().is_ok());
        assert!(!disk.is_corrupted(0, 50));
    }

    #[test]
    fn bytes_written_tracking() {
        let disk = DiskSim::new();
        disk.check_write(100).unwrap();
        disk.check_write(200).unwrap();
        assert_eq!(disk.total_bytes_written(), 300);
    }
}
