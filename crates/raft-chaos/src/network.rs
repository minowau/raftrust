use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashSet;
use std::time::Duration;

/// Simulated network with partition injection, latency, and message loss.
///
/// Used in chaos tests to simulate real-world network conditions:
/// - **Partitions**: isolate nodes from each other
/// - **Latency**: add delays to message delivery
/// - **Message loss**: randomly drop a percentage of messages
pub struct NetworkSim {
    /// Set of (from, to) pairs that are currently partitioned.
    partitions: RwLock<HashSet<(u64, u64)>>,
    /// Simulated one-way latency in milliseconds.
    latency_ms: RwLock<u64>,
    /// Message loss rate (0.0 to 1.0).
    loss_rate: RwLock<f64>,
}

impl NetworkSim {
    pub fn new() -> Self {
        Self {
            partitions: RwLock::new(HashSet::new()),
            latency_ms: RwLock::new(0),
            loss_rate: RwLock::new(0.0),
        }
    }

    /// Add a one-way partition: messages from `from` to `to` are dropped.
    pub fn partition(&self, from: u64, to: u64) {
        self.partitions.write().insert((from, to));
    }

    /// Add a bidirectional partition between two nodes.
    pub fn partition_bidirectional(&self, a: u64, b: u64) {
        let mut p = self.partitions.write();
        p.insert((a, b));
        p.insert((b, a));
    }

    /// Isolate a node from all others.
    pub fn isolate(&self, node: u64, all_nodes: &[u64]) {
        let mut p = self.partitions.write();
        for &other in all_nodes {
            if other != node {
                p.insert((node, other));
                p.insert((other, node));
            }
        }
    }

    /// Heal all partitions.
    pub fn heal_all(&self) {
        self.partitions.write().clear();
    }

    /// Heal a specific bidirectional partition.
    pub fn heal(&self, a: u64, b: u64) {
        let mut p = self.partitions.write();
        p.remove(&(a, b));
        p.remove(&(b, a));
    }

    /// Set simulated one-way latency.
    pub fn set_latency(&self, ms: u64) {
        *self.latency_ms.write() = ms;
    }

    /// Set message loss rate (0.0 = no loss, 1.0 = drop everything).
    pub fn set_loss_rate(&self, rate: f64) {
        *self.loss_rate.write() = rate.clamp(0.0, 1.0);
    }

    /// Check if a message from `from` to `to` should be delivered.
    /// Returns false if the link is partitioned or the message is randomly dropped.
    pub fn should_deliver(&self, from: u64, to: u64) -> bool {
        // Check partition
        if self.partitions.read().contains(&(from, to)) {
            return false;
        }

        // Check random loss
        let loss_rate = *self.loss_rate.read();
        if loss_rate > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen::<f64>() < loss_rate {
                return false;
            }
        }

        true
    }

    /// Get the simulated latency to apply to a message.
    pub fn delivery_delay(&self) -> Duration {
        let ms = *self.latency_ms.read();
        if ms > 0 {
            // Add some jitter (±20%)
            let mut rng = rand::thread_rng();
            let jitter = (ms as f64 * 0.2) as u64;
            let actual = if jitter > 0 {
                ms + rng.gen_range(0..=jitter) - jitter / 2
            } else {
                ms
            };
            Duration::from_millis(actual)
        } else {
            Duration::ZERO
        }
    }

    /// Number of active partitions.
    pub fn partition_count(&self) -> usize {
        self.partitions.read().len()
    }

    /// Check if two nodes are partitioned (one-way: from -> to).
    pub fn is_partitioned(&self, from: u64, to: u64) -> bool {
        self.partitions.read().contains(&(from, to))
    }
}

impl Default for NetworkSim {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_partition_delivers() {
        let net = NetworkSim::new();
        assert!(net.should_deliver(1, 2));
        assert!(net.should_deliver(2, 1));
    }

    #[test]
    fn one_way_partition() {
        let net = NetworkSim::new();
        net.partition(1, 2);

        assert!(!net.should_deliver(1, 2));
        assert!(net.should_deliver(2, 1)); // Other direction is fine
    }

    #[test]
    fn bidirectional_partition() {
        let net = NetworkSim::new();
        net.partition_bidirectional(1, 2);

        assert!(!net.should_deliver(1, 2));
        assert!(!net.should_deliver(2, 1));
        assert!(net.should_deliver(1, 3)); // Others unaffected
    }

    #[test]
    fn isolate_node() {
        let net = NetworkSim::new();
        net.isolate(2, &[1, 2, 3]);

        assert!(!net.should_deliver(2, 1));
        assert!(!net.should_deliver(2, 3));
        assert!(!net.should_deliver(1, 2));
        assert!(!net.should_deliver(3, 2));
        assert!(net.should_deliver(1, 3)); // 1<->3 still works
    }

    #[test]
    fn heal_partition() {
        let net = NetworkSim::new();
        net.partition_bidirectional(1, 2);
        assert!(!net.should_deliver(1, 2));

        net.heal(1, 2);
        assert!(net.should_deliver(1, 2));
        assert!(net.should_deliver(2, 1));
    }

    #[test]
    fn heal_all() {
        let net = NetworkSim::new();
        net.isolate(1, &[1, 2, 3]);
        assert!(net.partition_count() > 0);

        net.heal_all();
        assert_eq!(net.partition_count(), 0);
        assert!(net.should_deliver(1, 2));
    }

    #[test]
    fn full_loss_drops_all() {
        let net = NetworkSim::new();
        net.set_loss_rate(1.0);

        let mut delivered = 0;
        for _ in 0..100 {
            if net.should_deliver(1, 2) {
                delivered += 1;
            }
        }
        assert_eq!(delivered, 0);
    }

    #[test]
    fn zero_loss_delivers_all() {
        let net = NetworkSim::new();
        net.set_loss_rate(0.0);

        for _ in 0..100 {
            assert!(net.should_deliver(1, 2));
        }
    }

    #[test]
    fn partial_loss() {
        let net = NetworkSim::new();
        net.set_loss_rate(0.5);

        let mut delivered = 0;
        for _ in 0..1000 {
            if net.should_deliver(1, 2) {
                delivered += 1;
            }
        }
        // With 50% loss over 1000 trials, should be roughly 400-600
        assert!(delivered > 300 && delivered < 700);
    }

    #[test]
    fn latency_returns_duration() {
        let net = NetworkSim::new();
        assert_eq!(net.delivery_delay(), Duration::ZERO);

        net.set_latency(100);
        let delay = net.delivery_delay();
        // Should be roughly 80-120ms with jitter
        assert!(delay.as_millis() >= 80 && delay.as_millis() <= 120);
    }
}
