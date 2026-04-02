use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};

/// All Prometheus metrics for the distributed KV store.
///
/// Organized by subsystem:
/// - `raft_*`: Consensus metrics (elections, replication, commits)
/// - `kv_*`: Key-value operation metrics (gets, puts, deletes)
/// - `storage_*`: Storage engine metrics (compaction, WAL, SSTables)
/// - `lease_*`: Lease metrics (active leases, expirations)
/// - `watch_*`: Watch metrics (active watchers, events published)
pub struct Metrics {
    pub registry: Registry,

    // ── Raft Consensus ──
    pub raft_role: IntGauge,
    pub raft_term: IntGauge,
    pub raft_commit_index: IntGauge,
    pub raft_applied_index: IntGauge,
    pub raft_leader_id: IntGauge,
    pub raft_cluster_size: IntGauge,
    pub raft_elections_total: IntCounter,
    pub raft_elections_won: IntCounter,
    pub raft_leader_changes_total: IntCounter,
    pub raft_proposals_total: IntCounter,
    pub raft_proposals_failed: IntCounter,
    pub raft_replication_latency: Histogram,
    pub raft_heartbeat_latency: Histogram,
    pub raft_snapshot_count: IntCounter,

    // ── KV Operations ──
    pub kv_ops_total: IntCounterVec,
    pub kv_op_latency: HistogramVec,

    // ── Storage Engine ──
    pub storage_compaction_total: IntCounter,
    pub storage_compaction_duration: Histogram,
    pub storage_wal_bytes_written: IntCounter,
    pub storage_sstable_count: IntGauge,
    pub storage_memtable_size_bytes: IntGauge,

    // ── Leases ──
    pub lease_active: IntGauge,
    pub lease_grants_total: IntCounter,
    pub lease_revokes_total: IntCounter,
    pub lease_expirations_total: IntCounter,

    // ── Watch ──
    pub watch_active: IntGauge,
    pub watch_events_published: IntCounter,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        // Raft consensus metrics
        let raft_role = IntGauge::new(
            "raft_role",
            "Current Raft role (0=Follower, 1=Candidate, 2=Leader)",
        )
        .unwrap();
        let raft_term = IntGauge::new("raft_term", "Current Raft term").unwrap();
        let raft_commit_index =
            IntGauge::new("raft_commit_index", "Highest committed log index").unwrap();
        let raft_applied_index =
            IntGauge::new("raft_applied_index", "Highest applied log index").unwrap();
        let raft_leader_id =
            IntGauge::new("raft_leader_id", "Current leader node ID (0 if unknown)").unwrap();
        let raft_cluster_size =
            IntGauge::new("raft_cluster_size", "Number of nodes in the cluster").unwrap();
        let raft_elections_total =
            IntCounter::new("raft_elections_total", "Total elections started").unwrap();
        let raft_elections_won =
            IntCounter::new("raft_elections_won", "Total elections won").unwrap();
        let raft_leader_changes_total =
            IntCounter::new("raft_leader_changes_total", "Total leader changes observed").unwrap();
        let raft_proposals_total =
            IntCounter::new("raft_proposals_total", "Total proposals submitted").unwrap();
        let raft_proposals_failed =
            IntCounter::new("raft_proposals_failed", "Total proposals that failed").unwrap();
        let raft_replication_latency = Histogram::with_opts(
            HistogramOpts::new(
                "raft_replication_latency_seconds",
                "Log replication latency",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        )
        .unwrap();
        let raft_heartbeat_latency = Histogram::with_opts(
            HistogramOpts::new(
                "raft_heartbeat_latency_seconds",
                "Heartbeat round-trip latency",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1]),
        )
        .unwrap();
        let raft_snapshot_count =
            IntCounter::new("raft_snapshot_count", "Total snapshots created").unwrap();

        // KV operation metrics
        let kv_ops_total = IntCounterVec::new(
            Opts::new("kv_ops_total", "Total KV operations by type"),
            &["op"],
        )
        .unwrap();
        let kv_op_latency = HistogramVec::new(
            HistogramOpts::new("kv_op_latency_seconds", "KV operation latency by type")
                .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
            &["op"],
        )
        .unwrap();

        // Storage engine metrics
        let storage_compaction_total =
            IntCounter::new("storage_compaction_total", "Total compactions run").unwrap();
        let storage_compaction_duration = Histogram::with_opts(
            HistogramOpts::new("storage_compaction_duration_seconds", "Compaction duration")
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
        )
        .unwrap();
        let storage_wal_bytes_written =
            IntCounter::new("storage_wal_bytes_written", "Total bytes written to WAL").unwrap();
        let storage_sstable_count =
            IntGauge::new("storage_sstable_count", "Current number of SSTables").unwrap();
        let storage_memtable_size_bytes = IntGauge::new(
            "storage_memtable_size_bytes",
            "Current MemTable size in bytes",
        )
        .unwrap();

        // Lease metrics
        let lease_active = IntGauge::new("lease_active", "Number of active leases").unwrap();
        let lease_grants_total =
            IntCounter::new("lease_grants_total", "Total leases granted").unwrap();
        let lease_revokes_total =
            IntCounter::new("lease_revokes_total", "Total leases revoked").unwrap();
        let lease_expirations_total =
            IntCounter::new("lease_expirations_total", "Total leases expired").unwrap();

        // Watch metrics
        let watch_active = IntGauge::new("watch_active", "Number of active watchers").unwrap();
        let watch_events_published =
            IntCounter::new("watch_events_published", "Total watch events published").unwrap();

        // Register all metrics
        let r = &registry;
        r.register(Box::new(raft_role.clone())).unwrap();
        r.register(Box::new(raft_term.clone())).unwrap();
        r.register(Box::new(raft_commit_index.clone())).unwrap();
        r.register(Box::new(raft_applied_index.clone())).unwrap();
        r.register(Box::new(raft_leader_id.clone())).unwrap();
        r.register(Box::new(raft_cluster_size.clone())).unwrap();
        r.register(Box::new(raft_elections_total.clone())).unwrap();
        r.register(Box::new(raft_elections_won.clone())).unwrap();
        r.register(Box::new(raft_leader_changes_total.clone()))
            .unwrap();
        r.register(Box::new(raft_proposals_total.clone())).unwrap();
        r.register(Box::new(raft_proposals_failed.clone())).unwrap();
        r.register(Box::new(raft_replication_latency.clone()))
            .unwrap();
        r.register(Box::new(raft_heartbeat_latency.clone()))
            .unwrap();
        r.register(Box::new(raft_snapshot_count.clone())).unwrap();
        r.register(Box::new(kv_ops_total.clone())).unwrap();
        r.register(Box::new(kv_op_latency.clone())).unwrap();
        r.register(Box::new(storage_compaction_total.clone()))
            .unwrap();
        r.register(Box::new(storage_compaction_duration.clone()))
            .unwrap();
        r.register(Box::new(storage_wal_bytes_written.clone()))
            .unwrap();
        r.register(Box::new(storage_sstable_count.clone())).unwrap();
        r.register(Box::new(storage_memtable_size_bytes.clone()))
            .unwrap();
        r.register(Box::new(lease_active.clone())).unwrap();
        r.register(Box::new(lease_grants_total.clone())).unwrap();
        r.register(Box::new(lease_revokes_total.clone())).unwrap();
        r.register(Box::new(lease_expirations_total.clone()))
            .unwrap();
        r.register(Box::new(watch_active.clone())).unwrap();
        r.register(Box::new(watch_events_published.clone()))
            .unwrap();

        Self {
            registry,
            raft_role,
            raft_term,
            raft_commit_index,
            raft_applied_index,
            raft_leader_id,
            raft_cluster_size,
            raft_elections_total,
            raft_elections_won,
            raft_leader_changes_total,
            raft_proposals_total,
            raft_proposals_failed,
            raft_replication_latency,
            raft_heartbeat_latency,
            raft_snapshot_count,
            kv_ops_total,
            kv_op_latency,
            storage_compaction_total,
            storage_compaction_duration,
            storage_wal_bytes_written,
            storage_sstable_count,
            storage_memtable_size_bytes,
            lease_active,
            lease_grants_total,
            lease_revokes_total,
            lease_expirations_total,
            watch_active,
            watch_events_published,
        }
    }

    /// Encode all metrics in Prometheus text exposition format.
    pub fn encode(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_creation() {
        let m = Metrics::new();
        assert_eq!(m.raft_term.get(), 0);
        m.raft_term.set(5);
        assert_eq!(m.raft_term.get(), 5);
    }

    #[test]
    fn metrics_encode() {
        let m = Metrics::new();
        m.raft_term.set(3);
        m.raft_proposals_total.inc();
        m.kv_ops_total.with_label_values(&["put"]).inc();

        let output = m.encode();
        assert!(output.contains("raft_term 3"));
        assert!(output.contains("raft_proposals_total 1"));
        assert!(output.contains("kv_ops_total{op=\"put\"} 1"));
    }

    #[test]
    fn histogram_records() {
        let m = Metrics::new();
        m.raft_replication_latency.observe(0.005);
        m.raft_replication_latency.observe(0.015);

        let output = m.encode();
        assert!(output.contains("raft_replication_latency_seconds"));
    }

    #[test]
    fn kv_op_labels() {
        let m = Metrics::new();
        m.kv_ops_total.with_label_values(&["get"]).inc_by(10);
        m.kv_ops_total.with_label_values(&["put"]).inc_by(5);
        m.kv_ops_total.with_label_values(&["delete"]).inc_by(2);

        let output = m.encode();
        assert!(output.contains("kv_ops_total{op=\"get\"} 10"));
        assert!(output.contains("kv_ops_total{op=\"put\"} 5"));
        assert!(output.contains("kv_ops_total{op=\"delete\"} 2"));
    }
}
