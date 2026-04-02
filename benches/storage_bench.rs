use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use raft_mvcc::mvcc::MvccStore;
use raft_storage::lsm::{LsmConfig, LsmTree};
use std::sync::Arc;

fn create_store(dir: &std::path::Path) -> Arc<MvccStore> {
    let engine = Arc::new(
        LsmTree::open(
            dir,
            LsmConfig {
                memtable_size_limit: 4 * 1024 * 1024, // 4MB
                block_size: 4096,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    Arc::new(MvccStore::new(engine))
}

fn bench_sequential_puts(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_put");
    group.throughput(Throughput::Elements(1));

    group.bench_function("sequential_put", |b| {
        let dir = tempfile::tempdir().unwrap();
        let store = create_store(dir.path());
        let mut i = 0u64;

        b.iter(|| {
            let key = format!("key-{:010}", i);
            let value = format!("value-{:010}", i);
            store.put(key.as_bytes(), value.as_bytes()).unwrap();
            i += 1;
        });
    });

    group.finish();
}

fn bench_sequential_gets(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_get");
    group.throughput(Throughput::Elements(1));

    group.bench_function("sequential_get", |b| {
        let dir = tempfile::tempdir().unwrap();
        let store = create_store(dir.path());

        // Pre-populate 10k keys
        for i in 0..10_000u64 {
            let key = format!("key-{:010}", i);
            let value = format!("value-{:010}", i);
            store.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key-{:010}", i % 10_000);
            let _result = store.get(key.as_bytes()).unwrap();
            i += 1;
        });
    });

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_mixed");
    group.throughput(Throughput::Elements(1));

    group.bench_function("80_read_20_write", |b| {
        let dir = tempfile::tempdir().unwrap();
        let store = create_store(dir.path());

        // Pre-populate
        for i in 0..1_000u64 {
            let key = format!("key-{:010}", i);
            store.put(key.as_bytes(), b"value").unwrap();
        }

        let mut i = 0u64;
        b.iter(|| {
            if i.is_multiple_of(5) {
                // 20% writes
                let key = format!("key-{:010}", i);
                store.put(key.as_bytes(), b"updated").unwrap();
            } else {
                // 80% reads
                let key = format!("key-{:010}", i % 1_000);
                let _result = store.get(key.as_bytes()).unwrap();
            }
            i += 1;
        });
    });

    group.finish();
}

fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_scan");

    group.bench_function("scan_100_keys", |b| {
        let dir = tempfile::tempdir().unwrap();
        let store = create_store(dir.path());

        // Pre-populate with sorted keys
        for i in 0..1_000u64 {
            let key = format!("key-{:010}", i);
            store.put(key.as_bytes(), b"value").unwrap();
        }

        b.iter(|| {
            let _results = store.scan(b"key-0000000100", b"key-0000000200").unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_puts,
    bench_sequential_gets,
    bench_mixed_workload,
    bench_range_scan
);
criterion_main!(benches);
