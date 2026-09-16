//! Benchmarks for flowstats algorithms
//!
//! Run with: cargo bench --features full

// Require all features for benchmarks
#[cfg(not(all(
    feature = "cardinality",
    feature = "frequency",
    feature = "quantiles",
    feature = "membership",
    feature = "sampling",
    feature = "statistics"
)))]
compile_error!("Benchmarks require all features. Run: cargo bench --features full");

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use flowstats::cardinality::HyperLogLog;
use flowstats::frequency::{CountMinSketch, SpaceSaving};
use flowstats::membership::BloomFilter;
use flowstats::quantiles::TDigest;
use flowstats::sampling::ReservoirSampler;
use flowstats::statistics::RunningStats;
use flowstats::traits::{CardinalitySketch, HeavyHitters, QuantileSketch, Sketch};

// ============================================================================
// HyperLogLog Benchmarks
// ============================================================================

fn bench_hll(c: &mut Criterion) {
    let mut group = c.benchmark_group("hyperloglog");
    group.throughput(Throughput::Elements(1));

    for precision in [10, 12, 14, 16] {
        group.bench_function(format!("insert_p{}", precision), |b| {
            let mut hll = HyperLogLog::new(precision);
            let mut i = 0u64;
            b.iter(|| {
                hll.insert(&i.to_string());
                i = i.wrapping_add(1);
            });
        });
    }

    group.bench_function("estimate", |b| {
        let mut hll = HyperLogLog::new(14);
        for i in 0..100_000u64 {
            hll.insert(&i.to_string());
        }
        b.iter(|| black_box(hll.estimate()));
    });

    group.bench_function("merge", |b| {
        let mut hll1 = HyperLogLog::new(14);
        let mut hll2 = HyperLogLog::new(14);
        for i in 0..10_000u64 {
            hll1.insert(&i.to_string());
            hll2.insert(&(i + 10_000).to_string());
        }
        b.iter(|| {
            let mut h = hll1.clone();
            h.merge(black_box(&hll2)).unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Count-Min Sketch Benchmarks
// ============================================================================

fn bench_cms(c: &mut Criterion) {
    let mut group = c.benchmark_group("count_min_sketch");
    group.throughput(Throughput::Elements(1));

    group.bench_function("add", |b| {
        let mut cms = CountMinSketch::new(0.001, 0.01);
        let mut i = 0u64;
        b.iter(|| {
            cms.add(i.to_string().as_bytes(), 1);
            i = i.wrapping_add(1);
        });
    });

    group.bench_function("estimate", |b| {
        let mut cms = CountMinSketch::new(0.001, 0.01);
        for i in 0..100_000u64 {
            cms.add(i.to_string().as_bytes(), 1);
        }
        b.iter(|| black_box(cms.estimate(b"12345")));
    });

    group.bench_function("merge", |b| {
        let mut cms1 = CountMinSketch::new(0.001, 0.01);
        let mut cms2 = CountMinSketch::new(0.001, 0.01);
        for i in 0..10_000u64 {
            cms1.add(i.to_string().as_bytes(), 1);
            cms2.add((i + 10_000).to_string().as_bytes(), 1);
        }
        b.iter(|| {
            let mut c = cms1.clone();
            c.merge(black_box(&cms2)).unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Space-Saving Benchmarks
// ============================================================================

fn bench_space_saving(c: &mut Criterion) {
    let mut group = c.benchmark_group("space_saving");
    group.throughput(Throughput::Elements(1));

    for k in [100, 1000] {
        group.bench_function(format!("add_k{}", k), |b| {
            let mut ss: SpaceSaving<String> = SpaceSaving::new(k);
            let mut i = 0u64;
            b.iter(|| {
                ss.add(i.to_string());
                i = i.wrapping_add(1);
            });
        });
    }

    group.bench_function("top_k", |b| {
        let mut ss: SpaceSaving<String> = SpaceSaving::new(1000);
        for i in 0..100_000u64 {
            ss.add((i % 10_000).to_string());
        }
        b.iter(|| black_box(ss.top_k(10)));
    });

    group.finish();
}

// ============================================================================
// t-digest Benchmarks
// ============================================================================

fn bench_tdigest(c: &mut Criterion) {
    let mut group = c.benchmark_group("tdigest");
    group.throughput(Throughput::Elements(1));

    for compression in [50.0, 100.0, 200.0] {
        group.bench_function(format!("add_c{}", compression as u32), |b| {
            let mut td = TDigest::new(compression);
            let mut i = 0u64;
            b.iter(|| {
                td.add((i as f64) * 0.001);
                i = i.wrapping_add(1);
            });
        });
    }

    group.bench_function("quantile", |b| {
        let mut td = TDigest::new(100.0);
        for i in 0..100_000u64 {
            td.add(i as f64);
        }
        b.iter(|| black_box(td.quantile(0.99)));
    });

    group.bench_function("merge", |b| {
        let mut td1 = TDigest::new(100.0);
        let mut td2 = TDigest::new(100.0);
        for i in 0..10_000u64 {
            td1.add(i as f64);
            td2.add((i + 10_000) as f64);
        }
        b.iter(|| {
            let mut t = td1.clone();
            t.merge(black_box(&td2)).unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Bloom Filter Benchmarks
// ============================================================================

fn bench_bloom(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_filter");
    group.throughput(Throughput::Elements(1));

    group.bench_function("insert", |b| {
        let mut bloom = BloomFilter::new(1_000_000, 0.01);
        let mut i = 0u64;
        b.iter(|| {
            bloom.insert(i.to_string().as_bytes());
            i = i.wrapping_add(1);
        });
    });

    group.bench_function("contains_hit", |b| {
        let mut bloom = BloomFilter::new(100_000, 0.01);
        for i in 0..100_000u64 {
            bloom.insert(i.to_string().as_bytes());
        }
        let mut i = 0u64;
        b.iter(|| {
            let result = bloom.contains((i % 100_000).to_string().as_bytes());
            i = i.wrapping_add(1);
            black_box(result)
        });
    });

    group.bench_function("contains_miss", |b| {
        let mut bloom = BloomFilter::new(100_000, 0.01);
        for i in 0..100_000u64 {
            bloom.insert(i.to_string().as_bytes());
        }
        let mut i = 1_000_000u64;
        b.iter(|| {
            let result = bloom.contains(i.to_string().as_bytes());
            i = i.wrapping_add(1);
            black_box(result)
        });
    });

    group.finish();
}

// ============================================================================
// Reservoir Sampler Benchmarks
// ============================================================================

fn bench_reservoir(c: &mut Criterion) {
    let mut group = c.benchmark_group("reservoir");
    group.throughput(Throughput::Elements(1));

    for capacity in [100, 1000, 10000] {
        group.bench_function(format!("add_cap{}", capacity), |b| {
            let mut sampler: ReservoirSampler<u64> = ReservoirSampler::new(capacity);
            let mut i = 0u64;
            b.iter(|| {
                sampler.add(i);
                i = i.wrapping_add(1);
            });
        });
    }

    group.finish();
}

// ============================================================================
// Running Stats Benchmarks
// ============================================================================

fn bench_running_stats(c: &mut Criterion) {
    let mut group = c.benchmark_group("running_stats");
    group.throughput(Throughput::Elements(1));

    group.bench_function("add", |b| {
        let mut stats = RunningStats::new();
        let mut i = 0u64;
        b.iter(|| {
            stats.add(i as f64);
            i = i.wrapping_add(1);
        });
    });

    group.bench_function("query_all", |b| {
        let mut stats = RunningStats::new();
        for i in 0..100_000u64 {
            stats.add(i as f64);
        }
        b.iter(|| {
            black_box(stats.mean());
            black_box(stats.variance());
            black_box(stats.stddev());
            black_box(stats.min());
            black_box(stats.max());
        });
    });

    group.bench_function("merge", |b| {
        let mut s1 = RunningStats::new();
        let mut s2 = RunningStats::new();
        for i in 0..10_000u64 {
            s1.add(i as f64);
            s2.add((i + 10_000) as f64);
        }
        b.iter(|| {
            let mut s = s1.clone();
            s.merge(black_box(&s2)).unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Main
// ============================================================================

criterion_group!(
    benches,
    bench_hll,
    bench_cms,
    bench_space_saving,
    bench_tdigest,
    bench_bloom,
    bench_reservoir,
    bench_running_stats,
);

criterion_main!(benches);
