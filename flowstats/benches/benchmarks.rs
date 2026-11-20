// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 flowstats Contributors
// SPDX-FileContributor: https://github.com/vnvo/flowstats/blob/v0.1.2/src/benches/benchmarks.rs


//! Benchmarks for flowstats algorithms
//!
//! Run with: cargo bench --features full

// Require all features for benchmarks
#[cfg(not(all(
    feature = "frequency",
)))]
compile_error!("Benchmarks require all features. Run: cargo bench --features full");

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use flowstats::frequency::{CountMinSketch};
use flowstats::traits::{Sketch};

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
// Main
// ============================================================================

criterion_group!(
    benches,
    bench_cms,
);

criterion_main!(benches);
