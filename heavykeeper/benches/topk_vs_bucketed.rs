//! Head-to-head insert benchmark for `TopK`, `BucketedTopK`, and
//! `CuckooTopK` on identical streams, same parameters.
//! Workloads: Zipf(s=1.2) and uniform random.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;

use heavykeeper::{BucketedTopK, CuckooTopK, TopK};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Zipf};

const K: usize = 100;
const WIDTH: usize = 4096;
const DEPTH: usize = 4;
const DECAY: f64 = 0.9;
const SEED: u64 = 0xC0FFEE;

fn gen_zipf(n: usize, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let dist = Zipf::new(1_000_000.0, 1.2).expect("valid zipf");
    (0..n).map(|_| dist.sample(&mut rng) as u64).collect()
}

fn gen_uniform(n: usize, seed: u64, universe: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    use rand::Rng;
    (0..n).map(|_| rng.random_range(0..universe)).collect()
}

fn bench_workload(c: &mut Criterion, group_name: &str, data: &[u64]) {
    let mut group = c.benchmark_group(group_name);
    group.sample_size(30);
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.measurement_time(std::time::Duration::from_secs(8));
    group.throughput(criterion::Throughput::Elements(data.len() as u64));

    group.bench_with_input(BenchmarkId::new("TopK", data.len()), &data, |b, data| {
        b.iter_with_setup(
            || TopK::<u64>::with_seed(K, WIDTH, DEPTH, DECAY, SEED),
            |mut topk| {
                for k in data.iter() {
                    topk.add(black_box(k), 1);
                }
            },
        );
    });

    group.bench_with_input(
        BenchmarkId::new("BucketedTopK", data.len()),
        &data,
        |b, data| {
            b.iter_with_setup(
                || BucketedTopK::<u64>::with_seed(K, WIDTH, DEPTH, DECAY, SEED),
                |mut topk| {
                    for k in data.iter() {
                        topk.add(black_box(k), 1);
                    }
                },
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("CuckooTopK", data.len()),
        &data,
        |b, data| {
            b.iter_with_setup(
                || CuckooTopK::<u64>::with_seed(K, WIDTH, DEPTH, DECAY, SEED),
                |mut topk| {
                    for k in data.iter() {
                        topk.add(black_box(k), 1);
                    }
                },
            );
        },
    );

    group.finish();
}

fn bench_zipf(c: &mut Criterion) {
    for &n in &[100_000usize, 1_000_000] {
        let data = gen_zipf(n, SEED);
        bench_workload(c, "zipf_u64", &data);
    }
}

fn bench_random(c: &mut Criterion) {
    for &n in &[100_000usize, 1_000_000] {
        let data = gen_uniform(n, SEED, 100_000);
        bench_workload(c, "random_u64", &data);
    }
}

criterion_group!(benches, bench_zipf, bench_random);
criterion_main!(benches);
