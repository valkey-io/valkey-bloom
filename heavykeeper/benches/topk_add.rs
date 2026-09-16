use criterion::{criterion_group, criterion_main, Criterion};
use heavykeeper::TopK;
use rand::distr::StandardUniform;
use rand::prelude::*;
use std::hint::black_box;

fn benchmark_topk_add(c: &mut Criterion, num_adds: usize) {
    let mut rng = rand::rng();
    let mut topk = TopK::new(10, 1024, 2, 0.95);

    let mut data = vec![];
    for _ in 0..num_adds {
        let key = (rng.sample::<f64, StandardUniform>(StandardUniform).abs() * 100_000.0) as u64;
        data.push(key);
    }

    let mut group = c.benchmark_group(format!("TopK_Add_{}", num_adds));
    group.sample_size(60);
    group.warm_up_time(std::time::Duration::from_secs(3));
    group.measurement_time(std::time::Duration::from_secs(10));

    group.bench_function("Add", |b| {
        b.iter(|| {
            for &key in data.iter() {
                topk.add(black_box(&key), 1);
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    benchmark_topk_add_1,
    benchmark_topk_add_10,
    benchmark_topk_add_100,
    benchmark_topk_add_1_000,
    benchmark_topk_add_10_000,
    benchmark_topk_add_100_000,
    benchmark_topk_add_1_000_000
);
criterion_main!(benches);

fn benchmark_topk_add_1(c: &mut Criterion) {
    benchmark_topk_add(c, 1);
}

fn benchmark_topk_add_10(c: &mut Criterion) {
    benchmark_topk_add(c, 10);
}

fn benchmark_topk_add_100(c: &mut Criterion) {
    benchmark_topk_add(c, 100);
}

fn benchmark_topk_add_1_000(c: &mut Criterion) {
    benchmark_topk_add(c, 1_000);
}

fn benchmark_topk_add_10_000(c: &mut Criterion) {
    benchmark_topk_add(c, 10_000);
}

fn benchmark_topk_add_100_000(c: &mut Criterion) {
    benchmark_topk_add(c, 100_000);
}

fn benchmark_topk_add_1_000_000(c: &mut Criterion) {
    benchmark_topk_add(c, 1_000_000);
}
