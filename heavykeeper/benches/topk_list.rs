use criterion::{criterion_group, criterion_main, Criterion};
use heavykeeper::TopK;
use rand::prelude::*;
use std::hint::black_box;

// Benchmark TopK::list() to exercise TopKQueue::iter() with large k.
fn benchmark_topk_list(c: &mut Criterion) {
    let mut rng = rand::rng();

    let k = 5_000;
    let width = 10_000;
    let depth = 4;
    let decay = 0.95;

    let mut topk = TopK::new(k, width, depth, decay);

    // Fill the structure with more than k distinct keys so the priority
    // queue is full and iter() / list() operate on O(k) entries.
    for _ in 0..(k * 2) {
        let key: u64 = rng.random();
        topk.add(&key, 1);
    }

    let mut group = c.benchmark_group("TopK_list");
    group.sample_size(40);
    group.bench_function("list_k_5000", |b| {
        b.iter(|| {
            // list() internally uses TopKQueue::iter(); black_box to
            // prevent the optimizer from eliminating the call.
            black_box(topk.list());
        });
    });
    group.finish();
}

criterion_group!(benches, benchmark_topk_list);
criterion_main!(benches);
