//! Side-by-side accuracy: canonical `TopK` vs `BucketedTopK` vs `CuckooTopK`.
//! Metrics: Top-K Hit Ratio plus paper-style heavy-hitter precision,
//! recall, and Average Relative Error.
//! Run with `cargo test --test accuracy_compare --release -- --nocapture`.

use std::collections::{HashMap, HashSet};

use heavykeeper::{BucketedTopK, CuckooTopK, TopK};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Zipf};

const ZIPF_N: f64 = 1_000_000.0;
const STREAM_LEN: usize = 5_000_000;
const K: usize = 100;
// 1024 cells (256*4) for 1M distinct keys — tight enough to force eviction.
const WIDTH: usize = 256;
const DEPTH: usize = 4;
const DECAY: f64 = 0.9;
const SEED: u64 = 0xACC04ACC;

const PAPER_STREAM_LEN: usize = 1_000_000;
const PAPER_K: usize = 512;
const PAPER_PHI: f64 = 0.0005;
const PAPER_WIDTH: usize = 256;
const PAPER_DEPTH: usize = 4;
const PAPER_DECAY: f64 = 0.9;

fn gen_zipf(s: f64, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let dist = Zipf::new(ZIPF_N, s).expect("valid zipf");
    (0..STREAM_LEN)
        .map(|_| dist.sample(&mut rng) as u64)
        .collect()
}

fn gen_zipf_len(len: usize, s: f64, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let dist = Zipf::new(ZIPF_N, s).expect("valid zipf");
    (0..len).map(|_| dist.sample(&mut rng) as u64).collect()
}

fn ground_truth(stream: &[u64]) -> HashMap<u64, u64> {
    let mut m = HashMap::with_capacity(stream.len());
    for k in stream {
        *m.entry(*k).or_insert(0u64) += 1;
    }
    m
}

fn true_top_k(truth: &HashMap<u64, u64>) -> Vec<(u64, u64)> {
    let mut v: Vec<_> = truth.iter().map(|(k, c)| (*k, *c)).collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    v.truncate(K);
    v
}

fn true_heavy_hitters(truth: &HashMap<u64, u64>, threshold: u64) -> HashSet<u64> {
    truth
        .iter()
        .filter_map(|(item, count)| (*count >= threshold).then_some(*item))
        .collect()
}

struct Metrics {
    hit_ratio: f64,
    are: f64,
}

struct PaperMetrics {
    precision: f64,
    recall: f64,
    are: f64,
    reported: usize,
    true_heavy_hitters: usize,
}

fn metrics_topk(stream: &[u64], truth: &HashMap<u64, u64>, true_set: &HashSet<u64>) -> Metrics {
    let mut sketch: TopK<u64> = TopK::with_seed(K, WIDTH, DEPTH, DECAY, 12345);
    for k in stream {
        sketch.add(k, 1);
    }
    score(&sketch.list(), truth, true_set, |n| (n.item, n.count))
}

fn metrics_bucketed(stream: &[u64], truth: &HashMap<u64, u64>, true_set: &HashSet<u64>) -> Metrics {
    let mut sketch: BucketedTopK<u64> = BucketedTopK::with_seed(K, WIDTH, DEPTH, DECAY, 12345);
    for k in stream {
        sketch.add(k, 1);
    }
    score(&sketch.list(), truth, true_set, |n| (n.item, n.count))
}

fn metrics_cuckoo(stream: &[u64], truth: &HashMap<u64, u64>, true_set: &HashSet<u64>) -> Metrics {
    let mut sketch: CuckooTopK<u64> = CuckooTopK::with_seed(K, WIDTH, DEPTH, DECAY, 12345);
    for k in stream {
        sketch.add(k, 1);
    }
    score(&sketch.list(), truth, true_set, |n| (n.item, n.count))
}

fn score<N>(
    reported: &[N],
    truth: &HashMap<u64, u64>,
    true_set: &HashSet<u64>,
    extract: impl Fn(&N) -> (u64, u64),
) -> Metrics {
    let pairs: Vec<(u64, u64)> = reported.iter().map(&extract).collect();
    let hits = pairs
        .iter()
        .filter(|(item, _)| true_set.contains(item))
        .count();
    let hit_ratio = hits as f64 / K as f64;

    // ARE skips false positives — true count of 0 has no meaningful ratio.
    let mut sum = 0.0;
    let mut n = 0;
    for (item, est) in &pairs {
        if let Some(&true_c) = truth.get(item) {
            if true_c > 0 {
                sum += (*est as f64 - true_c as f64).abs() / true_c as f64;
                n += 1;
            }
        }
    }
    let are = if n == 0 { 0.0 } else { sum / n as f64 };
    Metrics { hit_ratio, are }
}

fn score_paper_metrics(
    reported: Vec<(u64, u64)>,
    truth: &HashMap<u64, u64>,
    true_set: &HashSet<u64>,
    threshold: u64,
    estimate: impl Fn(u64) -> u64,
) -> PaperMetrics {
    let reported_set: HashSet<u64> = reported
        .into_iter()
        .filter_map(|(item, estimate)| (estimate >= threshold).then_some(item))
        .collect();

    let hits = reported_set.intersection(true_set).count();
    let precision = if reported_set.is_empty() {
        0.0
    } else {
        hits as f64 / reported_set.len() as f64
    };
    let recall = if true_set.is_empty() {
        0.0
    } else {
        hits as f64 / true_set.len() as f64
    };

    let relative_error_sum = true_set
        .iter()
        .map(|item| {
            let true_count = truth[item];
            let estimated_count = estimate(*item);
            true_count.abs_diff(estimated_count) as f64 / true_count as f64
        })
        .sum::<f64>();
    let are = if true_set.is_empty() {
        0.0
    } else {
        relative_error_sum / true_set.len() as f64
    };

    PaperMetrics {
        precision,
        recall,
        are,
        reported: reported_set.len(),
        true_heavy_hitters: true_set.len(),
    }
}

fn run_case(s: f64) -> (Metrics, Metrics, Metrics) {
    let stream = gen_zipf(s, SEED);
    let truth = ground_truth(&stream);
    let true_top = true_top_k(&truth);
    let true_set: HashSet<u64> = true_top.iter().map(|(k, _)| *k).collect();

    let m_topk = metrics_topk(&stream, &truth, &true_set);
    let m_bkt = metrics_bucketed(&stream, &truth, &true_set);
    let m_ck = metrics_cuckoo(&stream, &truth, &true_set);

    println!(
        "  Zipf s={s:<5}  TopK         hit_ratio={:.4}  ARE={:.6}",
        m_topk.hit_ratio, m_topk.are
    );
    println!(
        "  Zipf s={s:<5}  BucketedTopK hit_ratio={:.4}  ARE={:.6}",
        m_bkt.hit_ratio, m_bkt.are
    );
    println!(
        "  Zipf s={s:<5}  CuckooTopK   hit_ratio={:.4}  ARE={:.6}",
        m_ck.hit_ratio, m_ck.are
    );

    (m_topk, m_bkt, m_ck)
}

#[test]
fn compare_accuracy_zipf() {
    println!();
    println!(
        "Stream: {STREAM_LEN} items, ZIPF_N={ZIPF_N}, k={K}, width={WIDTH}, depth={DEPTH}, decay={DECAY}"
    );
    println!();

    let (t1, b1, c1) = run_case(2.0);
    assert!(
        t1.hit_ratio >= 0.80,
        "TopK s=2.0 hit_ratio {} < 0.80",
        t1.hit_ratio
    );
    assert!(
        b1.hit_ratio >= 0.80,
        "BucketedTopK s=2.0 hit_ratio {} < 0.80",
        b1.hit_ratio
    );
    assert!(
        c1.hit_ratio >= 0.80,
        "CuckooTopK s=2.0 hit_ratio {} < 0.80",
        c1.hit_ratio
    );

    let (t2, b2, c2) = run_case(1.2);
    assert!(
        t2.hit_ratio >= 0.50,
        "TopK s=1.2 hit_ratio {} < 0.50",
        t2.hit_ratio
    );
    assert!(
        b2.hit_ratio >= 0.50,
        "BucketedTopK s=1.2 hit_ratio {} < 0.50",
        b2.hit_ratio
    );
    assert!(
        c2.hit_ratio >= 0.50,
        "CuckooTopK s=1.2 hit_ratio {} < 0.50",
        c2.hit_ratio
    );

    let (t3, b3, c3) = run_case(1.05);
    assert!(
        t3.hit_ratio >= 0.20,
        "TopK s=1.05 hit_ratio {} < 0.20",
        t3.hit_ratio
    );
    assert!(
        b3.hit_ratio >= 0.20,
        "BucketedTopK s=1.05 hit_ratio {} < 0.20",
        b3.hit_ratio
    );
    assert!(
        c3.hit_ratio >= 0.20,
        "CuckooTopK s=1.05 hit_ratio {} < 0.20",
        c3.hit_ratio
    );

    for (label, m) in [
        ("TopK 2.0", &t1),
        ("BucketedTopK 2.0", &b1),
        ("CuckooTopK 2.0", &c1),
        ("TopK 1.2", &t2),
        ("BucketedTopK 1.2", &b2),
        ("CuckooTopK 1.2", &c2),
    ] {
        assert!(m.are < 1.0, "{label} ARE {} >= 1.0", m.are);
    }
}

#[test]
fn compare_paper_style_heavy_hitter_metrics() {
    let stream = gen_zipf_len(PAPER_STREAM_LEN, 1.2, SEED);
    let truth = ground_truth(&stream);
    let threshold = (PAPER_PHI * PAPER_STREAM_LEN as f64).ceil() as u64;
    let true_set = true_heavy_hitters(&truth, threshold);

    assert!(
        !true_set.is_empty(),
        "paper-style test should generate at least one true heavy hitter"
    );
    assert!(
        true_set.len() < PAPER_K,
        "PAPER_K={PAPER_K} must exceed true heavy hitter count {} so recall is not heap-capped",
        true_set.len()
    );

    let mut topk: TopK<u64> =
        TopK::with_seed(PAPER_K, PAPER_WIDTH, PAPER_DEPTH, PAPER_DECAY, 12345);
    let mut bucketed: BucketedTopK<u64> =
        BucketedTopK::with_seed(PAPER_K, PAPER_WIDTH, PAPER_DEPTH, PAPER_DECAY, 12345);
    let mut cuckoo: CuckooTopK<u64> =
        CuckooTopK::with_seed(PAPER_K, PAPER_WIDTH, PAPER_DEPTH, PAPER_DECAY, 12345);
    for item in &stream {
        topk.add(item, 1);
        bucketed.add(item, 1);
        cuckoo.add(item, 1);
    }

    let topk_reported: Vec<_> = topk.list().into_iter().map(|n| (n.item, n.count)).collect();
    let bucketed_reported: Vec<_> = bucketed
        .list()
        .into_iter()
        .map(|n| (n.item, n.count))
        .collect();
    let cuckoo_reported: Vec<_> = cuckoo
        .list()
        .into_iter()
        .map(|n| (n.item, n.count))
        .collect();

    let topk_metrics = score_paper_metrics(topk_reported, &truth, &true_set, threshold, |item| {
        topk.count(&item)
    });
    let bucketed_metrics =
        score_paper_metrics(bucketed_reported, &truth, &true_set, threshold, |item| {
            bucketed.count(&item)
        });
    let cuckoo_metrics =
        score_paper_metrics(cuckoo_reported, &truth, &true_set, threshold, |item| {
            cuckoo.count(&item)
        });

    println!();
    println!(
        "Paper-style Zipf s=1.2: stream={PAPER_STREAM_LEN}, phi={PAPER_PHI}, threshold={threshold}, true_hh={}",
        true_set.len()
    );
    println!(
        "  TopK         precision={:.4} recall={:.4} ARE={:.6} reported={} true_hh={}",
        topk_metrics.precision,
        topk_metrics.recall,
        topk_metrics.are,
        topk_metrics.reported,
        topk_metrics.true_heavy_hitters,
    );
    println!(
        "  BucketedTopK precision={:.4} recall={:.4} ARE={:.6} reported={} true_hh={}",
        bucketed_metrics.precision,
        bucketed_metrics.recall,
        bucketed_metrics.are,
        bucketed_metrics.reported,
        bucketed_metrics.true_heavy_hitters,
    );
    println!(
        "  CuckooTopK precision={:.4} recall={:.4} ARE={:.6} reported={} true_hh={}",
        cuckoo_metrics.precision,
        cuckoo_metrics.recall,
        cuckoo_metrics.are,
        cuckoo_metrics.reported,
        cuckoo_metrics.true_heavy_hitters,
    );

    assert!(
        topk_metrics.precision >= 0.85,
        "TopK precision {} < 0.85",
        topk_metrics.precision
    );
    assert!(
        topk_metrics.recall >= 0.75,
        "TopK recall {} < 0.75",
        topk_metrics.recall
    );
    assert!(
        topk_metrics.are < 0.25,
        "TopK ARE {} >= 0.25",
        topk_metrics.are
    );

    assert!(
        bucketed_metrics.precision >= 0.85,
        "BucketedTopK precision {} < 0.85",
        bucketed_metrics.precision
    );
    assert!(
        bucketed_metrics.recall >= 0.75,
        "BucketedTopK recall {} < 0.75",
        bucketed_metrics.recall
    );
    assert!(
        bucketed_metrics.are < 0.25,
        "BucketedTopK ARE {} >= 0.25",
        bucketed_metrics.are
    );

    assert!(
        cuckoo_metrics.precision >= 0.85,
        "CuckooTopK precision {} < 0.85",
        cuckoo_metrics.precision
    );
    assert!(
        cuckoo_metrics.recall >= 0.75,
        "CuckooTopK recall {} < 0.75",
        cuckoo_metrics.recall
    );
    assert!(
        cuckoo_metrics.are < 0.25,
        "CuckooTopK ARE {} >= 0.25",
        cuckoo_metrics.are
    );
}
