use heavykeeper::{BucketedTopK, CuckooTopK, TopK};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::time::{Duration, Instant};

const KEY_SIZE: usize = 13;
const TOP_K: usize = 1000;
const DECAY: f64 = 0.95;

#[allow(dead_code)]
fn read_in_trace(
    trace_prefix: &str,
    max_item_num: usize,
) -> io::Result<(Vec<Vec<u8>>, HashMap<Vec<u8>, u32>)> {
    let mut count = 0;
    let mut keys = Vec::new();
    let mut actual_flow_sizes = HashMap::new();

    let datafile_cnt = 0;
    let trace_file_path = format!("{}{}.dat", trace_prefix, datafile_cnt);
    println!("Start reading {}", trace_file_path);

    let file = File::open(&trace_file_path)?;
    let mut reader = BufReader::new(file);
    let mut temp = vec![0; KEY_SIZE];

    while reader.read_exact(&mut temp).is_ok() {
        let key = temp.clone();
        keys.push(key.clone());
        let counter = actual_flow_sizes.entry(key).or_insert(0);
        *counter += 1;
        count += 1;

        if count >= max_item_num {
            panic!(
                "The dataset has more than {} items, set a larger value for max_item_num",
                max_item_num
            );
        }
    }

    println!(
        "Finished reading {} ({} items), the dataset now has {} items",
        trace_file_path,
        count,
        keys.len()
    );

    Ok((keys, actual_flow_sizes))
}

fn read_in_traces(
    trace_prefix: &str,
    max_item_num: usize,
) -> io::Result<(Vec<Vec<u8>>, HashMap<Vec<u8>, u32>)> {
    let mut count = 0;
    let mut keys = Vec::new();
    let mut actual_flow_sizes = HashMap::new();

    for datafile_cnt in 0..=10 {
        let trace_file_path = format!("{}{}.dat", trace_prefix, datafile_cnt);
        println!("Start reading {}", trace_file_path);

        let file = File::open(&trace_file_path)?;
        let mut reader = BufReader::new(file);
        let mut temp = vec![0; KEY_SIZE];

        while reader.read_exact(&mut temp).is_ok() {
            let key = temp.clone();
            keys.push(key.clone());
            let counter = actual_flow_sizes.entry(key).or_insert(0);
            *counter += 1;
            count += 1;

            if count > max_item_num {
                panic!(
                    "The dataset has more than {} items, set a larger value for max_item_num",
                    max_item_num
                );
            }
        }

        println!(
            "Finished reading {} ({} items), the dataset now has {} items",
            trace_file_path,
            count,
            keys.len()
        );
    }

    Ok((keys, actual_flow_sizes))
}

fn format_flow(item: &[u8]) -> String {
    let src_ip = format!("{}.{}.{}.{}", item[0], item[1], item[2], item[3]);
    let src_port = u16::from_be_bytes([item[4], item[5]]);
    let dst_ip = format!("{}.{}.{}.{}", item[6], item[7], item[8], item[9]);
    let dst_port = u16::from_be_bytes([item[10], item[11]]);
    let protocol = item[12];
    format!(
        "{} {}:{} -> {}:{}",
        protocol, src_ip, src_port, dst_ip, dst_port
    )
}

fn true_top_k(truth: &HashMap<Vec<u8>, u32>, k: usize) -> Vec<(Vec<u8>, u32)> {
    let mut v: Vec<(Vec<u8>, u32)> = truth.iter().map(|(k, c)| (k.clone(), *c)).collect();
    // Sort by count descending; ties broken by key bytes for determinism.
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    v.truncate(k);
    v
}

struct AccuracyMetrics {
    /// Fraction of reported items that are in the true top-K.
    hit_ratio: f64,
    /// Average relative error of reported items' counts vs. their true counts
    /// (skips reported items that don't appear in ground truth).
    are_reported: f64,
    /// Average relative error over the *true* top-K — uses the sketch's
    /// own `count(item)` query, so missing items count as zero.
    are_true_top_k: f64,
}

fn score_results(
    results: &[(Vec<u8>, u64)],
    truth: &HashMap<Vec<u8>, u32>,
    true_top_set: &HashSet<Vec<u8>>,
    sketch_count: impl Fn(&[u8]) -> u64,
) -> AccuracyMetrics {
    let hits = results
        .iter()
        .filter(|(item, _)| true_top_set.contains(item))
        .count();
    let hit_ratio = if results.is_empty() {
        0.0
    } else {
        hits as f64 / results.len() as f64
    };

    let mut sum = 0.0;
    let mut n = 0usize;
    for (item, est) in results {
        if let Some(&true_c) = truth.get(item) {
            if true_c > 0 {
                sum += (*est as f64 - true_c as f64).abs() / true_c as f64;
                n += 1;
            }
        }
    }
    let are_reported = if n == 0 { 0.0 } else { sum / n as f64 };

    let mut sum_true = 0.0;
    let true_n = true_top_set.len();
    for item in true_top_set {
        let est = sketch_count(item);
        let true_c = truth[item] as f64;
        sum_true += (est as f64 - true_c).abs() / true_c;
    }
    let are_true_top_k = if true_n == 0 {
        0.0
    } else {
        sum_true / true_n as f64
    };

    AccuracyMetrics {
        hit_ratio,
        are_reported,
        are_true_top_k,
    }
}

fn report(
    name: &str,
    num_keys: usize,
    duration: Duration,
    results: &[(Vec<u8>, u64)],
    metrics: &AccuracyMetrics,
) {
    let secs = duration.as_secs_f64();
    let throughput_mpps = (num_keys as f64 / 1_000_000.0) / secs;
    println!("\n=== {} ===", name);
    println!("inserts: {} in {:.3}s", num_keys, secs);
    println!(
        "throughput: {:.2} Mpps, {:.1} ns/op",
        throughput_mpps,
        1_000.0 / throughput_mpps
    );
    println!(
        "accuracy: hit_ratio={:.4}  ARE_reported={:.6}  ARE_true_top_k={:.6}",
        metrics.hit_ratio, metrics.are_reported, metrics.are_true_top_k
    );
    println!("top {} flows:", results.len().min(10));
    for (item, count) in results.iter().take(10) {
        println!("  {} count={}", format_flow(item), count);
    }
}

fn main() -> io::Result<()> {
    let max_item_num = 40 * 1_000_000;
    let (keys, actual_flow_sizes) = read_in_traces("data/", max_item_num)?;

    println!("number of items: {}", keys.len());
    println!("number of flows: {}", actual_flow_sizes.len());

    let truth_top_k = true_top_k(&actual_flow_sizes, TOP_K);
    let true_top_set: HashSet<Vec<u8>> = truth_top_k.iter().map(|(k, _)| k.clone()).collect();
    println!(
        "ground-truth top-{} threshold count: {} (smallest count in true top-K)",
        TOP_K,
        truth_top_k.last().map(|(_, c)| *c).unwrap_or(0)
    );

    // Power-of-two widths so all three variants can use AND-mask bucket
    // indexing instead of `%`. Roughly comparable cell budgets:
    //   TopK         : 2 * 16384 = 32768 cells
    //   BucketedTopK : 8192 * 4  = 32768 cells
    //   CuckooTopK   : 8192 * 5  = 40960 cells (1 lobby + 4 heavy per bucket)

    {
        let mut topk = TopK::<Vec<u8>>::new(TOP_K, 16384, 2, DECAY);
        let start = Instant::now();
        for key in &keys {
            topk.add(key.as_slice(), 1);
        }
        let duration = start.elapsed();
        let results: Vec<(Vec<u8>, u64)> =
            topk.list().into_iter().map(|n| (n.item, n.count)).collect();
        let metrics = score_results(&results, &actual_flow_sizes, &true_top_set, |item| {
            topk.count(item)
        });
        report(
            "TopK (HeavyKeeper)",
            keys.len(),
            duration,
            &results,
            &metrics,
        );
    }

    {
        let mut topk = BucketedTopK::<Vec<u8>>::new(TOP_K, 8192, 4, DECAY);
        let start = Instant::now();
        for key in &keys {
            topk.add(key.as_slice(), 1);
        }
        let duration = start.elapsed();
        let results: Vec<(Vec<u8>, u64)> =
            topk.list().into_iter().map(|n| (n.item, n.count)).collect();
        let metrics = score_results(&results, &actual_flow_sizes, &true_top_set, |item| {
            topk.count(item)
        });
        report("BucketedTopK", keys.len(), duration, &results, &metrics);
    }

    {
        let mut topk = CuckooTopK::<Vec<u8>>::new(TOP_K, 8192, 4, DECAY);
        let start = Instant::now();
        for key in &keys {
            topk.add(key.as_slice(), 1);
        }
        let duration = start.elapsed();
        let results: Vec<(Vec<u8>, u64)> =
            topk.list().into_iter().map(|n| (n.item, n.count)).collect();
        let metrics = score_results(&results, &actual_flow_sizes, &true_top_set, |item| {
            topk.count(item)
        });
        report("CuckooTopK", keys.len(), duration, &results, &metrics);
    }

    Ok(())
}
