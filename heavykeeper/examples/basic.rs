use heavykeeper::TopK;

fn main() {
    // Create a new TopK with:
    // - k=10 (number of top items to track)
    // - width=1000 (size of hash table, larger values use more memory but reduce collisions)
    // - depth=4 (number of hash functions, more depth increases accuracy but uses more CPU)
    // - decay=0.9 (decay factor for frequency counting, higher values give more weight to recent items)

    let mut topk: TopK<String> = TopK::new(10, 1000, 4, 0.9);

    // Add some example items multiple times to show frequency counting
    topk.add("frequent item", 5);
    topk.add("less frequent item", 3);
    topk.add("rare item", 1);

    // Print the items and their counts in order of frequency
    println!("Top items and their frequencies:");
    for node in topk.list() {
        println!("{}: {}", node.item, node.count);
    }

    // Demonstrate the count() method
    let item = "frequent item";
    println!("\nCount for '{}': {}", item, topk.count(item));

    // Demonstrate the contains() method
    println!(
        "Is '{}' present in the sketch? {}",
        item,
        if topk.contains(item) { "yes" } else { "no" }
    );
}
