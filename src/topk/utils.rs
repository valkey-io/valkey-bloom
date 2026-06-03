use heavykeeper::CuckooTopK;

/// KeySpace Notification Events
pub const RESERVE_EVENT: &str = "topk.reserve";
pub const ADD_EVENT: &str = "topk.add";
pub const DEFAULT_WIDTH: u32 = 8;
pub const DEFAULT_DEPTH: u32 = 7;
pub const DEFAULT_DECAY: f64 = 0.9;

/// Per-argument bounds.
/// The minimums are 1,The maximums are placeholders covering the
/// full u32 range until topk-specific configs are introduced; tighten them
/// once dedicated config knobs land.
// TODO: replace these with configurable bounds once topk configs exist.
pub const TOPK_K_MIN: u32 = 1;
pub const TOPK_K_MAX: u32 = u32::MAX;
pub const TOPK_WIDTH_MIN: u32 = 1;
pub const TOPK_WIDTH_MAX: u32 = u32::MAX;
pub const TOPK_DEPTH_MIN: u32 = 1;
pub const TOPK_DEPTH_MAX: u32 = u32::MAX;

/// Client Errors
pub const ERROR: &str = "ERROR";
pub const KEY_EXISTS: &str = "BUSYKEY Target key name already exists.";
pub const NOT_FOUND: &str = "ERR TopK: key does not exist";
pub const BAD_TOPK: &str = "ERR bad topk";
pub const BAD_WIDTH: &str = "ERR bad width";
pub const BAD_DEPTH: &str = "ERR bad depth";
pub const BAD_DECAY: &str = "ERR bad decay";
pub const INVALID_SEED: &str = "ERR invalid seed";
pub const TOPK_LARGER_THAN_0: &str = "ERR (topk should be larger than 0)";
pub const WIDTH_LARGER_THAN_0: &str = "ERR (width should be larger than 0)";
pub const DEPTH_LARGER_THAN_0: &str = "ERR (depth should be larger than 0)";
pub const DECAY_RANGE: &str = "ERR (0 < decay < 1)";

/// TopKObject wraps the underlying CuckooTopK sketch together with the
/// parameters used to construct it.
///  (k, width, depth, decay, seed)
pub struct TopKObject {
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
    seed: u64,
    sketch: CuckooTopK<Vec<u8>>,
}

impl TopKObject {
    /// Build a fresh TopKObject. Called from the TOPK.RESERVE command path
    /// after the handler has parsed and validated all parameters.
    pub fn new_reserved(k: u32, width: u32, depth: u32, decay: f64, seed: u64) -> TopKObject {
        let sketch = CuckooTopK::with_seed(k as usize, width as usize, depth as usize, decay, seed);
        TopKObject {
            k,
            width,
            depth,
            decay,
            seed,
            sketch,
        }
    }

    pub fn k(&self) -> u32 {
        self.k
    }
    pub fn width(&self) -> u32 {
        self.width
    }
    pub fn depth(&self) -> u32 {
        self.depth
    }
    pub fn decay(&self) -> f64 {
        self.decay
    }
    pub fn seed(&self) -> u64 {
        self.seed
    }
    pub fn sketch(&self) -> &CuckooTopK<Vec<u8>> {
        &self.sketch
    }
    pub fn sketch_mut(&mut self) -> &mut CuckooTopK<Vec<u8>> {
        &mut self.sketch
    }

    /// Add `increment` occurrences of `item` to the sketch and return the
    /// heavy-slot resident displaced by this insertion (if any). At most one
    /// item can be evicted per call.
    pub fn add(&mut self, item: &[u8], increment: u64) -> Option<Vec<u8>> {
        self.sketch
            .add_with_evicted(item, increment)
            .map(|node| node.item)
    }
}
