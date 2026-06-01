use crate::configs::TOPK_FIXED_SEED;
use heavykeeper::CuckooTopK;

/// KeySpace Notification Events
pub const RESERVE_EVENT: &str = "topk.reserve";

/// Default sketch parameters used when the user only provides `topk`. These
/// match RedisBloom's defaults so client behavior is consistent for users
/// migrating between modules.
pub const DEFAULT_WIDTH: u32 = 8;
pub const DEFAULT_DEPTH: u32 = 7;
pub const DEFAULT_DECAY: f64 = 0.9;

/// Client Errors
pub const ERROR: &str = "ERROR";
pub const KEY_EXISTS: &str = "BUSYKEY Target key name already exists.";
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
    is_seed_random: bool,
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
            is_seed_random: seed != TOPK_FIXED_SEED,
            sketch,
        }
    }

    pub fn from_existing(
        k: u32,
        width: u32,
        depth: u32,
        decay: f64,
        seed: u64,
        is_seed_random: bool,
    ) -> TopKObject {
        let sketch = CuckooTopK::with_seed(k as usize, width as usize, depth as usize, decay, seed);
        TopKObject {
            k,
            width,
            depth,
            decay,
            seed,
            is_seed_random,
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
    pub fn is_seed_random(&self) -> bool {
        self.is_seed_random
    }
    pub fn sketch(&self) -> &CuckooTopK<Vec<u8>> {
        &self.sketch
    }
    pub fn sketch_mut(&mut self) -> &mut CuckooTopK<Vec<u8>> {
        &mut self.sketch
    }
}
