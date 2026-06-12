use flowstats::CountMinSketch;

/// Client Errors
pub const ERROR: &str = "ERROR";
pub const NOT_FOUND: &str = "ERR not found";
pub const ITEM_EXISTS: &str = "ERR item exists";
pub const BAD_WIDTH: &str = "ERR bad width";
pub const BAD_DEPTH: &str = "ERR bad depth";
pub const BAD_ERROR_RATE: &str = "ERR bad error rate";
pub const ERROR_RATE_RANGE: &str = "ERR error rate should be between 0 and 1";
pub const BAD_PROBABILITY: &str = "ERR bad probability";
pub const PROBABILITY_RANGE: &str = "ERR probability rate should be between 0 and 1";
pub const KEY_EXISTS: &str = "ERR Target key name already exists.";

///Keyspace Notification Events
pub const INITBYPROB_EVENT: &str = "countminsketch.initbyprob";
pub const INITBYDIM_EVENT: &str = "countminsketch.initbydim";
pub const INCR_EVENT: &str = "countminsketch.incrby";

#[derive(Debug, PartialEq)]
pub enum CMSError {
    InvalidWidth,
    InvalidDepth,
    InvalidErrorRate,
    InvalidProbability,
}

impl CMSError {
    pub fn as_str(&self) -> &'static str {
        match self {
            CMSError::InvalidWidth => BAD_WIDTH,
            CMSError::InvalidDepth => BAD_DEPTH,
            CMSError::InvalidErrorRate => ERROR_RATE_RANGE,
            CMSError::InvalidProbability => PROBABILITY_RANGE,
        }
    }
}

pub struct CMSObject {
    width: u64,
    depth: u64,
    total: u64,
    cms: CMS,
}

impl CMSObject {
    pub fn new_by_dimension(width: u64, depth: u64) -> Result<CMSObject, CMSError> {
        if width < 1 {
            return Err(CMSError::InvalidWidth);
        }

        if depth < 1 {
            return Err(CMSError::InvalidDepth);
        }

        let cms = CMS::new_by_dimensions(width as usize, depth as usize)?;
        let obj = CMSObject {
            width,
            depth,
            total: 0,
            cms,
        };

        Ok(obj)
    }

    //Error_tolerance is max variance of the count
    // probability is the false positive rate
    pub fn new_by_probability(
        error_tolerance: f64,
        probability: f64,
    ) -> Result<CMSObject, CMSError> {
        if error_tolerance <= 0.0 || error_tolerance >= 1.0 {
            return Err(CMSError::InvalidErrorRate);
        }
        if probability <= 0.0 || probability >= 1.0 {
            return Err(CMSError::InvalidProbability);
        }

        let cms = CMS::new_by_probability(error_tolerance, probability)?;
        let obj = CMSObject {
            width: cms.sketch.width() as u64,
            depth: cms.sketch.depth() as u64,
            total: 0,
            cms,
        };

        Ok(obj)
    }

    pub fn width(&self) -> u64 {
        self.width
    }

    pub fn depth(&self) -> u64 {
        self.depth
    }

    pub fn total(&self) -> u64 {
        self.total
    }
}

struct CMS {
    sketch: CountMinSketch,
}

impl CMS {
    pub fn new_by_probability(epsilon: f64, probability: f64) -> Result<CMS, CMSError> {
        // epsilon: Maximum overcount as a fraction of total (e.g., 0.01 for 1%)
        // delta (probability): Probability of exceeding the error bound (e.g., 0.001 for 0.1%)
        let cms = CountMinSketch::new(epsilon, probability);
        Ok(CMS { sketch: cms })
    }

    pub fn new_by_dimensions(width: usize, depth: usize) -> Result<CMS, CMSError> {
        let cms = CountMinSketch::with_dimensions(width, depth);
        Ok(CMS { sketch: cms })
    }
}
