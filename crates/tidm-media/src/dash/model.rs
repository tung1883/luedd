use url::Url;

/// One DASH representation (a single quality/language track), equivalent of
/// `Representation`. `segments` is the ordered list of segment URLs, with an
/// init segment prepended when the template/list declares one.
#[derive(Debug, Clone, PartialEq)]
pub struct Representation {
    pub segments: Vec<Url>,
    pub width: i32,
    pub height: i32,
    pub codec: Option<String>,
    pub bandwidth: i64,
    /// Period duration in milliseconds.
    pub duration_ms: i64,
    pub mime_type: String,
    pub language: Option<String>,
}

/// One period's video/audio representation pairings. Mirrors the original's
/// `IList<KeyValuePair<Representation?, Representation?>>` per period: every
/// video paired with every audio if both exist, else one-sided.
pub type PeriodPairings = Vec<(Option<Representation>, Option<Representation>)>;
