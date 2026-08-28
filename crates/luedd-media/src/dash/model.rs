use url::Url;

#[derive(Debug, Clone, PartialEq)]
pub struct Representation {
    pub segments: Vec<Url>,
    pub width: i32,
    pub height: i32,
    pub codec: Option<String>,
    pub bandwidth: i64,
    pub duration_ms: i64,
    pub mime_type: String,
    pub language: Option<String>,
}

pub type PeriodPairings = Vec<(Option<Representation>, Option<Representation>)>;
