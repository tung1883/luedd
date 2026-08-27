//! General downloader engine: `progressive` is the byte-range multi-connection
//! HTTP downloader (M2), `jobs` dispatches a URL to the right downloader/muxer
//! pipeline (HTTP/HLS/DASH), and `queue` is the persisted download list,
//! concurrency-limited runner, and schedule-window checker (M3).
pub mod jobs;
pub mod naming;
pub mod progressive;
pub mod queue;
