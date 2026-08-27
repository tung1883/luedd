//! Browser-integration bridge (M4): `wire` is the stdio native-messaging frame
//! codec, `server` is the local HTTP server the extension's `connector.js`
//! talks to, and `manifest` generates the native-messaging host manifest a
//! browser needs to launch `tidm-nmhost` (registration itself is a manual,
//! user-run step - see the M4 summary).
pub mod manifest;
pub mod server;
pub mod wire;
