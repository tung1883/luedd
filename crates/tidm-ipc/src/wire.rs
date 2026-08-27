//! Chrome/Firefox native-messaging wire format: each message is a 4-byte
//! little-endian length prefix followed by that many bytes of UTF-8 JSON. This
//! is the actual browser-defined protocol (not an XDM invention) used by
//! whatever process a browser launches via `chrome.runtime.connectNative` /
//! `sendNativeMessage` - the Rust equivalent of `XDM.Messaging`'s
//! `NativeMessageSerializer`.

use std::io::{Read, Write};

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

/// Chrome documents a 1MB limit on messages sent *to* a native host and a 1MB
/// limit (4GB before Chrome 129) on messages sent *from* one; guard well above
/// the smaller bound only to reject obviously-corrupt input, not to enforce
/// the spec limit itself.
const MAX_MESSAGE_LEN: u32 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("message length {0} exceeds sanity limit {MAX_MESSAGE_LEN}")]
    TooLarge(u32),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Reads one length-prefixed JSON message from `r`. Returns `Ok(None)` on a
/// clean EOF before any bytes of the next length prefix are read (the normal
/// way a browser signals "no more messages, host may exit").
pub fn read_message<R: Read, T: DeserializeOwned>(r: &mut R) -> Result<Option<T>, WireError> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_MESSAGE_LEN {
        return Err(WireError::TooLarge(len));
    }

    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload)?;
    let value = serde_json::from_slice(&payload)?;
    Ok(Some(value))
}

/// Writes one length-prefixed JSON message to `w` and flushes.
pub fn write_message<W: Write, T: Serialize>(w: &mut W, value: &T) -> Result<(), WireError> {
    let payload = serde_json::to_vec(value)?;
    if payload.len() as u64 > MAX_MESSAGE_LEN as u64 {
        return Err(WireError::TooLarge(payload.len() as u32));
    }
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(&payload)?;
    w.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::io::Cursor;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Ping {
        action: String,
        n: u32,
    }

    #[test]
    fn round_trips_a_message() {
        let mut buf = Vec::new();
        let msg = Ping { action: "ping".into(), n: 42 };
        write_message(&mut buf, &msg).unwrap();

        let mut cursor = Cursor::new(buf);
        let read: Ping = read_message(&mut cursor).unwrap().unwrap();
        assert_eq!(read, msg);
    }

    #[test]
    fn reads_multiple_messages_sequentially() {
        let mut buf = Vec::new();
        write_message(&mut buf, &Ping { action: "a".into(), n: 1 }).unwrap();
        write_message(&mut buf, &Ping { action: "b".into(), n: 2 }).unwrap();

        let mut cursor = Cursor::new(buf);
        let first: Ping = read_message(&mut cursor).unwrap().unwrap();
        let second: Ping = read_message(&mut cursor).unwrap().unwrap();
        assert_eq!(first.n, 1);
        assert_eq!(second.n, 2);
    }

    #[test]
    fn returns_none_on_clean_eof() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result: Option<Ping> = read_message(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn rejects_length_prefix_over_sanity_limit() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_MESSAGE_LEN + 1).to_le_bytes());
        let mut cursor = Cursor::new(buf);
        let result: Result<Option<Ping>, WireError> = read_message(&mut cursor);
        assert!(matches!(result, Err(WireError::TooLarge(_))));
    }

    #[test]
    fn errors_on_truncated_payload() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_le_bytes());
        buf.extend_from_slice(b"{\"incomplete\":");
        let mut cursor = Cursor::new(buf);
        let result: Result<Option<Ping>, WireError> = read_message(&mut cursor);
        assert!(result.is_err());
    }
}
