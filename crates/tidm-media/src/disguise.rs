//! Detects MPEG-TS segments disguised behind a fake PNG/JPEG/WOFF2 header or
//! filler text, as described in `m3u8-guide.txt`. Real TS segments are a fixed
//! 188-byte packet stream where every packet starts with sync byte `0x47`; a
//! disguised segment prepends a valid-looking header of some other format before
//! the real TS bytes begin. Detection never trusts the claimed file extension or
//! magic bytes - it scans for the structural TS signature directly.

const SYNC_BYTE: u8 = 0x47;
const PACKET_LEN: usize = 188;
const SCAN_WINDOW: usize = 4096;
const DEFAULT_STREAK: usize = 8;

/// Scans the first ~4096 bytes of `buf` for a byte offset where `0x47` repeats
/// every 188 bytes for at least `need_streak` consecutive packets. Returns the
/// offset of the real TS stream start, or `None` if no such streak exists (the
/// file isn't a disguised TS segment, or uses a disguise this heuristic can't see).
pub fn find_hidden_ts_offset(buf: &[u8], need_streak: usize, packet_len: usize) -> Option<usize> {
    let cap = buf.len().min(SCAN_WINDOW);
    if cap < need_streak * packet_len {
        return None;
    }
    // Inclusive: `scan_limit` is itself a valid start offset whenever the real TS
    // streak reaches exactly to the end of the scanned window (e.g. a short fake
    // header immediately followed by a short segment) - the guide's own reference
    // pseudocode uses an exclusive `range(scan_limit)` here, which misses that case.
    let scan_limit = cap - need_streak * packet_len;

    for offset in 0..=scan_limit {
        if buf[offset] != SYNC_BYTE {
            continue;
        }
        let mut streak = 0usize;
        let mut pos = offset;
        while streak < need_streak && pos < buf.len() && buf[pos] == SYNC_BYTE {
            streak += 1;
            pos += packet_len;
        }
        if streak >= need_streak {
            return Some(offset);
        }
    }
    None
}

/// Convenience wrapper using the guide's defaults (8-packet streak, 188-byte packets).
pub fn find_hidden_ts_offset_default(buf: &[u8]) -> Option<usize> {
    find_hidden_ts_offset(buf, DEFAULT_STREAK, PACKET_LEN)
}

/// Returns the real TS payload, stripping any fake header/filler prefix if one is
/// detected. If no disguise is detected, returns the buffer unchanged (it's assumed
/// to already be a plain `.ts` segment).
pub fn extract_ts_payload(buf: &[u8]) -> &[u8] {
    match find_hidden_ts_offset_default(buf) {
        Some(offset) => &buf[offset..],
        None => buf,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ts_stream(num_packets: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(num_packets * PACKET_LEN);
        for i in 0..num_packets {
            buf.push(SYNC_BYTE);
            // Fill rest of packet with distinguishable, non-0x47 bytes.
            for j in 1..PACKET_LEN {
                buf.push(((i * 7 + j) % 251) as u8);
            }
        }
        buf
    }

    #[test]
    fn detects_plain_ts_at_offset_zero() {
        let ts = make_ts_stream(10);
        assert_eq!(find_hidden_ts_offset_default(&ts), Some(0));
    }

    #[test]
    fn detects_ts_hidden_behind_fake_png_header() {
        // Minimal PNG signature + IHDR-ish junk, then real TS payload.
        let mut fake_png = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        fake_png.extend_from_slice(&[0x00; 24]); // arbitrary header padding, no stray 0x47 run
        let ts = make_ts_stream(10);
        let mut disguised = fake_png.clone();
        disguised.extend_from_slice(&ts);

        let offset = find_hidden_ts_offset_default(&disguised).expect("should find offset");
        assert_eq!(offset, fake_png.len());
        assert_eq!(&disguised[offset..], &ts[..]);
    }

    #[test]
    fn detects_ts_hidden_behind_fake_woff2_header() {
        let fake_woff2 = vec![0x77, 0x4F, 0x46, 0x32, 0x00, 0x01, 0x00, 0x00];
        let ts = make_ts_stream(9);
        let mut disguised = fake_woff2.clone();
        disguised.extend_from_slice(&ts);

        let offset = find_hidden_ts_offset_default(&disguised).unwrap();
        assert_eq!(offset, fake_woff2.len());
    }

    #[test]
    fn detects_ts_hidden_behind_filler_text() {
        let filler = b"TOKEN=abc123&session=xyz\n".to_vec();
        let ts = make_ts_stream(8);
        let mut disguised = filler.clone();
        disguised.extend_from_slice(&ts);

        let offset = find_hidden_ts_offset_default(&disguised).unwrap();
        assert_eq!(offset, filler.len());
    }

    #[test]
    fn does_not_false_positive_on_random_noise_with_stray_sync_bytes() {
        // Bytes that happen to contain 0x47 here and there but never repeat
        // exactly every 188 bytes for 8 packets running.
        let mut noise = Vec::new();
        for i in 0..4096u32 {
            // Deterministic pseudo-random pattern with occasional 0x47 bytes,
            // but never aligned into a real 188-byte-spaced streak of 8+.
            let v = ((i.wrapping_mul(2654435761)) >> 16) as u8;
            noise.push(if i % 53 == 0 { SYNC_BYTE } else { v });
        }
        assert_eq!(find_hidden_ts_offset_default(&noise), None);
    }

    #[test]
    fn returns_none_for_buffer_smaller_than_required_streak() {
        let short = vec![SYNC_BYTE; 100];
        assert_eq!(find_hidden_ts_offset_default(&short), None);
    }

    #[test]
    fn extract_ts_payload_strips_disguise() {
        let fake_jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10];
        let ts = make_ts_stream(8);
        let mut disguised = fake_jpeg.clone();
        disguised.extend_from_slice(&ts);

        assert_eq!(extract_ts_payload(&disguised), &ts[..]);
    }

    #[test]
    fn extract_ts_payload_passthrough_when_no_disguise_detected() {
        let plain = vec![1, 2, 3, 4, 5];
        assert_eq!(extract_ts_payload(&plain), &plain[..]);
    }
}
