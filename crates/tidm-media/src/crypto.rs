//! AES-128-CBC segment decryption for `#EXT-X-KEY:METHOD=AES-128` playlists.
//! XDM's `HlsParser` only extracts key metadata (URL + IV); actual decryption
//! never existed in that codebase's parser layer, so this is new in the Rust port.

use aes::Aes128;
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockDecryptMut, KeyIvInit};
use thiserror::Error;

type Aes128CbcDec = cbc::Decryptor<Aes128>;

#[derive(Debug, Error)]
pub enum DecryptError {
    #[error("key must be exactly 16 bytes, got {0}")]
    BadKeyLength(usize),
    #[error("ciphertext padding/length invalid")]
    BadPadding,
}

/// Decrypts an AES-128-CBC encrypted segment in place, given the raw 16-byte key
/// and 16-byte IV (as produced by `hls::iv_from_media_sequence` or parsed from
/// `IV=` in the playlist). Assumes PKCS7 padding, per RFC 8216 §5.2.
pub fn decrypt_segment(ciphertext: &[u8], key: &[u8], iv: &[u8; 16]) -> Result<Vec<u8>, DecryptError> {
    if key.len() != 16 {
        return Err(DecryptError::BadKeyLength(key.len()));
    }
    let key: [u8; 16] = key.try_into().unwrap();
    let mut buf = ciphertext.to_vec();
    let decryptor = Aes128CbcDec::new(&key.into(), iv.into());
    let plaintext = decryptor
        .decrypt_padded_mut::<Pkcs7>(&mut buf)
        .map_err(|_| DecryptError::BadPadding)?;
    Ok(plaintext.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cbc::cipher::BlockEncryptMut;

    type Aes128CbcEnc = cbc::Encryptor<Aes128>;

    #[test]
    fn round_trips_encrypt_then_decrypt() {
        let key = [0x42u8; 16];
        let iv = [0x24u8; 16];
        let plaintext = b"this is a fake TS segment payload, more than one block long!!".to_vec();

        let mut buf = plaintext.clone();
        buf.resize(plaintext.len() + 16, 0);
        let ct_len = Aes128CbcEnc::new(&key.into(), &iv.into())
            .encrypt_padded_mut::<Pkcs7>(&mut buf, plaintext.len())
            .unwrap()
            .len();
        buf.truncate(ct_len);

        let decrypted = decrypt_segment(&buf, &key, &iv).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn rejects_bad_key_length() {
        let err = decrypt_segment(&[0u8; 32], &[0u8; 10], &[0u8; 16]).unwrap_err();
        assert!(matches!(err, DecryptError::BadKeyLength(10)));
    }
}
