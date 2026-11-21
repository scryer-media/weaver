// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's secrets at rest: its tracker URLs, which carry a private
// tracker's passkey, and its metainfo, which carries them again. Each is
// sealed with the instance key (AES-256-GCM) and opened only where it is
// used: a tracker URL as text (`enc:v1:`), a metainfo as bytes (`enc:b1:`,
// no base64, so it is stored at its own size plus a nonce and a tag). A
// value written before sealing reads as it was until the one pass that
// seals it.

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::encryption::{
    ENCRYPTED_BYTES_PREFIX, EncryptionKey, decrypt_bytes, decrypt_value, encrypt_bytes,
    encrypt_value, is_encrypted, keyed_digest,
};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

// What a metainfo sealed as text starts with: the form before `enc:b1:`,
// still read. A bencoded metainfo starts with `d`, so a stored metainfo is
// told from a sealed one by its first bytes.
// The rows a sealing pass reads at a time: a torrent brings its metainfo,
// or its tracker rows, so a page stays small whatever the table holds.
pub const SEAL_PAGE: i64 = 64;

const SEALED_TEXT: &[u8] = b"enc:v1:";

// The domain a tracker URL's keyed digest is made under.
const URL_KEY_DOMAIN: &str = "weaver/torrent-tracker-url/v1";

// The key a torrent's secrets are sealed with: the instance key, when a
// store keeps it. One held in memory only is gone at the next start, and
// every torrent sealed under it with it, so nothing is sealed under it.
pub fn kept_key(key: Option<&EncryptionKey>) -> Result<&EncryptionKey, StateError> {
    let key = key.ok_or_else(|| {
        StateError::Database("encryption key is required to store secrets".to_string())
    })?;
    if key.is_ephemeral() {
        return Err(StateError::Database(
            "torrents are not stored while the encryption key is held in memory only: \
             configure a key store or set WEAVER_ENCRYPTION_KEY"
                .to_string(),
        ));
    }
    Ok(key)
}

// A tracker URL as it is stored: sealed, whatever it starts with.
pub fn seal_url(key: Option<&EncryptionKey>, url: &str) -> Result<String, StateError> {
    encrypt_value(kept_key(key)?, url).map_err(StateError::Database)
}

// What says two stored tracker URLs are the same one: the URL's keyed
// digest, the same for the same URL where its ciphertext never is.
pub fn url_key(key: Option<&EncryptionKey>, url: &str) -> Result<Vec<u8>, StateError> {
    Ok(keyed_digest(kept_key(key)?, URL_KEY_DOMAIN, url.as_bytes()).to_vec())
}

// A stored tracker URL, opened.
pub fn open_url(key: Option<&EncryptionKey>, stored: &str) -> Result<String, String> {
    if !is_encrypted(stored) {
        return Ok(stored.to_string());
    }
    decrypt_value(
        key.ok_or("no instance key to decrypt a tracker URL")?,
        stored,
    )
}

// Whether a stored metainfo is sealed, in either form.
pub fn is_sealed(stored: &[u8]) -> bool {
    stored.starts_with(ENCRYPTED_BYTES_PREFIX) || stored.starts_with(SEALED_TEXT)
}

// A metainfo as it is stored: sealed as bytes.
pub fn seal_metainfo(key: Option<&EncryptionKey>, metainfo: &[u8]) -> Result<Vec<u8>, StateError> {
    encrypt_bytes(kept_key(key)?, metainfo).map_err(StateError::Database)
}

// A stored metainfo, opened. One that does not open is an error its reader
// handles: the torrent is held, with why.
pub fn open_metainfo(key: Option<&EncryptionKey>, stored: Vec<u8>) -> Result<Vec<u8>, String> {
    if stored.starts_with(ENCRYPTED_BYTES_PREFIX) {
        return decrypt_bytes(key.ok_or("no instance key to decrypt a metainfo")?, &stored);
    }
    if !stored.starts_with(SEALED_TEXT) {
        return Ok(stored);
    }
    let text = std::str::from_utf8(&stored)
        .map_err(|error| format!("a sealed metainfo is not text: {error}"))?;
    let encoded = decrypt_value(key.ok_or("no instance key to decrypt a metainfo")?, text)?;
    STANDARD
        .decode(encoded)
        .map_err(|error| format!("a sealed metainfo does not decode: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sealed_url_and_metainfo_open_to_what_was_sealed_and_hold_no_passkey() {
        let key = EncryptionKey::generate();
        let url = "https://tracker.example/announce?passkey=0123abcd";
        let sealed = seal_url(Some(&key), url).unwrap();
        assert!(!sealed.contains("0123abcd"));
        // A write always seals: what it is given is the URL, whatever it
        // starts with.
        let twice = seal_url(Some(&key), &sealed).unwrap();
        assert_ne!(twice, sealed);
        assert_eq!(open_url(Some(&key), &twice).unwrap(), sealed);
        // The same URL has the same key; another URL, or another instance
        // key, another.
        assert_eq!(
            url_key(Some(&key), url).unwrap(),
            url_key(Some(&key), url).unwrap()
        );
        assert_ne!(
            url_key(Some(&key), url).unwrap(),
            url_key(Some(&key), "https://tracker.example/announce").unwrap()
        );
        assert_eq!(open_url(Some(&key), &sealed).unwrap(), url);
        assert_eq!(open_url(Some(&key), url).unwrap(), url);

        let metainfo = format!("d8:announce{}:{url}4:infodee", url.len()).into_bytes();
        let stored = seal_metainfo(Some(&key), &metainfo).unwrap();
        assert!(is_sealed(&stored));
        assert!(!stored.windows(8).any(|at| at == b"0123abcd"));
        assert_eq!(open_metainfo(Some(&key), stored.clone()).unwrap(), metainfo);
        assert_eq!(
            open_metainfo(Some(&key), metainfo.clone()).unwrap(),
            metainfo
        );

        let other = EncryptionKey::generate();
        assert!(open_url(Some(&other), &sealed).is_err());
        assert!(open_metainfo(Some(&other), stored).is_err());
        assert!(seal_url(None, url).is_err());
        assert_ne!(
            url_key(Some(&other), url).unwrap(),
            url_key(Some(&key), url).unwrap()
        );

        // Sealed as bytes: its own size, a nonce and a tag, no base64.
        assert_eq!(
            seal_metainfo(Some(&key), &metainfo).unwrap().len(),
            metainfo.len() + ENCRYPTED_BYTES_PREFIX.len() + 12 + 16
        );
        // One sealed as text before still opens.
        let text = encrypt_value(&key, &STANDARD.encode(&metainfo))
            .unwrap()
            .into_bytes();
        assert!(is_sealed(&text));
        assert_eq!(open_metainfo(Some(&key), text).unwrap(), metainfo);
        // One cut short is an error, not a panic.
        assert!(open_metainfo(Some(&key), b"enc:b1:short".to_vec()).is_err());

        // Nothing is sealed under a key no store keeps.
        let unkept = EncryptionKey::generate().ephemeral();
        assert!(seal_url(Some(&unkept), url).is_err());
        assert!(seal_metainfo(Some(&unkept), &metainfo).is_err());
    }
}
