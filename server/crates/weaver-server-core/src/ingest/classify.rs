// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a submission is, read from its own bytes and never from a file name:
// a magnet, an infohash or an http(s) URL given as text, a bencoded torrent
// metainfo, or else an NZB. Every intake asks this one question: the Add
// dialog, the torrent client API and the watch folder.

use crate::torrents::service::AddSource;

// The longest a magnet, infohash or URL given as text can be. Anything longer
// is a document.
const TEXT_LIMIT: usize = 64 * 1024;

/// What a submission is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// An NZB, or anything else: the NZB path refuses what it cannot read,
    /// in its own words.
    Nzb,
    /// A bencoded torrent metainfo.
    Metainfo,
    /// A magnet URI. A bare infohash is given as its magnet.
    Magnet(String),
    /// An http(s) URL, whose bytes say what it is once fetched.
    Url(reqwest::Url),
}

impl Kind {
    /// Whether these bytes go to the torrent client.
    pub fn is_torrent(&self) -> bool {
        matches!(self, Self::Metainfo | Self::Magnet(_))
    }

    /// The torrent `bytes` hold, when this is one; the bytes back otherwise.
    pub fn torrent(self, bytes: Vec<u8>) -> Result<AddSource, Vec<u8>> {
        match self {
            Self::Metainfo => Ok(AddSource::Metainfo(bytes)),
            Self::Magnet(uri) => Ok(AddSource::Magnet(uri)),
            Self::Nzb | Self::Url(_) => Err(bytes),
        }
    }
}

/// What `bytes` are. Text is one token: a magnet, an infohash or a URL.
pub fn classify(bytes: &[u8]) -> Kind {
    // A metainfo is a bencoded dictionary: `d`, then its first key's length.
    if bytes.first() == Some(&b'd') && bytes.get(1).is_some_and(u8::is_ascii_digit) {
        return Kind::Metainfo;
    }
    if bytes.len() > TEXT_LIMIT {
        return Kind::Nzb;
    }
    let Some(text) = std::str::from_utf8(bytes)
        .ok()
        .map(str::trim)
        .filter(|text| !text.is_empty() && !text.contains(char::is_whitespace))
    else {
        return Kind::Nzb;
    };
    if text
        .get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("magnet:"))
    {
        return Kind::Magnet(text.to_owned());
    }
    if let Some(infohash) = infohash(text) {
        return Kind::Magnet(format!("magnet:?xt=urn:btih:{infohash}"));
    }
    match reqwest::Url::parse(text) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => Kind::Url(url),
        _ => Kind::Nzb,
    }
}

// A v1 infohash: 40 hex digits, or 32 base32 characters, which a magnet
// carries in upper case.
fn infohash(text: &str) -> Option<String> {
    match text.len() {
        40 if text.bytes().all(|byte| byte.is_ascii_hexdigit()) => Some(text.to_owned()),
        32 if text
            .bytes()
            .all(|byte| byte.is_ascii_alphabetic() || (b'2'..=b'7').contains(&byte)) =>
        {
            Some(text.to_ascii_uppercase())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "0123456789abcdef0123456789abcdef01234567";
    const BASE32: &str = "aerukz4jvpg66ajdivtytk6n54asgrlh";

    #[test]
    fn text_is_a_magnet_an_infohash_or_a_url() {
        let magnet = format!("magnet:?xt=urn:btih:{HEX}&dn=a");
        assert_eq!(
            classify(format!("  {magnet}\n").as_bytes()),
            Kind::Magnet(magnet)
        );
        assert_eq!(
            classify(HEX.as_bytes()),
            Kind::Magnet(format!("magnet:?xt=urn:btih:{HEX}"))
        );
        assert_eq!(
            classify(BASE32.as_bytes()),
            Kind::Magnet(format!(
                "magnet:?xt=urn:btih:{}",
                BASE32.to_ascii_uppercase()
            ))
        );
        let url = "https://indexer.invalid/get?id=1&apikey=k";
        assert_eq!(
            classify(url.as_bytes()),
            Kind::Url(reqwest::Url::parse(url).unwrap())
        );
    }

    #[test]
    fn a_bencoded_dictionary_is_a_metainfo() {
        assert_eq!(
            classify(b"d8:announce3:url4:infod4:name1:aee"),
            Kind::Metainfo
        );
        assert!(matches!(
            Kind::Metainfo.torrent(b"d4:infodee".to_vec()),
            Ok(AddSource::Metainfo(bytes)) if bytes == b"d4:infodee"
        ));
    }

    #[test]
    fn everything_else_is_an_nzb() {
        for input in [
            &b"<?xml version=\"1.0\"?><nzb></nzb>"[..],
            b"\x1f\x8b\x08\x00",
            b"ftp://example.invalid/a.torrent",
            b"two words",
            b"",
            b"done",
        ] {
            assert_eq!(classify(input), Kind::Nzb, "{input:?}");
        }
        // Too long to be text, whatever it starts with.
        let long = format!("https://example.invalid/{}", "a".repeat(TEXT_LIMIT));
        assert_eq!(classify(long.as_bytes()), Kind::Nzb);
    }

    #[test]
    fn only_a_torrent_leaves_its_bytes() {
        assert!(matches!(
            Kind::Nzb.torrent(b"<nzb/>".to_vec()),
            Err(bytes) if bytes == b"<nzb/>"
        ));
    }
}
