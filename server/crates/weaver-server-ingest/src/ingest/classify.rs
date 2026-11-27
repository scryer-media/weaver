// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a submission is, read from its own bytes and never from a file name:
// a magnet, an infohash or an http(s) URL given as text, a bencoded torrent
// metainfo, or else an NZB. Every intake asks this one question: the Add
// dialog, the torrent client API and the watch folder.

pub use weaver_server_jobs::jobs::model::Kind;
use weaver_server_jobs::jobs::model::Protocol;

/// The longest a magnet, infohash or URL given as text can be. Anything longer
/// is a document.
pub const SUBMISSION_TEXT_LIMIT_BYTES: usize = 64 * 1024;

/// The class of job a link's declared media type says it fetches, before
/// its bytes are read: a torrent file's type is a metainfo's class. `None`
/// for any other type, whose bytes say what they are once fetched.
pub fn declared_protocol(media_type: &str) -> Option<Protocol> {
    let essence = media_type.split(';').next().unwrap_or_default().trim();
    if essence.eq_ignore_ascii_case("application/x-bittorrent") {
        Kind::Metainfo.protocol()
    } else {
        None
    }
}

/// What `bytes` are. Text is one token: a magnet, an infohash or a URL.
pub fn classify(bytes: &[u8]) -> Kind {
    // A metainfo is a bencoded dictionary: `d`, then its first key's length.
    if bytes.first() == Some(&b'd') && bytes.get(1).is_some_and(u8::is_ascii_digit) {
        return Kind::Metainfo;
    }
    if bytes.len() > SUBMISSION_TEXT_LIMIT_BYTES {
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
    if let Some(topic) = topic(text) {
        return Kind::Magnet(format!("magnet:?xt=urn:{topic}"));
    }
    match url::Url::parse(text) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => Kind::Url(url),
        _ => Kind::Nzb,
    }
}

// The magnet topic a bare infohash names. A v1 hash is 40 hex digits, or 32
// base32 characters, which a magnet carries in upper case. A v2 hash is 64 hex
// digits, which a magnet carries as a SHA-256 multihash.
fn topic(text: &str) -> Option<String> {
    let hex = text.bytes().all(|byte| byte.is_ascii_hexdigit());
    match text.len() {
        40 if hex => Some(format!("btih:{text}")),
        64 if hex => Some(format!("btmh:1220{text}")),
        32 if text
            .bytes()
            .all(|byte| byte.is_ascii_alphabetic() || (b'2'..=b'7').contains(&byte)) =>
        {
            Some(format!("btih:{}", text.to_ascii_uppercase()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "0123456789abcdef0123456789abcdef01234567";
    const BASE32: &str = "aerukz4jvpg66ajdivtytk6n54asgrlh";
    const V2: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

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
            classify(V2.as_bytes()),
            Kind::Magnet(format!("magnet:?xt=urn:btmh:1220{V2}"))
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
            Kind::Url(url::Url::parse(url).unwrap())
        );
    }

    #[test]
    fn a_bencoded_dictionary_is_a_metainfo() {
        assert_eq!(
            classify(b"d8:announce3:url4:infod4:name1:aee"),
            Kind::Metainfo
        );
        assert_eq!(
            Kind::Metainfo.protocol(),
            Some(weaver_server_jobs::jobs::model::Protocol::Torrent)
        );
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
        let long = format!(
            "https://example.invalid/{}",
            "a".repeat(SUBMISSION_TEXT_LIMIT_BYTES),
        );
        assert_eq!(classify(long.as_bytes()), Kind::Nzb);
    }

    #[test]
    fn only_a_torrent_leaves_its_bytes() {
        // Each kind names its class; a URL names none until it is fetched.
        use weaver_server_jobs::jobs::model::Protocol;
        assert_eq!(Kind::Nzb.protocol(), Some(Protocol::Nzb));
        assert_eq!(
            Kind::Magnet("magnet:?xt=urn:btih:a".into()).protocol(),
            Some(Protocol::Torrent)
        );
        let url = url::Url::parse("https://indexer.invalid/a").unwrap();
        assert_eq!(Kind::Url(url).protocol(), None);
    }
}
