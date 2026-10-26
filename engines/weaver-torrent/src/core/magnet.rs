// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{net::SocketAddr, str::FromStr};

use anyhow::Context;

use crate::core::hash_id::{Id20, Id32, InfoHashes};

/// A parsed magnet link.
pub struct Magnet {
    id20: Option<Id20>,
    id32: Option<Id32>,
    pub trackers: Vec<String>,
    /// The peers the link names (`x.pe`), to connect to as any other.
    pub peers: Vec<SocketAddr>,
    /// The web seeds the link names (`ws`), BEP 19 URLs as written.
    pub web_seeds: Vec<String>,
    pub name: Option<String>,
    /// The torrent's length in bytes, as the link states it (`xl`).
    pub exact_length: Option<u64>,
    select_only: Option<Vec<usize>>,
}

#[derive(thiserror::Error, Debug)]
pub enum MagnetError {
    #[error("magnet link has neither v1 (btih) nor v2 (btmh) info hash")]
    NoInfoHash,
}

impl Magnet {
    pub fn as_id20(&self) -> Option<Id20> {
        self.id20
    }

    pub fn as_id32(&self) -> Option<Id32> {
        self.id32
    }

    /// The hashes the link names: a btih alone may be a v2 torrent's
    /// truncated hash, which its metadata settles.
    pub fn info_hashes(&self) -> InfoHashes {
        InfoHashes {
            v1: self.id20,
            v2: self.id32,
        }
    }
    pub fn get_select_only(&self) -> Option<Vec<usize>> {
        self.select_only.clone()
    }

    /// The link to a torrent: its hashes, name and trackers (libtorrent
    /// `make_magnet_uri`).
    pub fn of(info_hashes: InfoHashes, name: Option<String>, trackers: Vec<String>) -> Self {
        Self {
            id20: info_hashes.v1,
            id32: info_hashes.v2,
            trackers,
            peers: Vec::new(),
            web_seeds: Vec::new(),
            name,
            exact_length: None,
            select_only: None,
        }
    }

    pub fn from_id20(id20: Id20, trackers: Vec<String>, select_only: Option<Vec<usize>>) -> Self {
        Self {
            id20: Some(id20),
            id32: None,
            trackers,
            peers: Vec::new(),
            web_seeds: Vec::new(),
            name: None,
            exact_length: None,
            select_only,
        }
    }

    pub fn new(
        id20: Option<Id20>,
        id32: Option<Id32>,
        trackers: Vec<String>,
        select_only: Option<Vec<usize>>,
    ) -> Result<Self, MagnetError> {
        if id20.is_none() && id32.is_none() {
            return Err(MagnetError::NoInfoHash);
        }
        Ok(Self {
            id20,
            id32,
            trackers,
            peers: Vec::new(),
            web_seeds: Vec::new(),
            name: None,
            exact_length: None,
            select_only,
        })
    }

    /// Parse a magnet link.
    pub fn parse(url: &str) -> anyhow::Result<Magnet> {
        if url.len() == 40
            && let Ok(id20) = Id20::from_str(url)
        {
            return Ok(Magnet {
                id20: Some(id20),
                id32: None,
                name: None,
                trackers: vec![],
                peers: vec![],
                web_seeds: vec![],
                exact_length: None,
                select_only: None,
            });
        }
        let url = url::Url::parse(url).context("magnet link must be a valid URL")?;
        if url.scheme() != "magnet" {
            anyhow::bail!("expected scheme magnet");
        }
        let mut info_hash_found = false;
        let mut id20: Option<Id20> = None;
        let mut id32: Option<Id32> = None;
        let mut name: Option<String> = None;
        let mut exact_length: Option<u64> = None;
        let mut trackers = Vec::<String>::new();
        let mut peers = Vec::<SocketAddr>::new();
        let mut web_seeds = Vec::<String>::new();
        let mut files = Vec::<usize>::new();
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "xt" => {
                    if let Some(ih) = value.as_ref().strip_prefix("urn:btih:") {
                        let i = Id20::from_str(ih)?;
                        id20.replace(i);
                        info_hash_found = true;
                    } else if let Some(ih) = value.as_ref().strip_prefix("urn:btmh:") {
                        // A SHA-256 multihash in hex (libtorrent `parse_magnet_uri`).
                        let ih = ih.strip_prefix("1220").context("btmh is not SHA-256")?;
                        anyhow::ensure!(ih.len() == 64, "btmh is not 64 hex digits");
                        let i = Id32::from_str(ih)?;
                        id32.replace(i);
                        info_hash_found = true;
                    }
                    // Any other xt names nothing a torrent is found by.
                }
                "tr" => trackers.push(value.into()),
                // An address and port; one that is not is passed over
                // (libtorrent `parse_magnet_uri`, `parse_endpoint`).
                "x.pe" => peers.extend(SocketAddr::from_str(value.trim()).ok()),
                // A web seed, as written (libtorrent `parse_magnet_uri`, `ws`).
                "ws" => web_seeds.push(value.into_owned()),
                "dn" if !value.is_empty() => name = Some(value.into_owned()),
                "dn" => {}
                "xl" => exact_length = value.trim().parse().ok(),
                "so" => {
                    // Process 'so' values, but silently ignore any which fail parsing
                    for file_desc in value.split(',') {
                        if file_desc.is_empty() {
                            continue;
                        }
                        // Handling ranges of file indices
                        if let Some((start, end)) = file_desc.split_once('-') {
                            let maybe_start_idx: Result<usize, _> = start.parse();
                            let maybe_end_idx: Result<usize, _> = end.parse();
                            if let (Ok(start_idx), Ok(end_idx)) = (maybe_start_idx, maybe_end_idx) {
                                files.extend(start_idx..=end_idx);
                            }
                        } else {
                            // Handling single file index
                            let idx = file_desc.parse();
                            if let Ok(idx) = idx {
                                files.push(idx);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        match info_hash_found {
            true => Ok(Magnet {
                id20,
                id32,
                trackers,
                peers,
                web_seeds,
                name,
                exact_length,
                select_only: if files.is_empty() { None } else { Some(files) },
            }),
            false => {
                anyhow::bail!("did not find infohash")
            }
        }
    }
}

/// A value as libtorrent escapes it into a link: alphanumerics and
/// `-_!.~*()` as they are, every other byte as `%` and two lowercase hex
/// digits (libtorrent `escape_string`).
pub(crate) struct Escaped<'a>(pub(crate) &'a [u8]);

impl std::fmt::Display for Escaped<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        for &b in self.0 {
            if b.is_ascii_alphanumeric() || b"-_!.~*()".contains(&b) {
                write!(f, "{}", b as char)?;
            } else {
                write!(f, "%{b:02x}")?;
            }
        }
        Ok(())
    }
}

/// The link in libtorrent's order (`make_magnet_uri`): btih, btmh, dn,
/// then each tr, each ws and each x.pe; a file selection, which libtorrent
/// does not write, last.
impl std::fmt::Display for Magnet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "magnet:")?;
        let mut write_ampersand = {
            let mut written_so_far = 0;
            move |f: &mut std::fmt::Formatter<'_>| -> core::fmt::Result {
                if written_so_far == 0 {
                    write!(f, "?")?;
                } else {
                    write!(f, "&")?;
                }
                written_so_far += 1;
                Ok(())
            }
        };
        if let Some(id20) = self.id20 {
            write_ampersand(f)?;
            write!(f, "xt=urn:btih:{}", id20.as_string(),)?;
        }
        if let Some(id32) = self.id32 {
            write_ampersand(f)?;
            write!(f, "xt=urn:btmh:1220{}", id32.as_string(),)?;
        }
        if let Some(name) = self.name.as_deref().filter(|n| !n.is_empty()) {
            write_ampersand(f)?;
            write!(f, "dn={}", Escaped(name.as_bytes()))?;
        }
        for tracker in self.trackers.iter() {
            write_ampersand(f)?;
            write!(f, "tr={}", Escaped(tracker.as_bytes()))?;
        }
        for web_seed in self.web_seeds.iter() {
            write_ampersand(f)?;
            write!(f, "ws={}", Escaped(web_seed.as_bytes()))?;
        }
        for peer in self.peers.iter() {
            write_ampersand(f)?;
            write!(f, "x.pe={peer}")?;
        }
        if let Some(select_only) = &self.select_only
            && !select_only.is_empty()
        {
            write_ampersand(f)?;
            write!(f, "so=")?;
            for (index, file) in select_only.iter().enumerate() {
                if index > 0 {
                    write!(f, ",")?; // Add a comma before all but the first index
                }
                write!(f, "{file}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::core::Id20;

    use super::Magnet;

    #[test]
    fn test_parse_magnet_as_url() {
        let magnet = "magnet:?xt=urn:btih:a621779b5e3d486e127c3efbca9b6f8d135f52e5&dn=rutor.info_%D0%92%D0%BE%D0%B9%D0%BD%D0%B0+%D0%B1%D1%83%D0%B4%D1%83%D1%89%D0%B5%D0%B3%D0%BE+%2F+The+Tomorrow+War+%282021%29+WEB-DLRip+%D0%BE%D1%82+MegaPeer+%7C+P+%7C+NewComers&tr=udp://opentor.org:2710&tr=udp://opentor.org:2710&tr=http://retracker.local/announce";
        dbg!(url::Url::parse(magnet).unwrap());
    }

    #[test]
    fn test_parse_magnet_v2() {
        use super::Magnet;
        use crate::core::magnet::Id32;
        use std::str::FromStr;
        let magnet = "magnet:?xt=urn:btmh:1220caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e&dn=bittorrent-v2-test
";
        let info_hash =
            Id32::from_str("caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e")
                .unwrap();
        let m = Magnet::parse(magnet).unwrap();
        assert!(m.as_id32() == Some(info_hash));
    }

    #[test]
    fn v2_and_hybrid_links_parse_as_libtorrent_parses_them() {
        // libtorrent test_magnet.cpp parse_v2_hash, parse_v2_short_hash,
        // parse_v2_invalid_hash_prefix, parse_v2_invalid_hex_hash,
        // parse_hybrid_uri.
        let cd32 = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        let cd20 = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        let m = Magnet::parse(&format!("magnet:?xt=urn:btmh:1220{cd32}")).unwrap();
        assert_eq!(m.info_hashes().v2.unwrap().as_string(), cd32);
        assert_eq!(m.info_hashes().v1, None);
        for bad in [
            "magnet:?xt=urn:btmh:1220cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdccdcdcdcdcdcdcd",
            "magnet:?xt=urn:btmh:1221cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
            "magnet:?xt=urn:btmh:1220zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        ] {
            assert!(Magnet::parse(bad).is_err(), "{bad}");
        }
        let m = Magnet::parse(&format!(
            "magnet:?xt=urn:btmh:1220{cd32}&xt=urn:btih:{cd20}"
        ))
        .unwrap();
        assert_eq!(m.info_hashes().v1.unwrap().as_string(), cd20);
        assert_eq!(m.info_hashes().v2.unwrap().as_string(), cd32);
        // An xt of another kind is passed over.
        let m = Magnet::parse(&format!("magnet:?xt=urn:sha1:abc&xt=urn:btih:{cd20}")).unwrap();
        assert_eq!(m.info_hashes().v1.unwrap().as_string(), cd20);
        assert!(Magnet::parse("magnet:?xt=urn:sha1:abc").is_err());
    }

    #[test]
    fn test_magnet_to_string() {
        let id20 = Id20::from_str("a621779b5e3d486e127c3efbca9b6f8d135f52e5").unwrap();
        assert_eq!(
            &Magnet::from_id20(id20, Default::default(), None).to_string(),
            "magnet:?xt=urn:btih:a621779b5e3d486e127c3efbca9b6f8d135f52e5"
        );

        assert_eq!(
            &Magnet::from_id20(id20, vec!["foo".to_string(), "bar".to_string()], None).to_string(),
            "magnet:?xt=urn:btih:a621779b5e3d486e127c3efbca9b6f8d135f52e5&tr=foo&tr=bar"
        );

        assert_eq!(
            &Magnet::from_id20(id20, Default::default(), Some(vec![1, 2, 3])).to_string(),
            "magnet:?xt=urn:btih:a621779b5e3d486e127c3efbca9b6f8d135f52e5&so=1,2,3"
        );
    }

    #[test]
    fn a_torrents_link_is_written_as_libtorrent_writes_it() {
        use crate::core::{Id32, InfoHashes};
        // test_magnet.cpp `hybrid_info_hashes`, `v2_info_hashes`.
        let v1 = Id20::new([b'a'; 20]);
        let v2 = Id32::new([b'b'; 32]);
        let link =
            |v1, v2, name, trackers| Magnet::of(InfoHashes { v1, v2 }, name, trackers).to_string();
        assert_eq!(
            link(Some(v1), Some(v2), None, vec![]),
            "magnet:?xt=urn:btih:6161616161616161616161616161616161616161\
             &xt=urn:btmh:12206262626262626262626262626262626262626262626262626262626262626262"
        );
        assert_eq!(
            link(None, Some(Id32::new([b'a'; 32])), None, vec![]),
            "magnet:?xt=urn:btmh:12206161616161616161616161616161616161616161616161616161616161616161"
        );
        // `torrent_info_hash`: trackers escaped after the hashes; and the
        // name before them, escaped the same way (`make_magnet_uri2`).
        assert_eq!(
            link(
                Some(v1),
                None,
                Some("a b/c's (1).mkv".to_owned()),
                vec![
                    "http://foo.bar".to_owned(),
                    "udp://foo.bar/announce".to_owned()
                ]
            ),
            "magnet:?xt=urn:btih:6161616161616161616161616161616161616161\
             &dn=a%20b%2fc%27s%20(1).mkv\
             &tr=http%3a%2f%2ffoo.bar&tr=udp%3a%2f%2ffoo.bar%2fannounce"
        );
        // What is written reads back.
        let m = Magnet::parse(&link(
            Some(v1),
            Some(v2),
            Some("a b".to_owned()),
            vec!["udp://foo.bar/announce".to_owned()],
        ))
        .unwrap();
        assert_eq!(
            m.info_hashes(),
            InfoHashes {
                v1: Some(v1),
                v2: Some(v2)
            }
        );
        assert_eq!(m.name.as_deref(), Some("a b"));
        assert_eq!(m.trackers, vec!["udp://foo.bar/announce"]);
    }

    // libtorrent `parse_magnet_uri`: each `tr` is a tier of its own, in the
    // order the link gives them.
    #[test]
    fn a_magnets_trackers_are_read_in_the_order_the_link_gives() {
        let link = "magnet:?xt=urn:btih:a621779b5e3d486e127c3efbca9b6f8d135f52e5\
                    &tr=https%3A%2F%2Ftorrent.example%2Fannounce\
                    &tr=https%3A%2F%2Fipv6.torrent.example%2Fannounce";
        let m = Magnet::parse(link).unwrap();
        assert_eq!(
            m.trackers,
            vec![
                "https://torrent.example/announce",
                "https://ipv6.torrent.example/announce"
            ]
        );
    }

    // libtorrent test_magnet.cpp `parse_web_seeds`, and a torrent's link
    // carrying its web seeds escaped (`make_magnet_uri`).
    #[test]
    fn a_links_web_seeds_are_read_in_order_and_written_escaped() {
        let cd20 = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        let m = Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{cd20}&ws=http://foo.com/bar&ws=http://bar.com/foo"
        ))
        .unwrap();
        assert_eq!(m.web_seeds, ["http://foo.com/bar", "http://bar.com/foo"]);
        assert_eq!(
            m.to_string(),
            format!(
                "magnet:?xt=urn:btih:{cd20}&ws=http%3a%2f%2ffoo.com%2fbar\
                 &ws=http%3a%2f%2fbar.com%2ffoo"
            )
        );
        // An escaped value is read unescaped, and reads back as written.
        let m = Magnet::parse(&m.to_string()).unwrap();
        assert_eq!(m.web_seeds, ["http://foo.com/bar", "http://bar.com/foo"]);
    }

    // libtorrent test_magnet.cpp `parse_peer`, and the endpoints
    // `parse_endpoint` refuses.
    #[test]
    fn a_links_peers_are_read_and_written_and_a_bad_one_passed_over() {
        let cd20 = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        let m = Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{cd20}&dn=foo&x.pe=127.0.0.1:43&x.pe=<invalid1>\
             &x.pe=<invalid2>:100&x.pe=[::1]:45&x.pe=127.0.0.1:&x.pe=[::1"
        ))
        .unwrap();
        let peers: Vec<_> = m.peers.iter().map(|p| p.to_string()).collect();
        assert_eq!(peers, ["127.0.0.1:43", "[::1]:45"]);
        assert_eq!(
            m.to_string(),
            format!("magnet:?xt=urn:btih:{cd20}&dn=foo&x.pe=127.0.0.1:43&x.pe=[::1]:45")
        );
    }
}
