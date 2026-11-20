// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use weaver_torrent::core::InfoHashes;
use weaver_torrent::{NamedBy, SeedUrls, TorrentVersion, WebSeed, WebSeedKind, torrent_from_bytes};

use super::admission::Manifest;
use super::layout::{PieceLayout, layout_of};
use super::model::TrackerEntry;

// A tracker the torrent's metainfo names.
pub fn tracker(tier: u32, url: &[u8]) -> Option<TrackerEntry> {
    let url = std::str::from_utf8(url).ok()?.trim();
    (!url.is_empty()).then(|| TrackerEntry {
        tier,
        url: url.to_owned(),
        named_by: NamedBy::Torrent,
    })
}

// A torrent's metainfo, read.
pub struct Parsed {
    pub hashes: InfoHashes,
    pub manifest: Manifest,
    pub layout: Arc<PieceLayout>,
    pub private: bool,
    // The metainfo names a CA its peers' certificates are signed by.
    pub ssl: bool,
    pub trackers: Vec<TrackerEntry>,
    pub total: u64,
    pub about: About,
}

// What a torrent's metainfo says of where it came from.
#[derive(Debug, Clone, Default)]
pub struct About {
    pub version: Option<TorrentVersion>,
    pub created_by: Option<String>,
    pub created_at_ms: Option<i64>,
    pub comment: Option<String>,
}

fn text(bytes: Option<&[u8]>) -> Option<String> {
    let text = String::from_utf8_lossy(bytes?).trim().to_owned();
    (!text.is_empty()).then_some(text)
}

// Whether a torrent of this metainfo reads as private: what a restore of
// it reads, false for a metainfo that does not read.
pub fn metainfo_is_private(bytes: &[u8]) -> bool {
    parse(bytes).is_ok_and(|parsed| parsed.private)
}

pub fn parse(bytes: &[u8]) -> Result<Parsed, String> {
    let invalid = |error: &dyn std::fmt::Display| format!("not a torrent: {error}");
    let metainfo = torrent_from_bytes(bytes).map_err(|error| invalid(&error))?;
    let info = metainfo
        .info
        .data
        .validate()
        .map_err(|error| invalid(&error))?;
    let layout = Arc::new(layout_of(&info).map_err(|error| invalid(&error))?);
    let name = info
        .name()
        .map(|name| name.into_owned())
        .unwrap_or_default();
    let mut trackers: Vec<TrackerEntry> = metainfo
        .announce_list
        .iter()
        .enumerate()
        .flat_map(|(tier, urls)| {
            urls.iter()
                .filter_map(move |url| tracker(tier as u32, url.as_ref()))
        })
        .collect();
    if trackers.is_empty() {
        trackers.extend(
            metainfo
                .announce
                .as_ref()
                .and_then(|url| tracker(0, url.as_ref())),
        );
    }
    Ok(Parsed {
        hashes: metainfo.info_hashes,
        manifest: Manifest {
            name,
            single_file: info.is_single_file(),
            paths: layout
                .files()
                .filter(|file| !file.padding)
                .map(|file| file.name.clone())
                .collect(),
        },
        total: layout
            .files()
            .filter(|file| !file.padding)
            .map(|file| file.length)
            .sum(),
        private: info.info().private,
        ssl: info.info().ssl_cert.is_some(),
        layout,
        trackers,
        about: About {
            version: Some(info.version()),
            created_by: text(metainfo.created_by.as_ref().map(|by| by.as_ref())),
            created_at_ms: metainfo
                .creation_date
                .and_then(|secs| i64::try_from(secs).ok())
                .and_then(|secs| secs.checked_mul(1000)),
            comment: text(metainfo.comment.as_ref().map(|comment| comment.as_ref())),
        },
    })
}

// The metainfo a magnet's fetch gave, with the web seeds the link named
// (`ws`, in `added`) written into its BEP 19 `url-list` beside the info
// dict: it is stored, sealed, with them, and a restart reads them back from
// it (libtorrent keeps a magnet's `url_seeds` with the torrent it makes).
// The list holds the URLs it held, then each added one it lacks, in the
// link's order, each once. The info dict is written back as read, so the
// info hash is the torrent's; a fetch's metainfo holds only `announce`,
// `announce-list` and `info`, which read the same written back (an empty
// `announce-list` is left out). The metainfo comes back as fetched when the
// link named no seed, or when with the seeds it would pass what any torrent
// file may hold (`torrent_from_bytes`: `MAX_TORRENT_FILE_BYTES`,
// `MAX_TORRENT_TOKENS`).
pub fn with_web_seeds(metainfo: Bytes, added: &[WebSeed]) -> Bytes {
    use weaver_torrent::ByteBuf;
    use weaver_torrent::bencode::{BencodeValue, bencode_serialize_to_writer, from_bytes};

    let added: Vec<&[u8]> = added
        .iter()
        .filter(|seed| seed.kind == WebSeedKind::Url && !seed.url.is_empty())
        .map(|seed| seed.url.as_bytes())
        .collect();
    if added.is_empty() {
        return metainfo;
    }
    let mut list = Vec::new();
    let merged = (|| {
        let mut meta = torrent_from_bytes(&metainfo).ok()?;
        let mut urls: Vec<&[u8]> = Vec::new();
        if let Some(listed) = &meta.url_list {
            match from_bytes::<BencodeValue<ByteBuf<'_>>>(listed.0.0) {
                Ok(BencodeValue::Bytes(url)) => urls.push(url.0),
                Ok(BencodeValue::List(listed)) => {
                    urls.extend(listed.into_iter().filter_map(|url| match url {
                        BencodeValue::Bytes(url) => Some(url.0),
                        _ => None,
                    }))
                }
                _ => {}
            }
        }
        urls.extend(added.iter().copied());
        let mut seen = HashSet::new();
        list.push(b'l');
        for url in urls {
            if !url.is_empty() && seen.insert(url) {
                list.extend_from_slice(format!("{}:", url.len()).as_bytes());
                list.extend_from_slice(url);
            }
        }
        list.push(b'e');
        meta.url_list = Some(SeedUrls(ByteBuf(&list)));
        let mut out = Vec::with_capacity(metainfo.len() + list.len() + 12);
        bencode_serialize_to_writer(&meta, &mut out).ok()?;
        torrent_from_bytes(&out).ok()?;
        Some(Bytes::from(out))
    })();
    merged.unwrap_or_else(|| {
        tracing::warn!(
            seeds = added.len(),
            "a magnet's web seeds were not kept: its metainfo would not hold them"
        );
        metainfo
    })
}
