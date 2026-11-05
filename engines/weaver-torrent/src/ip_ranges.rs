// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use anyhow::{Context, Result};
use async_compression::tokio::bufread::GzipDecoder;
use futures::TryStreamExt;
use std::iter::empty;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::ops::Range;
use std::path::Path;
use std::str::FromStr;
use tokio::io::{AsyncBufRead, AsyncRead};
use tokio::{io::AsyncBufReadExt, io::BufReader};
use tokio_util::io::StreamReader;
use tracing::trace;
use url::Url;

// The ranges sorted by start with overlaps merged, so a lookup is one binary
// search. `len` counts the ranges as given.
struct SortedRanges<T> {
    ranges: Vec<Range<T>>,
    len: usize,
}

fn sorted_ranges<T: Copy + Ord>(it: impl Iterator<Item = Range<T>>) -> SortedRanges<T> {
    let mut len = 0;
    let mut ranges: Vec<Range<T>> = it
        .inspect(|_| len += 1)
        .filter(|r| r.start < r.end)
        .collect();
    ranges.sort_unstable_by_key(|r| r.start);
    let mut merged: Vec<Range<T>> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match merged.last_mut() {
            Some(last) if r.start <= last.end => last.end = last.end.max(r.end),
            _ => merged.push(r),
        }
    }
    SortedRanges {
        ranges: merged,
        len,
    }
}

impl<T: Ord> SortedRanges<T> {
    fn contains(&self, point: &T) -> bool {
        let after = self.ranges.partition_point(|r| r.start <= *point);
        after > 0 && *point < self.ranges[after - 1].end
    }
}

pub struct IpRanges {
    // We could store only one interval tree, but splitting them takes less memory,
    // as IpAddr is 17 bytes, Ipv4Addr is only 4 bytes (the majority of ranges).
    v4: SortedRanges<Ipv4Addr>,
    v6: SortedRanges<Ipv6Addr>,
    // Keep explicit mapped ranges separate before broader IPv6 ranges merge.
    // Their IPv6 exclusive endpoints can represent the end past IPv4's maximum.
    mapped_v4: SortedRanges<Ipv6Addr>,
}

impl Default for IpRanges {
    fn default() -> Self {
        Self {
            v4: sorted_ranges(empty()),
            v6: sorted_ranges(empty()),
            mapped_v4: sorted_ranges(empty()),
        }
    }
}

impl IpRanges {
    pub fn new(
        v4_ranges: impl IntoIterator<Item = Range<Ipv4Addr>>,
        v6_ranges: impl IntoIterator<Item = Range<Ipv6Addr>>,
    ) -> Self {
        let mapped_end = Ipv4Addr::BROADCAST.to_ipv6_mapped().to_bits() + 1;
        let (mapped, v6): (Vec<_>, Vec<_>) = v6_ranges.into_iter().partition(|range| {
            range.start.to_ipv4_mapped().is_some()
                && range.end.to_bits() <= mapped_end
                && range.start < range.end
        });
        Self {
            v4: sorted_ranges(v4_ranges.into_iter()),
            v6: sorted_ranges(v6.into_iter()),
            mapped_v4: sorted_ranges(mapped.into_iter()),
        }
    }

    /// The ranges of every one of `lists`, as one list (libtorrent's one
    /// `ip_filter`, which qBittorrent's `SessionImpl::processBannedIPs`
    /// adds the banned addresses to over the parsed filter file).
    pub fn union<'a>(lists: impl IntoIterator<Item = &'a IpRanges>) -> Self {
        let (mut v4, mut v6) = (Vec::new(), Vec::new());
        for list in lists {
            v4.extend(list.v4.ranges.iter().cloned());
            v6.extend(list.v6.ranges.iter().cloned());
            v6.extend(list.mapped_v4.ranges.iter().cloned());
        }
        Self::new(v4, v6)
    }

    /// The addresses from each first to its last, both included.
    pub fn of_inclusive(ranges: impl IntoIterator<Item = (IpAddr, IpAddr)>) -> Self {
        let (mut v4, mut v6) = (Vec::new(), Vec::new());
        for (first, last) in ranges {
            match inclusive(first, last) {
                Some(IpRange::V4(range)) => v4.push(range),
                Some(IpRange::V6(range)) => v6.push(range),
                None => {}
            }
        }
        Self::new(v4, v6)
    }

    pub fn len(&self) -> usize {
        self.v4.len + self.v6.len + self.mapped_v4.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The list at `url`: a file read from disk, anything else fetched
    /// with a client from `clients`.
    pub async fn load_from_url(clients: &dyn crate::HttpClients, url: &str) -> Result<Self> {
        let parsed_url = Url::parse(url).context("failed to parse URL")?;

        if parsed_url.scheme() == "file" {
            let path = parsed_url
                .to_file_path()
                .ok()
                .context("failed to convert file URL to path")?;
            return Self::load_from_file(path).await;
        }

        let response = clients
            .client(None, &crate::ClientShape::FETCH)
            .context("error building HTTP(S) client")?
            .get(parsed_url)
            .send()
            .await
            .context("error fetching list")?;
        if !response.status().is_success() {
            anyhow::bail!("error fetching list: HTTP {}", response.status());
        }

        let mut reader = StreamReader::new(response.bytes_stream().map_err(std::io::Error::other));
        let bl = Self::create_from_stream(&mut reader).await?;
        Ok(bl)
    }

    pub async fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = tokio::fs::File::open(path).await?;
        Self::create_from_stream(&mut file).await
    }

    /// The ranges read from `reader`: a list as text, or gzipped.
    pub async fn create_from_stream(reader: &mut (dyn AsyncRead + Unpin + Send)) -> Result<Self> {
        let mut peek_bytes = [0u8; 2];
        let mut reader = tokio::io::BufReader::new(reader);

        // Peek the first bytes by filling buffer
        let buffer = reader.fill_buf().await?;
        if buffer.len() >= 2 {
            peek_bytes.copy_from_slice(&buffer[0..2]);
        } else {
            anyhow::bail!("content too short: not enough data to determine compression");
        }

        // Check for Gzip magic bytes
        let is_gzip = peek_bytes == [0x1F, 0x8B];

        if is_gzip {
            trace!("detected gzip stream, decompressing");
            Self::create_from_decoded_stream(&mut BufReader::new(GzipDecoder::new(reader))).await
        } else {
            trace!("plain text file detected.");
            Self::create_from_decoded_stream(&mut reader).await
        }
    }

    async fn create_from_decoded_stream(
        reader: &mut (dyn AsyncBufRead + Unpin + Send),
    ) -> Result<Self> {
        let mut v4 = Vec::new();
        let mut v6 = Vec::new();

        let mut line = String::new();

        while reader.read_line(&mut line).await? > 0 {
            match parse_ip_range(&line) {
                Some(IpRange::V4(r)) => {
                    v4.push(r);
                }
                Some(IpRange::V6(r)) => {
                    v6.push(r);
                }
                None => {
                    tracing::debug!(line, "couldn't parse line");
                }
            }
            line.clear();
        }

        Ok(Self::new(v4, v6))
    }

    pub fn has(&self, ip: IpAddr) -> bool {
        match crate::network::canonical_ip_addr(ip) {
            IpAddr::V4(a) => self.v4.contains(&a) || self.mapped_v4.contains(&a.to_ipv6_mapped()),
            IpAddr::V6(a) => self.v6.contains(&a),
        }
    }
}

enum IpRange {
    V4(Range<Ipv4Addr>),
    V6(Range<Ipv6Addr>),
}

fn parse_ip_range(line: &str) -> Option<IpRange> {
    let line = line.trim();
    if line.starts_with('#') || line.is_empty() {
        return None;
    }

    let (_name, ips) = {
        let is_ipv6 = line.matches(":").count() > 2;
        if is_ipv6 {
            line.split_once(':')?
        } else {
            line.rsplit_once(':')?
        }
    };
    let (start, end) = ips.split_once('-')?;
    inclusive(IpAddr::from_str(start).ok()?, IpAddr::from_str(end).ok()?)
}

// The range from `start` to `end`, both included, when both are of one family.
fn inclusive(start: IpAddr, end: IpAddr) -> Option<IpRange> {
    match (start, end) {
        (IpAddr::V4(start), IpAddr::V4(end)) => {
            let end = Ipv4Addr::from_bits(end.to_bits().saturating_add(1));
            Some(IpRange::V4(start..end))
        }
        (IpAddr::V6(start), IpAddr::V6(end)) => {
            let end = Ipv6Addr::from_bits(end.to_bits().saturating_add(1));
            Some(IpRange::V6(start..end))
        }
        _ => None,
    }
}

#[cfg(test)]
mod mapped_ranges_tests;

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::tests::test_util::setup_test_logging;

    use super::*;
    use async_compression::tokio::write::GzipEncoder;
    use tokio::io::AsyncWriteExt;

    const LIST: &[u8] = br#"
    # test
    local:192.168.1.1-192.168.1.255
    localv6:2001:db8::1-2001:db8::ffff
    "#;

    #[tokio::test]
    async fn test_list_gzipped() -> Result<()> {
        let mut gzipped_list = Vec::new();
        {
            let mut encoder = GzipEncoder::new(&mut gzipped_list);
            encoder.write_all(LIST).await.unwrap();
            encoder.flush().await.unwrap();
            encoder.shutdown().await.unwrap();
        }
        let list = IpRanges::create_from_stream(&mut Cursor::new(gzipped_list)).await?;
        assert!(list.has("192.168.1.1".parse().unwrap()));
        assert!(!list.has("8.8.8.8".parse().unwrap()));

        Ok(())
    }

    #[tokio::test]
    async fn test_list_plaintext() -> Result<()> {
        let list = IpRanges::create_from_stream(&mut Cursor::new(LIST)).await?;
        assert!(list.has("192.168.1.1".parse().unwrap()));
        assert!(!list.has("8.8.8.8".parse().unwrap()));

        Ok(())
    }

    #[tokio::test]
    async fn test_list_from_plaintext_file() -> Result<()> {
        // Create a temporary file
        let mut temp_file = tokio::fs::File::create("temp_list.txt").await?;
        tokio::io::AsyncWriteExt::write_all(&mut temp_file, LIST).await?;
        drop(temp_file); // Close the file

        // Load the list from the file
        let list = IpRanges::load_from_file("temp_list.txt").await?;

        // Verify the list
        assert!(list.has("192.168.1.1".parse().unwrap()));
        assert!(!list.has("8.8.8.8".parse().unwrap()));
        assert!(list.has("2001:db8::1".parse().unwrap()));
        assert!(!list.has("2001:4860:4860::8888".parse().unwrap()));

        // Clean up the temporary file
        tokio::fs::remove_file("temp_list.txt").await?;

        Ok(())
    }

    // qBittorrent `SessionImpl::processBannedIPs` adds the banned ranges to
    // the filter parsed from the list: one filter holds both.
    #[test]
    fn a_union_holds_every_range_of_every_list() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let v4 = |a: &str, b: &str| a.parse::<Ipv4Addr>().unwrap()..b.parse().unwrap();
        let v6 = "2001:db8::".parse::<Ipv6Addr>().unwrap().."2001:db8::10".parse().unwrap();
        let list = IpRanges::new([v4("10.0.0.0", "10.0.1.0")], [v6]);
        let bans = IpRanges::of_inclusive([
            (ip("10.0.0.200"), ip("10.0.1.255")),
            (ip("192.0.2.7"), ip("192.0.2.7")),
            (ip("192.0.2.9"), ip("2001:db8::1")),
        ]);
        let union = IpRanges::union([&list, &bans]);
        for blocked in ["10.0.0.1", "10.0.1.255", "192.0.2.7", "2001:db8::f"] {
            assert!(union.has(ip(blocked)), "{blocked}");
        }
        for open in ["10.0.2.0", "192.0.2.8", "192.0.2.9", "2001:db8::10"] {
            assert!(!union.has(ip(open)), "{open}");
        }
        assert!(!IpRanges::union([]).has(ip("10.0.0.1")));
    }

    #[test]
    fn test_list_empty() {
        let list = IpRanges::default();
        assert!(!list.has("127.0.0.1".parse().unwrap()));
        assert!(!list.has("::1".parse().unwrap()));
    }

    #[test]
    fn test_manual_ranges() {
        // Add IPv4 range
        let start_v4: Ipv4Addr = "192.168.0.0".parse().unwrap();
        let end_v4: Ipv4Addr = "192.168.255.255".parse().unwrap();
        let ipv4_range = start_v4..end_v4;

        // Add IPv6 range
        let start_v6: Ipv6Addr = "2001:db8::".parse().unwrap();
        let end_v6: Ipv6Addr = "2001:db8::ffff".parse().unwrap();
        let ipv6_range = start_v6..end_v6;

        let list = IpRanges::new(Some(ipv4_range), Some(ipv6_range));
        // Test IPv4 addresses
        assert!(list.has("192.168.1.1".parse().unwrap()));
        assert!(!list.has("10.0.0.1".parse().unwrap()));

        // Test IPv6 addresses
        assert!(list.has("2001:db8::1".parse().unwrap()));
        assert!(!list.has("2001:db9::1".parse().unwrap()));
    }

    #[test]
    fn overlapping_and_touching_ranges_merge() {
        let sorted = sorted_ranges([10u32..20, 5..12, 20..25, 30..30, 40..41, 35..38].into_iter());
        assert_eq!(sorted.len, 6);
        assert_eq!(sorted.ranges, vec![5..25, 35..38, 40..41]);
        for (p, inside) in [
            (4, false),
            (5, true),
            (24, true),
            (25, false),
            (30, false),
            (37, true),
            (38, false),
            (40, true),
            (41, false),
        ] {
            assert_eq!(sorted.contains(&p), inside, "{p}");
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_list_real_url() {
        setup_test_logging();
        let clients = crate::test_support::Loopback::default();
        let _ = IpRanges::load_from_url(&clients, "https://raw.githubusercontent.com/Naunter/BT_BlockLists/refs/heads/master/bt_blocklists.gz")
            .await
            .unwrap();
    }
}
