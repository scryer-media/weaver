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
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};
use tokio::{io::AsyncBufReadExt, io::BufReader};
use tokio_util::io::StreamReader;
use tracing::trace;
use url::Url;

/// The most bytes a list may hold once decompressed. The largest lists in
/// use, the combined P2P blocklists of a few hundred thousand to some
/// million ranges, run to some tens of megabytes as text; this holds
/// several times that, and stops a small gzip that inflates without end.
pub const MAX_LIST_BYTES: u64 = 256 * 1024 * 1024;

/// The most lines a list may hold, comments and blanks counted: several
/// times the ranges of the largest lists in use.
pub const MAX_LIST_LINES: usize = 8_000_000;

// The text a list is parsed in, read off the stream a part at a time and
// parsed off the async runtime, so neither the whole text nor the parse
// sits on it.
const PARSE_CHUNK: usize = 1024 * 1024;

// What a list may hold, and the parts it is parsed in.
#[derive(Clone, Copy)]
struct Caps {
    bytes: u64,
    lines: usize,
    chunk: usize,
}

const CAPS: Caps = Caps {
    bytes: MAX_LIST_BYTES,
    lines: MAX_LIST_LINES,
    chunk: PARSE_CHUNK,
};

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
        let block = Ipv4Addr::UNSPECIFIED.to_ipv6_mapped().to_bits()
            ..Ipv4Addr::BROADCAST.to_ipv6_mapped().to_bits() + 1;
        let (mut mapped, mut v6) = (Vec::new(), Vec::new());
        for range in v6_ranges {
            let (start, end) = (range.start.to_bits(), range.end.to_bits());
            // A range with an endpoint in the mapped block covers its share of
            // the block; a broader IPv6 range does not reach IPv4 peers.
            let reaches = start < end && (block.contains(&start) || block.contains(&(end - 1)));
            if reaches {
                mapped.push(
                    Ipv6Addr::from_bits(start.max(block.start))
                        ..Ipv6Addr::from_bits(end.min(block.end)),
                );
            }
            if !reaches || start < block.start || end > block.end {
                v6.push(range);
            }
        }
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

    // Reads the text a part at a time, at most [`MAX_LIST_BYTES`] of it
    // and [`MAX_LIST_LINES`] lines, and parses each part's whole lines, then
    // sorts and merges the ranges, off the async runtime. A list past
    // either cap is refused whole.
    async fn create_from_decoded_stream(
        reader: &mut (dyn AsyncBufRead + Unpin + Send),
    ) -> Result<Self> {
        Self::parse_capped(reader, CAPS).await
    }

    async fn parse_capped(reader: &mut (dyn AsyncRead + Unpin + Send), caps: Caps) -> Result<Self> {
        let mut parsed = Parsed::default();
        let mut pending: Vec<u8> = Vec::new();
        let mut read: u64 = 0;
        loop {
            let mut chunk = std::mem::take(&mut pending);
            let had = chunk.len();
            // One byte past the cap tells a list at the cap from one past it.
            let room = (caps.bytes + 1 - read).min(caps.chunk as u64);
            let n = (&mut *reader).take(room).read_to_end(&mut chunk).await?;
            read += n as u64;
            if read > caps.bytes {
                anyhow::bail!("the list is larger than {} bytes decompressed", caps.bytes);
            }
            let end = n == 0;
            // A part's last line may go on in the next; the last part's
            // last line ends with the stream.
            let whole = if end {
                chunk.len()
            } else {
                match memchr::memrchr(b'\n', &chunk[had..]) {
                    Some(at) => had + at + 1,
                    None => {
                        pending = chunk;
                        continue;
                    }
                }
            };
            pending = chunk.split_off(whole);
            parsed = tokio::task::spawn_blocking(move || parsed.lines(&chunk, caps.lines))
                .await
                .context("parsing the list")??;
            if end {
                break;
            }
        }
        let Parsed { v4, v6, .. } = parsed;
        tokio::task::spawn_blocking(move || Self::new(v4, v6))
            .await
            .context("building the list")
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

// The ranges parsed so far, and the lines read.
#[derive(Default)]
struct Parsed {
    v4: Vec<Range<Ipv4Addr>>,
    v6: Vec<Range<Ipv6Addr>>,
    lines: usize,
}

impl Parsed {
    // Parses `text`, whole lines of the list: a line that is no range is
    // passed over. Text that is not UTF-8 refuses the list, as a line read
    // as a string does.
    fn lines(mut self, text: &[u8], max_lines: usize) -> Result<Self> {
        let text = std::str::from_utf8(text).context("the list is not UTF-8 text")?;
        for line in text.split_inclusive('\n') {
            self.lines += 1;
            if self.lines > max_lines {
                anyhow::bail!("the list has more than {max_lines} lines");
            }
            match parse_ip_range(line) {
                Some(IpRange::V4(r)) => self.v4.push(r),
                Some(IpRange::V6(r)) => self.v6.push(r),
                None => tracing::debug!(line, "couldn't parse line"),
            }
        }
        Ok(self)
    }
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

    // The list as each line read as a string parsed it, before lists were
    // read a part at a time: the reference the test below compares with.
    async fn read_by_line(text: &[u8]) -> Result<(Vec<Range<Ipv4Addr>>, Vec<Range<Ipv6Addr>>)> {
        let mut reader = BufReader::new(Cursor::new(text.to_vec()));
        let (mut v4, mut v6) = (Vec::new(), Vec::new());
        let mut line = String::new();
        while reader.read_line(&mut line).await? > 0 {
            match parse_ip_range(&line) {
                Some(IpRange::V4(r)) => v4.push(r),
                Some(IpRange::V6(r)) => v6.push(r),
                None => {}
            }
            line.clear();
        }
        Ok((v4, v6))
    }

    // A list read a part at a time holds what reading it line by line
    // gave, over seeded lists of ranges, comments, junk, long lines and a
    // last line with no newline, at part sizes that split lines anywhere.
    #[tokio::test]
    async fn a_list_read_in_parts_holds_what_reading_by_line_gives() {
        let mut rng = 0xa54f_f53a_5f1d_36f1u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for run in 0..60 {
            let mut text = String::new();
            for _ in 0..(next() % 200) {
                let r = next();
                match r % 6 {
                    0 => text.push_str("# a comment\n"),
                    1 => text.push_str("  \n"),
                    2 => text.push_str(&format!(
                        "name é{}:not-a-range\n",
                        "x".repeat((r >> 8) as usize % 300)
                    )),
                    3 => {
                        let v6 = |b: u64| Ipv6Addr::from_bits(u128::from(b) << 64);
                        text.push_str(&format!(
                            "v6:{}-{}\n",
                            v6(r >> 40),
                            v6((r >> 40) + (r >> 8) % 9)
                        ));
                    }
                    _ => {
                        let a = Ipv4Addr::from_bits(u32::try_from(r >> 32).unwrap());
                        let b = Ipv4Addr::from_bits(
                            a.to_bits()
                                .saturating_add(u32::try_from((r >> 8) % 1000).unwrap()),
                        );
                        text.push_str(&format!("list {run}:{a}-{b}\n"));
                    }
                }
            }
            if next() % 2 == 0 {
                text.push_str("last:10.0.0.1-10.0.0.9");
            }
            let (v4, v6) = read_by_line(text.as_bytes()).await.unwrap();
            let by_line = IpRanges::new(v4, v6);
            for chunk in [1, 7, 64, 4096] {
                let caps = Caps { chunk, ..CAPS };
                let parts =
                    IpRanges::parse_capped(&mut Cursor::new(text.clone().into_bytes()), caps)
                        .await
                        .unwrap();
                assert_eq!(parts.len(), by_line.len(), "run {run} chunk {chunk}");
                assert_eq!(
                    parts.v4.ranges, by_line.v4.ranges,
                    "run {run} chunk {chunk}"
                );
                assert_eq!(
                    parts.v6.ranges, by_line.v6.ranges,
                    "run {run} chunk {chunk}"
                );
                assert_eq!(
                    parts.mapped_v4.ranges, by_line.mapped_v4.ranges,
                    "run {run} chunk {chunk}"
                );
            }
        }
        // Text that is not UTF-8 refuses the list either way.
        let bad = b"a:10.0.0.1-10.0.0.2\n\xff\xfe:1.1.1.1-1.1.1.2\n";
        assert!(read_by_line(bad).await.is_err());
        assert!(
            IpRanges::parse_capped(&mut Cursor::new(bad.to_vec()), CAPS)
                .await
                .is_err()
        );
    }

    // A list at the caps loads; a byte or a line past either is refused
    // with an error naming the cap, a gzip that inflates past it too.
    #[tokio::test]
    async fn a_list_past_its_caps_is_refused() {
        let line = b"a:10.0.0.1-10.0.0.2\n";
        let text: Vec<u8> = line.repeat(10);
        let bytes = text.len() as u64;
        let at = |bytes, lines| Caps {
            bytes,
            lines,
            chunk: 16,
        };
        let load = |text: Vec<u8>, caps| async move {
            IpRanges::parse_capped(&mut Cursor::new(text), caps).await
        };
        assert_eq!(load(text.clone(), at(bytes, 10)).await.unwrap().len(), 10);
        let err = load(text.clone(), at(bytes - 1, 10)).await.err().unwrap();
        assert!(err.to_string().contains("larger than"), "{err}");
        let err = load(text.clone(), at(bytes, 9)).await.err().unwrap();
        assert!(err.to_string().contains("more than 9 lines"), "{err}");

        // A gzip far smaller than what it inflates to.
        let mut gzipped = Vec::new();
        {
            let mut encoder = GzipEncoder::new(&mut gzipped);
            encoder.write_all(&vec![b'#'; 1 << 20]).await.unwrap();
            encoder.shutdown().await.unwrap();
        }
        assert!(gzipped.len() < 1 << 16);
        let mut reader = tokio::io::BufReader::new(Cursor::new(gzipped));
        let mut inflated = BufReader::new(GzipDecoder::new(&mut reader));
        let err = IpRanges::parse_capped(&mut inflated, at(1 << 16, 10))
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("larger than"), "{err}");
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
