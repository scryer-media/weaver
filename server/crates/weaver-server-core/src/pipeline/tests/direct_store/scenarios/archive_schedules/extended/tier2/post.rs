//! What a tier-two job posts, article by article, as the wire carries it.
//!
//! Every article is rendered to the bytes a server would send and decoded by
//! the production fused decoder, so a damaged article reaches the pipeline the
//! way a real one does: through the decoder's own verdict, not a hand-built
//! result.
use super::*;

/// What one posted article carries instead of a clean copy of its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Wire {
    /// The server has no copy.
    Absent,
    Damaged(Damage),
}

/// The ways an article can arrive wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) enum Damage {
    /// The part CRC disagrees; the body is intact.
    CrcWrong,
    /// The body stops short of the declared range; the trailer still says
    /// the full size.
    Truncated,
    /// The body runs past the declared range.
    Overlong,
    /// Wrong payload bytes under a CRC recomputed to match: only a recovery
    /// set's block hashes can see it.
    Recomputed,
    /// The body of another article of the same file under this one's header.
    Swapped,
    /// Junk lines after `=yend`.
    TrailingJunk,
    /// Two copies arrive and the first differs from the posted bytes.
    DifferingDuplicate,
    /// `=yend` with no checksum at all over a wrong payload.
    NoChecksum,
}

pub(in super::super) const DAMAGES: [Damage; 8] = [
    Damage::CrcWrong,
    Damage::Truncated,
    Damage::Overlong,
    Damage::Recomputed,
    Damage::Swapped,
    Damage::TrailingJunk,
    Damage::DifferingDuplicate,
    Damage::NoChecksum,
];

impl Damage {
    /// Bytes this damage may leave wrong on disk, as a superset: the
    /// article's own range, and for an over-long body the range after it.
    fn may_spill(self) -> bool {
        self == Self::Overlong
    }
}

/// How a file's articles are encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Encoding {
    Yenc {
        line: usize,
        /// Multi-part headers. A single-article file may be posted without
        /// `=ypart`, as a single-part article with a whole-file `crc32=`.
        ypart: bool,
    },
    Uu(UuStyle),
}

impl Encoding {
    pub(in super::super) const YENC: Self = Self::Yenc {
        line: 128,
        ypart: true,
    };
}

/// One uuencode dialect, as the posting tools in the field write it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) struct UuStyle {
    pub header: UuHeader,
    /// Free text ahead of the `begin` line.
    pub preamble: bool,
    /// A zero sextet as a space instead of a backtick.
    pub space_zero: bool,
    /// The final group carries only the bytes it needs instead of a full
    /// four characters.
    pub unpadded_tail: bool,
    /// The last line's length character overstates its bytes by one.
    pub wrong_last_length: bool,
    pub end: UuEnd,
    /// Bare LF line endings instead of CRLF.
    pub bare_lf: bool,
    /// Bytes per encoded line: 45 is the standard, shorter lines are posted.
    pub line_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) enum UuHeader {
    /// `begin 644 name`.
    Mode,
    /// `begin name`, no mode.
    NoMode,
    /// No `begin` line at all.
    Missing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) enum UuEnd {
    /// An empty terminating line and `end`.
    Full,
    /// `end` with no empty terminating line before it.
    NoTerminator,
    /// No `end` line at all.
    Missing,
}

impl UuStyle {
    pub(in super::super) const STANDARD: Self = Self {
        header: UuHeader::Mode,
        preamble: false,
        space_zero: false,
        unpadded_tail: false,
        wrong_last_length: false,
        end: UuEnd::Full,
        bare_lf: false,
        line_bytes: 45,
    };
}

/// What a posted file is to a schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Role {
    /// Its articles are what a schedule's slots name.
    Data,
    /// A recovery index: arrives first or last, as the schedule says.
    Index,
    /// Recovery volumes: arrive only when the job asks for them.
    Recovery,
}

/// One file of the post.
#[derive(Clone, Debug)]
pub(in super::super) struct Posted {
    /// The name the NZB and the article headers carry.
    pub name: String,
    pub bytes: Vec<u8>,
    /// Decoded bytes per article; the last article carries the remainder.
    pub article: usize,
    pub role: Role,
    pub encoding: Encoding,
    pub wire: BTreeMap<u32, Wire>,
}

impl Posted {
    pub(in super::super) fn new(name: impl Into<String>, bytes: Vec<u8>, article: usize, role: Role) -> Self {
        Self {
            name: name.into(),
            bytes,
            article: article.max(1),
            role,
            encoding: Encoding::YENC,
            wire: BTreeMap::new(),
        }
    }

    pub(in super::super) fn articles(&self) -> u32 {
        self.bytes.len().div_ceil(self.article).max(1) as u32
    }

    pub(in super::super) fn extent(&self, article: u32) -> Range<usize> {
        let start = (article as usize * self.article).min(self.bytes.len());
        let end = ((article as usize + 1) * self.article).min(self.bytes.len());
        start..end
    }

    /// Every article of the file absent.
    pub(in super::super) fn absent(&mut self) {
        for article in 0..self.articles() {
            self.wire.insert(article, Wire::Absent);
        }
    }

    pub(in super::super) fn is_absent(&self, article: u32) -> bool {
        self.wire.get(&article) == Some(&Wire::Absent)
    }

    /// The bytes this article puts on disk when the pipeline takes it at its
    /// word, for the lower bound of what it leaves wrong; `None` where the
    /// outcome depends on what the pipeline keeps.
    fn definite(&self, article: u32) -> Option<Vec<u8>> {
        let range = self.extent(article);
        match self.wire.get(&article) {
            None => Some(self.bytes[range].to_vec()),
            Some(Wire::Absent) => Some(Vec::new()),
            Some(Wire::Damaged(damage)) => match damage {
                Damage::Recomputed | Damage::NoChecksum => Some(altered(&self.bytes[range])),
                Damage::Swapped => Some(self.swapped_body(article)),
                Damage::Truncated => Some(self.bytes[range.start..range.start + range.len() / 2].to_vec()),
                Damage::CrcWrong
                | Damage::Overlong
                | Damage::TrailingJunk
                | Damage::DifferingDuplicate => None,
            },
        }
    }

    fn swapped_body(&self, article: u32) -> Vec<u8> {
        let len = self.extent(article).len();
        let donor = (0..self.articles())
            .filter(|&other| other != article && self.extent(other).len() == len)
            .min_by_key(|&other| other.abs_diff(article + 1))
            .unwrap_or(article);
        if donor == article {
            altered(&self.bytes[self.extent(article)])
        } else {
            self.bytes[self.extent(donor)].to_vec()
        }
    }

    /// Byte ranges of the file this post may leave wrong, as a superset
    /// (`upper`) and as the ranges it certainly leaves wrong (`lower`).
    pub(in super::super) fn wrong_ranges(&self, lost: &BTreeSet<u32>) -> (Vec<Range<usize>>, Vec<Range<usize>>) {
        let mut upper = Vec::new();
        let mut lower = Vec::new();
        // A uuencode part carries no offset: it is placed after the decoded
        // bytes of the parts before it, so one that is missing or decodes to
        // the wrong length may leave everything behind it misplaced.
        let uu = matches!(self.encoding, Encoding::Uu(_));
        for article in 0..self.articles() {
            let range = self.extent(article);
            if lost.contains(&article) || self.is_absent(article) {
                upper.push(if uu { range.start..self.bytes.len() } else { range.clone() });
                lower.push(range);
                continue;
            }
            let Some(wire) = self.wire.get(&article) else {
                continue;
            };
            let Wire::Damaged(damage) = wire else {
                unreachable!("absence handled above")
            };
            let mut reach = range.clone();
            if uu && matches!(damage, Damage::Truncated | Damage::Overlong) {
                reach.end = self.bytes.len();
            } else if damage.may_spill() {
                reach.end = (reach.end + self.article).min(self.bytes.len());
            }
            upper.push(reach);
            if let Some(written) = self.definite(article) {
                // Positions the written copy leaves different, or does not
                // reach at all.
                for (at, truth) in self.bytes[range.clone()].iter().enumerate() {
                    if written.get(at) != Some(truth) {
                        lower.push(range.start + at..range.start + at + 1);
                    }
                }
            }
        }
        (upper, lower)
    }

    /// The wire copies a request for `article` is answered with, in order.
    pub(in super::super) fn wire_copies(&self, article: u32) -> Vec<Vec<u8>> {
        let range = self.extent(article);
        let data = &self.bytes[range.clone()];
        let part = Part {
            number: article + 1,
            total: self.articles(),
            begin: range.start as u64 + 1,
            end: range.end as u64,
            size: self.bytes.len() as u64,
        };
        let Encoding::Yenc { line, ypart } = self.encoding else {
            let Encoding::Uu(style) = self.encoding else {
                unreachable!()
            };
            let mut body = data.to_vec();
            if let Some(Wire::Damaged(damage)) = self.wire.get(&article) {
                return uu_damaged(&self.name, &body, article, self.articles(), style, *damage);
            }
            body.truncate(data.len());
            return vec![uu_article(&self.name, &body, article, self.articles(), style)];
        };
        let single = !ypart && self.articles() == 1;
        let clean = YencArticle {
            name: &self.name,
            part: (!single).then_some(part),
            line,
            body: data.to_vec(),
            yend_size: data.len() as u64,
            pcrc: (!single).then(|| checksum::crc32(data)),
            crc32: single.then(|| checksum::crc32(data)),
            cut: false,
            junk: false,
        };
        let Some(Wire::Damaged(damage)) = self.wire.get(&article) else {
            return vec![clean.render()];
        };
        let mut copy = clean.clone();
        match damage {
            Damage::CrcWrong => {
                copy.pcrc = copy.pcrc.map(|crc| !crc);
                copy.crc32 = copy.crc32.map(|crc| !crc);
            }
            Damage::Truncated => copy.cut = true,
            Damage::Overlong => {
                copy.body.extend((0..self.article.min(97)).map(|n| (n * 31 + 7) as u8));
            }
            Damage::Recomputed => {
                copy.body = altered(data);
                copy.restate();
            }
            Damage::Swapped => {
                copy.body = self.swapped_body(article);
                copy.restate();
            }
            Damage::TrailingJunk => copy.junk = true,
            Damage::DifferingDuplicate => {
                let mut first = clean.clone();
                first.body = altered(data);
                first.restate();
                return vec![first.render(), clean.render()];
            }
            Damage::NoChecksum => {
                copy.body = altered(data);
                copy.pcrc = None;
                copy.crc32 = None;
            }
        }
        vec![copy.render()]
    }
}

/// Every byte flipped: a wrong copy of the same length.
fn altered(data: &[u8]) -> Vec<u8> {
    data.iter().map(|byte| byte ^ 0x5a).collect()
}

#[derive(Clone, Copy, Debug)]
struct Part {
    number: u32,
    total: u32,
    begin: u64,
    end: u64,
    size: u64,
}

#[derive(Clone, Debug)]
struct YencArticle<'a> {
    name: &'a str,
    part: Option<Part>,
    line: usize,
    body: Vec<u8>,
    yend_size: u64,
    pcrc: Option<u32>,
    crc32: Option<u32>,
    /// The second half of the body's lines are lost in transit.
    cut: bool,
    /// Lines of junk follow the trailer.
    junk: bool,
}

impl YencArticle<'_> {
    /// Checksums restated over the current body.
    fn restate(&mut self) {
        let crc = checksum::crc32(&self.body);
        self.pcrc = self.pcrc.map(|_| crc);
        self.crc32 = self.crc32.map(|_| crc);
    }

    fn render(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self.part {
            Some(part) => {
                out.extend_from_slice(
                    format!(
                        "=ybegin part={} total={} line={} size={} name={}\r\n=ypart begin={} end={}\r\n",
                        part.number, part.total, self.line, part.size, self.name, part.begin, part.end
                    )
                    .as_bytes(),
                );
            }
            None => out.extend_from_slice(
                format!(
                    "=ybegin line={} size={} name={}\r\n",
                    self.line, self.yend_size, self.name
                )
                .as_bytes(),
            ),
        }
        let mut lines = yenc_lines(&self.body, self.line);
        if self.cut {
            lines.truncate(lines.len() / 2);
        }
        for line in lines {
            out.extend_from_slice(&line);
            out.extend_from_slice(b"\r\n");
        }
        let mut trailer = format!("=yend size={}", self.yend_size);
        if let Some(part) = self.part {
            trailer.push_str(&format!(" part={}", part.number));
        }
        if let Some(crc) = self.pcrc {
            trailer.push_str(&format!(" pcrc32={crc:08x}"));
        }
        if let Some(crc) = self.crc32 {
            trailer.push_str(&format!(" crc32={crc:08x}"));
        }
        out.extend_from_slice(trailer.as_bytes());
        out.extend_from_slice(b"\r\n");
        if self.junk {
            out.extend_from_slice(b"-- posted with a signature block --\r\nsee you on the other side\r\n");
        }
        out
    }
}

/// The encoded data lines of `body`, without line endings.
fn yenc_lines(body: &[u8], line: usize) -> Vec<Vec<u8>> {
    if body.is_empty() {
        return Vec::new();
    }
    let mut encoded = Vec::new();
    weaver_yenc::encode(body, &mut encoded, line, "x").unwrap();
    let start = encoded.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
    let end = encoded
        .windows(7)
        .rposition(|w| w == b"\r\n=yend")
        .unwrap();
    encoded[start..end]
        .split(|&byte| byte == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line).to_vec())
        .collect()
}

fn uu_article(name: &str, bytes: &[u8], ordinal: u32, count: u32, style: UuStyle) -> Vec<u8> {
    uu_render(name, bytes, ordinal, count, style, None)
}

/// A uuencode article with one of the damage kinds a uuencode post can carry.
fn uu_damaged(
    name: &str,
    bytes: &[u8],
    ordinal: u32,
    count: u32,
    style: UuStyle,
    damage: Damage,
) -> Vec<Vec<u8>> {
    match damage {
        Damage::DifferingDuplicate => vec![
            uu_render(name, &altered(bytes), ordinal, count, style, None),
            uu_render(name, bytes, ordinal, count, style, None),
        ],
        damage => vec![uu_render(name, bytes, ordinal, count, style, Some(damage))],
    }
}

/// The uuencode damage kinds, as [`Damage`] values: a line cut short, a
/// length character that overstates, a byte outside the alphabet, and a
/// differing duplicate. A missing part is [`Wire::Absent`].
pub(in super::super) const UU_DAMAGES: [Damage; 4] = [
    Damage::Truncated,
    Damage::Overlong,
    Damage::NoChecksum,
    Damage::DifferingDuplicate,
];

fn uu_render(
    name: &str,
    bytes: &[u8],
    ordinal: u32,
    count: u32,
    style: UuStyle,
    damage: Option<Damage>,
) -> Vec<u8> {
    let eol: &[u8] = if style.bare_lf { b"\n" } else { b"\r\n" };
    let sextet = |value: u8| {
        if value & 63 == 0 {
            if style.space_zero { b' ' } else { b'`' }
        } else {
            (value & 63) + 32
        }
    };
    let mut out = Vec::new();
    if ordinal == 0 {
        if style.preamble {
            out.extend_from_slice(b"posted in several parts, reassemble in order");
            out.extend_from_slice(eol);
            out.extend_from_slice(eol);
        }
        match style.header {
            UuHeader::Mode => out.extend_from_slice(format!("begin 644 {name}").as_bytes()),
            UuHeader::NoMode => out.extend_from_slice(format!("begin {name}").as_bytes()),
            UuHeader::Missing => {}
        }
        if style.header != UuHeader::Missing {
            out.extend_from_slice(eol);
        }
    }
    let lines: Vec<&[u8]> = bytes.chunks(style.line_bytes).collect();
    let damaged_line = lines.len() / 2;
    for (index, line) in lines.iter().enumerate() {
        let last = index + 1 == lines.len();
        let mut length = line.len() as u8;
        if last && style.wrong_last_length {
            length += 1;
        }
        if damage == Some(Damage::Overlong) && index == damaged_line {
            length += 3;
        }
        out.push(sextet(length));
        let mut encoded = Vec::new();
        for triple in line.chunks(3) {
            let a = triple[0];
            let b = triple.get(1).copied().unwrap_or(0);
            let c = triple.get(2).copied().unwrap_or(0);
            let group = [
                sextet(a >> 2),
                sextet((a << 4) | (b >> 4)),
                sextet((b << 2) | (c >> 6)),
                sextet(c),
            ];
            let keep = if style.unpadded_tail && last {
                triple.len() + 1
            } else {
                4
            };
            encoded.extend_from_slice(&group[..keep]);
        }
        if damage == Some(Damage::Truncated) && index == damaged_line {
            encoded.truncate(encoded.len() / 2);
        }
        if damage == Some(Damage::NoChecksum) && index == damaged_line && !encoded.is_empty() {
            let at = encoded.len() / 2;
            encoded[at] = b'~' + 1;
        }
        out.extend_from_slice(&encoded);
        out.extend_from_slice(eol);
    }
    if ordinal + 1 == count {
        match style.end {
            UuEnd::Full => {
                out.push(sextet(0));
                out.extend_from_slice(eol);
                out.extend_from_slice(b"end");
                out.extend_from_slice(eol);
            }
            UuEnd::NoTerminator => {
                out.extend_from_slice(b"end");
                out.extend_from_slice(eol);
            }
            UuEnd::Missing => {}
        }
    }
    out
}

/// The NNTP response a server sends for one article body: a status line,
/// the body with dot-stuffing, and the terminating dot.
pub(in super::super) fn on_the_wire(body: &[u8]) -> Vec<u8> {
    let mut wire = b"222 0 <tier2@example.invalid> body follows\r\n".to_vec();
    for line in body.split_inclusive(|&byte| byte == b'\n') {
        if line.first() == Some(&b'.') {
            wire.push(b'.');
        }
        wire.extend_from_slice(line);
    }
    if !body.ends_with(b"\n") {
        wire.extend_from_slice(b"\r\n");
    }
    wire.extend_from_slice(b".\r\n");
    wire
}

/// A whole job as posted.
#[derive(Clone, Debug)]
pub(in super::super) struct Post {
    pub files: Vec<Posted>,
    pub password: Option<String>,
    /// What a complete job must publish, by path under its output
    /// directory, byte for byte.
    pub expected: Vec<(String, Vec<u8>)>,
    /// Further files a complete job may publish besides `expected`, such as
    /// furniture posted beside the archive.
    pub allowed: Vec<String>,
}

impl Post {
    pub(in super::super) fn spec(&self) -> JobSpec {
        JobSpec {
            name: "Silver Horizon".to_string(),
            password: self.password.clone(),
            total_bytes: self
                .files
                .iter()
                .map(|file| u64::from(yenc_declared_bytes(file.bytes.len() as u32)))
                .sum(),
            category: None,
            metadata: vec![],
            files: self
                .files
                .iter()
                .enumerate()
                .map(|(index, file)| FileSpec {
                    filename: file.name.clone(),
                    role: FileRole::from_filename(&file.name),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: (0..file.articles())
                        .map(|article| {
                            segment_spec! {
                                number: article,
                                bytes: yenc_declared_bytes(file.extent(article).len() as u32),
                                message_id: format!("tier2-{index}-{article}@example.invalid"),
                            }
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    pub(in super::super) fn index_of(&self, role: Role) -> Vec<usize> {
        (0..self.files.len())
            .filter(|&index| self.files[index].role == role)
            .collect()
    }
}
