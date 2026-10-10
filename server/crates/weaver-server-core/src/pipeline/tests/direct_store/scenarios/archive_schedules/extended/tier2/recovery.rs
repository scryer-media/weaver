//! Recovery sets authored over a post, and the oracle that says what they can
//! mend.
//!
//! Sets come from the real writers, `par2_rs::create` and `par3_rs::creation`,
//! over the true bytes of the data files. The oracle counts source blocks in
//! the writers' own geometry: what a run may leave wrong (an upper bound) and
//! what it certainly leaves wrong (a lower bound), against the recovery blocks
//! that survive the post's own damage to its recovery files.
use super::post::{Post, Posted, Role};
use super::run::Verdict;
use super::*;

/// How much recovery a set carries against what the post loses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) enum Margin {
    /// Enough and more: a tenth over, at least two blocks, and never under
    /// the redundancy the set was posted with.
    With,
    /// Exactly the blocks the worst case needs.
    Exact,
    /// One block short of what the post certainly lost.
    OneShort,
    /// One block more than the worst case needs.
    OneOver,
}

pub(in super::super) const MARGINS: [Margin; 3] = [Margin::With, Margin::Exact, Margin::OneShort];

/// The PAR2 realism family's margins: the three above and the one-block
/// edge above exact.
pub(in super::super) const PAR2_MARGINS: [Margin; 4] =
    [Margin::With, Margin::OneOver, Margin::Exact, Margin::OneShort];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) enum Code {
    Cauchy,
    Fft,
}

/// The geometry a run's recovery set was authored in.
#[derive(Clone, Copy, Debug)]
pub(in super::super) struct Geometry {
    /// Source block size.
    pub block: usize,
    /// Small files share tail blocks, so a certain loss cannot be counted
    /// per file: the lower bound is not used to rule a failure.
    pub packed: bool,
}

/// Distinct `block`-sized blocks the ranges touch.
fn blocks_touched(ranges: &[Range<usize>], block: usize) -> usize {
    let mut touched = BTreeSet::new();
    for range in ranges {
        if range.is_empty() {
            continue;
        }
        for index in range.start / block..=(range.end - 1) / block {
            touched.insert(index);
        }
    }
    touched.len()
}

/// Byte ranges of each recovery packet in a PAR2 or PAR3 file.
pub(in super::super) fn recovery_packets(bytes: &[u8]) -> Vec<Range<usize>> {
    let mut packets = Vec::new();
    let mut at = 0;
    while at + 64 <= bytes.len() {
        let packet = &bytes[at..];
        let (length, recovery) = if packet.starts_with(b"PAR2\0PKT") {
            (
                u64::from_le_bytes(packet[8..16].try_into().unwrap()) as usize,
                &packet[48..64] == b"PAR 2.0\0RecvSlic",
            )
        } else if packet.starts_with(b"PAR3\0PKT") {
            (
                u64::from_le_bytes(packet[24..32].try_into().unwrap()) as usize,
                &packet[40..48] == b"PAR REC\0",
            )
        } else {
            at += 4;
            continue;
        };
        if length < 48 || at + length > bytes.len() {
            at += 4;
            continue;
        }
        if recovery {
            packets.push(at..at + length);
        }
        at += length;
    }
    packets
}

/// Source blocks the post may leave wrong and certainly leaves wrong.
pub(in super::super) fn needed(
    post: &Post,
    geometry: Geometry,
    lost: &BTreeMap<usize, BTreeSet<u32>>,
) -> (usize, usize) {
    let none = BTreeSet::new();
    let mut upper = 0;
    let mut lower = 0;
    for file in post.index_of(Role::Data) {
        let (may, must) = post.files[file].wrong_ranges(lost.get(&file).unwrap_or(&none));
        upper += blocks_touched(&may, geometry.block);
        lower += blocks_touched(&must, geometry.block);
    }
    if geometry.packed {
        lower = lower.min(1);
    }
    (upper, lower)
}

/// Recovery blocks that survive the post: at least and at most.
pub(in super::super) fn surviving(
    post: &Post,
    lost: &BTreeMap<usize, BTreeSet<u32>>,
    starved: bool,
) -> (usize, usize) {
    if starved {
        return (0, 0);
    }
    let none = BTreeSet::new();
    let mut least = 0;
    let mut most = 0;
    for file in post.index_of(Role::Recovery) {
        let posted = &post.files[file];
        let packets = recovery_packets(&posted.bytes);
        let (may, must) = posted.wrong_ranges(lost.get(&file).unwrap_or(&none));
        let hit = |ranges: &[Range<usize>], packet: &Range<usize>| {
            ranges
                .iter()
                .any(|range| range.start < packet.end && packet.start < range.end)
        };
        least += packets.iter().filter(|packet| !hit(&may, packet)).count();
        most += packets.iter().filter(|packet| !hit(&must, packet)).count();
    }
    (least, most)
}

/// What a run of `post` must end in, given what its schedule loses.
pub(in super::super) fn verdict(
    post: &Post,
    geometry: Option<Geometry>,
    lost: &BTreeMap<usize, BTreeSet<u32>>,
    starved: bool,
) -> Verdict {
    let block = geometry.map_or(1, |geometry| geometry.block);
    let (upper, lower) = needed(
        post,
        geometry.unwrap_or(Geometry {
            block,
            packed: false,
        }),
        lost,
    );
    if upper == 0 {
        return Verdict::Completes;
    }
    if geometry.is_none() {
        // Nothing to mend from: the bytes the archive does not check may
        // still let it complete correctly, so only identity or a named
        // failure is required.
        return Verdict::Either;
    }
    let (least, most) = surviving(post, lost, starved);
    if upper <= least {
        Verdict::Completes
    } else if lower > most {
        Verdict::Fails
    } else {
        Verdict::Either
    }
}

/// Writes `sources` under a scratch directory, returning it and the paths.
fn scratch_sources(sources: &[(String, Vec<u8>)]) -> (TempDir, PathBuf, Vec<PathBuf>) {
    let scratch = tempfile::tempdir().unwrap();
    let base = scratch.path().join("protected");
    let mut paths = Vec::new();
    for (name, bytes) in sources {
        let path = base.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        paths.push(path);
    }
    (scratch, base, paths)
}

fn read_outputs(paths: &[PathBuf]) -> Vec<(String, Vec<u8>)> {
    paths
        .iter()
        .map(|path| {
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect()
}

/// How a PAR2 set's recovery volumes are cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) enum Par2Volumes {
    /// One recovery volume.
    One,
    /// Volumes that double in size, the exponent scheme posters name
    /// `vol00+01`, `vol01+02`, `vol03+04`.
    Exponent,
    /// Equal volumes.
    Uniform,
}

/// A PAR2 set over `sources`: the index first, then its recovery volumes.
pub(in super::super) fn par2_set(
    sources: &[(String, Vec<u8>)],
    slice: usize,
    recovery: usize,
    volumes: Par2Volumes,
) -> Vec<(String, Vec<u8>)> {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount, VolumeScheme};
    let (scratch, base, paths) = scratch_sources(sources);
    let out = scratch.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let mut options = Par2CreatorOptions::with_output(out.join("silver.horizon.par2"), Some(base), paths);
    options.block_sizing = BlockSizing::Bytes(slice as u64);
    options.recovery_amount = RecoveryAmount::Count(recovery as u32);
    match volumes {
        Par2Volumes::One => {
            options.volume_count = Some(u32::from(recovery > 0));
        }
        Par2Volumes::Exponent => options.volume_scheme = VolumeScheme::Variable,
        Par2Volumes::Uniform => {
            options.volume_scheme = VolumeScheme::Uniform;
            options.volume_count = (recovery > 0).then_some(recovery.clamp(1, 4) as u32);
        }
    }
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().expect("a PAR2 plan over the fixture")).expect("a PAR2 set over the fixture");
    read_outputs(&created.output_paths)
}

/// A PAR3 set over `sources`: the index first, then its recovery volumes.
pub(in super::super) fn par3_set(
    sources: &[(String, Vec<u8>)],
    block: usize,
    recovery: usize,
    code: Code,
    per_volume: u64,
) -> Vec<(String, Vec<u8>)> {
    use par3_rs::creation::{CreationCodec, CreationOptions, CreationPlan, CreationSource, VolumeLayout};
    use par3_rs::source::{MemorySourceAccess, SourceId};
    let mut access = MemorySourceAccess::default();
    let mut named = Vec::new();
    for (index, (name, bytes)) in sources.iter().enumerate() {
        access.insert(SourceId(index as u64 + 1), 1, bytes.clone().into());
        named.push(CreationSource {
            name: name.clone(),
            source: SourceId(index as u64 + 1),
        });
    }
    let recovery = recovery.max(1) as u64;
    let codec = match code {
        Code::Cauchy => CreationCodec::Cauchy,
        Code::Fft => CreationCodec::Fft {
            capacity_log2: recovery.next_power_of_two().trailing_zeros() as i8,
            interleave: 0,
        },
    };
    let options = CreationOptions {
        block_size: block as u64,
        codec,
        recovery_count: recovery,
        volumes: VolumeLayout::Uniform(per_volume.max(1)),
        ..CreationOptions::default()
    };
    let scratch = tempfile::tempdir().unwrap();
    let plan = CreationPlan::build(Arc::new(access), &named, options).expect("a PAR3 plan over the fixture");
    let mut written = read_outputs(&plan.execute(&scratch.path().join("silver.horizon"), scratch.path()).expect("a PAR3 set over the fixture"));
    written.sort_by_key(|(name, _)| (name.contains(".vol"), name.clone()));
    written
}

/// Appends a recovery set's files to the post: the index as an index, the
/// rest as recovery volumes, each posted in `article`-sized articles.
pub(in super::super) fn post_set(post: &mut Post, set: Vec<(String, Vec<u8>)>, article: usize) {
    for (name, bytes) in set {
        let role = if name.contains(".vol") {
            Role::Recovery
        } else {
            Role::Index
        };
        post.files.push(Posted::new(name, bytes, article, role));
    }
}

/// How many recovery blocks a margin asks for, given what the post needs and
/// what its damage to the recovery files takes.
pub(in super::super) fn blocks_for(margin: Margin, (upper, lower): (usize, usize), killed: usize, floor: usize) -> usize {
    match margin {
        Margin::With => (upper + (upper / 10).max(2)).max(floor) + killed,
        Margin::Exact => upper + killed,
        Margin::OneOver => upper + 1 + killed,
        Margin::OneShort => {
            let short = if lower > 0 { lower - 1 } else { upper.saturating_sub(1) };
            short + killed
        }
    }
}

/// PAR2 packet types the structure axis rewrites.
pub(in super::super) mod par2_type {
    pub const MAIN: &[u8; 16] = b"PAR 2.0\0Main\0\0\0\0";
    pub const FILE_DESC: &[u8; 16] = b"PAR 2.0\0FileDesc";
    pub const IFSC: &[u8; 16] = b"PAR 2.0\0IFSC\0\0\0\0";
    pub const CREATOR: &[u8; 16] = b"PAR 2.0\0Creator\0";
    pub const UNICODE_NAME: &[u8; 16] = b"PAR 2.0\0UniFileN";
}

/// One PAR2 packet in a file: where it is and what it is.
#[derive(Clone, Debug)]
pub(in super::super) struct Par2Packet {
    pub range: Range<usize>,
    pub kind: [u8; 16],
}

/// Every well-formed PAR2 packet in `bytes`, in order.
pub(in super::super) fn par2_packets(bytes: &[u8]) -> Vec<Par2Packet> {
    let mut packets = Vec::new();
    let mut at = 0;
    while at + 64 <= bytes.len() {
        let packet = &bytes[at..];
        if !packet.starts_with(b"PAR2\0PKT") {
            at += 4;
            continue;
        }
        let length = u64::from_le_bytes(packet[8..16].try_into().unwrap()) as usize;
        if length < 64 || at + length > bytes.len() {
            at += 4;
            continue;
        }
        packets.push(Par2Packet {
            range: at..at + length,
            kind: packet[48..64].try_into().unwrap(),
        });
        at += length;
    }
    packets
}

/// A PAR2 packet of `kind` in recovery set `set`, its body padded to four
/// bytes and its hash computed as the format requires.
pub(in super::super) fn par2_packet(set: &[u8], kind: &[u8; 16], body: &[u8]) -> Vec<u8> {
    let mut body = body.to_vec();
    while body.len() % 4 != 0 {
        body.push(0);
    }
    let mut hashed = Vec::with_capacity(32 + body.len());
    hashed.extend_from_slice(&set[..16]);
    hashed.extend_from_slice(kind);
    hashed.extend_from_slice(&body);
    let mut packet = Vec::with_capacity(32 + hashed.len());
    packet.extend_from_slice(b"PAR2\0PKT");
    packet.extend_from_slice(&((32 + hashed.len()) as u64).to_le_bytes());
    packet.extend_from_slice(&par2_rs::checksum::md5(&hashed));
    packet.extend_from_slice(&hashed);
    packet
}

/// The recovery set ID every packet in `bytes` carries.
pub(in super::super) fn par2_set_id(bytes: &[u8]) -> Vec<u8> {
    let packet = par2_packets(bytes).into_iter().next().expect("a PAR2 packet");
    bytes[packet.range.start + 32..packet.range.start + 48].to_vec()
}

/// The file IDs of every FileDesc packet in a PAR2 set.
fn described(set: &[(String, Vec<u8>)]) -> BTreeSet<Vec<u8>> {
    set.iter()
        .flat_map(|(_, bytes)| {
            par2_packets(bytes)
                .into_iter()
                .filter(|packet| &packet.kind == par2_type::FILE_DESC)
                .map(|packet| bytes[packet.range.start + 64..packet.range.start + 80].to_vec())
        })
        .collect()
}

/// Whether every file the set describes keeps at least one FileDesc packet
/// in bytes the post certainly delivers whole.
pub(in super::super) fn descriptions_survive(
    post: &Post,
    lost: &BTreeMap<usize, BTreeSet<u32>>,
    starved: bool,
) -> bool {
    let none = BTreeSet::new();
    let par2: Vec<usize> = post
        .files
        .iter()
        .enumerate()
        .filter(|(_, posted)| posted.role != Role::Data && posted.bytes.starts_with(b"PAR2\0PKT"))
        .map(|(file, _)| file)
        .collect();
    let all: Vec<(String, Vec<u8>)> = par2
        .iter()
        .map(|&file| (post.files[file].name.clone(), post.files[file].bytes.clone()))
        .collect();
    let wanted = described(&all);
    let mut kept = BTreeSet::new();
    for file in par2 {
        let posted = &post.files[file];
        if starved && posted.role == Role::Recovery {
            continue;
        }
        let (may, _) = posted.wrong_ranges(lost.get(&file).unwrap_or(&none));
        for packet in par2_packets(&posted.bytes) {
            if &packet.kind == par2_type::FILE_DESC
                && !may
                    .iter()
                    .any(|range| range.start < packet.range.end && packet.range.start < range.end)
            {
                kept.insert(
                    posted.bytes[packet.range.start + 64..packet.range.start + 80].to_vec(),
                );
            }
        }
    }
    wanted.is_subset(&kept)
}
