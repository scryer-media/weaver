// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::Provenance;

use super::*;
use crate::grid::crc32;
use crate::testing::{Job, payload};

const OWN: ObjectId = ObjectId(9);

// A RAR5 recovery volume's header and no parity: the counts, its row, and a
// table of the data volumes' sizes when `sizes` names them, each with the
// CRC32 of the volume `data` makes at that size.
fn rev5(data: u16, rec: u16, row: u16, sizes: &[u64]) -> Vec<u8> {
    let mut raw = vec![1u8];
    raw.extend_from_slice(&data.to_le_bytes());
    raw.extend_from_slice(&rec.to_le_bytes());
    raw.extend_from_slice(&row.to_le_bytes());
    raw.extend_from_slice(&0u32.to_le_bytes());
    for size in sizes {
        raw.extend_from_slice(&size.to_le_bytes());
        raw.extend_from_slice(&crc32(&payload(5, *size as usize)).to_le_bytes());
    }
    let size = (raw.len() as u32).to_le_bytes();
    let mut rev = REV5_SIGN.to_vec();
    rev.extend_from_slice(&crc32(&[size.as_slice(), raw.as_slice()].concat()).to_le_bytes());
    rev.extend_from_slice(&size);
    rev.extend_from_slice(&raw);
    rev.extend_from_slice(&[0x5A; 40]);
    rev
}

// A RAR3 recovery volume as wide as its data volumes: parity, the footer in
// place of its last seven bytes.
fn rev3(data: u8, width: usize) -> Vec<u8> {
    let mut rev = payload(3, width - RAR3_FOOTER);
    rev.extend_from_slice(&[data - 1, 0, 0]);
    let crc = crc32(&rev);
    rev.extend_from_slice(&crc.to_le_bytes());
    rev
}

fn job() -> Job<RarRecovery> {
    Job::new(RarRecovery::new(OWN), OWN)
}

// A recovery volume as the tree hands one over: claimed, bound, sized,
// delivered in articles, closed.
fn rev(job: &mut Job<RarRecovery>, name: &str, bytes: &[u8]) -> Option<VolumeIndex> {
    let volume = arriving(job, name, bytes)?;
    job.object.volume_closed(volume, &mut Vec::new());
    Some(volume)
}

// A recovery volume every byte of which has arrived, not yet closed.
fn arriving(job: &mut Job<RarRecovery>, name: &str, bytes: &[u8]) -> Option<VolumeIndex> {
    let front = &bytes[..bytes.len().min(64)];
    let volume = match job.object.claim(name, front, None)[..] {
        [volume] => volume,
        [] if job.store.volumes.is_empty() => VolumeIndex(0),
        _ => return None,
    };
    let member = job.namespace.announce(name, Some(bytes.len() as u64));
    job.arrive(member, 0, bytes, Provenance::Fresh);
    job.namespace.bind(member, OWN);
    job.object.bound(volume, name, front, None);
    job.store.volumes.insert(volume, bytes.to_vec());
    let mut out = Vec::new();
    job.object.sized(volume, bytes.len() as u64, &mut out);
    for (at, piece) in bytes.chunks(16).enumerate() {
        job.object.deliver(
            Delivery {
                volume,
                offset: (at * 16) as u64,
                bytes: &weaver_graph::Evidenced::from(piece.to_vec()),
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            &mut out,
        );
    }
    Some(volume)
}

fn data(job: &mut Job<RarRecovery>, name: &str) -> MemberId {
    volume(job, name, &payload(5, 300))
}

// A data volume every byte of which arrived clean, closed.
fn volume(job: &mut Job<RarRecovery>, name: &str, bytes: &[u8]) -> MemberId {
    let member = job.member(name, bytes);
    job.namespace.get_mut(member).expect("announced").closed = true;
    member
}

fn bind(job: &mut Job<RarRecovery>) {
    job.object.bind(&mut job.namespace, &mut Vec::new());
}

fn ruled(job: &mut Job<RarRecovery>, outcome: RepairOutcome) {
    job.object.last = Some(Last {
        ruling: Ruling::Reached(outcome),
        judged: BTreeMap::new(),
    });
}

fn verdict(job: &Job<RarRecovery>, member: MemberId) -> Vec<SliceVerdict> {
    let mut verdicts = Vec::new();
    job.object.verdicts(member, &mut verdicts);
    verdicts
}

// A stored RAR 1.5 to 4.x volume of one member's `part`, every header under
// its CRC16, its end-of-archive header stating the CRC32 of every byte
// before it and the volume's number, and keeping seven zero bytes for a
// recovery volume's footer, as RAR writes a volume.
fn rar4_volume(part: &[u8], number: u16, last: bool) -> Vec<u8> {
    fn block(kind: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let mut header = vec![0, 0, kind];
        header.extend_from_slice(&flags.to_le_bytes());
        header.extend_from_slice(&((7 + body.len()) as u16).to_le_bytes());
        header.extend_from_slice(body);
        let crc = (crc32(&header[2..]) & 0xffff) as u16;
        header[..2].copy_from_slice(&crc.to_le_bytes());
        header
    }
    let name = b"lantern.bin";
    let mut file = Vec::new();
    file.extend_from_slice(&(part.len() as u32).to_le_bytes());
    file.extend_from_slice(&(part.len() as u32).to_le_bytes());
    file.push(3);
    file.extend_from_slice(&crc32(part).to_le_bytes());
    file.extend_from_slice(&0u32.to_le_bytes());
    file.extend_from_slice(&[29, 0x30]);
    file.extend_from_slice(&(name.len() as u16).to_le_bytes());
    file.extend_from_slice(&0o644u32.to_le_bytes());
    file.extend_from_slice(name);
    let mut volume = b"Rar!\x1a\x07\x00".to_vec();
    // A volume, named by part.
    volume.extend(block(0x73, 0x0001 | 0x0010, &[0; 6]));
    volume.extend(block(0x74, 0x8000, &file));
    volume.extend_from_slice(part);
    let crc = crc32(&volume);
    let next = if last { 0 } else { 0x0001 };
    let mut end = crc.to_le_bytes().to_vec();
    end.extend_from_slice(&number.to_le_bytes());
    end.extend_from_slice(&[0; RAR3_FOOTER]);
    volume.extend(block(0x7b, 0x0002 | 0x0004 | 0x0008 | next, &end));
    volume
}

// The one RAR3 recovery volume of `volumes`, which are as long as each
// other: the Reed-Solomon parity of each byte across them, which for one
// parity byte is the bytes folded by `x -> 2x` in GF(2^8) under 0x11d, the
// footer in place of the parity of the zeros every volume keeps for it.
fn rar3_rev(volumes: &[Vec<u8>]) -> Vec<u8> {
    let double = |byte: u8| (byte << 1) ^ if byte & 0x80 != 0 { 0x1d } else { 0 };
    let width = volumes[0].len();
    let mut rev: Vec<u8> = (0..width - RAR3_FOOTER)
        .map(|at| {
            volumes
                .iter()
                .fold(0, |parity, volume| double(volume[at] ^ parity))
        })
        .collect();
    rev.extend_from_slice(&[volumes.len() as u8 - 1, 0, 0]);
    let crc = crc32(&rev);
    rev.extend_from_slice(&crc.to_le_bytes());
    rev
}

fn rar4_volumes() -> Vec<Vec<u8>> {
    (0..3)
        .map(|at| rar4_volume(&payload(at + 1, 200), at as u16, at == 2))
        .collect()
}

fn rar4_name(at: usize) -> String {
    format!("lantern.part0{}.rar", at + 1)
}

// Three RAR4 data volumes, as long as each other, beside their one RAR3
// recovery volume.
fn rar4_set(job: &mut Job<RarRecovery>) -> Vec<MemberId> {
    let volumes = rar4_volumes();
    rev(job, "lantern.part1.rev", &rar3_rev(&volumes));
    volumes
        .iter()
        .enumerate()
        .map(|(at, bytes)| volume(job, &rar4_name(at), bytes))
        .collect()
}

// Every article of a member, cut as a post carries it, its CRC32 verified.
fn fed(job: &mut Job<RarRecovery>, member: MemberId) {
    let bytes = job.store.members[&member].clone();
    job.object
        .feed(member, &crate::testing::segments(0, &bytes, 64, 64, true));
}

#[test]
fn a_rar5_header_reads_once_its_front_is_whole() {
    let bytes = rev5(3, 1, 3, &[300, 300, 120]);
    let Read::Short(needed) = header::rar5(&bytes[..10]) else {
        panic!("a sign alone is short");
    };
    assert_eq!(needed, 16);
    let Read::Short(needed) = header::rar5(&bytes[..16]) else {
        panic!("a prefix alone is short");
    };
    let Read::Header(read) = header::rar5(&bytes[..needed]) else {
        panic!("the whole header reads");
    };
    assert_eq!((read.data_count, read.rec_count), (3, 1));
    assert_eq!(read.sizes, vec![300, 300, 120]);
    let mut torn = bytes.clone();
    torn[17] ^= 1;
    assert!(matches!(header::rar5(&torn), Read::Not));
    assert!(matches!(header::rar5(b"Rar!\x1a\x07\x01\x00"), Read::Not));
}

// Bound on its sign alone, then handed the whole volume at once: the front
// learns the header's length partway through that delivery and takes the
// rest of the header from it, so the header reads however the bytes were cut.
#[test]
fn a_rar5_header_reads_from_one_delivery_past_a_front_bound_short() {
    let mut job = job();
    let bytes = rev5(3, 1, 3, &[300, 300, 120]);
    let volume = VolumeIndex(0);
    job.object
        .bound(volume, "silver.part1.rev", &bytes[..8], None);
    job.object.deliver(
        Delivery {
            volume,
            offset: 0,
            bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
    let read = job.object.header().expect("the header reads");
    assert_eq!(read.sizes, vec![300, 300, 120]);
}

#[test]
fn a_rar3_footer_gives_the_set_and_its_width() {
    let bytes = rev3(4, 250);
    let footer: [u8; RAR3_FOOTER] = bytes[bytes.len() - RAR3_FOOTER..].try_into().unwrap();
    let read = header::rar3(&footer, bytes.len() as u64).expect("a footer");
    assert_eq!((read.data_count, read.rec_count), (4, 1));
    assert_eq!(read.sizes, vec![250; 4]);
}

#[test]
fn recovery_volumes_of_one_set_are_claimed_and_another_sets_are_not() {
    let mut job = job();
    let sizes = [300, 300, 300];
    assert_eq!(
        rev(&mut job, "lantern.part1.rev", &rev5(3, 2, 3, &sizes)),
        Some(VolumeIndex(0))
    );
    assert_eq!(
        rev(&mut job, "Lantern.part2.REV", &rev5(3, 2, 4, &[])),
        Some(VolumeIndex(1))
    );
    assert_eq!(
        rev(&mut job, "beacon.part1.rev", &rev5(3, 1, 3, &sizes)),
        None
    );
    assert_eq!(
        job.object
            .claim("lantern.part2.rar", b"Rar!\x1a\x07\x01\x00", None),
        Vec::<VolumeIndex>::new()
    );
}

#[test]
fn the_set_binds_by_its_naming_and_names_the_volumes_no_member_carries() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(4, 2, 4, &[300, 300, 300, 90]),
    );
    let first = data(&mut job, "lantern.part01.rar");
    let third = data(&mut job, "lantern.part03.rar");
    let stray = data(&mut job, "beacon.part02.rar");
    bind(&mut job);
    assert!(job.object.covers(first) && job.object.covers(third));
    assert!(!job.object.covers(stray));
    let mut absent = Vec::new();
    job.object.absent(&mut absent);
    assert_eq!(
        absent,
        vec![
            ("lantern.part02.rar".to_owned(), 300),
            ("lantern.part04.rar".to_owned(), 90)
        ]
    );
}

#[test]
fn rev_clean_is_revoked_when_bound_source_loses_coverage() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    let first = data(&mut job, "lantern.part01.rar");
    data(&mut job, "lantern.part02.rar");
    data(&mut job, "lantern.part03.rar");
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    bind(&mut job);
    assert_eq!(
        job.object.outcome(),
        Some(RepairOutcome::Clean),
        "stable input retains its proof"
    );
    job.namespace
        .get_mut(first)
        .unwrap()
        .coverage
        .write(ByteRange::new(32, 64), Provenance::Damaged);
    bind(&mut job);
    assert_eq!(
        job.object.outcome(),
        None,
        "same member IDs cannot preserve a proof after coverage loss"
    );
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
}

#[test]
fn a_set_rules_on_how_many_volumes_are_missing_against_its_recovery_volumes() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    for name in [
        "lantern.part01.rar",
        "lantern.part02.rar",
        "lantern.part03.rar",
    ] {
        data(&mut job, name);
    }
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );

    let mut job = self::job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    let whole = data(&mut job, "lantern.part01.rar");
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        }
    );
    let mut verdicts = Vec::new();
    job.object.verdicts(whole, &mut verdicts);
    assert_eq!(verdicts, vec![SliceVerdict::Intact]);
}

// Every article of a RAR5 set's volumes came clean, but one volume was
// changed before it was posted: its CRC32 is not its table row's, so it is
// not whole, and the set rules a restore of it, which this namespace, keeping
// no files, cannot run.
#[test]
fn a_rar5_volume_that_arrived_whole_is_judged_by_its_table_row() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    let first = data(&mut job, "lantern.part01.rar");
    let changed = data(&mut job, "lantern.part02.rar");
    data(&mut job, "lantern.part03.rar");
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    assert!(job.store.reads > 0, "a clean set read its volumes back");

    job.store.members.get_mut(&changed).unwrap()[150] ^= 0x01;
    job.object.last = None;
    assert!(job.namespace.get(changed).unwrap().is_whole());
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
    assert_eq!(verdict(&job, changed), vec![SliceVerdict::Damaged]);
    assert_eq!(verdict(&job, first), vec![SliceVerdict::Intact]);
}

// The same for a RAR 1.5 to 4.x set, whose recovery volumes state no
// checksum of the data volumes: each volume is held to the CRC32 its own
// end-of-archive header states of the bytes before it.
#[test]
fn a_rar4_volume_that_arrived_whole_is_judged_by_its_end_header() {
    let mut job = job();
    let volumes = rar4_set(&mut job);
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    assert!(job.store.reads > 0, "a clean set read its volumes back");

    let changed = volumes[1];
    let bytes = job.store.members.get_mut(&changed).unwrap();
    let inside = bytes.len() - 11 - 100;
    bytes[inside] ^= 0x01;
    job.object.last = None;
    assert!(job.namespace.get(changed).unwrap().is_whole());
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
    assert_eq!(verdict(&job, changed), vec![SliceVerdict::Damaged]);
    assert_eq!(verdict(&job, volumes[0]), vec![SliceVerdict::Intact]);
}

// A RAR4 volume whose headers do not hold, a CRC16 among them, is no more
// whole than one whose data changed.
#[test]
fn a_rar4_volume_whose_header_fails_its_crc16_is_not_whole() {
    let mut job = job();
    let volumes = rar4_set(&mut job);
    bind(&mut job);
    let changed = volumes[2];
    // The main header's reserved bytes, under its CRC16 only.
    job.store.members.get_mut(&changed).unwrap()[7 + 8] ^= 0x01;
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
    assert_eq!(verdict(&job, changed), vec![SliceVerdict::Damaged]);
}

// Every article of a RAR5 set's volumes arrived with its CRC32 verified: the
// articles' CRC32s, folded over each volume, are its table row's, and the
// set rules clean without reading a byte back.
#[test]
fn a_rar5_set_whose_articles_all_verified_rules_clean_reading_nothing_back() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    let members: Vec<MemberId> = (1..=3)
        .map(|at| data(&mut job, &format!("lantern.part0{at}.rar")))
        .collect();
    bind(&mut job);
    for member in &members {
        fed(&mut job, *member);
    }
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    assert_eq!((job.store.reads, job.store.read_bytes), (0, 0));

    // A volume changed before it was posted carries articles whose CRC32s
    // fold to another: refuted the same way, still unread.
    let mut job = self::job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    let mut changed = payload(5, 300);
    changed[150] ^= 0x01;
    let members = [
        data(&mut job, "lantern.part01.rar"),
        volume(&mut job, "lantern.part02.rar", &changed),
        data(&mut job, "lantern.part03.rar"),
    ];
    bind(&mut job);
    for member in &members {
        fed(&mut job, *member);
    }
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
    assert_eq!(verdict(&job, members[1]), vec![SliceVerdict::Damaged]);
    assert_eq!((job.store.reads, job.store.read_bytes), (0, 0));
}

// The same for a RAR 1.5 to 4.x set: the fold proves the end header's CRC32
// through the few bytes from that header on, so only the headers are read.
#[test]
fn a_rar4_set_whose_articles_all_verified_reads_back_only_its_headers() {
    let mut job = job();
    let members = rar4_set(&mut job);
    bind(&mut job);
    for member in &members {
        fed(&mut job, *member);
    }
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    // Each volume's headers twice over at most: every byte but its member's.
    let headers = members.len() as u64 * (job.store.members[&members[0]].len() as u64 - 200);
    assert!(
        job.store.read_bytes <= 2 * headers,
        "read {} bytes back, the headers are {headers}",
        job.store.read_bytes
    );
}

// A volume whose bytes the grid holds no fold of is read back once: a set
// that came up short and rules again keeps what it judged of every volume.
#[test]
fn a_volume_read_back_once_is_not_read_again_when_the_set_rules_again() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(4, 2, 4, &[300; 4]));
    let proven = data(&mut job, "lantern.part01.rar");
    data(&mut job, "lantern.part02.rar");
    bind(&mut job);
    fed(&mut job, proven);
    let second = arriving(&mut job, "lantern.part2.rev", &rev5(4, 2, 5, &[]));
    let second = second.expect("claimed");
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    assert_eq!((job.store.reads, job.store.read_bytes), (1, 300));
    job.object.volume_closed(second, &mut Vec::new());
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 2,
            reachable: 2
        }
    );
    assert_eq!((job.store.reads, job.store.read_bytes), (1, 300));
}

// A RAR4 set one of whose volumes never arrived, in a namespace that keeps
// files: the set stages the others and its recovery volume for the restore.
fn rar4_restore(job: &mut Job<RarRecovery>, dir: &tempfile::TempDir) -> (Vec<u8>, MemberId) {
    job.store.scratch = Some(dir.path().to_owned());
    let volumes = rar4_volumes();
    rev(job, "lantern.part1.rev", &rar3_rev(&volumes));
    volume(job, &rar4_name(0), &volumes[0]);
    volume(job, &rar4_name(2), &volumes[2]);
    let lost = job
        .namespace
        .announce(&rar4_name(1), Some(volumes[1].len() as u64));
    job.namespace.get_mut(lost).expect("announced").closed = true;
    (volumes[1].clone(), lost)
}

// The restore rebuilds the volume, which is judged again from the file it
// rebuilt and, whole, written back.
#[test]
fn a_rebuilt_volume_is_judged_again_from_what_was_rebuilt() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let mut job = job();
    let (bytes, lost) = rar4_restore(&mut job, &dir);
    job.store.members.insert(lost, vec![0; bytes.len()]);
    job.store
        .holes
        .insert(lost, vec![ByteRange::new(0, bytes.len() as u64)]);
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert_eq!(job.store.members[&lost], bytes);
    assert_eq!(verdict(&job, lost), vec![SliceVerdict::Intact]);
}

// The restore rebuilds the volume, but its member, which has no length, takes
// none of it: nothing was written, and the volume is still not whole.
#[test]
fn a_restore_that_writes_nothing_leaves_a_volume_that_is_not_whole_unsound() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let mut job = job();
    let (_, lost) = rar4_restore(&mut job, &dir);
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
    assert_eq!(job.store.writes, 0);
    assert_eq!(verdict(&job, lost), vec![SliceVerdict::Damaged]);
}

#[test]
fn acknowledged_rev_growth_retains_repaired_proof_but_later_loss_revokes_it() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 300]));
    let first = data(&mut job, "lantern.part01.rar");
    data(&mut job, "lantern.part02.rar");
    job.namespace
        .get_mut(first)
        .unwrap()
        .coverage
        .write(ByteRange::new(32, 64), Provenance::Damaged);
    bind(&mut job);
    let repaired = RepairOutcome::Repaired { slices: 1 };
    ruled(&mut job, repaired);
    let segment = Segment {
        range: ByteRange::new(32, 64),
        crc32: 0,
        verified: true,
    };
    job.object.feed_proven(first, &[segment]);
    job.namespace
        .get_mut(first)
        .unwrap()
        .coverage
        .write(segment.range, Provenance::Repaired);
    bind(&mut job);
    assert_eq!(job.object.outcome(), Some(repaired));
    bind(&mut job);
    job.object.feed(first, &[segment]);
    assert_eq!(
        job.object.outcome(),
        Some(repaired),
        "stable binding and unchanged replay stay settled"
    );
    job.namespace
        .get_mut(first)
        .unwrap()
        .coverage
        .write(segment.range, Provenance::Damaged);
    bind(&mut job);
    assert_eq!(job.object.outcome(), None);
}

#[test]
fn damaged_replayed_source_revokes_restored_rev_proof() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 300]));
    let first = data(&mut job, "lantern.part01.rar");
    data(&mut job, "lantern.part02.rar");
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    let mut restored: RarRecovery = job.object.facts().read(Kind::RarRecovery).unwrap();
    job.namespace
        .get_mut(first)
        .unwrap()
        .coverage
        .write(ByteRange::new(32, 64), Provenance::Damaged);
    restored.bind(&mut job.namespace, &mut Vec::new());
    assert_eq!(restored.outcome(), None);
    assert_eq!(
        restored.repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
}

// A data volume none of whose bytes came, booked longer than the size the
// RAR5 table gives, takes the table's size once the set has repaired it,
// and keeps its name. Before the repair it keeps what was booked.
#[test]
fn a_repaired_volume_booked_longer_than_its_table_size_takes_that_size() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 250]));
    data(&mut job, "lantern.part01.rar");
    let lost = job.namespace.announce("lantern.part02.rar", Some(400));
    job.namespace.get_mut(lost).expect("announced").closed = true;
    bind(&mut job);
    assert_eq!(job.namespace.get(lost).unwrap().length, Some(400));
    ruled(&mut job, RepairOutcome::Repaired { slices: 1 });
    bind(&mut job);
    let record = job.namespace.get(lost).unwrap();
    assert_eq!(record.name, "lantern.part02.rar");
    assert_eq!(record.length, Some(250));
}

// A data volume that arrived whole and clean but longer than the size the
// RAR5 table gives keeps what arrived until the set rules on it; once the
// set rules it clean, the volume is the table's size and is listed as
// settled, as par2 rebuilds a file of another size at its described size.
#[test]
fn a_volume_arrived_longer_than_its_table_size_takes_that_size_once_ruled_clean() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 250]));
    data(&mut job, "lantern.part01.rar");
    let long = job.namespace.announce("lantern.part02.rar", Some(400));
    job.arrive(long, 0, &payload(6, 400), Provenance::Fresh);
    job.namespace.get_mut(long).expect("announced").closed = true;
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
    assert_eq!(job.namespace.get(long).unwrap().length, Some(400));
    assert!(renamed.is_empty());
    ruled(&mut job, RepairOutcome::Clean);
    job.object.bind(&mut job.namespace, &mut renamed);
    let record = job.namespace.get(long).unwrap();
    assert_eq!(record.name, "lantern.part02.rar");
    assert_eq!(record.length, Some(250));
    assert_eq!(renamed, vec![long]);
}

// A RAR3 footer describes every data volume but the last as the recovery
// volume's width: a repaired volume booked longer takes that width, while
// the last, which falls short of the width, keeps what was booked.
#[test]
fn a_repaired_rar3_volume_booked_longer_than_the_width_takes_it_but_the_last() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev3(3, 300));
    data(&mut job, "lantern.part01.rar");
    let booked = |job: &mut Job<RarRecovery>, name: &str| {
        let member = job.namespace.announce(name, Some(400));
        job.namespace.get_mut(member).expect("announced").closed = true;
        member
    };
    let middle = booked(&mut job, "lantern.part02.rar");
    let last = booked(&mut job, "lantern.part03.rar");
    ruled(&mut job, RepairOutcome::Repaired { slices: 2 });
    bind(&mut job);
    let record = job.namespace.get(middle).unwrap();
    assert_eq!(record.name, "lantern.part02.rar");
    assert_eq!(record.length, Some(300));
    assert_eq!(job.namespace.get(last).unwrap().length, Some(400));
}

// A set short of recovery volumes waits while the one it needs arrives, and
// is due again in the step that one closes: no repair between.
#[test]
fn a_short_set_waits_until_the_volumes_it_needs_are_in_hand() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 2, 3, &[300, 300, 300]),
    );
    data(&mut job, "lantern.part01.rar");
    bind(&mut job);
    let second = arriving(&mut job, "lantern.part2.rev", &rev5(3, 2, 4, &[]));
    let second = second.expect("claimed");
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Waiting));
    job.object.volume_closed(second, &mut Vec::new());
    assert_eq!(job.object.outcome(), None);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 2,
            reachable: 2
        }
    );
}

// A namespace that keeps no files lends no scratch, and the restore, which
// works on files, cannot run.
#[test]
fn a_set_that_could_rebuild_without_scratch_is_impossible() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 300]));
    data(&mut job, "lantern.part01.rar");
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
}

#[test]
fn a_set_survives_a_restart() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 300]));
    data(&mut job, "lantern.part01.rar");
    bind(&mut job);
    let back =
        crate::restore(&job.object.facts(), weaver_graph::Budget::default()).expect("restores");
    assert_eq!(back.kind(), Kind::RarRecovery);
    let mut absent = Vec::new();
    back.recovery().expect("a recovery").absent(&mut absent);
    assert_eq!(absent, vec![("lantern.part02.rar".to_owned(), 300)]);
}

// A recovery volume whose front was handed over, the rest still to come.
fn sampled(job: &mut Job<RarRecovery>, name: &str, bytes: &[u8]) -> VolumeIndex {
    let front = &bytes[..bytes.len().min(64)];
    let volume = match job.object.claim(name, front, None)[..] {
        [volume] => volume,
        _ => VolumeIndex(0),
    };
    job.object.bound(volume, name, front, None);
    job.object
        .sized(volume, bytes.len() as u64, &mut Vec::new());
    volume
}

fn wants(object: &RarRecovery) -> Vec<Seek> {
    let mut out = Vec::new();
    Recovery::wants(object, &mut out);
    out
}

// Its front told what it is; the rest waits until the set is short.
#[test]
fn a_sampled_recovery_volume_is_deferred_while_nothing_is_short() {
    let mut job = job();
    let bytes = rev5(3, 2, 3, &[300, 300, 300]);
    let volume = sampled(&mut job, "lantern.part1.rev", &bytes);
    assert_eq!(
        wants(&job.object),
        vec![Seek::deferred(volume, ByteRange::new(0, u64::MAX))]
    );
}

// A data volume is gone: as many recovery volumes as the set is short of
// are wanted whole, now, and the rest stay deferred.
#[test]
fn a_lost_data_volume_raises_as_many_recovery_volumes_as_it_needs() {
    let mut job = job();
    let sizes = [300, 300, 300];
    let first = sampled(&mut job, "lantern.part1.rev", &rev5(3, 2, 3, &sizes));
    let second = sampled(&mut job, "lantern.part2.rev", &rev5(3, 2, 4, &[]));
    data(&mut job, "lantern.part01.rar");
    data(&mut job, "lantern.part02.rar");
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    assert_eq!(
        wants(&job.object),
        vec![
            Seek::recovery(first, ByteRange::new(0, u64::MAX), Weight::FIRST),
            Seek::deferred(second, ByteRange::new(0, u64::MAX)),
        ]
    );
}

// A RAR3 recovery volume says what it is at its end: the footer is wanted
// now, and the bytes between the front and it wait.
#[test]
fn a_rar3_recovery_volume_wants_its_footer_and_defers_the_rest() {
    let mut job = job();
    let bytes = rev3(3, 250);
    let volume = sampled(&mut job, "lantern.part1.rev", &bytes);
    let footer = (250 - RAR3_FOOTER) as u64;
    assert_eq!(
        wants(&job.object),
        vec![
            Seek::recovery(volume, ByteRange::new(footer, 250), Weight::FIRST),
            Seek::deferred(volume, ByteRange::new(0, footer)),
        ]
    );
}

// Nothing of the deferral is kept: the object a restart rebuilds defers the
// same bytes from what it parsed.
#[test]
fn a_deferral_is_said_again_after_a_restart() {
    let mut job = job();
    let bytes = rev5(3, 2, 3, &[300, 300, 300]);
    sampled(&mut job, "lantern.part1.rev", &bytes);
    let back =
        crate::restore(&job.object.facts(), weaver_graph::Budget::default()).expect("restores");
    let mut again = Vec::new();
    back.wants(&mut again);
    assert_eq!(again, wants(&job.object));
}

#[test]
fn repaired_data_reopens_an_impossible_volume_recovery() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 300]));
    let member = data(&mut job, "lantern.part01.rar");
    bind(&mut job);
    let impossible = RepairOutcome::Impossible {
        damaged: 1,
        reachable: 1,
    };
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        impossible
    );
    let mut segment = Segment {
        range: ByteRange::new(0, 16),
        crc32: 0,
        verified: false,
    };
    job.object.feed(member, &[segment]);
    assert_eq!(job.object.outcome(), Some(impossible));
    segment.verified = true;
    job.object.feed(MemberId(u32::MAX), &[segment]);
    assert_eq!(job.object.outcome(), Some(impossible));
    job.object.feed(member, &[segment]);
    assert_eq!(job.object.outcome(), None);
    for ruling in [RepairOutcome::Clean, RepairOutcome::Repaired { slices: 1 }] {
        ruled(&mut job, ruling);
        job.object.feed(member, &[segment]);
        assert_eq!(job.object.outcome(), Some(ruling));
    }
}

// A data volume posted twice, the posting listed first lost whole: the set
// binds the posting that arrived, and nothing is missing.
#[test]
fn a_volume_posted_twice_binds_the_posting_that_arrived() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    let lost = job.namespace.announce("lantern.part2.rar", Some(300));
    job.namespace.get_mut(lost).expect("announced").closed = true;
    Recovery::refused(&mut job.object, lost, ByteRange::new(0, 300));
    data(&mut job, "lantern.part01.rar");
    let second = data(&mut job, "lantern.part02.rar");
    data(&mut job, "lantern.part03.rar");
    bind(&mut job);
    assert!(job.object.covers(second));
    assert!(!job.object.covers(lost));
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
}
