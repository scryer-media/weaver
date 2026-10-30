// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

struct Unsegmented;

impl Segments for Unsegmented {
    fn segments_for(&self, _: NodeId, _: VolumeIndex, _: ByteRange, _: &mut Vec<Segment>) {}
}

fn send(
    tree: &mut Tree,
    writer: &mut NamespaceWriter,
    objects: &mut weaver_objects::Objects,
    member: MemberId,
    offset: usize,
    bytes: &[u8],
) {
    writer
        .deliver(
            tree,
            objects,
            &Unsegmented,
            Arrival {
                node: NodeId::ROOT,
                member,
                offset: offset as u64,
                bytes: Bytes::copy_from_slice(bytes),
                provenance: Provenance::Fresh,
            },
            &mut Vec::new(),
        )
        .unwrap();
}

#[test]
fn a_clean_proof_replays_real_store_rejection_after_overlapping_fresh_copies() {
    fragmented_proof(false);
}

#[test]
fn unchanged_clean_proof_of_an_intrinsically_wrong_archive_checksum_does_not_cycle() {
    fragmented_proof(true);
}

fn fragmented_proof(intrinsic: bool) {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    let mut seed = 41_u64;
    let payload: Vec<_> = (0..4096)
        .map(|_| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 56) as u8
        })
        .collect();
    let mut archive = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    archive.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    let mut entry = ArchiveEntry::new_file("inside.bin");
    entry.size = payload.len() as u64;
    archive
        .push_archive_entry(entry, Some(std::io::Cursor::new(payload.clone())))
        .unwrap();
    let mut archive = archive.finish().unwrap().into_inner();
    let footer = 32 + u64::from_le_bytes(archive[12..20].try_into().unwrap()) as usize;
    assert_eq!(footer, 32 + payload.len());
    let stated = if intrinsic {
        !crc32(&payload)
    } else {
        crc32(&payload)
    };
    if intrinsic {
        let original = crc32(&payload).to_le_bytes();
        let positions: Vec<_> = archive[footer..]
            .windows(4)
            .enumerate()
            .filter_map(|(at, bytes)| (bytes == original).then_some(footer + at))
            .collect();
        assert!(!positions.is_empty());
        for at in positions {
            archive[at..at + 4].copy_from_slice(&stated.to_le_bytes());
        }
        let next_crc = crc32(&archive[footer..]);
        archive[28..32].copy_from_slice(&next_crc.to_le_bytes());
        let start_crc = crc32(&archive[12..32]);
        archive[8..12].copy_from_slice(&start_crc.to_le_bytes());
    }
    let authored = tempfile::tempdir().unwrap();
    let input = authored.path().join("archive.7z");
    std::fs::write(&input, &archive).unwrap();
    let mut options = Par2CreatorOptions::with_output(
        authored.path().join("proof.par2"),
        Some(authored.path().to_path_buf()),
        vec![input],
    );
    options.block_sizing = BlockSizing::Bytes(512);
    options.recovery_amount = RecoveryAmount::Count(0);
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    let index = std::fs::read(&created.output_paths[0]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        Arc::new(HoldsAccountant::new(PLENTY)),
        CapacityReader::unknown(),
    );
    let mut tree = Tree::new();
    let mut objects = weaver_objects::Objects::default();
    let member = tree.announce_root("archive.7z", Some(archive.len() as u64), &mut Vec::new());
    writer
        .announced(
            &mut tree,
            &mut objects,
            &Unsegmented,
            NodeId::ROOT,
            member,
            &mut Vec::new(),
        )
        .unwrap();
    // Knowing the map before payload delivery keeps these copies on the
    // ordinary direct path, rather than rebuilding their checksum spans
    // as held data is released.
    send(
        &mut tree,
        &mut writer,
        &mut objects,
        member,
        0,
        &archive[..32],
    );
    send(
        &mut tree,
        &mut writer,
        &mut objects,
        member,
        footer,
        &archive[footer..],
    );
    let bad_at = 32 + 100;
    let middle = 32 + payload.len() / 2;
    let mut first = archive[32..middle].to_vec();
    first[bad_at - 32] ^= 0x5a;
    send(&mut tree, &mut writer, &mut objects, member, 32, &first);
    // A partial overlapping CRC cannot compose the untouched portions of
    // its older span. The file's own byte fold remains complete.
    send(
        &mut tree,
        &mut writer,
        &mut objects,
        member,
        64,
        &archive[64..81],
    );
    send(
        &mut tree,
        &mut writer,
        &mut objects,
        member,
        middle,
        &archive[middle..footer],
    );
    writer
        .close(
            &mut tree,
            &mut objects,
            &Unsegmented,
            NodeId::ROOT,
            member,
            &mut Vec::new(),
        )
        .unwrap();
    let (store, _) = tree.child_of(NodeId::ROOT, member).unwrap();
    assert_eq!(tree.kind(store), Some(Kind::SevenZipStore));
    let root = tree.namespace(NodeId::ROOT).unwrap().get(member).unwrap();
    assert!(
        root.coverage
            .runs()
            .all(|run| run.provenance == Provenance::Fresh),
        "the store lacked a complete source fold: {:?}",
        root.coverage
    );
    let derived = tree.namespace(store).unwrap().get(MemberId(0)).unwrap();
    assert_eq!(derived.crc32, Some(stated));
    assert!(
        derived.coverage.damaged_ranges().next().is_some(),
        "the complete file fold rejected the old copy"
    );
    send(
        &mut tree,
        &mut writer,
        &mut objects,
        member,
        bad_at,
        &archive[bad_at..bad_at + 1],
    );
    assert!(
        tree.namespace(NodeId::ROOT)
            .unwrap()
            .get(member)
            .unwrap()
            .coverage
            .runs()
            .all(|run| run.provenance == Provenance::Fresh)
    );
    assert!(
        !tree.rejected_inputs(NodeId::ROOT, [member]).is_empty(),
        "current source bytes corrected, old derived rejection remains"
    );
    let carrier = tree.announce_root("proof.par2", Some(index.len() as u64), &mut Vec::new());
    writer
        .announced(
            &mut tree,
            &mut objects,
            &Unsegmented,
            NodeId::ROOT,
            carrier,
            &mut Vec::new(),
        )
        .unwrap();
    send(&mut tree, &mut writer, &mut objects, carrier, 0, &index);
    writer
        .close(
            &mut tree,
            &mut objects,
            &Unsegmented,
            NodeId::ROOT,
            carrier,
            &mut Vec::new(),
        )
        .unwrap();
    let set = writer.sets(&tree)[0].1;
    let mut lent = Inline::default();
    writer
        .repair(&mut tree, NodeId::ROOT, &[set], &mut lent)
        .unwrap();
    assert_eq!(lent.0.len(), 1);
    let returned = lent
        .0
        .pop()
        .unwrap()
        .run(weaver_graph::Budget::default(), &Default::default());
    assert_eq!(returned.outcome, Some(RepairOutcome::Clean));
    writer
        .returned(
            &mut tree,
            &mut objects,
            &Unsegmented,
            &weaver_objects::Objects::default(),
            returned,
            &mut Vec::new(),
        )
        .unwrap();
    assert!(
        writer.rechecks_waiting(),
        "a real Clean proof must replay the already rejected output"
    );
    let replay = writer
        .take_replay(&tree)
        .unwrap()
        .unwrap()
        .run(&Default::default());
    writer
        .replayed(
            &mut tree,
            &mut objects,
            &Unsegmented,
            replay,
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(
        tree.namespace(store)
            .unwrap()
            .get(MemberId(0))
            .unwrap()
            .is_whole(),
        !intrinsic
    );
    let mut actual = Vec::new();
    assert!(writer.access(&tree).read_at(
        store,
        MemberId(0),
        ByteRange::new(0, payload.len() as u64),
        &mut actual
    ));
    assert_eq!(actual, payload);
    assert!(!writer.rechecks_waiting());
    let mut again = Inline::default();
    writer
        .rule(
            &mut tree,
            &mut objects,
            &Unsegmented,
            (NodeId::ROOT, &|_| false),
            &mut again,
            &mut Vec::new(),
        )
        .unwrap();
    assert!(
        again.0.is_empty(),
        "unchanged proof does not restart recovery"
    );
    assert!(
        !writer.rechecks_waiting(),
        "an unchanged intrinsic checksum failure does not cycle through replay"
    );
}

#[test]
fn a_bad_replacement_header_does_not_destroy_the_inputs_owned_by_the_previous_map() {
    replacement_header_custody(false);
}

#[test]
fn replacement_payload_sharing_the_changed_header_delivery_reaches_the_new_owner() {
    replacement_header_custody(true);
}

fn replacement_header_custody(spanning: bool) {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    let payload: Vec<_> = (0..4096)
        .map(|n| ((n * 17 + n / 127) % 251) as u8)
        .collect();
    let mut archive = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    archive.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    let mut entry = ArchiveEntry::new_file("inside.bin");
    entry.size = payload.len() as u64;
    archive
        .push_archive_entry(entry, Some(std::io::Cursor::new(payload.clone())))
        .unwrap();
    let archive = archive.finish().unwrap().into_inner();
    let footer = 32 + u64::from_le_bytes(archive[12..20].try_into().unwrap()) as usize;
    assert_eq!(footer, 32 + payload.len());
    replacement_map_custody("archive.7z", archive, payload, 32, footer, spanning);
}

fn replacement_map_custody(
    name: &str,
    archive: Vec<u8>,
    payload: Vec<u8>,
    data_at: usize,
    footer: usize,
    spanning: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        Arc::new(HoldsAccountant::new(PLENTY)),
        CapacityReader::unknown(),
    );
    let mut tree = Tree::new();
    let mut objects = weaver_objects::Objects::default();
    let member = tree.announce_root(name, Some(archive.len() as u64), &mut Vec::new());
    writer
        .announced(
            &mut tree,
            &mut objects,
            &Unsegmented,
            NodeId::ROOT,
            member,
            &mut Vec::new(),
        )
        .unwrap();
    send(&mut tree, &mut writer, &mut objects, member, 0, &archive);
    let payload_range = ByteRange::new(data_at as u64, footer as u64);
    let mut before = Vec::new();
    assert!(
        writer
            .access(&tree)
            .read_at(NodeId::ROOT, member, payload_range, &mut before)
    );
    assert_eq!(before, payload);
    let from = if spanning { footer - 64 } else { footer };
    let mut replacement = archive[from..].to_vec();
    replacement[footer - from] ^= 0x5a;
    let mut current_payload = payload.clone();
    if spanning {
        replacement[0] ^= 0x5a;
        current_payload[from - data_at] ^= 0x5a;
    }
    send(
        &mut tree,
        &mut writer,
        &mut objects,
        member,
        from,
        &replacement,
    );
    let mut still_owned = Vec::new();
    assert!(
        writer
            .access(&tree)
            .read_at(NodeId::ROOT, member, payload_range, &mut still_owned),
        "rejected replacement metadata cannot revoke source-byte custody"
    );
    assert!(
        still_owned == current_payload,
        "the replacement's current bytes supersede the old inverse copy"
    );
    send(
        &mut tree,
        &mut writer,
        &mut objects,
        member,
        from,
        &archive[from..],
    );
    let mut whole = Vec::new();
    assert!(writer.access(&tree).read_at(
        NodeId::ROOT,
        member,
        ByteRange::new(0, archive.len() as u64),
        &mut whole
    ));
    assert_eq!(whole, archive);
}

#[test]
fn replacement_zip_directory_does_not_destroy_the_inputs_owned_by_the_previous_map() {
    use std::io::Write;
    let payload: Vec<_> = (0..4096)
        .map(|n| ((n * 19 + n / 131) % 251) as u8)
        .collect();
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file(
            "inside.bin",
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored),
        )
        .unwrap();
    writer.write_all(&payload).unwrap();
    let archive = writer.finish().unwrap().into_inner();
    let data_at = archive
        .windows(payload.len())
        .position(|bytes| bytes == payload)
        .unwrap();
    let footer = archive
        .windows(4)
        .position(|bytes| bytes == b"PK\x01\x02")
        .unwrap();
    assert_eq!(data_at + payload.len(), footer);
    replacement_map_custody("archive.zip", archive, payload, data_at, footer, true);
}
