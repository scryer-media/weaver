// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

struct Unsegmented;

impl Segments for Unsegmented {
    fn segments_for(&self, _: NodeId, _: VolumeIndex, _: ByteRange, _: &mut Vec<Segment>) {}
}

// One stored member, "inside.bin", in an archive whose end header follows
// its body, as every such archive lays out.
fn stored(payload: &[u8]) -> Vec<u8> {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    let mut archive = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    archive.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    let mut entry = ArchiveEntry::new_file("inside.bin");
    entry.size = payload.len() as u64;
    archive
        .push_archive_entry(entry, Some(std::io::Cursor::new(payload.to_vec())))
        .unwrap();
    archive.finish().unwrap().into_inner()
}

// What a download of the archive came to, its end header last.
struct HeaderLast {
    // Bytes held once the whole body had arrived.
    held: u64,
    // Ranges located by the passes the body's deliveries ran.
    located_while_held: usize,
    // Ranges located by the passes the end header ran.
    located_at_release: usize,
    // The most passes over the holds that ran at once.
    deepest: usize,
    held_after: u64,
    payload: Vec<u8>,
    inside: Vec<u8>,
}

// The archive's signature, then its body `pieces` pieces of `piece` bytes
// each, then its end header.
fn header_last(pieces: usize, piece: usize) -> HeaderLast {
    let payload: Vec<u8> = (0..pieces * piece)
        .map(|n| ((n * 31 + n / 97) % 251) as u8)
        .collect();
    let archive = stored(&payload);
    let data_at = 32;
    let footer = data_at + payload.len();
    assert_eq!(
        footer,
        data_at + u64::from_le_bytes(archive[12..20].try_into().unwrap()) as usize
    );
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
    let mut send = |writer: &mut NamespaceWriter, tree: &mut Tree, offset: usize, bytes: &[u8]| {
        writer
            .deliver(
                tree,
                &mut objects,
                &Unsegmented,
                Arrival {
                    node: NodeId::ROOT,
                    member,
                    offset: offset as u64,
                    bytes: Bytes::copy_from_slice(bytes).into(),
                    provenance: Provenance::Fresh,
                },
                &mut Vec::new(),
            )
            .unwrap();
    };
    send(&mut writer, &mut tree, 0, &archive[..data_at]);
    RELEASE_DEPTH.set((0, 0));
    RELEASE_LOCATED.set(0);
    for (index, bytes) in payload.chunks(piece).enumerate() {
        send(&mut writer, &mut tree, data_at + index * piece, bytes);
    }
    let held = writer.held_bytes(NodeId::ROOT);
    let located_while_held = RELEASE_LOCATED.get();
    RELEASE_LOCATED.set(0);
    send(&mut writer, &mut tree, footer, &archive[footer..]);
    HeaderLast {
        held,
        located_while_held,
        located_at_release: RELEASE_LOCATED.get(),
        deepest: RELEASE_DEPTH.get().1,
        held_after: writer.held_bytes(NodeId::ROOT),
        payload,
        inside: std::fs::read(dir.path().join("inside.bin")).unwrap_or_default(),
    }
}

// An archive whose end header comes last has its whole body held, a piece
// per article, until the header places it. The header's delivery releases
// every piece from one pass over the holds, never one inside another, and
// each piece is located once there; the member is written whole. While the
// body comes the store's placement number stands, so only the first hold
// is ever located before the header.
#[test]
fn a_body_held_in_thousands_of_pieces_releases_from_one_pass() {
    for pieces in [256, 2048] {
        let run = header_last(pieces, 16);
        assert_eq!(run.held, run.payload.len() as u64, "{pieces}: all held");
        assert_eq!(run.located_while_held, 1, "{pieces}: the first hold only");
        assert_eq!(run.deepest, 1, "{pieces}: no pass inside another");
        assert_eq!(run.located_at_release, pieces, "{pieces}: each piece once");
        assert_eq!(run.held_after, 0, "{pieces}: nothing left held");
        assert!(run.inside == run.payload, "{pieces}: the member is whole");
    }
}
