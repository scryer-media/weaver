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

// An archive whose body came whole, as one hold larger than a read-back
// piece, and whose end header was heard last without a release after it:
// the store places the body, which is still held. Its files are written
// through a volume's own writer thread.
struct PlacedBehind {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
    writer: NamespaceWriter,
    tree: Tree,
    objects: weaver_objects::Objects,
    member: MemberId,
    payload: Vec<u8>,
    _writers: weaver_server_disk::operations::disk::writer::VolumeWriters,
    volume: Arc<weaver_server_disk::operations::disk::volume::Volume>,
    submitter: Arc<weaver_server_disk::operations::disk::writer::Submitter>,
}

fn placed_behind() -> PlacedBehind {
    use crate::pipeline::graph::disk::JobDisk;
    use weaver_server_disk::operations::disk::volume::Volume;
    use weaver_server_disk::operations::disk::writer::VolumeWriters;
    let payload: Vec<u8> = (0..(5 * READ_BACK / 2) as usize)
        .map(|n| ((n * 31 + n / 97) % 251) as u8)
        .collect();
    let archive = stored(&payload);
    let data_at = 32;
    let footer = data_at + payload.len();
    let dir = tempfile::tempdir().unwrap();
    let writers = VolumeWriters::reporting(|_| {});
    let volume = Arc::new(Volume::new(None, "test".to_owned()));
    let submitter = writers.test_submitter(&volume, 1);
    let mut writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        Arc::new(HoldsAccountant::new(PLENTY)),
        CapacityReader::unknown(),
    );
    writer.move_disk_for_test(JobDisk::new(Arc::clone(&submitter), None));
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
    let arrival = |offset: usize, bytes: &[u8]| Arrival {
        node: NodeId::ROOT,
        member,
        offset: offset as u64,
        bytes: Bytes::copy_from_slice(bytes).into(),
        provenance: Provenance::Fresh,
    };
    for (at, end) in [(0, data_at), (data_at, footer)] {
        writer
            .deliver(
                &mut tree,
                &mut objects,
                &Unsegmented,
                arrival(at, &archive[at..end]),
                &mut Vec::new(),
            )
            .unwrap();
    }
    writer
        .arrive(
            &mut tree,
            &mut objects,
            &Unsegmented,
            crate::pipeline::graph::namespace::Write::New {
                entered: NodeId::ROOT,
                posting: member,
            },
            arrival(footer, &archive[footer..]),
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(writer.held_bytes(NodeId::ROOT), payload.len() as u64);
    PlacedBehind {
        path: dir.path().join("inside.bin"),
        _dir: dir,
        writer,
        tree,
        objects,
        member,
        payload,
        _writers: writers,
        volume,
        submitter,
    }
}

impl PlacedBehind {
    fn release_paid(&mut self, paid: &mut bool, out: &mut Vec<Effect>) -> Option<(u64, NodeId)> {
        self.writer
            .release_held_paid(&mut self.tree, &mut self.objects, &Unsegmented, paid, out)
            .unwrap()
    }

    // What the member's file holds once every write submitted returned.
    fn inside(&self) -> Vec<u8> {
        self.writer.disk().unwrap().barrier().wait_aside();
        std::fs::read(&self.path).unwrap_or_default()
    }
}

// A paid release onto a volume with no room for the first span stands with
// that span's bytes, a read-back piece of the larger hold, credited in the
// store's namespace; nothing leaves the ledger, and the hold is not counted
// as asked. The next pass asks it again and, with room, releases it whole.
#[test]
fn a_paid_release_with_no_room_stands_and_the_next_pass_asks_again() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let mut run = placed_behind();
    let (child, _) = run.tree.child_of(NodeId::ROOT, run.member).expect("a 7z");
    // Another producer counts the volume's whole limit.
    let hard = 64 * 1024;
    run.volume.set_buffer_limits(hard, hard);
    let full = Credit::reserve(std::iter::once(&run.submitter), hard, Stand::UntilStop);
    assert!(full.covers(hard));
    let mut out = Vec::new();
    let mut paid = false;
    assert_eq!(
        run.release_paid(&mut paid, &mut out),
        Some((READ_BACK, child))
    );
    assert_eq!(
        run.writer.held_bytes(NodeId::ROOT),
        run.payload.len() as u64,
        "the span is still held"
    );
    assert!(
        !run.writer.holds_asked(NodeId::ROOT, run.member),
        "the hold is asked again"
    );
    drop(full);
    run.volume.set_buffer_limits(u64::MAX, u64::MAX);
    RELEASE_LOCATED.set(0);
    assert_eq!(run.release_paid(&mut paid, &mut out), None);
    assert!(RELEASE_LOCATED.get() > 0, "the next pass asked the hold");
    assert_eq!(run.writer.held_bytes(NodeId::ROOT), 0);
    assert!(run.inside() == run.payload, "the member is whole");
}

// With room, the release the loop runs after a write (`release_held`) and
// a paid one hand the store the same bytes, in the same order, and say the
// same, as the release did before it was counted on the job's volumes,
// over the same holds: the span goes whole, as it did, never cut.
#[test]
fn a_release_with_room_releases_as_the_uncounted_one_did() {
    let mut before = placed_behind();
    let mut said_before = Vec::new();
    before
        .writer
        .release_held_uncounted(
            &mut before.tree,
            &mut before.objects,
            &Unsegmented,
            &mut said_before,
        )
        .unwrap();
    let mut unpaid = placed_behind();
    let mut said_unpaid = Vec::new();
    unpaid
        .writer
        .release_held(
            &mut unpaid.tree,
            &mut unpaid.objects,
            &Unsegmented,
            &mut said_unpaid,
        )
        .unwrap();
    assert_eq!(unpaid.writer.release_owed(), None, "nothing is owed");
    let mut paid = placed_behind();
    let mut said_paid = Vec::new();
    let mut prepaid = false;
    assert_eq!(paid.release_paid(&mut prepaid, &mut said_paid), None);
    for run in [&before, &unpaid, &paid] {
        assert_eq!(run.writer.held_bytes(NodeId::ROOT), 0);
    }
    assert_eq!(said_unpaid, said_before);
    assert_eq!(said_paid, said_before);
    // What the store's member recorded of the bytes it was handed.
    let coverage = |run: &PlacedBehind| {
        let (child, _) = run.tree.child_of(NodeId::ROOT, run.member).expect("a 7z");
        let record = run
            .tree
            .namespace(child)
            .and_then(|below| below.find_by_name("inside.bin"))
            .expect("the store's member");
        format!("{:?}", record.coverage)
    };
    assert_eq!(coverage(&unpaid), coverage(&before));
    assert_eq!(coverage(&paid), coverage(&before));
    let bytes_before = before.inside();
    assert!(bytes_before == before.payload, "the member is whole");
    assert!(
        unpaid.inside() == bytes_before,
        "the release's member is the same"
    );
    assert!(paid.inside() == bytes_before, "the paid member is the same");
}

// The release the loop runs after a write, onto a volume with no room for
// the held span, writes nothing and never stands the loop still: the bytes
// stay held, and what it stood for is owed, a read-back piece credited in
// the store's namespace. Paid that off the loop (`release_paid`), the
// release goes on and the member is written whole; nothing is owed after.
#[test]
fn a_release_with_no_room_keeps_the_bytes_held_until_it_is_paid() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let mut run = placed_behind();
    let (child, _) = run.tree.child_of(NodeId::ROOT, run.member).expect("a 7z");
    // Another producer counts the volume's whole limit.
    let hard = 64 * 1024;
    run.volume.set_buffer_limits(hard, hard);
    let full = Credit::reserve(std::iter::once(&run.submitter), hard, Stand::UntilStop);
    assert!(full.covers(hard));
    let mut out = Vec::new();
    run.writer
        .release_held(&mut run.tree, &mut run.objects, &Unsegmented, &mut out)
        .unwrap();
    assert_eq!(
        run.writer.held_bytes(NodeId::ROOT),
        run.payload.len() as u64,
        "the span is still held"
    );
    assert_eq!(run.writer.release_owed(), Some((READ_BACK, child)));
    // The volume's room comes back, and a thread counts what is owed.
    drop(full);
    run.volume.set_buffer_limits(u64::MAX, u64::MAX);
    let credit = Credit::reserve(std::iter::once(&run.submitter), READ_BACK, Stand::UntilStop);
    assert!(credit.covers(READ_BACK));
    run.writer
        .release_paid(
            &mut run.tree,
            &mut run.objects,
            &Unsegmented,
            credit,
            &mut out,
        )
        .unwrap();
    assert_eq!(run.writer.release_owed(), None, "nothing is owed");
    assert_eq!(run.writer.held_bytes(NodeId::ROOT), 0);
    assert!(run.inside() == run.payload, "the member is whole");
}
