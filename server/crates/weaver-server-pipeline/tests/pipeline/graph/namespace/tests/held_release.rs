// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_server_disk::operations::disk::volume::LoopSite;

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
                site: LoopSite::LeafWrite,
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
    fn release_paid(&mut self, paid: &mut u64, out: &mut Vec<Effect>) -> Option<(u64, NodeId)> {
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
    let mut paid = 0;
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

// A paid release that stands for a refused write, and the pass after it
// with room, end every pass as the walk over every hold ends it: the same
// answer, the same holds asked, the same holds left, the same effects.
#[test]
fn a_release_that_stands_ends_every_pass_as_the_walk_over_every_hold() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let run = |reference: bool| {
        RELEASE_REFERENCE.set(reference);
        RELEASE_LOG.set(Some(Vec::new()));
        let mut run = placed_behind();
        let hard = 64 * 1024;
        run.volume.set_buffer_limits(hard, hard);
        let full = Credit::reserve(std::iter::once(&run.submitter), hard, Stand::UntilStop);
        assert!(full.covers(hard));
        let mut out = Vec::new();
        let mut paid = 0;
        let stood = run.release_paid(&mut paid, &mut out);
        let asked_after_standing = run.writer.holds_asked(NodeId::ROOT, run.member);
        drop(full);
        run.volume.set_buffer_limits(u64::MAX, u64::MAX);
        let after = run.release_paid(&mut paid, &mut out);
        let passes = RELEASE_LOG.take().unwrap_or_default();
        RELEASE_REFERENCE.set(false);
        let dir = run._dir.path().display().to_string();
        let said = format!("{out:?}").replace(&dir, "<dir>");
        let inside = run.inside();
        (
            passes,
            stood.map(|(bytes, _)| bytes),
            asked_after_standing,
            after,
            said,
            inside,
        )
    };
    let kept = run(false);
    let reference = run(true);
    assert_eq!(kept.1, Some(READ_BACK), "the first pass stands");
    assert!(kept.0.len() >= 2, "{} passes", kept.0.len());
    assert_eq!(kept.0, reference.0, "every pass");
    assert_eq!(kept.1, reference.1);
    assert_eq!(kept.2, reference.2);
    assert_eq!(kept.3, reference.3);
    assert_eq!(kept.4, reference.4);
    assert!(kept.5 == reference.5, "the same member bytes");
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
    let mut prepaid = 0;
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

// The member's close comes while the release of its held bytes is owed:
// the close waits, the member stays open, and once the release is paid the
// close is made after the bytes went down. The job ends with the same
// files, bytes, coverage and effects, in the same order, as the same job
// with room on the volume, where the release went and the close followed.
#[test]
fn a_close_while_a_release_is_owed_ends_as_the_job_with_room() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let closed = |run: &PlacedBehind| {
        run.tree
            .namespace(NodeId::ROOT)
            .and_then(|root| root.get(run.member))
            .is_some_and(|record| record.closed)
    };
    let coverage = |run: &PlacedBehind| {
        let (child, _) = run.tree.child_of(NodeId::ROOT, run.member).expect("a 7z");
        let record = run
            .tree
            .namespace(child)
            .and_then(|below| below.find_by_name("inside.bin"))
            .expect("the store's member");
        format!("{:?} closed {}", record.coverage, record.closed)
    };
    let close = |run: &mut PlacedBehind, out: &mut Vec<Effect>| {
        let member = run.member;
        run.writer
            .close(
                &mut run.tree,
                &mut run.objects,
                &Unsegmented,
                NodeId::ROOT,
                member,
                out,
            )
            .unwrap();
    };
    // With room: the release goes, then the close.
    let mut room = placed_behind();
    let mut said_room = Vec::new();
    room.writer
        .release_held(
            &mut room.tree,
            &mut room.objects,
            &Unsegmented,
            &mut said_room,
        )
        .unwrap();
    assert_eq!(room.writer.release_owed(), None);
    close(&mut room, &mut said_room);
    assert!(closed(&room));
    // With no room: the release is owed, and the close waits for it.
    let mut owed = placed_behind();
    let hard = 64 * 1024;
    owed.volume.set_buffer_limits(hard, hard);
    let full = Credit::reserve(std::iter::once(&owed.submitter), hard, Stand::UntilStop);
    assert!(full.covers(hard));
    let mut said_owed = Vec::new();
    owed.writer
        .release_held(
            &mut owed.tree,
            &mut owed.objects,
            &Unsegmented,
            &mut said_owed,
        )
        .unwrap();
    assert!(owed.writer.release_owed().is_some(), "the release is owed");
    close(&mut owed, &mut said_owed);
    assert!(owed.writer.closes_owed(), "the close waits");
    assert!(!closed(&owed), "the member stays open while bytes are held");
    // The same close again is owed once.
    close(&mut owed, &mut said_owed);
    assert_eq!(
        owed.writer.held_bytes(NodeId::ROOT),
        owed.payload.len() as u64
    );
    drop(full);
    owed.volume.set_buffer_limits(u64::MAX, u64::MAX);
    while let Some((bytes, _)) = owed.writer.release_owed() {
        let credit = Credit::reserve(std::iter::once(&owed.submitter), bytes, Stand::UntilStop);
        assert!(credit.covers(bytes));
        owed.writer
            .release_paid(
                &mut owed.tree,
                &mut owed.objects,
                &Unsegmented,
                credit,
                &mut said_owed,
            )
            .unwrap();
    }
    assert!(!owed.writer.closes_owed(), "the owed close was made");
    assert!(closed(&owed));
    for run in [&room, &owed] {
        assert_eq!(run.writer.held_bytes(NodeId::ROOT), 0);
    }
    assert_eq!(said_owed, said_room);
    assert_eq!(coverage(&owed), coverage(&room));
    let bytes_room = room.inside();
    assert!(bytes_room == room.payload, "the member is whole");
    assert!(
        owed.inside() == bytes_room,
        "the owed job's member is the same"
    );
}

// One stored member of the given name, as `stored` makes "inside.bin".
fn stored_as(name: &str, payload: &[u8]) -> Vec<u8> {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    let mut archive = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    archive.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    let mut entry = ArchiveEntry::new_file(name);
    entry.size = payload.len() as u64;
    archive
        .push_archive_entry(entry, Some(std::io::Cursor::new(payload.to_vec())))
        .unwrap();
    archive.finish().unwrap().into_inner()
}

// Two archives, A small and B larger, each with its body held behind its
// end header, written through a volume's own writer thread. A's header has
// been heard without a release after it, so A's body is placeable; B's
// header has not come.
struct TwoHeld {
    dir: tempfile::TempDir,
    writer: NamespaceWriter,
    tree: Tree,
    objects: weaver_objects::Objects,
    a: MemberId,
    b: MemberId,
    b_archive: Vec<u8>,
    a_payload: Vec<u8>,
    b_payload: Vec<u8>,
    _writers: weaver_server_disk::operations::disk::writer::VolumeWriters,
    volume: Arc<weaver_server_disk::operations::disk::volume::Volume>,
    submitter: Arc<weaver_server_disk::operations::disk::writer::Submitter>,
}

const DATA_AT: usize = 32;

fn two_held() -> TwoHeld {
    use crate::pipeline::graph::disk::JobDisk;
    use weaver_server_disk::operations::disk::volume::Volume;
    use weaver_server_disk::operations::disk::writer::VolumeWriters;
    let bytes = |len: u64, seed: usize| -> Vec<u8> {
        (0..len as usize)
            .map(|n| ((n * 31 + n / 97 + seed) % 251) as u8)
            .collect()
    };
    let a_payload = bytes(4096, 7);
    let b_payload = bytes(5 * READ_BACK / 2, 13);
    let a_archive = stored_as("a.bin", &a_payload);
    let b_archive = stored_as("b.bin", &b_payload);
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
    let a = tree.announce_root("a.7z", Some(a_archive.len() as u64), &mut Vec::new());
    let b = tree.announce_root("b.7z", Some(b_archive.len() as u64), &mut Vec::new());
    for member in [a, b] {
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
    }
    for (member, archive, payload) in [(a, &a_archive, &a_payload), (b, &b_archive, &b_payload)] {
        let footer = DATA_AT + payload.len();
        for (at, end) in [(0, DATA_AT), (DATA_AT, footer)] {
            writer
                .deliver(
                    &mut tree,
                    &mut objects,
                    &Unsegmented,
                    held_arrival(member, at, &archive[at..end]),
                    &mut Vec::new(),
                )
                .unwrap();
        }
    }
    let footer = DATA_AT + a_payload.len();
    writer
        .arrive(
            &mut tree,
            &mut objects,
            &Unsegmented,
            crate::pipeline::graph::namespace::Write::New {
                entered: NodeId::ROOT,
                posting: a,
                site: LoopSite::LeafWrite,
            },
            held_arrival(a, footer, &a_archive[footer..]),
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(
        writer.held_bytes(NodeId::ROOT),
        (a_payload.len() + b_payload.len()) as u64
    );
    TwoHeld {
        dir,
        writer,
        tree,
        objects,
        a,
        b,
        b_archive,
        a_payload,
        b_payload,
        _writers: writers,
        volume,
        submitter,
    }
}

fn held_arrival(member: MemberId, offset: usize, bytes: &[u8]) -> Arrival {
    Arrival {
        node: NodeId::ROOT,
        member,
        offset: offset as u64,
        bytes: Bytes::copy_from_slice(bytes).into(),
        provenance: Provenance::Fresh,
    }
}

impl TwoHeld {
    fn release(&mut self, out: &mut Vec<Effect>) {
        self.writer
            .release_held(&mut self.tree, &mut self.objects, &Unsegmented, out)
            .unwrap();
    }

    // B's end header, by the delivery path, which releases after it.
    fn b_header(&mut self, out: &mut Vec<Effect>) {
        let footer = DATA_AT + self.b_payload.len();
        let arrival = held_arrival(self.b, footer, &self.b_archive[footer..]);
        self.writer
            .deliver(
                &mut self.tree,
                &mut self.objects,
                &Unsegmented,
                arrival,
                out,
            )
            .unwrap();
    }

    // Each member's file once every write submitted returned.
    fn files(&self) -> (Vec<u8>, Vec<u8>) {
        self.writer.disk().unwrap().barrier().wait_aside();
        let read = |name: &str| std::fs::read(self.dir.path().join(name)).unwrap_or_default();
        (read("a.bin"), read("b.bin"))
    }

    fn loop_waits(&self) -> u64 {
        self.volume
            .writer
            .loop_waits
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

// One release at a time. A release stands for A's small span on a full
// volume; B's end header then makes a larger span placeable. That delivery
// leaves every hold to the release that stands: it walks no held span and
// the owed release is unchanged. Once room comes and the mail is heard,
// the release goes on through both members, with no loop wait on the
// volume, and both files end as the same sequence with room wrote them.
#[test]
fn a_delivery_while_a_release_is_owed_walks_no_holds_and_both_members_end_as_with_room() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let mut room = two_held();
    let mut said_room = Vec::new();
    room.release(&mut said_room);
    room.b_header(&mut said_room);
    assert_eq!(room.writer.release_owed(), None);
    assert_eq!(room.writer.held_bytes(NodeId::ROOT), 0);
    let mut owed = two_held();
    let (child_a, _) = owed.tree.child_of(NodeId::ROOT, owed.a).expect("a 7z");
    let hard = 4 * READ_BACK;
    owed.volume.set_buffer_limits(hard, hard);
    let full = Credit::reserve(std::iter::once(&owed.submitter), hard, Stand::UntilStop);
    assert!(full.covers(hard));
    let mut said_owed = Vec::new();
    owed.release(&mut said_owed);
    assert_eq!(owed.writer.release_owed(), Some((4096, child_a)));
    RELEASE_ASKED.set(0);
    RELEASE_LOCATED.set(0);
    owed.b_header(&mut said_owed);
    assert_eq!(RELEASE_ASKED.get(), 0, "no hold was asked");
    assert_eq!(RELEASE_LOCATED.get(), 0, "no held span was walked");
    assert_eq!(owed.writer.release_owed(), Some((4096, child_a)));
    assert_eq!(
        owed.writer.held_bytes(NodeId::ROOT),
        (owed.a_payload.len() + owed.b_payload.len()) as u64,
        "both members are still held"
    );
    drop(full);
    owed.volume.set_buffer_limits(u64::MAX, u64::MAX);
    while let Some((bytes, _)) = owed.writer.release_owed() {
        let credit = Credit::reserve(std::iter::once(&owed.submitter), bytes, Stand::UntilStop);
        assert!(credit.covers(bytes));
        owed.writer
            .release_paid(
                &mut owed.tree,
                &mut owed.objects,
                &Unsegmented,
                credit,
                &mut said_owed,
            )
            .unwrap();
    }
    assert_eq!(owed.writer.held_bytes(NodeId::ROOT), 0);
    let (a_room, b_room) = room.files();
    assert!(a_room == room.a_payload, "A is whole with room");
    assert!(b_room == room.b_payload, "B is whole with room");
    let (a_owed, b_owed) = owed.files();
    assert!(a_owed == a_room, "A ends as with room");
    assert!(b_owed == b_room, "B ends as with room");
    for run in [&room, &owed] {
        assert_eq!(run.loop_waits(), 0, "the loop never waited on the volume");
    }
}

// A credit short of the bytes the release stood for is the stop's case:
// the bytes stay held, the release stays owed as it was, and nothing is
// written on the loop from it.
#[test]
fn a_short_credit_leaves_the_release_owed_and_the_bytes_held() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let mut run = placed_behind();
    let hard = 64 * 1024;
    run.volume.set_buffer_limits(hard, hard);
    let full = Credit::reserve(std::iter::once(&run.submitter), hard, Stand::UntilStop);
    assert!(full.covers(hard));
    let mut out = Vec::new();
    run.writer
        .release_held(&mut run.tree, &mut run.objects, &Unsegmented, &mut out)
        .unwrap();
    let owed = run.writer.release_owed().expect("the release is owed");
    drop(full);
    run.volume.set_buffer_limits(u64::MAX, u64::MAX);
    let short = Credit::reserve(
        std::iter::once(&run.submitter),
        owed.0 - 1,
        Stand::UntilStop,
    );
    assert!(!short.covers(owed.0));
    let said = out.len();
    let paid = run.writer.release_paid(
        &mut run.tree,
        &mut run.objects,
        &Unsegmented,
        short,
        &mut out,
    );
    assert!(matches!(paid, Err(WriteError::LeftAtStop)), "{paid:?}");
    assert_eq!(run.writer.release_owed(), Some(owed), "still owed");
    assert_eq!(
        run.writer.held_bytes(NodeId::ROOT),
        run.payload.len() as u64,
        "the bytes stay held"
    );
    assert_eq!(out.len(), said, "nothing was said");
    assert!(run.inside().is_empty(), "nothing was written");
}

// A step settled inside another, as a held release's piece is inside the
// step that hears a run's output, leaves the carried credit holding what
// the members carry. The outer step then keeps only what is not carried
// already: the loop carries what the members do, not twice that.
#[test]
fn a_step_settled_inside_another_leaves_the_credit_the_members_carry() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let mut run = placed_behind();
    let held = run.payload.len() as u64;
    let reserve = |bytes| {
        let credit = Credit::reserve(std::iter::once(&run.submitter), bytes, Stand::UntilStop);
        assert!(credit.covers(bytes));
        credit
    };
    // A first step carries the member's held bytes.
    let first = reserve(held);
    run.writer
        .carry_held_for_test(&run.tree, (NodeId::ROOT, run.member), held, first);
    assert_eq!(run.writer.carried_on_for_test(&run.volume), (held, held));
    // The next step takes it, with fresh credit of its own.
    let outer = run.writer.begin_step_for_test(reserve(held));
    // A step inside it is settled first and carries the member again.
    let inner = run.writer.begin_step_for_test(reserve(held));
    run.writer.settle_step_for_test(&run.tree, inner);
    assert_eq!(run.writer.carried_on_for_test(&run.volume), (held, held));
    run.writer.settle_step_for_test(&run.tree, outer);
    assert_eq!(
        run.writer.carried_on_for_test(&run.volume),
        (held, held),
        "the loop carries what the members carry"
    );
    assert_eq!(run.volume.carried_bytes(), held);
}

// The first `count` volumes of the rarlab set of 64 stored RAR5 volumes,
// `rar5_stored_sixtyfour`, which hold one 252 000-byte member.
fn sixty_four(count: usize) -> Vec<(String, Vec<u8>)> {
    (1..=count)
        .map(|part| {
            let name = format!("rar5_stored_sixtyfour.part{part:02}.rar");
            let bytes = std::fs::read(format!(
                "{}/../weaver-server-core/tests/fixtures/extraction_profiles/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            (name, bytes)
        })
        .collect()
}

// What a download of the set in `order`, each volume in `pieces`
// deliveries, came to: the held volumes each delivery's passes visited,
// the bytes held after each volume, and the member's file.
struct SetRun {
    visits: Vec<Vec<usize>>,
    held: Vec<u64>,
    written: Vec<u8>,
}

fn set_run(volumes: &[(String, Vec<u8>)], order: &[usize], pieces: usize) -> SetRun {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        Arc::new(HoldsAccountant::new(PLENTY)),
        CapacityReader::unknown(),
    );
    let mut tree = Tree::new();
    let mut objects = weaver_objects::Objects::default();
    let members: Vec<MemberId> = volumes
        .iter()
        .map(|(name, bytes)| tree.announce_root(name, Some(bytes.len() as u64), &mut Vec::new()))
        .collect();
    for member in &members {
        writer
            .announced(
                &mut tree,
                &mut objects,
                &Unsegmented,
                NodeId::ROOT,
                *member,
                &mut Vec::new(),
            )
            .unwrap();
    }
    let mut visits = Vec::new();
    let mut held = Vec::new();
    for &index in order {
        let bytes = &volumes[index].1;
        let piece = bytes.len().div_ceil(pieces);
        let mut each = Vec::new();
        for (at, chunk) in bytes.chunks(piece).enumerate() {
            RELEASE_VISITED.set(0);
            writer
                .deliver(
                    &mut tree,
                    &mut objects,
                    &Unsegmented,
                    held_arrival(members[index], at * piece, chunk),
                    &mut Vec::new(),
                )
                .unwrap();
            each.push(RELEASE_VISITED.get());
        }
        visits.push(each);
        held.push(writer.held_bytes(NodeId::ROOT));
    }
    SetRun {
        visits,
        held,
        written: std::fs::read(dir.path().join("stored64.bin")).unwrap_or_default(),
    }
}

// The set's first volume, then every one after the second, then the
// second, each in four deliveries. Each volume after the second is held,
// waiting on the second, and each of its deliveries moves the store's
// placement for it alone: the passes visit that one held volume, never
// the ones held before it. The second volume's last delivery lays every
// held volume, and the pass visits each once and writes it. So the passes
// visit 4 (V - 2) + (V - 2) = 5 (V - 2) held volumes: 150 at V = 32 and
// 310 at V = 64, not a number that grows with the square of V. The first
// 32 volumes are the set as it stands with its last 32 not yet posted:
// what they hold of the member is written, and nothing stays held.
#[test]
fn held_volumes_of_a_rar_set_arriving_one_by_one_are_visited_linearly() {
    let member: Vec<u8> = (0..252_000usize)
        .map(|n| ((n * 31 + n / 97) % 251) as u8)
        .collect();
    for (count, total) in [(32usize, 150usize), (64, 310)] {
        let volumes = sixty_four(count);
        let order: Vec<usize> = [0].into_iter().chain(2..count).chain([1]).collect();
        let run = set_run(&volumes, &order, 4);
        assert_eq!(
            run.visits[0], [0; 4],
            "{count}: the first volume holds nothing"
        );
        for (at, each) in run.visits[1..count - 1].iter().enumerate() {
            assert_eq!(each, &[1; 4], "{count}: volume {}", at + 2);
        }
        assert_eq!(
            run.visits[count - 1],
            [0, 0, 0, count - 2],
            "{count}: the second volume"
        );
        assert_eq!(run.visits.iter().flatten().sum::<usize>(), total, "{count}");
        assert!(
            run.held[1..count - 1].is_sorted() && run.held[count - 2] > 0,
            "{count}: each volume after the second is held"
        );
        assert_eq!(*run.held.last().unwrap(), 0, "{count}: nothing stays held");
        assert!(
            member.starts_with(&run.written) && !run.written.is_empty(),
            "{count}: the member as far as the volumes hold it"
        );
        if count == 64 {
            assert!(run.written == member, "the member whole");
        }
    }
}
