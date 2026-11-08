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
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance: Provenance::Fresh,
            },
            &mut Vec::new(),
        )
        .unwrap();
}

const PASSWORD: &str = "lantern";

// An article is one recovery slice, so each carries the checksum that proves
// its slice.
const ARTICLE: usize = 1024;

// The source's checksums, by root member: one per article of what was
// posted, each verified, as the CRC an article states over its own bytes is.
struct Posted(Vec<Vec<u8>>);

impl Segments for Posted {
    fn segments_for(
        &self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        out: &mut Vec<Segment>,
    ) {
        let Some(bytes) = self
            .0
            .get(volume.0 as usize)
            .filter(|_| node == NodeId::ROOT)
        else {
            return;
        };
        for (at, article) in bytes.chunks(ARTICLE).enumerate() {
            let start = (at * ARTICLE) as u64;
            let piece = ByteRange::new(start, start + article.len() as u64);
            if piece.intersect(range).is_some() {
                out.push(Segment {
                    range: piece,
                    crc32: crc32(article),
                    verified: true,
                });
            }
        }
    }
}

// What a job does with what the writer passes on: an object that asks for a
// password is offered the one the set was written under, once, and what it
// says back is applied.
fn answer(
    tree: &mut Tree,
    writer: &mut NamespaceWriter,
    objects: &mut weaver_objects::Objects,
    source: &Posted,
    mut out: Vec<Effect>,
    offered: &mut std::collections::BTreeSet<NodeId>,
) {
    loop {
        let mut effects = Vec::new();
        let mut any = false;
        for effect in out.drain(..) {
            if let Effect::NeedsPassword { node } = effect
                && offered.insert(node)
            {
                tree.offer_password(node, PASSWORD, &mut writer.placing(objects), &mut effects);
                any = true;
            }
        }
        if !any {
            return;
        }
        writer
            .apply_all(tree, objects, source, effects, &mut out)
            .unwrap();
    }
}

// A posted file's articles, in order, each answered as a job answers it.
fn articles(
    tree: &mut Tree,
    writer: &mut NamespaceWriter,
    objects: &mut weaver_objects::Objects,
    source: &Posted,
    member: MemberId,
    offered: &mut std::collections::BTreeSet<NodeId>,
) {
    let bytes = &source.0[member.0 as usize];
    for (at, article) in bytes.chunks(ARTICLE).enumerate() {
        let mut out = Vec::new();
        writer
            .deliver(
                tree,
                objects,
                source,
                Arrival {
                    node: NodeId::ROOT,
                    member,
                    offset: (at * ARTICLE) as u64,
                    bytes: Bytes::copy_from_slice(article).into(),
                    provenance: Provenance::Fresh,
                },
                &mut out,
            )
            .unwrap();
        answer(tree, writer, objects, source, out, offered);
    }
}

// One file of the posting, announced, its articles delivered, and closed at
// the root. A file whose articles never come is announced and closed.
fn post(
    tree: &mut Tree,
    writer: &mut NamespaceWriter,
    objects: &mut weaver_objects::Objects,
    source: &Posted,
    (name, length): (&str, usize),
    offered: &mut std::collections::BTreeSet<NodeId>,
) -> MemberId {
    let member = tree.announce_root(name, Some(length as u64), &mut Vec::new());
    let mut out = Vec::new();
    writer
        .announced(tree, objects, source, NodeId::ROOT, member, &mut out)
        .unwrap();
    answer(tree, writer, objects, source, out, offered);
    articles(tree, writer, objects, source, member, offered);
    let mut out = Vec::new();
    writer
        .close(tree, objects, source, NodeId::ROOT, member, &mut out)
        .unwrap();
    answer(tree, writer, objects, source, out, offered);
    member
}

// A posting under a recovery set, at the root of a namespace driven the way a
// job drives it, so no root intake rule is on the path.
struct Rig {
    _dir: tempfile::TempDir,
    writer: NamespaceWriter,
    tree: Tree,
    objects: weaver_objects::Objects,
    offered: std::collections::BTreeSet<NodeId>,
    source: Posted,
}

impl Rig {
    // The volumes as posted, after them the PAR2 set made over the volumes
    // as written, one slice per article.
    fn new(written: &[(String, Vec<u8>)], posted: Vec<Vec<u8>>) -> (Self, Vec<(String, usize)>) {
        use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};
        let authored = tempfile::tempdir().unwrap();
        let inputs: Vec<_> = written
            .iter()
            .map(|(name, bytes)| {
                let path = authored.path().join(name);
                std::fs::write(&path, bytes).unwrap();
                path
            })
            .collect();
        let mut options = Par2CreatorOptions::with_output(
            authored.path().join("proof.par2"),
            Some(authored.path().to_path_buf()),
            inputs,
        );
        options.block_sizing = BlockSizing::Bytes(ARTICLE as u64);
        options.recovery_amount = RecoveryAmount::Count(24);
        let creator = Par2Creator::new(options);
        let created = creator.create(&creator.plan().unwrap()).unwrap();
        let mut posted = posted;
        let mut files: Vec<_> = written
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.len()))
            .collect();
        for path in &created.output_paths {
            let bytes = std::fs::read(path).unwrap();
            files.push((
                path.file_name().unwrap().to_string_lossy().into_owned(),
                bytes.len(),
            ));
            posted.push(bytes);
        }
        let dir = tempfile::tempdir().unwrap();
        let rig = Self {
            writer: NamespaceWriter::new(
                1,
                dir.path().to_path_buf(),
                Arc::new(HoldsAccountant::new(PLENTY)),
                CapacityReader::unknown(),
            ),
            _dir: dir,
            tree: Tree::new(),
            objects: weaver_objects::Objects::default(),
            offered: std::collections::BTreeSet::new(),
            source: Posted(posted),
        };
        (rig, files)
    }

    fn post(&mut self, file: &(String, usize)) -> MemberId {
        let file = (file.0.as_str(), file.1);
        post(
            &mut self.tree,
            &mut self.writer,
            &mut self.objects,
            &self.source,
            file,
            &mut self.offered,
        )
    }

    // One article of a posted file, again, as the source gives it.
    fn article(&mut self, member: MemberId, at: usize) {
        let bytes = &self.source.0[member.0 as usize][at..at + ARTICLE];
        let arrival = Arrival {
            node: NodeId::ROOT,
            member,
            offset: at as u64,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        };
        let mut out = Vec::new();
        let source = &self.source;
        self.writer
            .deliver(&mut self.tree, &mut self.objects, source, arrival, &mut out)
            .unwrap();
        answer(
            &mut self.tree,
            &mut self.writer,
            &mut self.objects,
            source,
            out,
            &mut self.offered,
        );
    }

    fn marked(&self, member: MemberId) -> bool {
        let namespace = self.tree.namespace(NodeId::ROOT).unwrap();
        namespace
            .get(member)
            .unwrap()
            .coverage
            .damaged_ranges()
            .next()
            .is_some()
    }

    // The member `name` of the store bound to the root's `volume`.
    fn inside(&self, volume: MemberId, name: &str) -> (NodeId, weaver_graph::Member) {
        let (store, _) = self.tree.child_of(NodeId::ROOT, volume).unwrap();
        let namespace = self.tree.namespace(store).unwrap();
        let member = namespace
            .members()
            .find(|member| member.name == name)
            .unwrap();
        (store, member.clone())
    }

    // The job's walk: every namespace rules its sets, parents first, each
    // repair lent goes out and comes back, and each replay waiting runs,
    // until nothing more is lent or waits.
    fn prove(&mut self) {
        let source = &self.source;
        loop {
            let mut lent = Inline::default();
            let mut walked = None;
            while let Some(node) = self.tree.next_live(walked) {
                walked = Some(node);
                let mut out = Vec::new();
                let ruling: (NodeId, &dyn Fn(NodeId) -> bool) = (node, &|_| false);
                self.writer
                    .rule(
                        &mut self.tree,
                        &mut self.objects,
                        source,
                        ruling,
                        &mut lent,
                        &mut out,
                    )
                    .unwrap();
                answer(
                    &mut self.tree,
                    &mut self.writer,
                    &mut self.objects,
                    source,
                    out,
                    &mut self.offered,
                );
            }
            let replay = self.writer.take_replay(&self.tree).unwrap();
            if lent.0.is_empty() && replay.is_none() {
                return;
            }
            for loan in lent.0.drain(..) {
                let returned = loan.run(weaver_graph::Budget::default(), &Default::default());
                let mut out = Vec::new();
                let fresh = weaver_objects::Objects::default();
                self.writer
                    .returned(
                        &mut self.tree,
                        &mut self.objects,
                        source,
                        &fresh,
                        returned,
                        &mut out,
                    )
                    .unwrap();
                answer(
                    &mut self.tree,
                    &mut self.writer,
                    &mut self.objects,
                    source,
                    out,
                    &mut self.offered,
                );
            }
            if let Some(replay) = replay {
                let replay = replay.run(&Default::default());
                let mut out = Vec::new();
                self.writer
                    .replayed(&mut self.tree, &mut self.objects, source, replay, &mut out)
                    .unwrap();
                answer(
                    &mut self.tree,
                    &mut self.writer,
                    &mut self.objects,
                    source,
                    out,
                    &mut self.offered,
                );
            }
        }
    }

    // What the member's file holds over `range`.
    fn read(&self, store: NodeId, member: MemberId, range: ByteRange) -> Vec<u8> {
        let mut bytes = Vec::new();
        assert!(
            self.writer
                .access(&self.tree)
                .read_at(store, member, range, &mut bytes)
        );
        bytes
    }
}

// The payload every set here stores.
fn payload() -> Vec<u8> {
    let mut seed = 7_u64;
    (0..48 * 1024)
        .map(|_| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 56) as u8
        })
        .collect()
}

// One article of `volume` is another's bytes under its own CRC: the source
// takes it, and only a checksum over more than the article can say it is
// wrong.
fn swapped(volume: &[u8], at: usize) -> Vec<u8> {
    let mut swapped = volume.to_vec();
    for byte in &mut swapped[at..at + ARTICLE] {
        *byte ^= 0x5a;
    }
    swapped
}

// How the first volume's bytes come again after the store's verdict on it.
enum Again {
    // The whole posting, as a root that lost its record of what it holds
    // intact admits it after a restart.
    Posting,
    // A second copy of the one article the verdict is about.
    Article,
}

// A swapped article in the first volume of an encrypted RAR5 set, so the
// part's checksum over the volume's data fails: the store's verdict. The
// first volume's bytes then come again, and the store does not say a
// verdict it has said once again. The verdict stands until the recovery set
// proves the volume, and the proof reaches every byte under it, not only
// the slices it rebuilt. The member's checksum is keyed, and the last
// volume, which carries it, never comes: nothing judges the member but the
// part's checksum.
fn verdict_until_proof(again: Again) {
    use crate::pipeline::tests::direct_store::scenarios::encrypted_store_set;
    let payload = payload();
    let volumes = encrypted_store_set("inside.mkv", &payload, 3, PASSWORD, Some(PASSWORD), true);
    let middle = volumes[0].1.len() / 2 / ARTICLE * ARTICLE;
    let posted = vec![
        swapped(&volumes[0].1, middle),
        volumes[1].1.clone(),
        Vec::new(),
    ];
    let (mut rig, files) = Rig::new(&volumes, posted);
    let first = rig.post(&files[0]);
    rig.post(&files[1]);
    let absent = rig.post(&files[2]);
    let mut out = Vec::new();
    let lost = vec![(VolumeIndex(absent.0), ByteRange::new(0, files[2].1 as u64))];
    rig.writer
        .lost(
            &mut rig.tree,
            &mut rig.objects,
            &rig.source,
            (NodeId::ROOT, lost),
            &mut out,
        )
        .unwrap();
    answer(
        &mut rig.tree,
        &mut rig.writer,
        &mut rig.objects,
        &rig.source,
        out,
        &mut rig.offered,
    );
    assert!(
        rig.marked(first),
        "the part's checksum judged the first volume"
    );
    let judged: Vec<_> = rig
        .inside(first, "inside.mkv")
        .1
        .coverage
        .damaged_ranges()
        .collect();
    assert!(!judged.is_empty(), "the verdict reached the member");

    match again {
        Again::Posting => articles(
            &mut rig.tree,
            &mut rig.writer,
            &mut rig.objects,
            &rig.source,
            first,
            &mut rig.offered,
        ),
        Again::Article => rig.article(first, middle),
    }
    // Every byte the verdict was said of is still under it, here and below.
    let still = |coverage: &weaver_graph::Coverage, range: ByteRange| {
        coverage.covered(range)
            && coverage
                .overlapping(range)
                .all(|run| run.provenance.is_damaged())
    };
    let root = rig
        .tree
        .namespace(NodeId::ROOT)
        .unwrap()
        .get(first)
        .unwrap();
    let swapped = ByteRange::at(middle as u64, ARTICLE as u64);
    assert!(
        still(&root.coverage, swapped),
        "the bytes coming again lifted the verdict"
    );
    let (_, mkv) = rig.inside(first, "inside.mkv");
    for &range in &judged {
        assert!(
            still(&mkv.coverage, range),
            "the bytes coming again lifted the verdict below"
        );
    }

    for file in &files[3..] {
        rig.post(file);
    }
    rig.prove();
    let (store, mkv) = rig.inside(first, "inside.mkv");
    for range in judged {
        assert!(
            mkv.coverage.covered(range) && !mkv.coverage.damaged(range),
            "the proof reached every byte the verdict covered: {:?}",
            mkv.coverage
        );
        let expected = &payload[range.start as usize..range.end as usize];
        assert_eq!(rig.read(store, mkv.id, range), expected);
    }
}

#[test]
fn a_readmitted_copy_keeps_the_store_verdict_until_proof() {
    verdict_until_proof(Again::Posting);
}

#[test]
fn a_second_copy_of_an_article_does_not_lift_the_store_verdict() {
    verdict_until_proof(Again::Article);
}

// A plain RAR5 set over four volumes. The heads of the second and fourth
// volumes are silently wrong, so the store holds their bytes, and one more
// article of each is wrong too. The repair rebuilds the second volume's head
// first: its held bytes go down, and its part's checksum fails on them, so
// the verdict reaches the member before the bytes it judged, in one route.
// The verdict stands over those bytes until the repair reaches them, and the
// member is not judged whole on bytes nothing has proved yet. Once the
// repair is down, the member is whole.
#[test]
fn a_part_verdict_said_before_its_own_bytes_stands_until_the_repair() {
    use crate::pipeline::tests::direct_store::single_member_store_set;
    let payload = payload();
    let volumes = single_member_store_set("inside.mkv", &payload, 4);
    let posted: Vec<_> = volumes
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| match index {
            1 | 3 => swapped(&swapped(bytes, 0), bytes.len() / 2 / ARTICLE * ARTICLE),
            _ => bytes.clone(),
        })
        .collect();
    let (mut rig, files) = Rig::new(&volumes, posted);
    let first = rig.post(&files[0]);
    for file in &files[1..] {
        rig.post(file);
    }
    rig.prove();
    let (store, mkv) = rig.inside(first, "inside.mkv");
    let whole = ByteRange::new(0, payload.len() as u64);
    assert!(
        mkv.closed && mkv.coverage.covered(whole) && !mkv.coverage.damaged(whole),
        "the member is whole once the repair is down: {:?}",
        mkv.coverage
    );
    assert_eq!(rig.read(store, mkv.id, whole), payload);
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
    // Which copy of the payload a read shows is not this contract: in a job
    // the root drops a second copy of an article it holds intact.
    assert_eq!(still_owned.len(), current_payload.len());
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
