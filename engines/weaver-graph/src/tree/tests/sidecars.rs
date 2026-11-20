// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Below the root, a compressed sidecar opens by its name at once, and what
// it holds lands beside the archive it came out of. Any other compressed
// file beside a file of its own waits for the quiet and is refused as
// packed there.

use super::*;
use crate::deliver::Posted;
use crate::sniff::stream_member_name;

const XZ: &[u8] = &[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, 0x00, 0x04];
const GZIP: &[u8] = &[0x1F, 0x8B, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0x03];

// A pretend store that says nothing of its own: the test tells its members.
struct Outer;

impl Object for Outer {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Placement>) {}
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::NotYet
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

// A pretend single-stream compressor: its one member is named for its
// volume with the compression taken off, as a stream's is.
struct Stream {
    kind: Kind,
    name: String,
    announced: bool,
}

impl Object for Stream {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        self.kind
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if !self.announced {
            self.announced = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: stream_member_name(&self.name),
                length: None,
            });
        }
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        if volume == VolumeIndex(0) {
            name.clone_into(&mut self.name);
        }
    }
}

struct Builds;
impl Builder for Builds {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => Some(Box::new(Outer)),
            Kind::Xz | Kind::Gzip => Some(Box::new(Stream {
                kind,
                name: String::new(),
                announced: false,
            })),
            _ => None,
        }
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// A tree whose root `show.rar` opened, and whose namespace below holds
// `members`, each name with its bytes, whole and closed.
fn below(members: &[(&str, Vec<u8>)]) -> (Tree, NodeId, Vec<Effect>) {
    let mut tree = Tree::new();
    let mut effects = Vec::new();
    let root = tree.announce_root("show.rar", None, &mut effects);
    let mut bytes = SIGNATURE.to_vec();
    bytes.extend_from_slice(b"hello");
    tree.deliver(
        NodeId::ROOT,
        root,
        Posted {
            offset: 0,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        },
        &mut Builds,
        &mut effects,
    );
    let child = tree
        .child_of(NodeId::ROOT, root)
        .expect("the store opens")
        .0;
    for (at, (name, bytes)) in members.iter().enumerate() {
        let member = MemberId(at as u32);
        let length = bytes.len() as u64;
        // The front first, which founds what it signs, then the rest, which
        // reaches the object it founded.
        let front = bytes.len().min(XZ.len());
        let emits = vec![
            Emit::Member {
                member,
                name: (*name).into(),
                length: Some(length),
            },
            Emit::Bytes {
                member,
                offset: 0,
                bytes: bytes[..front].to_vec().into(),
                provenance: Provenance::Fresh,
            },
            Emit::Bytes {
                member,
                offset: front as u64,
                bytes: bytes[front..].to_vec().into(),
                provenance: Provenance::Fresh,
            },
            Emit::MemberClosed(member),
        ];
        tree.emitted(child, emits, &mut Builds, &mut effects);
    }
    (tree, child, effects)
}

fn compressed(signature: &[u8]) -> Vec<u8> {
    let mut bytes = signature.to_vec();
    bytes.extend_from_slice(&[0x55; 32]);
    bytes
}

fn refused(effects: &[Effect], node: NodeId, member: MemberId) -> Option<Why> {
    effects.iter().find_map(|effect| match effect {
        Effect::Refused {
            node: at,
            member: refused,
            why,
        } if *at == node && *refused == member => Some(*why),
        _ => None,
    })
}

fn output_name(tree: &Tree, node: NodeId, member: MemberId) -> String {
    tree.namespace(node)
        .and_then(|namespace| namespace.get(member))
        .map(|record| record.output_name.clone())
        .expect("member is there")
}

#[test]
fn a_compressed_sidecar_beside_a_payload_opens_at_once_and_lands_beside_the_archive() {
    let (tree, child, effects) = below(&[
        ("test-media.mkv", b"matroska payload".to_vec()),
        ("release.nfo.xz", compressed(XZ)),
    ]);
    assert!(!tree.layers_pending(), "nothing waits for the quiet");
    assert_eq!(refused(&effects, child, MemberId(1)), None);
    let (stream, _) = tree
        .child_of(child, MemberId(1))
        .expect("the sidecar opens by its name");
    assert_eq!(tree.kind(stream), Some(Kind::Xz));
    assert_eq!(
        tree.landing(stream, MemberId(0)).as_deref(),
        Some("release.nfo")
    );
    assert_eq!(output_name(&tree, stream, MemberId(0)), "release.nfo");
    assert_eq!(tree.child_of(child, MemberId(0)), None);
}

#[test]
fn a_compressed_file_of_its_folder_beside_a_file_of_its_own_is_packed_at_the_quiet() {
    let (mut tree, child, mut effects) = below(&[
        ("test-media.mkv", b"matroska payload".to_vec()),
        ("data/app.bin", b"application data".to_vec()),
        ("data/index.json.gz", compressed(GZIP)),
    ]);
    assert!(tree.layers_pending(), "it waits for the quiet");
    assert_eq!(tree.child_of(child, MemberId(2)), None);
    tree.decide_layers(&mut Builds, &mut effects);
    assert_eq!(
        refused(&effects, child, MemberId(2)),
        Some(Why::Packed),
        "{effects:?}"
    );
    assert_eq!(tree.child_of(child, MemberId(2)), None);
    assert!(!tree.layers_pending());
}

// A sidecar beside its own uncompressed copy: both land in one folder, and
// the name ledger numbers the one that came second, as any two names that
// collide are.
#[test]
fn a_compressed_sidecar_beside_its_own_name_is_numbered_by_the_ledger() {
    let (tree, child, effects) = below(&[
        (
            "movie.srt",
            b"1\n00:00:01,000 --> 00:00:02,000\nhi\n".to_vec(),
        ),
        ("movie.srt.xz", compressed(XZ)),
    ]);
    assert_eq!(refused(&effects, child, MemberId(1)), None);
    let (stream, _) = tree
        .child_of(child, MemberId(1))
        .expect("the sidecar opens by its name");
    assert_eq!(output_name(&tree, child, MemberId(0)), "movie.srt");
    assert_eq!(output_name(&tree, stream, MemberId(0)), "movie (1).srt");
}
