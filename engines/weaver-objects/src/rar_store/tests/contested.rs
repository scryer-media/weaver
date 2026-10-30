// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume a recovery kind holds by its name alone, and a posting of the
// same volume whose front came: through the tree, with the real objects.

use bytes::Bytes;
use weaver_graph::{MemberId, NodeId, Posted, Provenance, Tree};

use crate::Objects;
use crate::grid::crc32;
use crate::testing::{par2_set, par3_set, payload};

// The member at the root, with its whole bytes when it has any.
fn post(tree: &mut Tree, name: &str, bytes: Option<&[u8]>) -> MemberId {
    let mut effects = Vec::new();
    let member = tree.announce_root(name, bytes.map(|bytes| bytes.len() as u64), &mut effects);
    if let Some(bytes) = bytes {
        tree.deliver(
            NodeId::ROOT,
            member,
            Posted {
                offset: 0,
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance: Provenance::Fresh,
            },
            &mut Objects::default(),
            &mut effects,
        );
    }
    member
}

// A RAR5 recovery volume's header, its row `row` of two, and parity bytes.
fn rev5(row: u16) -> Vec<u8> {
    let mut raw = vec![1u8];
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&row.to_le_bytes());
    raw.extend_from_slice(&0u32.to_le_bytes());
    let size = (raw.len() as u32).to_le_bytes();
    let mut rev = b"Rar!\x1aRev".to_vec();
    rev.extend_from_slice(&crc32(&[size.as_slice(), raw.as_slice()].concat()).to_le_bytes());
    rev.extend_from_slice(&size);
    rev.extend_from_slice(&raw);
    rev.extend_from_slice(&[0x5A; 40]);
    rev
}

// Each recovery kind as (kind, the volume that founds its object, the
// volume held by name and posted again). A RAR data volume, recovery
// record or not, is never held by its name: only a recovery name is
// offered before its bytes come.
type Volume = (String, Vec<u8>);

fn kinds() -> Vec<(&'static str, Volume, Volume)> {
    let body = payload(131, 4096);
    let par2 = par2_set("rr", &[("payload.bin", &body)], 512, 2, 1);
    let par3 = par3_set("rr", &[("payload.bin", &body)], 512, 2);
    let index = |files: &[(String, Vec<u8>)], recovery: bool| {
        files
            .iter()
            .find(|(name, _)| name.contains(".vol") == recovery)
            .cloned()
            .unwrap()
    };
    vec![
        ("PAR2", index(&par2, false), index(&par2, true)),
        ("PAR3", index(&par3, false), index(&par3, true)),
        (
            ".rev",
            ("rr.part1.rev".to_owned(), rev5(0)),
            ("rr.part2.rev".to_owned(), rev5(1)),
        ),
    ]
}

#[test]
fn a_rev_held_by_name_is_released_to_a_contested_copy() {
    for (kind, (first, first_bytes), (held, held_bytes)) in kinds() {
        for restarted in [false, true] {
            let case = format!("{kind} restarted={restarted}");
            let mut tree = Tree::new();
            let founder = post(&mut tree, &first, Some(&first_bytes));
            let (node, _) = tree.child_of(NodeId::ROOT, founder).expect(&case);
            let holder = post(&mut tree, &held, None);
            assert_eq!(
                tree.child_of(NodeId::ROOT, holder).map(|(at, _)| at),
                Some(node),
                "{case}: its name alone binds the volume"
            );
            if restarted {
                tree = Tree::rebuild(&tree.records(), &Objects::default()).expect(&case);
            }
            let copy = post(&mut tree, &held, Some(&held_bytes));
            assert_eq!(
                tree.child_of(NodeId::ROOT, copy).map(|(at, _)| at),
                Some(node),
                "{case}: the posting whose front came is the object's volume"
            );
            assert!(
                !tree.undecided(NodeId::ROOT).any(|member| member == copy),
                "{case}: nothing waits on the holder"
            );
        }
    }
}
