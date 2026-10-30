// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::graph::hardware::Shutdown;
use std::sync::atomic::AtomicBool;

// A PAR3 set over `body` as `payload.bin`: `recovery` blocks of 512 bytes,
// at most `per_volume` to a volume, the index first.
fn carriers(body: &[u8], recovery: u64, per_volume: u64) -> Vec<(String, Vec<u8>)> {
    use par3_rs::creation::{CreationOptions, CreationPlan, CreationSource, VolumeLayout};
    use par3_rs::source::{MemorySourceAccess, SourceId};
    let mut sources = MemorySourceAccess::default();
    sources.insert(SourceId(1), 1, body.to_vec().into());
    let plan = CreationPlan::build(
        Arc::new(sources),
        &[CreationSource {
            name: "payload.bin".into(),
            source: SourceId(1),
        }],
        CreationOptions {
            block_size: 512,
            recovery_count: recovery,
            volumes: VolumeLayout::Uniform(per_volume),
            ..Default::default()
        },
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut paths = plan
        .execute(&dir.path().join("carrier"), dir.path())
        .unwrap();
    paths.sort_by_key(|path| path.to_string_lossy().contains(".vol"));
    paths
        .into_iter()
        .map(|path| {
            (
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect()
}

fn announce(job: &mut Job, name: &str, bytes: &[u8]) -> MemberId {
    let member = job
        .tree
        .announce_root(name, Some(bytes.len() as u64), &mut Vec::new());
    job.writer
        .announced(
            &mut job.tree,
            &mut weaver_objects::Objects::default(),
            &Source,
            NodeId::ROOT,
            member,
            &mut job.out,
        )
        .unwrap();
    member
}

fn deliver(job: &mut Job, member: MemberId, offset: u64, bytes: &[u8]) {
    job.writer
        .deliver(
            &mut job.tree,
            &mut weaver_objects::Objects::default(),
            &Source,
            Arrival {
                node: NodeId::ROOT,
                member,
                offset,
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance: Provenance::Fresh,
            },
            &mut job.out,
        )
        .unwrap();
}

fn close(job: &mut Job, member: MemberId) {
    job.writer
        .close(
            &mut job.tree,
            &mut weaver_objects::Objects::default(),
            &Source,
            NodeId::ROOT,
            member,
            &mut job.out,
        )
        .unwrap();
}

fn lend(job: &mut Job, set: NodeId) -> Loan {
    job.writer
        .lend(&mut job.tree, NodeId::ROOT, set)
        .unwrap()
        .unwrap()
}

fn accept(job: &mut Job, returned: super::super::loan::Returned) {
    job.writer
        .returned(
            &mut job.tree,
            &mut weaver_objects::Objects::default(),
            &Source,
            &weaver_objects::Objects::default(),
            returned,
            &mut job.out,
        )
        .unwrap();
}

#[test]
fn cancelled_and_stale_par3_returns_keep_damaged_carriers_repairable() {
    for cancelled in [false, true] {
        let body: Vec<_> = (0u32..4096)
            .map(|n| ((n.wrapping_mul(1664525).wrapping_add(n * n * 31)) >> 8) as u8)
            .collect();
        let files = carriers(&body, 4, 4);
        let mut job = Job::new(PLENTY);
        let mut set = None;
        for (name, mut bytes) in files {
            let member = announce(&mut job, &name, &bytes);
            // Bind a later named carrier before its damaged leading packet
            // arrives, as the download path does once the index is known.
            if name.contains(".vol") {
                bytes[0] ^= 0x55;
            }
            deliver(&mut job, member, 0, &bytes);
            close(&mut job, member);
            let child = job.tree.child_of(NodeId::ROOT, member).unwrap().0;
            assert_eq!(*set.get_or_insert(child), child);
        }
        let set = set.unwrap();
        let mut wrong = body.clone();
        wrong[1200] ^= 0x55;
        let member = announce(&mut job, "payload.bin", &wrong);
        deliver(&mut job, member, 0, &wrong);
        close(&mut job, member);
        job.writer
            .bind_recoveries(
                &mut job.tree,
                &mut weaver_objects::Objects::default(),
                &Source,
                NodeId::ROOT,
                &mut job.out,
            )
            .unwrap();
        job.tree.refuse(set, member, ByteRange::new(1024, 1536));
        let loan = lend(&mut job, set);
        let shutdown = Shutdown::of(Arc::new(AtomicBool::new(cancelled)));
        let returned = loan.run(weaver_graph::Budget::default(), &shutdown);
        assert_eq!(returned.stopped(), cancelled);
        if !cancelled {
            assert!(
                matches!(returned.outcome, Some(RepairOutcome::Repaired { .. })),
                "{returned:?}"
            );
            // A revision to the same source leaf invalidates the completed
            // worker's staged repair before acknowledgment.
            wrong[1700] ^= 0x55;
            job.writer
                .files_mut(NodeId::ROOT)
                .leaves
                .get_mut(&member)
                .unwrap()
                .write(1700, &wrong[1700..1701], None)
                .unwrap();
            assert!(
                job.file("payload.bin") == wrong,
                "the source revision is installed before acknowledgment"
            );
            job.tree.refuse(set, member, ByteRange::new(1536, 2048));
        }
        accept(&mut job, returned);
        assert!(!job.tree.lent(set));
        assert!(
            job.file("payload.bin") == wrong,
            "no stopped or stale write is installed"
        );
        let returned =
            lend(&mut job, set).run(weaver_graph::Budget::default(), &Shutdown::default());
        assert!(
            matches!(returned.outcome, Some(RepairOutcome::Repaired { .. })),
            "cancelled={cancelled}: {returned:?}"
        );
        accept(&mut job, returned);
        assert_eq!(job.file("payload.bin"), body);
    }
}

// Where the second `PAR REC` packet of a PAR3 volume lies.
fn second_block(volume: &[u8]) -> (u64, u64) {
    let mut at = 0;
    let mut blocks = Vec::new();
    while at + 48 <= volume.len() {
        let length = u64::from_le_bytes(volume[at + 24..at + 32].try_into().unwrap()) as usize;
        if &volume[at + 40..at + 48] == b"PAR REC\0" {
            blocks.push((at as u64, (at + length) as u64));
        }
        at += length;
    }
    blocks[1]
}

// A PAR3 set of two recovery blocks in one volume, the second still to come
// when a repair that needs one block is lent: the block lands while the
// repair is out. The repair read none of it, so its ruling stands, on the
// one block it had.
#[test]
fn a_par3_ruling_stands_when_its_own_volume_grows_while_the_repair_is_out() {
    let body: Vec<_> = (0u32..4096)
        .map(|n| ((n.wrapping_mul(1664525).wrapping_add(n * n * 31)) >> 8) as u8)
        .collect();
    let mut job = Job::new(PLENTY);
    let mut set = None;
    let mut withheld = None;
    for (name, bytes) in carriers(&body, 2, 2) {
        let member = announce(&mut job, &name, &bytes);
        if name.contains(".vol") {
            let (from, to) = second_block(&bytes);
            deliver(&mut job, member, 0, &bytes[..from as usize]);
            if (to as usize) < bytes.len() {
                deliver(&mut job, member, to, &bytes[to as usize..]);
            }
            withheld = Some((member, from, bytes[from as usize..to as usize].to_vec()));
        } else {
            deliver(&mut job, member, 0, &bytes);
            close(&mut job, member);
        }
        let child = job.tree.child_of(NodeId::ROOT, member).unwrap().0;
        assert_eq!(*set.get_or_insert(child), child);
    }
    let set = set.unwrap();
    let mut wrong = body.clone();
    wrong[1200] ^= 0x55;
    let member = announce(&mut job, "payload.bin", &wrong);
    deliver(&mut job, member, 0, &wrong);
    close(&mut job, member);
    job.writer
        .bind_recoveries(
            &mut job.tree,
            &mut weaver_objects::Objects::default(),
            &Source,
            NodeId::ROOT,
            &mut job.out,
        )
        .unwrap();
    job.tree.refuse(set, member, ByteRange::new(1024, 1536));
    let returned = lend(&mut job, set).run(weaver_graph::Budget::default(), &Shutdown::default());
    assert_eq!(
        returned.outcome,
        Some(RepairOutcome::Repaired { slices: 1 }),
        "{returned:?}"
    );
    let (volume, from, block) = withheld.unwrap();
    deliver(&mut job, volume, from, &block);
    close(&mut job, volume);
    assert!(
        !job.writer.moved_since(&job.tree, &returned),
        "a block the repair never read leaves its ruling standing"
    );
    accept(&mut job, returned);
    let mut rulings = Vec::new();
    job.writer.rulings(&job.tree, set, &mut rulings);
    assert_eq!(
        rulings
            .iter()
            .map(|ruling| (ruling.outcome, ruling.blocks_reachable))
            .collect::<Vec<_>>(),
        vec![(Some(RepairOutcome::Repaired { slices: 1 }), 1)]
    );
    assert_eq!(job.file("payload.bin"), body);
}
