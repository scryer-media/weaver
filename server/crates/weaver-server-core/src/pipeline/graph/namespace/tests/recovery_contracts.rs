// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::graph::hardware::Shutdown;
use std::sync::atomic::AtomicBool;

fn carriers(body: &[u8]) -> Vec<(String, Vec<u8>)> {
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
            recovery_count: 4,
            volumes: VolumeLayout::Uniform(4),
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
                bytes: Bytes::copy_from_slice(bytes),
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
        let files = carriers(&body);
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
