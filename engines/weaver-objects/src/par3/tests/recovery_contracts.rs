// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn replay(job: &mut Job<Par3>) {
    for (&volume, bytes) in &job.store.volumes {
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
        job.object.volume_closed(volume, &mut Vec::new());
    }
}

fn damaged_front_job(files: &[(String, Vec<u8>)]) -> Job<Par3> {
    let mut job = Job::new(Par3::new(OWN), OWN);
    for (name, bytes) in files {
        let volume = job.name(name, bytes);
        if name.contains(".vol") {
            job.store.volumes.get_mut(&volume).unwrap()[0] ^= 0x55;
        }
    }
    replay(&mut job);
    job
}

#[test]
fn damaged_carrier_fronts_repair_equally_after_reconstruction() {
    let body = payload(105, 4096);
    let files = par3_set("carrier", &[("payload.bin", &body)], 512, 4);
    for reconstruction in 0..2 {
        let mut job = damaged_front_job(&files);
        let mut wrong = body.clone();
        wrong[1200] ^= 0x55;
        let member = job.member("payload.bin", &wrong);
        bind(&mut job);
        Recovery::refused(&mut job.object, member, ByteRange::new(1024, 1536));
        if reconstruction > 0 {
            job.object = super::super::facts::par3(&job.object.facts()).unwrap();
            replay(&mut job);
            bind(&mut job);
        }
        let result = job
            .object
            .repair(&mut job.store, weaver_graph::Budget::default());
        assert!(
            matches!(result, RepairOutcome::Repaired { .. }),
            "reconstruction={reconstruction}: {result:?}"
        );
        assert_eq!(job.store.members[&member], body);
    }
}

#[test]
fn reconstructed_carrier_capacity_does_not_invent_recovery() {
    let body = payload(106, 4096);
    let files = par3_set("carrier", &[("payload.bin", &body)], 512, 1);
    let mut job = damaged_front_job(&files);
    let mut wrong = body.clone();
    wrong[1200] ^= 0x55;
    wrong[1700] ^= 0x55;
    let member = job.member("payload.bin", &wrong);
    bind(&mut job);
    Recovery::refused(&mut job.object, member, ByteRange::new(1024, 2048));
    job.object = super::super::facts::par3(&job.object.facts()).unwrap();
    replay(&mut job);
    bind(&mut job);
    let result = job
        .object
        .repair(&mut job.store, weaver_graph::Budget::default());
    assert!(
        matches!(
            result,
            RepairOutcome::Impossible {
                damaged: 2,
                reachable: 1
            }
        ),
        "{result:?}"
    );
    assert_eq!(job.store.members[&member], wrong);
    assert_eq!(job.store.writes, 0);
}
