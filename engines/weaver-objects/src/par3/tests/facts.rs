// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{Delivery, Facts, Kind, MemberId, Object, Provenance, Recovery, VolumeIndex};

use super::super::Par3;
use super::super::facts::{Par3Facts, par3};
use super::{BLOCK, OWN, name_of};
use crate::testing::{Job, par3_set, payload};

const ARTICLE: u64 = 700;

#[derive(Clone, Copy)]
enum Act {
    Deliver(usize, u64, u64),
    Close(usize),
    Bind,
    Repair,
}

// What an act came to, so the live object and the restored one compare.
#[derive(Debug, PartialEq, Eq)]
enum Said {
    Nothing,
    Renamed(Vec<MemberId>),
    Repaired(RepairOutcome),
}

struct Twin {
    job: Job<Par3>,
    volumes: Vec<(VolumeIndex, Vec<u8>)>,
    hidden: MemberId,
}

// A damaged member under a hex name and a named one beside it, with the
// set's files announced and claimed; nothing of them delivered yet.
fn twin(files: &[(String, Vec<u8>)], one: &[u8], two: &[u8]) -> Twin {
    let mut job = Job::new(Par3::new(OWN), OWN);
    let mut wrong = one.to_vec();
    wrong[5000] ^= 0x42;
    let hidden = job.member("5b84d63f9a0c71e2", &wrong);
    job.arrive(hidden, 4900, &wrong[4900..5100], Provenance::Damaged);
    job.member("two.bin", two);
    let mut volumes = Vec::new();
    for (name, bytes) in files {
        let file = job.namespace.announce(name, Some(bytes.len() as u64));
        job.arrive(file, 0, bytes, Provenance::Fresh);
        job.namespace.bind(file, OWN);
        volumes.push((job.name(name, bytes), bytes.clone()));
    }
    Twin {
        job,
        volumes,
        hidden,
    }
}

fn deliver(object: &mut Par3, volumes: &[(VolumeIndex, Vec<u8>)], act: Act) {
    let Act::Deliver(index, offset, len) = act else {
        return;
    };
    let (volume, bytes) = &volumes[index];
    object.deliver(
        Delivery {
            volume: *volume,
            offset,
            bytes: &bytes[offset as usize..(offset + len) as usize],
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
}

fn act(twin: &mut Twin, act: Act) -> Said {
    match act {
        Act::Deliver(..) => {
            deliver(&mut twin.job.object, &twin.volumes, act);
            Said::Nothing
        }
        Act::Close(index) => {
            twin.job
                .object
                .volume_closed(twin.volumes[index].0, &mut Vec::new());
            Said::Nothing
        }
        Act::Bind => {
            let mut renamed = Vec::new();
            twin.job.object.bind(&mut twin.job.namespace, &mut renamed);
            Said::Renamed(renamed)
        }
        Act::Repair => Said::Repaired(twin.job.object.repair(&mut twin.job.store)),
    }
}

fn acts(files: &[(String, Vec<u8>)]) -> Vec<Act> {
    let mut acts = Vec::new();
    for (index, (_, bytes)) in files.iter().enumerate() {
        let len = bytes.len() as u64;
        for offset in (0..len).step_by(ARTICLE as usize) {
            acts.push(Act::Deliver(index, offset, ARTICLE.min(len - offset)));
        }
        acts.push(Act::Close(index));
        acts.push(Act::Bind);
    }
    acts.extend([Act::Repair, Act::Bind, Act::Repair]);
    acts
}

// Restored mid-volume, between volumes, after the content match and after
// the repair, from facts and a replay of only the volumes' bytes, the
// object binds, renames and repairs as the live one does. A match made
// before the checkpoint renames on the next bind without a second readback.
#[test]
fn a_par3_object_restored_at_every_point_repairs_as_the_live_one() {
    let one = payload(21, 9000);
    let two = payload(22, 4000);
    let files = par3_set("set", &[("one.bin", &one), ("two.bin", &two)], BLOCK, 3);
    assert!(files.len() >= 2);
    let acts = acts(&files);
    let first_close = acts
        .iter()
        .position(|act| matches!(act, Act::Close(0)))
        .expect("closes");
    let repair = acts.len() - 3;
    let cuts = [
        1,
        first_close / 2,
        first_close + 2,
        repair - 1,
        repair + 1,
        repair + 2,
        acts.len(),
    ];
    for cut in cuts {
        let (mut live, mut again) = (twin(&files, &one, &two), twin(&files, &one, &two));
        for step in &acts[..cut] {
            act(&mut live, *step);
            act(&mut again, *step);
        }
        again.job.object = par3(&live.job.object.facts()).expect("restores");
        for step in &acts[..cut] {
            deliver(&mut again.job.object, &again.volumes, *step);
        }
        for step in &acts[cut..] {
            let reads = (live.job.store.reads, again.job.store.reads);
            assert_eq!(act(&mut live, *step), act(&mut again, *step), "cut {cut}");
            assert_eq!(
                live.job.store.reads - reads.0,
                again.job.store.reads - reads.1,
                "cut {cut}: the restored object reads members back as the live one does"
            );
        }
        // The repair that rebuilt the member stands: the bind after it
        // only renamed what it had already bound.
        assert_eq!(
            again.job.object.outcome(),
            Some(RepairOutcome::Repaired { slices: 1 }),
            "cut {cut}"
        );
        assert!(again.job.store.members[&again.hidden] == one, "cut {cut}");
        assert_eq!(name_of(&again.job, again.hidden), "one.bin", "cut {cut}");
        assert_eq!(
            live.job.object.facts(),
            again.job.object.facts(),
            "cut {cut}"
        );
    }
}

// After the readback the facts keep the content match by described path.
#[test]
fn par3_facts_after_a_readback_keep_what_it_identified() {
    let one = payload(23, 9000);
    let two = payload(24, 4000);
    let files = par3_set("set", &[("one.bin", &one), ("two.bin", &two)], BLOCK, 3);
    let mut live = twin(&files, &one, &two);
    let acts = acts(&files);
    for step in &acts[..acts.len() - 2] {
        act(&mut live, *step);
    }
    let kept: Par3Facts = live.job.object.facts().read(Kind::Par3).expect("decodes");
    assert_eq!(
        kept.identified.get(&live.hidden).map(String::as_str),
        Some("one.bin")
    );
    assert_eq!(kept.bound.get("one.bin"), Some(&live.hidden));
}

#[test]
fn par3_facts_identifying_a_member_it_could_not_are_refused() {
    let one = payload(25, 9000);
    let two = payload(26, 4000);
    let files = par3_set("set", &[("one.bin", &one), ("two.bin", &two)], BLOCK, 3);
    let mut live = twin(&files, &one, &two);
    for step in acts(&files) {
        act(&mut live, step);
    }
    let mut kept: Par3Facts = live.job.object.facts().read(Kind::Par3).expect("decodes");
    kept.identified.insert(MemberId(9999), "one.bin".to_owned());
    assert!(matches!(
        crate::restore(&Facts::of(Kind::Par3, &kept)),
        Err(weaver_graph::RestoreError::Impossible { .. })
    ));
}
