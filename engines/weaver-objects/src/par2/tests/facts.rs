// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{ByteRange, Delivery, Kind, MemberId, Object, Provenance, Recovery, Seek};
use weaver_graph::{Facts, VolumeIndex};

use super::super::Par2;
use super::super::facts::{Par2Facts, par2};
use super::{ARTICLE, BLOCK, OWN, damaged_member, feed, verdicts};
use crate::testing::{Job, par2_set, payload};

#[derive(Clone, Copy)]
enum Act {
    Deliver(usize, u64, u64),
    Close(usize),
    Bind,
    Feed,
}

struct Twin {
    job: Job<Par2>,
    volumes: Vec<(VolumeIndex, Vec<u8>)>,
    member: MemberId,
    bad: ByteRange,
}

// One damaged member and its set's recovery files, announced and claimed;
// nothing of the recovery files delivered yet.
fn twin(files: &[(String, Vec<u8>)], data: &[u8]) -> Twin {
    let mut job = Job::new(Par2::new(OWN), OWN);
    let (member, bad) = damaged_member(&mut job, "payload.bin", data, 2100);
    let mut volumes = Vec::new();
    for (name, bytes) in files {
        let file = job.namespace.announce(name, Some(bytes.len() as u64));
        job.arrive(file, 0, bytes, Provenance::Fresh);
        job.namespace.bind(file, OWN);
        let volume = job.name(name, bytes);
        volumes.push((volume, bytes.clone()));
    }
    Twin {
        job,
        volumes,
        member,
        bad,
    }
}

fn deliver(object: &mut Par2, twin_volumes: &[(VolumeIndex, Vec<u8>)], act: Act) {
    let Act::Deliver(index, offset, len) = act else {
        return;
    };
    let (volume, bytes) = &twin_volumes[index];
    let piece = &bytes[offset as usize..(offset + len) as usize];
    object.deliver(
        Delivery {
            volume: *volume,
            offset,
            bytes: piece,
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
}

// What the act leaves the object saying: its verdicts on the member and
// the recovery it asks for.
fn act(twin: &mut Twin, act: Act) -> (Vec<weaver_graph::SliceVerdict>, Vec<Seek>) {
    match act {
        Act::Deliver(..) => deliver(&mut twin.job.object, &twin.volumes, act),
        Act::Close(index) => twin
            .job
            .object
            .volume_closed(twin.volumes[index].0, &mut Vec::new()),
        Act::Bind => {
            twin.job
                .object
                .bind(&mut twin.job.namespace, &mut Vec::new());
        }
        Act::Feed => {
            let wrong = twin.job.store.members[&twin.member].clone();
            feed(&mut twin.job, twin.member, &wrong, Some(twin.bad));
        }
    }
    let mut seeks = Vec::new();
    Recovery::wants(&mut twin.job.object, &mut seeks);
    (verdicts(&twin.job, twin.member), seeks)
}

fn acts(files: &[(String, Vec<u8>)]) -> Vec<Act> {
    let mut acts = Vec::new();
    for (index, (_, bytes)) in files.iter().enumerate() {
        let len = bytes.len() as u64;
        for offset in (0..len).step_by(ARTICLE as usize) {
            acts.push(Act::Deliver(index, offset, ARTICLE.min(len - offset)));
        }
        acts.push(Act::Close(index));
    }
    acts.extend([Act::Bind, Act::Feed]);
    acts
}

// Restored after the first header, mid-packet, at a volume boundary, after
// the bind and after the feed, the object answers, asks and repairs as the
// live one does, from facts that replay only the volumes' bytes.
#[test]
fn a_par2_object_restored_at_every_point_repairs_as_the_live_one() {
    let data = payload(31, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 3, 2);
    assert!(files.len() >= 2);
    let acts = acts(&files);
    let close = acts
        .iter()
        .position(|act| matches!(act, Act::Close(0)))
        .expect("closes");
    let bind = acts.len() - 2;
    let cuts = [
        1,
        close / 2,
        close + 1,
        close + (acts.len() - close) / 2,
        bind + 1,
        acts.len(),
    ];
    for cut in cuts {
        let (mut live, mut again) = (twin(&files, &data), twin(&files, &data));
        for step in &acts[..cut] {
            act(&mut live, *step);
            act(&mut again, *step);
        }
        let facts = live.job.object.facts();
        let restored = par2(&facts).expect("restores");
        again.job.object = restored;
        for step in &acts[..cut] {
            deliver(&mut again.job.object, &again.volumes, *step);
        }
        for step in &acts[cut..] {
            assert_eq!(act(&mut live, *step), act(&mut again, *step), "cut {cut}");
        }
        let ours = live.job.object.repair(&mut live.job.store);
        let theirs = again.job.object.repair(&mut again.job.store);
        assert_eq!(ours, theirs, "cut {cut}");
        assert!(again.job.store.members[&again.member] == data, "cut {cut}");
        assert_eq!(
            live.job.object.facts(),
            again.job.object.facts(),
            "cut {cut}"
        );
    }
}

// After a bind the facts name the members the set binds, by file.
#[test]
fn par2_facts_after_a_bind_record_what_it_binds() {
    let data = payload(32, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 3, 2);
    let mut live = twin(&files, &data);
    for step in acts(&files) {
        act(&mut live, step);
    }
    let kept: Par2Facts = live.job.object.facts().read(Kind::Par2).expect("decodes");
    let bound: Vec<_> = kept
        .sets
        .iter()
        .flat_map(|set| set.members.iter().map(|(_, member)| *member))
        .collect();
    assert_eq!(bound, vec![live.member]);
}

#[test]
fn par2_facts_naming_a_packet_in_no_volume_are_refused() {
    let data = payload(33, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 3, 2);
    let mut live = twin(&files, &data);
    for step in acts(&files) {
        act(&mut live, step);
    }
    let mut kept: Par2Facts = live.job.object.facts().read(Kind::Par2).expect("decodes");
    kept.volumes.truncate(1);
    assert!(matches!(
        crate::restore(&Facts::of(Kind::Par2, &kept)),
        Err(weaver_graph::RestoreError::Impossible { .. })
    ));
}
