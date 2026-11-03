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
            bytes: &weaver_graph::Evidenced::from(piece.to_vec()),
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
    Recovery::wants(&twin.job.object, &mut seeks);
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
        let ours = live
            .job
            .object
            .repair(&mut live.job.store, weaver_graph::Budget::default());
        let theirs = again
            .job
            .object
            .repair(&mut again.job.store, weaver_graph::Budget::default());
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
        crate::restore(
            &Facts::of(Kind::Par2, &kept),
            weaver_graph::Budget::default()
        ),
        Err(weaver_graph::RestoreError::Impossible { .. })
    ));
}

// A Unicode Filename packet spelling `name` for `file` of `set`. par2-rs's
// creator writes none and has no builder for one, so it is framed here:
// header, file ID, then the name in UTF-16LE padded to four bytes.
fn unicode_filename(set: [u8; 16], file: [u8; 16], name: &str) -> Vec<u8> {
    use par2_rs::FileHashState;
    use par2_rs::packet::header::{HEADER_SIZE, MAGIC, TYPE_UNICODE_FILENAME};

    let mut body = file.to_vec();
    for unit in name.encode_utf16() {
        body.extend_from_slice(&unit.to_le_bytes());
    }
    while !body.len().is_multiple_of(4) {
        body.push(0);
    }
    let length = (HEADER_SIZE + body.len()) as u64;
    let mut packet = Vec::with_capacity(length as usize);
    packet.extend_from_slice(MAGIC);
    packet.extend_from_slice(&length.to_le_bytes());
    packet.extend_from_slice(&[0; 16]);
    packet.extend_from_slice(&set);
    packet.extend_from_slice(TYPE_UNICODE_FILENAME);
    packet.extend_from_slice(&body);
    let mut hasher = FileHashState::new();
    hasher.update(&packet[32..]);
    let hash = hasher.finalize();
    packet[16..32].copy_from_slice(&hash);
    packet
}

// A set whose File Description carries a non-ASCII name folded to ASCII,
// and whose Unicode Filename packet spells it: the set claims its member by
// the Unicode spelling, and its facts give back both packets as posted, so
// the restored set claims by the same name.
#[test]
fn a_unicode_filename_names_the_claim_and_survives_the_facts() {
    use par2_rs::Packet;

    let data = payload(34, 40_000);
    let mut files = par2_set("set", &[("_t_.bin", &data)], BLOCK, 2, 1);
    let index = par2_rs::Par2FileSet::from_files(&[files[0].1.as_slice()]).expect("an index");
    let file = *index.files.keys().next().expect("one file");
    let named = unicode_filename(
        *index.recovery_set_id.as_bytes(),
        *file.as_bytes(),
        "\u{c9}t\u{e9}.bin",
    );
    files[0].1.extend_from_slice(&named);

    let mut job = Job::new(Par2::new(OWN), OWN);
    for (name, bytes) in &files {
        job.volume(name, bytes, ARTICLE as usize, false);
    }
    let member = job.member("a8f3c1e09b", &data);
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
    assert_eq!(renamed, vec![member]);
    assert_eq!(job.namespace.get(member).unwrap().name, "\u{c9}t\u{e9}.bin");

    let restored = par2(&job.object.facts()).expect("restores");
    assert_eq!(restored.facts(), job.object.facts());
    for object in [&job.object, &restored] {
        let set = object.sets.values().next().expect("one set");
        let described = set.index.as_ref().expect("an index");
        let description = described.file_description(&file).expect("described");
        assert_eq!(description.filename, "\u{c9}t\u{e9}.bin");
        let packets = &set.packets().get().0;
        assert!(packets.iter().any(|packet| matches!(
            packet,
            Packet::FileDescription(posted) if posted.par2_name_bytes == b"_t_.bin"
        )));
        assert!(packets.iter().any(|packet| matches!(
            packet,
            Packet::UnicodeFilename(spelled) if spelled.name == "\u{c9}t\u{e9}.bin"
        )));
    }
}

// Packet facts written before the File Description kept its name bytes
// still decode, the decoded name's UTF-8 bytes standing in for them.
#[test]
fn par2_packet_facts_written_without_name_bytes_still_decode() {
    use par2_rs::Packet;

    use super::super::facts::Packets;

    // The packet facts as they were encoded before the name bytes.
    #[derive(serde::Serialize)]
    enum Before {
        File {
            file: [u8; 16],
            hash_full: [u8; 16],
            hash_16k: [u8; 16],
            length: u64,
            name: String,
            par2_name: String,
        },
    }

    let before = vec![Before::File {
        file: [1; 16],
        hash_full: [2; 16],
        hash_16k: [3; 16],
        length: 4,
        name: "a.bin".into(),
        par2_name: "a.bin".into(),
    }];
    let bytes = rmp_serde::to_vec(&before).expect("encodes");
    let Packets(packets) = rmp_serde::from_slice(&bytes).expect("decodes");
    let [Packet::FileDescription(description)] = packets.as_slice() else {
        panic!("one file description: {packets:?}");
    };
    assert_eq!(description.par2_name, "a.bin");
    assert_eq!(description.par2_name_bytes, b"a.bin");
    assert_eq!(description.file_length, 4);
}
