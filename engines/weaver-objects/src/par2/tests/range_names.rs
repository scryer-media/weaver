// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The set's volumes renamed from `<start>+<count>` to `<start>-<end>`, the
// first block and the one past the last, numbers three digits wide, with
// the blocks each carries.
fn range_named(files: &[(String, Vec<u8>)]) -> Vec<(String, Vec<u8>, u64)> {
    files
        .iter()
        .map(|(name, bytes)| {
            let (stem, tail) = name.rsplit_once(".vol").expect("a volume name");
            let (start, count) = tail
                .strip_suffix(".par2")
                .and_then(|tail| tail.split_once('+'))
                .expect("a counted volume name");
            let (start, count): (u64, u64) = (start.parse().unwrap(), count.parse().unwrap());
            let renamed = format!("{stem}.vol{start:03}-{:03}.par2", start + count);
            (renamed, bytes.clone(), count)
        })
        .collect()
}

// A volume claimed by its name alone, before any byte of it arrived.
fn claimed_by_name(job: &mut Job<Par2>, name: &str, bytes: &[u8]) -> VolumeIndex {
    let claimed = job.object.claim(name, &[], None);
    let [volume] = claimed[..] else {
        panic!("{name} is claimed by its name: {claimed:?}");
    };
    job.object.bound(volume, name, &[], None);
    job.store.volumes.insert(volume, bytes.to_vec());
    volume
}

// A damaged post whose volumes are named by their block ranges, none of
// them arrived: each counts the blocks its name says, the set waits on
// them rather than ruling the repair impossible, and only the volumes
// with the fewest blocks that cover the damage are asked for.
#[test]
fn range_named_volumes_count_their_blocks_and_the_fewest_cover_the_damage() {
    let data = payload(61, 5000);
    let files = par2_set("fixture.release", &[("payload.bin", &data)], BLOCK, 8, 3);
    let volumes = range_named(&files[1..]);
    assert!(
        volumes.len() >= 3,
        "{:?}",
        volumes.iter().map(|v| &v.0).collect::<Vec<_>>()
    );
    let mut job = job(&files[..1]);
    let claimed: Vec<(VolumeIndex, u64)> = volumes
        .iter()
        .map(|(name, bytes, count)| (claimed_by_name(&mut job, name, bytes), *count))
        .collect();

    let mut wrong = data.clone();
    for at in [100, 1100, 2100] {
        wrong[at] ^= 0x33;
    }
    let bad = ByteRange::new(0, 2800);
    let member = job.member("payload.bin", &wrong);
    job.arrive(member, 0, &wrong[..2800], Provenance::Damaged);
    bind(&mut job);
    feed(&mut job, member, &wrong, Some(bad));

    let packet = job.object.smallest_packet();
    for (volume, count) in &claimed {
        let held = &job.object.volumes[volume.0 as usize];
        assert_eq!(held.named_blocks, Some(*count));
        assert_eq!(held.left(), *count);
        assert_eq!(held.coming(packet), *count);
    }
    let total: u64 = claimed.iter().map(|(_, count)| count).sum();
    assert_eq!(reachable(&job.object), total);

    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );

    // Three damaged slices: the volumes with the fewest blocks, until
    // their blocks reach three.
    let mut by_blocks = claimed.clone();
    by_blocks.sort_by_key(|(volume, count)| (*count, *volume));
    let mut expected = Vec::new();
    let mut covered = 0;
    for (volume, count) in by_blocks {
        if covered >= 3 {
            break;
        }
        covered += count;
        expected.push(volume);
    }
    let mut seeks = Vec::new();
    Recovery::wants(&job.object, &mut seeks);
    let mut now: Vec<VolumeIndex> = seeks
        .iter()
        .filter(|seek| seek.when == weaver_graph::When::Now)
        .map(|seek| seek.volume)
        .collect();
    now.dedup();
    expected.sort();
    now.sort();
    assert_eq!(now, expected);
    assert!(
        now.len() < claimed.len(),
        "a volume the damage does not need is left"
    );
}
