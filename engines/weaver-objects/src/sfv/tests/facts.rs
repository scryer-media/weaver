// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{Kind, Object, Recovery, SliceVerdict};

use super::super::Sfv;
use super::{OWN, bind, job, sfv_text};
use crate::grid::crc32;
use crate::testing::replay::{Step, frames, restores_alike};
use crate::testing::{payload, segments};

#[test]
fn an_sfv_restored_mid_text_reads_the_rest_as_the_live_one() {
    let text = sfv_text(&[("one.bin", 0x1234_5678), ("two.bin", 0x9abc_def0)]);
    let steps: Vec<Step<'_>> = frames(0, &text, 9);
    restores_alike(Box::new(Sfv::new(OWN)), &steps, steps.len() / 2, &[]);
}

// Bound and half fed, restored: the restored one covers the same members
// and, fed the other half, comes to the same verdicts.
#[test]
fn an_sfv_restored_with_a_member_half_fed_checks_it_alike() {
    let one = payload(1, 5000);
    let mut job = job(&sfv_text(&[("one.bin", crc32(&one))]));
    let member = job.member("one.bin", &one);
    bind(&mut job);
    let pieces = segments(0, &one, 700, 700, true);
    let (first, rest) = pieces.split_at(pieces.len() / 2);
    job.object.feed(member, first);
    let facts = job.object.facts();
    let mut restored: Sfv = facts.read(Kind::Sfv).expect("decodes");
    assert_eq!(restored.covers(member), job.object.covers(member));
    job.object.feed(member, rest);
    restored.feed(member, rest);
    let (mut ours, mut theirs) = (Vec::new(), Vec::new());
    job.object.verdicts(member, &mut ours);
    restored.verdicts(member, &mut theirs);
    assert_eq!(
        (ours.as_slice(), theirs),
        (
            [SliceVerdict::Intact].as_slice(),
            vec![SliceVerdict::Intact]
        )
    );
    assert_eq!(restored.facts(), job.object.facts());
}
