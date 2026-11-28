// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{Kind, Object};

use super::super::Sfv;
use super::{OWN, bind, job, sfv_text};
use crate::grid::crc32;
use crate::testing::payload;
use crate::testing::replay::{Step, frames, restores_alike};

#[test]
fn an_sfv_restored_mid_text_reads_the_rest_as_the_live_one() {
    let text = sfv_text(&[("one.bin", 0x1234_5678), ("two.bin", 0x9abc_def0)]);
    let steps: Vec<Step<'_>> = frames(0, &text, 9);
    restores_alike(Box::new(Sfv::new(OWN)), &steps, steps.len() / 2, &[]);
}

// Facts written when a listing still ruled are not read: the run they came
// from may have marked a member damaged, and only a fresh start takes that
// back.
#[test]
fn an_sfv_written_at_version_three_is_refused() {
    let one = payload(3, 2000);
    let mut job = job(&sfv_text(&[("one.bin", crc32(&one))]));
    job.member("one.bin", &one);
    bind(&mut job);
    let mut facts = job.object.facts();
    facts.version = 3;
    assert!(matches!(
        facts.read::<Sfv>(Kind::Sfv),
        Err(weaver_graph::RestoreError::Version {
            kind: Kind::Sfv,
            version: 3
        })
    ));
}
