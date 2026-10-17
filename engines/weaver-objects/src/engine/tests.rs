// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{Object, Seek, VolumeIndex};

use crate::rar_engine::RarEngine;
use crate::sevenz_engine::SevenZipEngine;
use crate::testing::replay::{Step, contains, restores_alike};
use crate::zip_engine::ZipEngine;

const PASSWORD: &str = "a-test-passphrase";

// Two volumes bound, the run past the first, a password offered: restored,
// the engine claims and bids alike, and its facts hold no password.
fn restored(engine: Box<dyn Object>, names: [&str; 3]) {
    let steps = [
        Step::Bound(0, names[0]),
        Step::Bound(2, names[2]),
        Step::Password(PASSWORD),
    ];
    let (facts, mut live, mut again) = restores_alike(engine, &steps, 3, &[]);
    assert!(!contains(&facts.bytes, PASSWORD.as_bytes()));
    for object in [&mut live, &mut again] {
        if let Some(engine) = object.engine_mut() {
            engine.reached(VolumeIndex(1), 0);
        }
    }
    let claim = |object: &mut Box<dyn Object>| object.claim(names[1], &[]);
    assert_eq!(claim(&mut live), claim(&mut again));
    assert_eq!(claim(&mut live), Some(VolumeIndex(1)));
    let wants = |object: &mut Box<dyn Object>| {
        let mut out: Vec<Seek> = Vec::new();
        object.wants(&mut out);
        out
    };
    assert_eq!(wants(&mut live), wants(&mut again));
}

#[test]
fn every_engine_restored_from_its_facts_claims_and_bids_alike() {
    restored(
        Box::new(RarEngine::new()),
        ["alpha.part1.rar", "alpha.part2.rar", "alpha.part3.rar"],
    );
    restored(
        Box::new(SevenZipEngine::new()),
        ["alpha.7z.001", "alpha.7z.002", "alpha.7z.003"],
    );
    restored(
        Box::new(ZipEngine::new()),
        ["alpha.z01", "alpha.z02", "alpha.zip"],
    );
}
