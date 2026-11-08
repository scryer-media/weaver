// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::super::Tar;
use super::mixed;
use crate::testing::replay::{Step, frames, restores_alike};

#[test]
fn a_tar_restored_mid_walk_goes_on_as_the_live_one() {
    let bytes = mixed().bytes;
    let steps = frames(0, &bytes, 777);
    for cut in [1, steps.len() / 2, steps.len() - 1] {
        restores_alike(Box::new(Tar::new()), &steps, cut, &[]);
    }
}

#[test]
fn a_tar_restored_with_bytes_held_ahead_of_its_walk_places_them_alike() {
    let bytes = mixed().bytes;
    let mut steps = frames(0, &bytes, 1024);
    steps.reverse();
    steps.push(Step::Closed(0));
    restores_alike(Box::new(Tar::new()), &steps, steps.len() / 2, &[]);
}
