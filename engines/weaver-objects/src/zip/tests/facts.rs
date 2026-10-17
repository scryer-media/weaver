// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::super::Zip;
use super::{archive, spanned};
use crate::testing::replay::{Step, frames, restores_alike};

#[test]
fn a_zip_restored_before_its_directory_arrives_goes_on_as_the_live_one() {
    let bytes = archive();
    let mut steps = vec![
        Step::Bound(0, "alpha.zip"),
        Step::Sized(0, bytes.len() as u64),
    ];
    steps.extend(frames(0, &bytes, 4096));
    steps.push(Step::Closed(0));
    for cut in [2, steps.len() / 2, steps.len() - 1] {
        restores_alike(Box::new(Zip::new()), &steps, cut, &[]);
    }
}

#[test]
fn a_spanned_zip_restored_between_its_volumes_goes_on_as_the_live_one() {
    let volumes = spanned(&archive(), 3);
    let mut steps = Vec::new();
    for (index, (name, bytes)) in volumes.iter().enumerate() {
        steps.push(Step::Bound(index as u32, name));
        steps.push(Step::Sized(index as u32, bytes.len() as u64));
    }
    for index in [2, 0, 1] {
        steps.extend(frames(index as u32, &volumes[index].1, 4096));
    }
    for index in [2, 0, 1] {
        steps.push(Step::Closed(index));
    }
    restores_alike(Box::new(Zip::new()), &steps, steps.len() / 2, &[]);
}
