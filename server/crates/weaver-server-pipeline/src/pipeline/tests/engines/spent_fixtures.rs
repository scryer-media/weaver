// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Mutex;

use super::harness::Scenario;
use crate::JobId;
use crate::pipeline::Pipeline;
use crate::pipeline::completion::finalize::FIXTURE_SPENT_HOOKS;

pub fn pin_spent(scenario: Scenario, hook: impl FnOnce(&[PathBuf]) + Send + 'static) -> Scenario {
    let hook = Mutex::new(Some(hook));
    scenario.setup(move |pipeline| {
        if let Some(hook) = hook.lock().unwrap().take() {
            FIXTURE_SPENT_HOOKS
                .lock()
                .unwrap()
                .insert(pipeline.complete_dir.clone(), Box::new(hook));
        }
    })
}

pub fn owned_working_dirs(
    pipeline: &Pipeline,
    jobs: &BTreeSet<JobId>,
) -> BTreeMap<JobId, Vec<PathBuf>> {
    crate::jobs::working_dir::job_working_dirs(&pipeline.intermediate_dir, jobs).unwrap()
}
