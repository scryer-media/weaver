// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::Path;

use crate::Database;
use crate::post_processing::instances::{
    InstanceTrigger, PostProcessingInstancesQueries, ScriptInstanceDraft,
};
use crate::post_processing::model::{PostProcessingSettings, QueueEvent, ScriptName};
use crate::post_processing::settings::PostProcessingSettingsQueries;

pub fn queue_instance(event: QueueEvent) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(
        ScriptName::new("queue.sh").unwrap(),
        InstanceTrigger::Queue(event),
    )
}

pub fn enable_queue_script(db: &Database, root: &Path) {
    let scripts = db
        .initialize_post_processing_script_directory(root, None)
        .unwrap();
    std::fs::write(
        scripts.join("queue.sh"),
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nexit 0\n",
    )
    .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..Default::default()
    })
    .unwrap();
    // One instance for each thing that can happen to a download.
    for event in QueueEvent::ALL {
        db.create_script_instance(queue_instance(event)).unwrap();
    }
}
