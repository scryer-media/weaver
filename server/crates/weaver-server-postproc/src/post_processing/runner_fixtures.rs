// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::model::{
    NzbgetCompatibilityName, PipelineFailureStage, PipelineOutcome, ScriptAdapter, ScriptManifest,
};
use super::runner::{
    CompatibilityFacts, InterpreterConfig, JobExecutionContext, RunIdentity,
    ScriptExecutionRequest, adapter_contract,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

fn manifest(adapter: ScriptAdapter) -> ScriptManifest {
    let compatibility_name = match adapter {
        ScriptAdapter::Nzbget => Some(NzbgetCompatibilityName::new("email").unwrap()),
        ScriptAdapter::Sabnzbd => None,
    };
    ScriptManifest::new(
        adapter,
        compatibility_name,
        "Runner".into(),
        Some("1.0.0".into()),
        "run.sh".into(),
        vec![],
        vec![],
    )
    .unwrap()
}

pub(super) fn request(adapter: ScriptAdapter) -> ScriptExecutionRequest {
    ScriptExecutionRequest {
        manifest: manifest(adapter),
        root: PathBuf::from("/scripts"),
        options: vec![],
        context: JobExecutionContext {
            job_id: 42,
            name: "Example Job".into(),
            nzb_filename: "example.nzb".into(),
            category: Some("movies".into()),
            group: Some("alt.binaries.test".into()),
            source_url: Some("https://example.invalid/failure".into()),
            working_directory: PathBuf::from("/work/job"),
            final_directory: PathBuf::from("/complete/job"),
            pipeline_outcome: PipelineOutcome::Failed {
                stage: PipelineFailureStage::Extract,
                code: "extract_failed".into(),
                message: "archive failed".into(),
            },
            par_status: 2,
            unpack_status: 2,
            compatibility: CompatibilityFacts::default(),
        },
        identity: RunIdentity {
            run_id: "run-1".into(),
            instance_id: "instance-1".into(),
            instance_name: "Example instance".into(),
            trigger: "post_processing".into(),
            api_url: None,
            token: None,
            output: Default::default(),
        },
        timeout: Some(Duration::from_secs(60)),
        termination_grace: Duration::from_secs(10),
        interpreters: InterpreterConfig::default(),
        supervisor_executable: None,
    }
}

pub(super) fn contract(
    request: &ScriptExecutionRequest,
) -> (Vec<String>, BTreeMap<String, String>) {
    adapter_contract(request).unwrap()
}

// The variables that tell a script how its job's download ended, across
// both adapters.
#[cfg(all(unix, feature = "test-support"))]
const STATUS_ENV: [&str; 9] = [
    "NZBPP_STATUS",
    "NZBPP_TOTALSTATUS",
    "NZBPP_PARSTATUS",
    "NZBPP_UNPACKSTATUS",
    "WEAVER_STATUS",
    "SAB_PP_STATUS",
    "SAB_FAIL_MSG",
    "SAB_REPAIR",
    "SAB_UNPACK",
];

// What a script of either adapter handed `context` is told about how the
// job's download ended.
#[cfg(all(unix, feature = "test-support"))]
pub fn status_env(context: &JobExecutionContext) -> BTreeMap<String, String> {
    let mut status = BTreeMap::new();
    for adapter in [ScriptAdapter::Nzbget, ScriptAdapter::Sabnzbd] {
        let mut request = request(adapter);
        request.context = context.clone();
        let (_, env) = contract(&request);
        status.extend(
            env.into_iter()
                .filter(|(name, _)| STATUS_ENV.contains(&name.as_str())),
        );
    }
    status
}
