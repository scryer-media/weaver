// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::super::instances::InstanceInput;
use super::super::model::OptionName;
use super::super::secrets::SecretRef;
use super::*;
use crate::post_processing::instances::PostProcessingInstancesInternalQueries;
use crate::post_processing::instances::PostProcessingInstancesQueries;
use crate::post_processing::secrets::PostProcessingSecretsQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;

fn declared(name: &str, value: Option<&str>, secret: bool) -> PresetInput {
    PresetInput {
        name: name.into(),
        value: value.unwrap_or_default().into(),
        secret,
    }
}

// A saved input; a secret one is linked to the secret `value` names.
fn saved(name: &str, value: &str, secret: bool) -> InstanceInput {
    InstanceInput {
        name: OptionName::new(name).unwrap(),
        value: if secret { String::new() } else { value.into() },
        secret: secret.then(|| SecretRef {
            id: value.into(),
            name: value.into(),
        }),
        sealed: false,
    }
}

// A saved input that is a secret of the instance's own.
fn sealed(name: &str) -> InstanceInput {
    InstanceInput {
        name: OptionName::new(name).unwrap(),
        value: String::new(),
        secret: None,
        sealed: true,
    }
}

fn instance(inputs: Vec<InstanceInput>) -> ScriptInstance {
    ScriptInstance {
        id: "one".into(),
        name: "one".into(),
        script: ScriptName::new("notify.sh").unwrap(),
        trigger: InstanceTrigger::Scan,
        inputs,
        categories: Vec::new(),
        enabled: true,
        blocking: true,
        timeout_seconds: None,
        run_order: 0,
        schedule: InstanceSchedule::default(),
    }
}

fn preset(inputs: Vec<PresetInput>) -> ScriptPreset {
    ScriptPreset {
        triggers: vec![InstanceTrigger::PostProcessing],
        task_times: Vec::new(),
        inputs,
    }
}

#[test]
fn a_changed_value_or_a_secret_not_yet_given_is_not_drift() {
    let header = preset(vec![
        declared("Server", Some("localhost"), false),
        declared("Token", None, true),
    ]);
    assert!(!header.drifted_from(&instance(vec![saved("server", "example.test", false)])));
    assert!(!header.drifted_from(&instance(vec![
        saved("Server", "example.test", false),
        saved("Token", "token-secret", true),
    ])));
}

#[test]
fn a_missing_extra_or_reclassified_input_is_drift() {
    let header = preset(vec![declared("Server", Some("localhost"), false)]);
    assert!(header.drifted_from(&instance(Vec::new())));
    assert!(header.drifted_from(&instance(vec![
        saved("Server", "localhost", false),
        saved("Port", "80", false),
    ])));
    assert!(header.drifted_from(&instance(vec![saved("Server", "", true)])));
    // A trigger the header does not name is a choice the operator made.
    assert!(!header.drifted_from(&instance(vec![saved("Server", "localhost", false)])));
}

#[test]
fn reapplying_keeps_saved_values_and_drops_what_the_header_dropped() {
    let header = preset(vec![
        declared("Server", Some("localhost"), false),
        declared("Port", Some("80"), false),
        declared("Token", None, true),
        declared("Mode", Some("fast"), false),
        declared("Password", None, true),
        declared("ApiKey", None, true),
    ]);
    let before = instance(vec![
        saved("server", "example.test", false),
        saved("Token", "token-secret", true),
        saved("Mode", "mode-secret", true),
        saved("ApiKey", "typed in clear", false),
        saved("Gone", "x", false),
    ]);
    let inputs = header.reapplied_to(&before).inputs;
    assert_eq!(
        inputs,
        [
            InstanceInputDraft::plain("Server", "example.test"),
            InstanceInputDraft::plain("Port", "80"),
            // The link is kept as it was.
            InstanceInputDraft::secret("Token", "token-secret"),
            // Was a secret, is plain now: starts again from the header,
            // and the secret's value is never what fills it.
            InstanceInputDraft::plain("Mode", "fast"),
            // A secret slot with no link yet is left for the operator to
            // link; one whose saved value was plain keeps that value.
            InstanceInputDraft::plain("ApiKey", "typed in clear"),
        ]
    );
}

#[test]
fn reapplying_keeps_a_plain_value_the_header_now_calls_secret_and_still_shows_drift() {
    let header = preset(vec![declared("ApiKey", None, true)]);
    let before = instance(vec![saved("apikey", "typed in clear", false)]);
    let inputs = header.reapplied_to(&before).inputs;
    assert_eq!(
        inputs,
        [InstanceInputDraft::plain("ApiKey", "typed in clear")]
    );

    // Saved back, the input is still plain where the header wants a
    // secret, so the drift stays visible until the operator links one.
    let after = instance(vec![saved("ApiKey", "typed in clear", false)]);
    assert!(header.drifted_from(&after));

    // A linked secret is never turned into a plain value, whichever way
    // the header classifies the option.
    let linked = instance(vec![saved("ApiKey", "stored", true)]);
    assert_eq!(
        header.reapplied_to(&linked).inputs,
        [InstanceInputDraft::secret("ApiKey", "stored")]
    );
    let now_plain = preset(vec![declared("ApiKey", Some("default"), false)]);
    assert_eq!(
        now_plain.reapplied_to(&linked).inputs,
        [InstanceInputDraft::plain("ApiKey", "default")]
    );
}

#[test]
fn a_secret_of_the_instances_own_drifts_exactly_as_a_linked_one_does() {
    let header = preset(vec![
        declared("Server", Some("localhost"), false),
        declared("Token", None, true),
    ]);
    // On a secret slot it is what the header asks for.
    assert!(!header.drifted_from(&instance(vec![
        saved("Server", "example.test", false),
        sealed("token"),
    ])));
    // On an input the header calls plain, the two no longer line up.
    assert!(header.drifted_from(&instance(vec![sealed("Server")])));
    assert_eq!(
        header.drifted_from(&instance(vec![sealed("Server")])),
        header.drifted_from(&instance(vec![saved("Server", "linked", true)])),
    );
    // Under a name the header does not declare it is one more extra.
    assert!(header.drifted_from(&instance(vec![
        saved("Server", "example.test", false),
        sealed("Extra"),
    ])));
}

#[test]
fn reapplying_keeps_a_secret_of_the_instances_own_under_a_declared_name_only() {
    let header = preset(vec![
        declared("Server", Some("localhost"), false),
        declared("Token", None, true),
        declared("Mode", Some("fast"), false),
    ]);
    let before = instance(vec![sealed("token"), sealed("MODE"), sealed("Gone")]);
    let inputs = header.reapplied_to(&before).inputs;
    assert_eq!(
        inputs,
        [
            InstanceInputDraft::plain("Server", "localhost"),
            // Kept as it is stored: no value is carried, so none is needed.
            InstanceInputDraft::kept_sealed("Token"),
            // Kept under a plain slot too, where the drift stays visible.
            InstanceInputDraft::kept_sealed("Mode"),
        ]
    );
    assert!(
        inputs
            .iter()
            .all(|input| !input.sealed || input.value.is_none())
    );
    let after = instance(vec![
        saved("Server", "localhost", false),
        sealed("Token"),
        sealed("Mode"),
    ]);
    assert!(header.drifted_from(&after));

    // A new instance of the same script is never handed a sibling's own
    // secret: only links are shared.
    let links = secret_links_of([&before]);
    assert!(links.is_empty());
    assert_eq!(
        header
            .draft(
                &ScriptName::new("notify.sh").unwrap(),
                InstanceTrigger::PostProcessing,
                &links,
            )
            .inputs,
        [
            InstanceInputDraft::plain("Server", "localhost"),
            InstanceInputDraft::plain("Mode", "fast"),
        ]
    );
}

#[test]
fn a_new_instance_starts_linked_to_the_secrets_its_siblings_use() {
    let header = preset(vec![
        declared("Server", Some("localhost"), false),
        declared("Token", None, true),
        declared("Password", None, true),
    ]);
    let sibling = instance(vec![saved("token", "shared", true)]);
    let links = secret_links_of([&sibling]);
    let draft = header.draft(
        &ScriptName::new("notify.sh").unwrap(),
        InstanceTrigger::PostProcessing,
        &links,
    );
    assert_eq!(
        draft.inputs,
        [
            InstanceInputDraft::plain("Server", "localhost"),
            InstanceInputDraft::secret("Token", "shared"),
        ]
    );
}

#[test]
fn header_setup_and_reapply_keep_a_secret_link() {
    let db = Database::open_in_memory().unwrap();
    let root = tempfile::tempdir().unwrap();
    let scripts =
        crate::post_processing::settings::normalize_script_directory(&root.path().join("scripts"))
            .unwrap();
    std::fs::create_dir_all(&scripts).unwrap();
    std::fs::write(
        scripts.join("notify.sh"),
        "#!/bin/sh\n\
         ### NZBGET POST-PROCESSING/QUEUE SCRIPT ###\n\
         ### QUEUE EVENTS: NZB_ADDED ###\n\
         ### OPTIONS ###\n\
         # Where to send it.\n\
         #Server=localhost\n\
         # The account's API token.\n\
         #ApiToken=do-not-use\n\
         ### NZBGET POST-PROCESSING/QUEUE SCRIPT ###\n",
    )
    .unwrap();
    db.replace_post_processing_script_directory(&scripts)
        .unwrap();
    let script = ScriptName::new("notify.sh").unwrap();

    let token = db.create_secret("Notify token", "first").unwrap();
    let first = db
        .create_script_instance(
            ScriptInstanceDraft::new(script.clone(), InstanceTrigger::PostProcessing)
                .input("Server", "example.test")
                .secret_input("ApiToken", &token.id),
        )
        .unwrap();

    // The queue instance is new, and starts linked to the same secret.
    let setup = db.set_up_script_from_header(&script).unwrap();
    assert_eq!(setup.instances.len(), 1);
    let added = &setup.instances[0];
    assert_eq!(
        added.inputs[1]
            .secret
            .as_ref()
            .map(|secret| secret.id.as_str()),
        Some(token.id.as_str())
    );
    // The header's default for the token is never offered.
    assert_eq!(added.inputs[0].value, "localhost");

    let reapplied = db.reapply_script_header(&first.id).unwrap();
    assert_eq!(reapplied.inputs, first.inputs);
}

#[test]
fn reapplying_a_header_carries_a_secret_of_the_instances_own_across_sealed() {
    use super::super::model::OptionValue;

    let db = Database::open_in_memory().unwrap();
    let root = tempfile::tempdir().unwrap();
    let scripts =
        crate::post_processing::settings::normalize_script_directory(&root.path().join("scripts"))
            .unwrap();
    std::fs::create_dir_all(&scripts).unwrap();
    std::fs::write(
        scripts.join("notify.sh"),
        "#!/bin/sh\n\
         ### NZBGET POST-PROCESSING SCRIPT ###\n\
         ### OPTIONS ###\n\
         # Where to send it.\n\
         #Server=localhost\n\
         # The account's API token.\n\
         #ApiToken=do-not-use\n\
         ### NZBGET POST-PROCESSING SCRIPT ###\n",
    )
    .unwrap();
    db.replace_post_processing_script_directory(&scripts)
        .unwrap();
    let script = ScriptName::new("notify.sh").unwrap();
    let saved = db
        .create_script_instance(
            ScriptInstanceDraft::new(script, InstanceTrigger::PostProcessing)
                .sealed_input("apitoken", "own-token")
                .sealed_input("Extra", "own-extra"),
        )
        .unwrap();

    let reapplied = db.reapply_script_header(&saved.id).unwrap();
    assert_eq!(
        reapplied
            .inputs
            .iter()
            .map(|input| (input.name.as_str(), input.value.as_str(), input.sealed))
            .collect::<Vec<_>>(),
        // The declared input keeps its secret, under the header's casing;
        // the one the header does not declare goes like any other.
        [("Server", "localhost", false), ("ApiToken", "", true)]
    );
    let run = db.script_instance_run_inputs(&saved.id).unwrap().unwrap();
    assert_eq!(run.len(), 2);
    assert!(matches!(
        run[1].value(),
        OptionValue::Secret(value) if value.expose_for_execution() == "own-token"
    ));
    // Nothing of it became a named secret along the way.
    assert!(db.secrets().unwrap().is_empty());
}
