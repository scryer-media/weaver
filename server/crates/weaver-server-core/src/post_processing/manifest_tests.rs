use super::manifest::{
    ManifestError, bare_script_options, detect_bare_script_adapter, option_name_suggests_secret,
    parse_nzbget_manifest,
};
use super::model::{OptionValue, ScriptAdapter, ScriptOptionType, ScriptSelectValue};

#[test]
fn a_bare_nzbget_header_declares_its_options_and_hints_credentials_secret() {
    let script = "#!/usr/bin/env python3\n\
        ##############################################################################\n\
        ### NZBGET POST-PROCESSING SCRIPT                                          ###\n\
        \n\
        # Sends a notice.\n\
        \n\
        ##############################################################################\n\
        ### OPTIONS                                                                ###\n\
        \n\
        # Server to send to.\n\
        #\n\
        # A host name.\n\
        #Server=localhost\n\
        \n\
        # Account password.\n\
        #Password=changeme\n\
        #ApiKey=\n\
        #UserToken=abc\n\
        #ClientSecret=x\n\
        #Passphrase=y\n\
        #Mode=fast\n\
        #Mode=again\n\
        \n\
        ### NZBGET POST-PROCESSING SCRIPT                                          ###\n\
        ##############################################################################\n\
        #Ignored=after the header\n\
        import sys\n";
    let options = bare_script_options(script);
    let shape = options
        .iter()
        .map(|option| {
            (
                option.name().as_str(),
                option.option_type(),
                option.default().cloned(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        shape,
        [
            (
                "Server",
                ScriptOptionType::String,
                Some(OptionValue::String("localhost".into()))
            ),
            // A credential keeps no default from the header.
            ("Password", ScriptOptionType::Secret, None),
            ("ApiKey", ScriptOptionType::Secret, None),
            ("UserToken", ScriptOptionType::Secret, None),
            ("ClientSecret", ScriptOptionType::Secret, None),
            ("Passphrase", ScriptOptionType::Secret, None),
            (
                "Mode",
                ScriptOptionType::String,
                Some(OptionValue::String("fast".into()))
            ),
        ]
    );
    assert_eq!(
        options[0].description(),
        ["Server to send to.", "A host name."]
    );
    assert_eq!(options[1].description(), ["Account password."]);

    // A script that is not an NZBGet one declares nothing.
    assert!(bare_script_options("#!/bin/sh\n### OPTIONS ###\n#Token=x\n").is_empty());
    for name in ["apikey", "KEY", "token", "Password", "pass", "SECRET"] {
        assert!(option_name_suggests_secret(name), "{name}");
    }
    for name in ["Server", "Host", "Category"] {
        assert!(!option_name_suggests_secret(name), "{name}");
    }
}

const NZBGET_V2_MANIFEST: &str = include_str!("fixtures/nzbget-v2-post-processing-manifest.json");

#[test]
fn ingests_current_nzbget_v2_fields_sections_and_numeric_select_values() {
    let manifest = parse_nzbget_manifest(NZBGET_V2_MANIFEST).unwrap();
    assert_eq!(manifest.adapter(), ScriptAdapter::Nzbget);
    assert_eq!(manifest.compatibility_name().unwrap().as_str(), "email");
    assert_eq!(manifest.display_name(), "Email");
    assert_eq!(manifest.version(), Some("1.0.0"));
    assert_eq!(manifest.entrypoint(), "email.py");
    assert_eq!(manifest.sections().len(), 3);
    assert_eq!(manifest.sections()[0].name(), "Categories");
    assert_eq!(manifest.sections()[2].name(), "Server");
    assert_eq!(manifest.sections()[2].prefix(), "Server");
    assert!(!manifest.sections()[2].multi());
    assert!(
        manifest
            .sections()
            .iter()
            .all(|section| !section.name().eq_ignore_ascii_case("options"))
    );
    assert_eq!(manifest.options().len(), 4);
    assert_eq!(
        manifest.options()[1].option_type(),
        ScriptOptionType::Integer
    );
    assert!(matches!(
        manifest.options()[1].select()[0],
        ScriptSelectValue::Number(_)
    ));
    assert_eq!(
        manifest.options()[3].option_type(),
        ScriptOptionType::Number
    );
    assert!(matches!(
        manifest.options()[3].select()[0],
        ScriptSelectValue::Number(_)
    ));
    assert_eq!(manifest.options()[2].section(), Some("Categories"));
}

#[test]
fn upstream_arrays_default_sections_and_malformed_entries_follow_nzbget_compatibility() {
    let mut value: serde_json::Value = serde_json::from_str(NZBGET_V2_MANIFEST).unwrap();
    value["queueEvents"] = serde_json::json!("");
    value["taskTime"] = serde_json::json!("");
    value["description"] = serde_json::json!([]);
    value["requirements"] = serde_json::json!([]);
    value["sections"][3] = serde_json::json!({ "name": "options" });
    value["options"][0]["section"] = serde_json::json!("OPTIONS");
    value["options"][0]["description"] = serde_json::json!([42, "retained"]);
    value["options"][0]["select"] = serde_json::json!(["Always", true, 2.5]);
    value["options"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({ "name": "malformed" }));
    value["sections"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!(false));
    value["sections"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "name": "Server Settings",
            "prefix": "Server Settings",
            "multi": false
        }));
    value["options"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "section": "Server Settings",
            "name": "Server.Settings.Delay",
            "displayName": "Delay",
            "value": 0.5,
            "description": [],
            "select": [0.5, false, 1.0]
        }));

    let manifest = parse_nzbget_manifest(&value.to_string()).unwrap();
    assert_eq!(manifest.options()[0].section(), None);
    assert_eq!(manifest.options()[0].description(), ["retained"]);
    assert_eq!(manifest.options()[0].select().len(), 2);
    assert_eq!(manifest.options().len(), 5);
    assert!(
        manifest
            .sections()
            .iter()
            .any(|section| section.name() == "Server Settings")
    );
    assert_eq!(manifest.options()[4].section(), Some("Server Settings"));

    let mut scalar_root = value;
    scalar_root["description"] = serde_json::json!("not-an-array");
    assert!(parse_nzbget_manifest(&scalar_root.to_string()).is_err());
}

#[test]
fn an_option_can_opt_into_the_settings_encryption_envelope() {
    let mut value: serde_json::Value = serde_json::from_str(NZBGET_V2_MANIFEST).unwrap();
    value["options"][0]["secret"] = serde_json::json!(true);
    let manifest = parse_nzbget_manifest(&value.to_string()).unwrap();
    assert_eq!(
        manifest.options()[0].option_type(),
        ScriptOptionType::Secret
    );
    assert!(manifest.options()[0].is_secret());
    // A secret never carries a manifest default, so nothing sensitive can sit in
    // the package itself.
    assert!(manifest.options()[0].default().is_none());
}

#[test]
fn manifest_validation_rejects_malformed_shapes_kinds_and_entrypoints() {
    assert!(matches!(
        parse_nzbget_manifest("not json"),
        Err(ManifestError::InvalidJson)
    ));
    assert!(matches!(
        parse_nzbget_manifest("[]"),
        Err(ManifestError::InvalidShape)
    ));
    assert!(parse_nzbget_manifest(&NZBGET_V2_MANIFEST.replace("POST-PROCESSING", "QUEUE")).is_ok());
    assert!(matches!(
        parse_nzbget_manifest(&NZBGET_V2_MANIFEST.replace("\"author\":", "\"author_missing\":")),
        Err(ManifestError::InvalidShape)
    ));
    for entrypoint in [
        "/bin/cleanup",
        r"C:\\work\\cleanup",
        "bin/../cleanup",
        "bin//cleanup",
        "bin/cleanup/",
        "bin/file:stream",
        "con",
        "CON ",
        "bin/PRN.txt",
        "bin/AUX",
        "bin/NUL",
        "bin/COM1",
        "bin/COM9",
        "bin/COM¹.txt",
        "bin/LPT²",
        "bin/CLOCK$",
        "bin/CONIN$",
        "bin/CON .txt",
    ] {
        assert!(
            parse_nzbget_manifest(&NZBGET_V2_MANIFEST.replace("email.py", entrypoint)).is_err(),
            "accepted entrypoint {entrypoint:?}"
        );
    }
}

#[test]
fn duplicate_sections_and_qualified_option_names_are_rejected() {
    assert!(
        parse_nzbget_manifest(&NZBGET_V2_MANIFEST.replace(
            "{\n      \"name\": \"Server\",\n      \"prefix\": \"Server\",\n      \"multi\": false\n    }",
            "{\n      \"name\": \"Server\",\n      \"prefix\": \"Server\",\n      \"multi\": false\n    }, {\n      \"name\": \"server\",\n      \"prefix\": \"Duplicate\",\n      \"multi\": true\n    }"
        ))
        .is_err()
    );
    let mut value: serde_json::Value = serde_json::from_str(NZBGET_V2_MANIFEST).unwrap();
    value["options"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "name": "SENDMAIL",
            "displayName": "Duplicate",
            "value": "Always",
            "description": [],
            "select": []
        }));
    assert!(parse_nzbget_manifest(&value.to_string()).is_err());
}

#[test]
fn bare_script_detection_stops_when_executable_content_begins() {
    assert_eq!(
        detect_bare_script_adapter("#!/usr/bin/env python\n### NZBGET POST-PROCESSING SCRIPT ###"),
        ScriptAdapter::Nzbget
    );
    assert_eq!(
        detect_bare_script_adapter(
            "\u{feff}#!/usr/bin/env python\r\n### NZBGET POST-PROCESSING SCRIPT ###\r\n"
        ),
        ScriptAdapter::Nzbget
    );
    assert_eq!(
        detect_bare_script_adapter("\"\"\"\n### NZBGET POST-PROCESSING SCRIPT ###\n\"\"\""),
        ScriptAdapter::Sabnzbd
    );
    assert_eq!(
        detect_bare_script_adapter("print('run')\n### NZBGET POST-PROCESSING SCRIPT ###"),
        ScriptAdapter::Sabnzbd
    );
    let late_header = format!(
        "{}\n### NZBGET POST-PROCESSING SCRIPT ###",
        "# comment\n".repeat(64)
    );
    assert_eq!(
        detect_bare_script_adapter(&late_header),
        ScriptAdapter::Sabnzbd
    );
    // No header at all is the SABnzbd contract, which is the ecosystem default.
    assert_eq!(
        detect_bare_script_adapter("#!/bin/sh"),
        ScriptAdapter::Sabnzbd
    );
}

#[test]
fn manifest_retains_kinds_queue_subscriptions_and_task_times() {
    use super::model::{QueueEvent, ScriptKind, ScriptTaskTime};
    let mut value: serde_json::Value = serde_json::from_str(NZBGET_V2_MANIFEST).unwrap();
    value["kind"] = serde_json::json!("POST-PROCESSING/QUEUE/SCAN/SCHEDULER/FEED/FUTURE");
    value["queueEvents"] = serde_json::json!("NZB_ADDED,NZB_DOWNLOADED");
    value["taskTime"] = serde_json::json!("*;*:00,*:30;23:59;24:00");
    let manifest = parse_nzbget_manifest(&value.to_string()).unwrap();
    assert_eq!(
        manifest.kinds().iter().copied().collect::<Vec<_>>(),
        ScriptKind::ALL
    );
    assert_eq!(
        manifest.queue_events().iter().copied().collect::<Vec<_>>(),
        [QueueEvent::NzbAdded, QueueEvent::NzbDownloaded]
    );
    assert_eq!(
        manifest.task_times(),
        [
            ScriptTaskTime::Startup,
            ScriptTaskTime::Hourly { minute: 0 },
            ScriptTaskTime::Hourly { minute: 30 },
            ScriptTaskTime::Daily {
                hour: 23,
                minute: 59
            }
        ]
    );
    assert_eq!(manifest.declaration_problems().len(), 2);

    value["kind"] = serde_json::json!("prefixQUEUEsuffix");
    value["queueEvents"] = serde_json::json!("");
    let manifest = parse_nzbget_manifest(&value.to_string()).unwrap();
    assert_eq!(
        manifest.kinds().iter().copied().collect::<Vec<_>>(),
        [ScriptKind::Queue]
    );
    assert_eq!(manifest.queue_events().len(), QueueEvent::ALL.len());
    assert!(manifest.task_times().is_empty());
    value["queueEvents"] = serde_json::json!("NZB_NAMED");
    assert_eq!(
        parse_nzbget_manifest(&value.to_string())
            .unwrap()
            .queue_events()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        [QueueEvent::NzbNamed]
    );
    value["queueEvents"] = serde_json::json!("FUTURE_EVENT");
    assert!(
        parse_nzbget_manifest(&value.to_string())
            .unwrap()
            .queue_events()
            .is_empty()
    );
    value["kind"] = serde_json::json!("FUTURE");
    let manifest = parse_nzbget_manifest(&value.to_string()).unwrap();
    assert!(manifest.kinds().is_empty());
    assert_eq!(manifest.declaration_problems().len(), 1);
}

#[test]
fn legacy_kinds_and_metadata_survive_long_option_headers() {
    use super::manifest::{MAX_LEGACY_METADATA_BYTES, apply_bare_script_declarations};
    use super::model::{QueueEvent, ScriptKind, ScriptTaskTime};
    let base = parse_nzbget_manifest(NZBGET_V2_MANIFEST).unwrap();
    for kind in ScriptKind::ALL {
        let source = format!("#!/bin/sh\n### NZBGET {} SCRIPT ###\n", kind.as_str());
        assert_eq!(detect_bare_script_adapter(&source), ScriptAdapter::Nzbget);
        let manifest = apply_bare_script_declarations(base.clone(), &source);
        assert_eq!(manifest.kinds().iter().copied().collect::<Vec<_>>(), [kind]);
    }
    let source = format!(
        "### TASK TIME: * ###\n### NZBGET QUEUE/SCHEDULER SCRIPT ###\n{}\n### QUEUE EVENTS: NZB_DOWNLOADED ###\n### TASK TIME: *:30 ###\n",
        "# option\n".repeat(1500)
    );
    let manifest = apply_bare_script_declarations(base.clone(), &source);
    assert_eq!(
        manifest.queue_events().iter().copied().collect::<Vec<_>>(),
        [QueueEvent::NzbDownloaded]
    );
    assert_eq!(
        manifest.task_times(),
        [ScriptTaskTime::Hourly { minute: 30 }]
    );
    let bounded = format!(
        "### NZBGET SCHEDULER SCRIPT ###\n{}\n### TASK TIME: * ###\n",
        "#".repeat(MAX_LEGACY_METADATA_BYTES)
    );
    assert!(
        apply_bare_script_declarations(base, &bounded)
            .task_times()
            .is_empty()
    );
}

#[test]
fn script_task_times_reject_out_of_range_and_malformed_values() {
    use super::model::ScriptTaskTime;
    for value in [
        "24:00", "12:60", "-1:00", "1:*", "**", "1:2:3", ":01", "1:", "+1:00",
    ] {
        assert!(value.parse::<ScriptTaskTime>().is_err(), "accepted {value}");
    }
}

#[test]
fn stored_results_default_to_terminal_event_and_event_labels_round_trip() {
    use super::model::{QueueEvent, ScriptEventLabel, ScriptResult};
    let result: ScriptResult = serde_json::from_value(serde_json::json!({
        "script": "notify.sh", "adapter": "nzbget", "status": "succeeded",
        "exitCode": 93, "durationMs": 1, "finishedAtEpochMs": 1
    }))
    .unwrap();
    assert_eq!(result.event, ScriptEventLabel::PostProcessing);
    for event in [
        ScriptEventLabel::PostProcessing,
        ScriptEventLabel::Queue(QueueEvent::NzbDownloaded),
        ScriptEventLabel::Scan,
        ScriptEventLabel::Scheduler(0),
        ScriptEventLabel::Feed(7),
    ] {
        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(value.as_str(), Some(event.to_string().as_str()));
        assert_eq!(
            serde_json::from_value::<ScriptEventLabel>(value).unwrap(),
            event
        );
    }
}
