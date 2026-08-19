use super::manifest::{ManifestError, detect_bare_script_adapter, parse_nzbget_manifest};
use super::model::{ScriptAdapter, ScriptOptionType, ScriptSelectValue};

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
    assert!(matches!(
        parse_nzbget_manifest(&NZBGET_V2_MANIFEST.replace("POST-PROCESSING", "QUEUE")),
        Err(ManifestError::UnsupportedKind)
    ));
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
