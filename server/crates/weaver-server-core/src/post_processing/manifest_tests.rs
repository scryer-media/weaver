use super::manifest::{
    BareScriptAdapter, ManifestError, detect_bare_script_adapter, parse_native_manifest,
    parse_nzbget_manifest,
};
use super::model::{
    ExtensionAdapter, ExtensionOptionType, ExtensionSelectValue, VerifiedExtensionDigest,
};

const NATIVE_MANIFEST: &str = r#"{
  "schema_version": 1,
  "kind": "native",
  "id": "example.cleanup",
  "name": "Example Cleanup",
  "version": "1.0.0",
  "entrypoint": "bin/cleanup",
  "commands": [{"name": "cleanup", "section": "Actions"}],
  "options": [{"name": "api_key", "type": "secret"}]
}"#;

const NZBGET_V2_MANIFEST: &str = include_str!("fixtures/nzbget-v2-post-processing-manifest.json");

fn verified_digest(seed: char) -> VerifiedExtensionDigest {
    VerifiedExtensionDigest::from_verified_package_digest(
        super::model::ExtensionDigest::new(format!("sha256:{}", seed.to_string().repeat(64)))
            .unwrap(),
    )
}

#[test]
fn native_manifests_bind_revision_identity_to_the_verified_package_digest() {
    let first = parse_native_manifest(NATIVE_MANIFEST, verified_digest('a')).unwrap();
    let second = parse_native_manifest(NATIVE_MANIFEST, verified_digest('b')).unwrap();
    assert_eq!(first.adapter(), ExtensionAdapter::Native);
    assert_eq!(first.revision().declared_version().as_str(), "1.0.0");
    assert_ne!(
        first.revision().revision_id(),
        second.revision().revision_id()
    );
    assert_ne!(first.revision().digest(), second.revision().digest());
}

#[test]
fn ingests_current_nzbget_v2_fields_sections_and_numeric_select_values() {
    let manifest = parse_nzbget_manifest(NZBGET_V2_MANIFEST, verified_digest('c')).unwrap();
    assert_eq!(manifest.adapter(), ExtensionAdapter::Nzbget);
    assert_eq!(manifest.compatibility_name().unwrap().as_str(), "email");
    assert_eq!(manifest.display_name(), "Email");
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
        ExtensionOptionType::Integer
    );
    assert!(matches!(
        manifest.options()[1].select()[0],
        ExtensionSelectValue::Number(_)
    ));
    assert_eq!(
        manifest.options()[3].option_type(),
        ExtensionOptionType::Number
    );
    assert!(matches!(
        manifest.options()[3].select()[0],
        ExtensionSelectValue::Number(_)
    ));
    assert_eq!(manifest.commands()[1].section(), Some("Feeds"));
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
    value["commands"][0]["section"] = serde_json::json!("options");
    value["options"][0]["description"] = serde_json::json!([42, "retained"]);
    value["options"][0]["select"] = serde_json::json!(["Always", true, 2.5]);
    value["options"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({ "name": "malformed" }));
    value["commands"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!(false));
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

    let manifest = parse_nzbget_manifest(&value.to_string(), verified_digest('d')).unwrap();
    assert_eq!(manifest.options()[0].section(), None);
    assert_eq!(manifest.commands()[0].section(), None);
    assert_eq!(manifest.options()[0].description(), ["retained"]);
    assert_eq!(manifest.options()[0].select().len(), 2);
    assert_eq!(manifest.options().len(), 5);
    assert_eq!(manifest.commands().len(), 2);
    assert!(
        manifest
            .sections()
            .iter()
            .any(|section| section.name() == "Server Settings")
    );
    assert_eq!(manifest.options()[4].section(), Some("Server Settings"));

    let mut scalar_root = value;
    scalar_root["description"] = serde_json::json!("not-an-array");
    assert!(parse_nzbget_manifest(&scalar_root.to_string(), verified_digest('e')).is_err());
}

#[test]
fn manifest_validation_rejects_malformed_shapes_entrypoints_and_secrets() {
    assert!(matches!(
        parse_native_manifest("not json", verified_digest('a')),
        Err(ManifestError::InvalidJson)
    ));
    assert!(matches!(
        parse_native_manifest("[]", verified_digest('a')),
        Err(ManifestError::InvalidShape)
    ));
    assert!(matches!(
        parse_native_manifest(
            &NATIVE_MANIFEST.replace("\"schema_version\": 1", "\"schema_version\": 2"),
            verified_digest('a')
        ),
        Err(ManifestError::UnsupportedSchema(2))
    ));
    assert!(matches!(
        parse_nzbget_manifest(
            &NZBGET_V2_MANIFEST.replace("POST-PROCESSING", "QUEUE"),
            verified_digest('a')
        ),
        Err(ManifestError::UnsupportedKind)
    ));
    assert!(matches!(
        parse_nzbget_manifest(
            &NZBGET_V2_MANIFEST.replace("\"author\":", "\"author_missing\":"),
            verified_digest('a')
        ),
        Err(ManifestError::InvalidShape)
    ));
    for entrypoint in [
        "/bin/cleanup",
        r"C:\\work\\cleanup",
        r"\\\\server\\share\\cleanup",
        "bin/../cleanup",
        r"bin\\..\\cleanup",
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
        "bin/control\ncharacter",
    ] {
        assert!(
            parse_native_manifest(
                &NATIVE_MANIFEST.replace("bin/cleanup", entrypoint),
                verified_digest('a')
            )
            .is_err()
        );
    }
    assert!(
        parse_native_manifest(
            &NATIVE_MANIFEST.replace(
                "{\"name\": \"api_key\", \"type\": \"secret\"}",
                "{\"name\": \"api_key\", \"type\": \"secret\", \"default\": \"plaintext\"}"
            ),
            verified_digest('a')
        )
        .is_err()
    );
}

#[test]
fn manifest_conflicts_and_qualified_duplicate_rules_are_rejected() {
    assert!(matches!(
        parse_native_manifest(
            &NATIVE_MANIFEST.replace(
                "\"entrypoint\": \"bin/cleanup\"",
                "\"main\": \"cleanup.py\""
            ),
            verified_digest('a')
        ),
        Err(ManifestError::ShapeConflict)
    ));
    assert!(matches!(
        parse_nzbget_manifest(NATIVE_MANIFEST, verified_digest('a')),
        Err(ManifestError::ShapeConflict)
    ));
    assert!(matches!(
        parse_native_manifest(
            &NATIVE_MANIFEST.replace(
                "\"version\": \"1.0.0\",",
                "\"version\": \"1.0.0\", \"trust\": \"approved\","
            ),
            verified_digest('a')
        ),
        Err(ManifestError::ShapeConflict)
    ));
    assert!(parse_native_manifest(
        &NATIVE_MANIFEST.replace(
            "[{\"name\": \"cleanup\", \"section\": \"Actions\"}]",
            "[{\"name\": \"cleanup\", \"section\": \"Actions\"}, {\"name\": \"CLEANUP\", \"section\": \"actions\"}]"
        ),
        verified_digest('a')
    )
    .is_err());
    assert!(parse_native_manifest(
        &NATIVE_MANIFEST.replace(
            "[{\"name\": \"api_key\", \"type\": \"secret\"}]",
            "[{\"name\": \"api_key\", \"type\": \"secret\"}, {\"name\": \"API_KEY\", \"type\": \"secret\", \"section\": \"options\"}]"
        ),
        verified_digest('a')
    )
    .is_err());
    assert!(parse_nzbget_manifest(
        &NZBGET_V2_MANIFEST.replace(
            "{\n      \"name\": \"Server\",\n      \"prefix\": \"Server\",\n      \"multi\": false\n    }",
            "{\n      \"name\": \"Server\",\n      \"prefix\": \"Server\",\n      \"multi\": false\n    }, {\n      \"name\": \"server\",\n      \"prefix\": \"Duplicate\",\n      \"multi\": true\n    }"
        ),
        verified_digest('a')
    )
    .is_err());
}

#[test]
fn aggregate_serde_revalidates_commands_options_and_adapter_compatibility() {
    let manifest = parse_native_manifest(NATIVE_MANIFEST, verified_digest('a')).unwrap();
    let mut wire = serde_json::to_value(manifest).unwrap();
    wire["commands"][0]["action"] = serde_json::Value::String(String::new());
    assert!(serde_json::from_value::<super::model::ExtensionManifest>(wire).is_err());

    let manifest = parse_nzbget_manifest(NZBGET_V2_MANIFEST, verified_digest('a')).unwrap();
    let mut wire = serde_json::to_value(manifest).unwrap();
    wire["adapter"] = serde_json::Value::String("sabnzbd".to_string());
    assert!(serde_json::from_value::<super::model::ExtensionManifest>(wire).is_err());
}

#[test]
fn bare_script_detection_stops_when_executable_content_begins() {
    assert_eq!(
        detect_bare_script_adapter(
            "#!/usr/bin/env python\n### NZBGET POST-PROCESSING SCRIPT ###",
            None
        ),
        ExtensionAdapter::Nzbget
    );
    assert_eq!(
        detect_bare_script_adapter(
            "\u{feff}#!/usr/bin/env python\r\n### NZBGET POST-PROCESSING SCRIPT ###\r\n",
            None
        ),
        ExtensionAdapter::Nzbget
    );
    assert_eq!(
        detect_bare_script_adapter(
            "\"\"\"\n### NZBGET POST-PROCESSING SCRIPT ###\n\"\"\"",
            None
        ),
        ExtensionAdapter::Sabnzbd
    );
    assert_eq!(
        detect_bare_script_adapter("print('run')\n### NZBGET POST-PROCESSING SCRIPT ###", None),
        ExtensionAdapter::Sabnzbd
    );
    let late_header = format!(
        "{}\n### NZBGET POST-PROCESSING SCRIPT ###",
        "# comment\n".repeat(64)
    );
    assert_eq!(
        detect_bare_script_adapter(&late_header, None),
        ExtensionAdapter::Sabnzbd
    );
    assert_eq!(
        detect_bare_script_adapter("#!/bin/sh", Some(BareScriptAdapter::Nzbget)),
        ExtensionAdapter::Nzbget
    );
}
