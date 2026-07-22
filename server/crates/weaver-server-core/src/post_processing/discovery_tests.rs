use std::fs;

use super::discovery::{
    DiscoveryError, DiscoveryOptions, approve_discovered_extension, discover_and_record_extensions,
    discover_extensions,
};
use super::manifest::BareScriptAdapter;
use super::model::{ExtensionAdapter, TrustState};
use crate::persistence::Database;

#[test]
fn discovery_is_disabled_by_default() {
    let data = tempfile::tempdir().unwrap();
    assert!(matches!(
        discover_extensions(data.path(), DiscoveryOptions::default()),
        Err(DiscoveryError::Disabled)
    ));
    assert!(!data.path().join("scripts").exists());
}

#[test]
fn native_and_bare_packages_are_digest_bound_and_managed() {
    let data = tempfile::tempdir().unwrap();
    let scripts = data.path().join("scripts");
    let native = scripts.join("native-example");
    fs::create_dir_all(&native).unwrap();
    fs::write(
        native.join("weaver-extension.json"),
        r#"{
            "schema_version": 1,
            "kind": "native",
            "id": "example.native",
            "name": "Native Example",
            "version": "1.0.0",
            "entrypoint": "run.sh",
            "commands": [],
            "options": []
        }"#,
    )
    .unwrap();
    fs::write(native.join("run.sh"), "#!/bin/sh\nexit 0\n").unwrap();
    fs::write(
        scripts.join("legacy.py"),
        "#!/usr/bin/env python3\nprint('ok')\n",
    )
    .unwrap();

    let db = Database::open_in_memory().unwrap();
    let discovered = discover_and_record_extensions(
        &db,
        data.path(),
        DiscoveryOptions {
            enabled: true,
            bare_script_adapter: Some(BareScriptAdapter::Sabnzbd),
        },
        10,
    )
    .unwrap();
    assert_eq!(discovered.len(), 2);
    assert!(
        discovered
            .iter()
            .any(|item| item.manifest.adapter() == ExtensionAdapter::Native)
    );
    assert!(
        discovered
            .iter()
            .any(|item| item.manifest.adapter() == ExtensionAdapter::Sabnzbd)
    );

    let item = discovered
        .iter()
        .find(|item| item.manifest.adapter() == ExtensionAdapter::Native)
        .unwrap();
    let revision = item.manifest.revision();
    let managed = approve_discovered_extension(
        &db,
        data.path(),
        revision.extension_id(),
        revision.revision_id(),
        20,
    )
    .unwrap();
    assert!(managed.join("run.sh").is_file());
    let stored = db
        .extension_revision(revision.extension_id(), revision.revision_id())
        .unwrap()
        .unwrap();
    assert_eq!(stored.trust_state, TrustState::Approved);
}

#[test]
fn approval_rejects_source_changes_after_discovery() {
    let data = tempfile::tempdir().unwrap();
    let scripts = data.path().join("scripts");
    fs::create_dir_all(&scripts).unwrap();
    let script = scripts.join("changed.sh");
    fs::write(&script, "#!/bin/sh\nexit 0\n").unwrap();
    let db = Database::open_in_memory().unwrap();
    let discovered = discover_and_record_extensions(
        &db,
        data.path(),
        DiscoveryOptions {
            enabled: true,
            bare_script_adapter: None,
        },
        10,
    )
    .unwrap();
    let revision = discovered[0].manifest.revision();
    fs::write(&script, "#!/bin/sh\nexit 1\n").unwrap();
    assert!(matches!(
        approve_discovered_extension(
            &db,
            data.path(),
            revision.extension_id(),
            revision.revision_id(),
            20,
        ),
        Err(DiscoveryError::PackageChanged)
    ));
}
