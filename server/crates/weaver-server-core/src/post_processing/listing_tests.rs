use std::fs;
use std::path::Path;

use super::listing::{list_scripts, resolve_script, scripts_dir};
use super::model::{ScriptAdapter, ScriptName};

fn write_script(root: &Path, name: &str, body: &str) {
    let path = scripts_dir(root).join(name);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, body).unwrap();
}

fn write_manifest_package(root: &Path, name: &str, manifest: serde_json::Value) {
    let package = scripts_dir(root).join(name);
    fs::create_dir_all(&package).unwrap();
    fs::write(package.join("manifest.json"), manifest.to_string()).unwrap();
    fs::write(package.join("email.py"), "#!/usr/bin/env python3\n").unwrap();
}

fn nzbget_manifest() -> serde_json::Value {
    serde_json::json!({
        "main": "email.py",
        "name": "email",
        "kind": "POST-PROCESSING",
        "displayName": "Email",
        "version": "1.0.0",
        "author": "Author",
        "homepage": "https://example.invalid",
        "license": "GNU",
        "about": "About",
        "description": [],
        "requirements": [],
        "queueEvents": "",
        "taskTime": "",
        "sections": [],
        "commands": [],
        "options": [{
            "name": "sendMail",
            "displayName": "SendMail",
            "value": "Always",
            "description": [],
            "select": ["Always", "OnFailure"]
        }]
    })
}

#[test]
fn a_missing_scripts_directory_is_created_and_lists_empty() {
    let root = tempfile::tempdir().unwrap();
    let listing = list_scripts(root.path()).unwrap();
    assert!(listing.scripts.is_empty());
    assert!(listing.problems.is_empty());
    assert!(scripts_dir(root.path()).is_dir());
}

#[test]
fn a_bare_script_defaults_to_the_sabnzbd_contract() {
    let root = tempfile::tempdir().unwrap();
    write_script(root.path(), "notify.sh", "#!/bin/sh\necho hi\n");
    let listing = list_scripts(root.path()).unwrap();
    assert_eq!(listing.scripts.len(), 1);
    let script = &listing.scripts[0];
    assert_eq!(script.name.as_str(), "notify.sh");
    assert_eq!(script.manifest.adapter(), ScriptAdapter::Sabnzbd);
    assert_eq!(script.manifest.entrypoint(), "notify.sh");
    assert!(script.manifest.compatibility_name().is_none());
    // A bare script's root is the scripts directory itself.
    assert_eq!(script.root, scripts_dir(root.path()));
}

#[test]
fn the_legacy_nzbget_header_selects_the_nzbget_contract() {
    let root = tempfile::tempdir().unwrap();
    write_script(
        root.path(),
        "videosort.py",
        "#!/usr/bin/env python3\n### NZBGET POST-PROCESSING SCRIPT ###\n",
    );
    let listing = list_scripts(root.path()).unwrap();
    assert_eq!(listing.scripts.len(), 1);
    assert_eq!(listing.scripts[0].manifest.adapter(), ScriptAdapter::Nzbget);
    assert_eq!(
        listing.scripts[0]
            .manifest
            .compatibility_name()
            .unwrap()
            .as_str(),
        "videosort.py"
    );
}

#[test]
fn a_manifest_package_supplies_the_display_name_adapter_and_options() {
    let root = tempfile::tempdir().unwrap();
    write_manifest_package(root.path(), "email", nzbget_manifest());
    let listing = list_scripts(root.path()).unwrap();
    assert_eq!(listing.scripts.len(), 1);
    let script = &listing.scripts[0];
    assert_eq!(script.name.as_str(), "email");
    assert_eq!(script.manifest.display_name(), "Email");
    assert_eq!(script.manifest.adapter(), ScriptAdapter::Nzbget);
    assert_eq!(script.manifest.options().len(), 1);
    assert_eq!(script.root, scripts_dir(root.path()).join("email"));
}

#[test]
fn an_unparsable_manifest_becomes_a_visible_problem_rather_than_a_silent_absence() {
    let root = tempfile::tempdir().unwrap();
    let package = scripts_dir(root.path()).join("broken");
    fs::create_dir_all(&package).unwrap();
    fs::write(package.join("manifest.json"), "{ not json").unwrap();
    let listing = list_scripts(root.path()).unwrap();
    assert!(listing.scripts.is_empty());
    assert_eq!(listing.problems.len(), 1);
    assert_eq!(listing.problems[0].name, "broken");
    assert!(listing.problems[0].message.contains("JSON"));
}

#[test]
fn directories_without_a_manifest_and_dotfiles_are_ignored() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir_all(scripts_dir(root.path()).join("not-a-package")).unwrap();
    write_script(root.path(), ".hidden.sh", "#!/bin/sh\n");
    write_script(root.path(), "notes.txt", "not a script\n");
    let listing = list_scripts(root.path()).unwrap();
    assert!(listing.scripts.is_empty());
    assert!(listing.problems.is_empty());
}

#[cfg(unix)]
#[test]
fn symlinked_entries_are_skipped() {
    let root = tempfile::tempdir().unwrap();
    write_script(root.path(), "real.sh", "#!/bin/sh\n");
    std::os::unix::fs::symlink(
        scripts_dir(root.path()).join("real.sh"),
        scripts_dir(root.path()).join("link.sh"),
    )
    .unwrap();
    let listing = list_scripts(root.path()).unwrap();
    assert_eq!(listing.scripts.len(), 1);
    assert_eq!(listing.scripts[0].name.as_str(), "real.sh");
}

#[test]
fn resolving_by_name_matches_the_listing_and_fails_loudly_when_absent() {
    let root = tempfile::tempdir().unwrap();
    write_script(root.path(), "notify.sh", "#!/bin/sh\n");
    let resolved = resolve_script(root.path(), &ScriptName::new("notify.sh").unwrap()).unwrap();
    assert_eq!(resolved.manifest.adapter(), ScriptAdapter::Sabnzbd);
    assert!(resolve_script(root.path(), &ScriptName::new("gone.sh").unwrap()).is_err());
}
