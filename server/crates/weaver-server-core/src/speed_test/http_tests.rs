// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use std::ffi::OsStr;

fn written(dir: &tempfile::TempDir, name: &str, body: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, body).unwrap();
    path
}

fn refusal(path: &std::path::Path) -> String {
    match extra_roots(Some(path.as_os_str())) {
        Ok(roots) => panic!("{} was taken with {} roots", path.display(), roots.len()),
        Err(error) => error,
    }
}

#[test]
fn no_extra_roots_file_trusts_the_platform_roots_alone() {
    assert!(extra_roots(None).unwrap().is_empty());
    assert!(extra_roots(Some(OsStr::new(""))).unwrap().is_empty());
}

#[test]
fn an_extra_roots_file_is_read_whole() {
    let dir = tempfile::tempdir().unwrap();
    let (_, _, pem) = super::super::fixture::loopback_tls();
    let one = written(&dir, "one.pem", &pem);
    assert_eq!(extra_roots(Some(one.as_os_str())).unwrap().len(), 1);
    let two = written(&dir, "two.pem", &format!("{pem}\n{pem}"));
    assert_eq!(extra_roots(Some(two.as_os_str())).unwrap().len(), 2);
}

#[test]
fn a_missing_extra_roots_file_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent.pem");
    let error = refusal(&missing);
    assert!(error.starts_with(EXTRA_ROOTS_ENV), "{error}");
    assert!(error.contains("absent.pem"), "{error}");
    assert!(error.contains("cannot be read"), "{error}");
}

#[test]
fn a_malformed_extra_roots_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let no_pem = written(&dir, "notes.pem", "not a certificate\n");
    assert!(refusal(&no_pem).contains("holds no certificate"));

    let broken_base64 = written(
        &dir,
        "base64.pem",
        "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n",
    );
    assert!(refusal(&broken_base64).contains("is not readable PEM"));

    // Well-formed PEM around bytes that are no certificate.
    let not_der = written(
        &dir,
        "der.pem",
        "-----BEGIN CERTIFICATE-----\nAAECAwQFBgcICQ==\n-----END CERTIFICATE-----\n",
    );
    let error = refusal(&not_der);
    assert!(error.contains("not usable"), "{error}");
    assert!(error.starts_with(EXTRA_ROOTS_ENV), "{error}");
}
