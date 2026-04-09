use std::path::Path;

use weaver_nzb::{Nzb, NzbMeta};
use weaver_server_core::ingest::nzb_to_spec;

#[test]
fn nzb_to_spec_extracts_password_from_filename() {
    let path = Path::new("/downloads/Some.Release.{{mysecret}}.nzb");
    let nzb = Nzb {
        meta: NzbMeta::default(),
        files: vec![],
    };

    let spec = nzb_to_spec(&nzb, path, None, vec![]);

    assert_eq!(spec.password, Some("mysecret".to_string()));
}

#[test]
fn nzb_to_spec_extracts_password_from_meta() {
    let path = Path::new("/downloads/normal.nzb");
    let nzb = Nzb {
        meta: NzbMeta {
            password: Some("fromxml".to_string()),
            ..Default::default()
        },
        files: vec![],
    };

    let spec = nzb_to_spec(&nzb, path, None, vec![]);

    assert_eq!(spec.password, Some("fromxml".to_string()));
}

#[test]
fn nzb_to_spec_prefers_meta_password() {
    let path = Path::new("/downloads/release.{{filepw}}.nzb");
    let nzb = Nzb {
        meta: NzbMeta {
            password: Some("metapw".to_string()),
            ..Default::default()
        },
        files: vec![],
    };

    let spec = nzb_to_spec(&nzb, path, None, vec![]);

    assert_eq!(spec.password, Some("metapw".to_string()));
}

#[test]
fn nzb_to_spec_leaves_password_empty_when_missing() {
    let path = Path::new("/downloads/normal.nzb");
    let nzb = Nzb {
        meta: NzbMeta::default(),
        files: vec![],
    };

    let spec = nzb_to_spec(&nzb, path, None, vec![]);

    assert_eq!(spec.password, None);
}
