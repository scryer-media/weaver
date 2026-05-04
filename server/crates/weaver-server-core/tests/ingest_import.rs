use std::path::Path;

use weaver_nzb::{Nzb, NzbFile, NzbMeta, NzbSegment};
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

#[test]
fn nzb_to_spec_uses_dense_ordinals_for_sparse_segments() {
    let path = Path::new("/downloads/Sparse.Release.nzb");
    let nzb = Nzb {
        meta: NzbMeta::default(),
        files: vec![NzbFile {
            poster: "poster@example.com".to_string(),
            subject: "Sparse - \"episode.bin\" yEnc (1/3)".to_string(),
            date: 1_700_000_000,
            groups: vec!["alt.binaries.test".to_string()],
            segments: vec![
                NzbSegment {
                    number: 1,
                    bytes: 10,
                    message_id: "one@example.com".to_string(),
                },
                NzbSegment {
                    number: 4,
                    bytes: 20,
                    message_id: "four@example.com".to_string(),
                },
                NzbSegment {
                    number: 6,
                    bytes: 30,
                    message_id: "six@example.com".to_string(),
                },
            ],
        }],
    };

    let spec = nzb_to_spec(&nzb, path, None, vec![]);
    let segments = &spec.files[0].segments;

    assert_eq!(
        segments
            .iter()
            .map(|segment| segment.ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        segments
            .iter()
            .map(|segment| segment.article_number)
            .collect::<Vec<_>>(),
        vec![1, 4, 6]
    );
    assert_eq!(
        segments
            .iter()
            .map(|segment| segment.message_id.as_str())
            .collect::<Vec<_>>(),
        vec!["one@example.com", "four@example.com", "six@example.com"]
    );
}
