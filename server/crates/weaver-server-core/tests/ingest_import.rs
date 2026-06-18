use std::path::Path;

use weaver_model::files::FileRole;
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
fn nzb_to_spec_sanitizes_trailing_quote_filename() {
    let path = Path::new("/downloads/Fixture.Release.nzb");
    let nzb = Nzb {
        meta: NzbMeta::default(),
        files: vec![NzbFile {
            poster: "poster@example.com".to_string(),
            subject: "Fixture.Release Fixture.Payload.mkv\" yEnc (1/1)".to_string(),
            date: 1_700_000_000,
            groups: vec!["alt.binaries.test".to_string()],
            segments: vec![NzbSegment {
                number: 1,
                bytes: 10,
                message_id: "one@example.com".to_string(),
            }],
        }],
    };

    let spec = nzb_to_spec(&nzb, path, None, vec![]);

    assert_eq!(spec.files[0].filename, "Fixture.Payload.mkv_");
    assert_eq!(spec.files[0].role, FileRole::Unknown);
}

#[test]
fn nzb_to_spec_preserves_colliding_sanitized_filenames() {
    let path = Path::new("/downloads/Fixture.Release.nzb");
    let nzb = Nzb {
        meta: NzbMeta::default(),
        files: vec![
            NzbFile {
                poster: "poster@example.com".to_string(),
                subject: "Fixture.Release - \"A/B.rar\" yEnc (1/1)".to_string(),
                date: 1_700_000_000,
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![NzbSegment {
                    number: 1,
                    bytes: 10,
                    message_id: "one@example.com".to_string(),
                }],
            },
            NzbFile {
                poster: "poster@example.com".to_string(),
                subject: "Fixture.Release - \"A_B.rar\" yEnc (1/1)".to_string(),
                date: 1_700_000_000,
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![NzbSegment {
                    number: 2,
                    bytes: 10,
                    message_id: "two@example.com".to_string(),
                }],
            },
        ],
    };

    let spec = nzb_to_spec(&nzb, path, None, vec![]);

    assert_eq!(spec.files[0].filename, "A_B.rar");
    assert_eq!(spec.files[1].filename, "A_B.duplicate1.rar");
    assert_eq!(spec.files[0].role, FileRole::RarVolume { volume_number: 0 });
    assert_eq!(spec.files[1].role, FileRole::RarVolume { volume_number: 0 });
}

#[test]
fn nzb_to_spec_sanitizes_and_classifies_archive_roles() {
    let path = Path::new("/downloads/Fixture.Release.nzb");
    let nzb = Nzb {
        meta: NzbMeta::default(),
        files: vec![
            NzbFile {
                poster: "poster@example.com".to_string(),
                subject: "Fixture.Release Fixture.Payload.rar\" yEnc (1/1)".to_string(),
                date: 1_700_000_000,
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![NzbSegment {
                    number: 1,
                    bytes: 10,
                    message_id: "one@example.com".to_string(),
                }],
            },
            NzbFile {
                poster: "poster@example.com".to_string(),
                subject: "Fixture.Release Fixture.Payload.vol00+01.par2\" yEnc (1/1)".to_string(),
                date: 1_700_000_000,
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![NzbSegment {
                    number: 2,
                    bytes: 10,
                    message_id: "two@example.com".to_string(),
                }],
            },
        ],
    };

    let spec = nzb_to_spec(&nzb, path, None, vec![]);

    assert_eq!(spec.files[0].filename, "Fixture.Payload.rar_");
    assert_eq!(spec.files[0].role, FileRole::RarVolume { volume_number: 0 });
    assert_eq!(spec.files[1].filename, "Fixture.Payload.vol00+01.par2_");
    assert_eq!(
        spec.files[1].role,
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 1
        }
    );
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
