use weaver_model::files::FileRole;
use weaver_nzb::{Nzb, NzbFile, NzbMeta, NzbSegment};
use weaver_server_core::ingest::{nzb_password_candidates, nzb_to_submission_spec};

fn nzb_with_password(password: Option<&str>) -> Nzb {
    Nzb {
        meta: NzbMeta {
            password: password.map(str::to_string),
            ..NzbMeta::default()
        },
        files: vec![NzbFile {
            poster: "poster@example.com".to_string(),
            subject: "Release - \"episode.rar\" yEnc (1/1)".to_string(),
            date: 1_700_000_000,
            groups: vec!["alt.binaries.test".to_string()],
            segments: vec![NzbSegment {
                number: 1,
                bytes: 10,
                message_id: "one@example.com".to_string(),
            }],
        }],
    }
}

#[test]
fn nzb_to_submission_spec_uses_dense_ordinals_for_sparse_segments() {
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

    let spec = nzb_to_submission_spec(&nzb, Some("Sparse.Release.nzb"), None, None, vec![]);
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

#[test]
fn nzb_to_submission_spec_sanitizes_trailing_quote_filename() {
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

    let spec = nzb_to_submission_spec(&nzb, Some("Fixture.Release.nzb"), None, None, vec![]);

    assert_eq!(spec.files[0].filename, "Fixture.Payload.mkv_");
    assert_eq!(spec.files[0].role, FileRole::Unknown);
}

#[test]
fn nzb_to_submission_spec_preserves_colliding_sanitized_filenames() {
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

    let spec = nzb_to_submission_spec(&nzb, Some("Fixture.Release.nzb"), None, None, vec![]);

    assert_eq!(spec.files[0].filename, "A_B.rar");
    assert_eq!(spec.files[1].filename, "A_B.duplicate1.rar");
    assert_eq!(spec.files[0].role, FileRole::RarVolume { volume_number: 0 });
    assert_eq!(spec.files[1].role, FileRole::RarVolume { volume_number: 0 });
}

#[test]
fn nzb_to_submission_spec_sanitizes_and_classifies_archive_roles() {
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

    let spec = nzb_to_submission_spec(&nzb, Some("Fixture.Release.nzb"), None, None, vec![]);

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
fn nzb_password_candidates_ignore_placeholder_explicit_password() {
    let nzb = nzb_with_password(Some("nzb-secret"));

    let candidates = nzb_password_candidates(&nzb, std::path::Path::new("Release.nzb"), Some("1"));

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].source().as_str(), "nzb_meta");
    assert_eq!(candidates[0].value(), "nzb-secret");
}

#[test]
fn nzb_password_candidates_keep_real_explicit_then_nzb_meta() {
    let nzb = nzb_with_password(Some("nzb-secret"));

    let candidates = nzb_password_candidates(
        &nzb,
        std::path::Path::new("Release.nzb"),
        Some("indexer-secret"),
    );

    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.source().as_str())
            .collect::<Vec<_>>(),
        vec!["explicit", "nzb_meta"]
    );
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.value())
            .collect::<Vec<_>>(),
        vec!["indexer-secret", "nzb-secret"]
    );
}

#[test]
fn nzb_to_submission_spec_uses_nzb_meta_when_explicit_is_placeholder() {
    let nzb = nzb_with_password(Some("nzb-secret"));

    let spec = nzb_to_submission_spec(
        &nzb,
        Some("Release.nzb"),
        Some("1".to_string()),
        None,
        vec![],
    );

    assert_eq!(spec.password.as_deref(), Some("nzb-secret"));
}
