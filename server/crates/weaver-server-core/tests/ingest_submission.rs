use weaver_nzb::{Nzb, NzbFile, NzbMeta, NzbSegment};
use weaver_server_core::ingest::nzb_to_submission_spec;

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
