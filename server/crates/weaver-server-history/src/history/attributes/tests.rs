// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn internal_metadata_is_hidden_from_history_attributes() {
    let metadata = vec![
        ("source".into(), "api".into()),
        (
            "weaver.submission.source_url".into(),
            "https://example.invalid/nzb?token=private".into(),
        ),
        (CLIENT_REQUEST_ID_ATTRIBUTE_KEY.into(), "request-1".into()),
    ];
    let (request_id, attributes) = split_history_metadata(&metadata);
    assert_eq!(request_id.as_deref(), Some("request-1"));
    assert_eq!(attributes, vec![("source".into(), "api".into())]);
}

#[test]
fn the_no_memory_limit_flag_is_read_from_metadata_and_kept_private() {
    let flagged = vec![(NO_MEMORY_LIMIT_ATTRIBUTE_KEY.into(), "1".into())];
    assert!(no_memory_limit(&flagged));
    assert!(!no_memory_limit(&[]));
    assert!(public_history_attributes(&flagged).is_empty());
}

// Only the mark the failure recorded offers a retry with no limit: an
// error's text never does, and the mark stays out of the attributes.
#[test]
fn only_a_job_marked_failed_over_the_ceiling_is_failed_for_memory() {
    let marked = vec![(FAILED_FOR_MEMORY_ATTRIBUTE_KEY.to_string(), "1".to_string())];
    assert!(failed_for_memory(&marked));
    assert!(!failed_for_memory(&[]));
    assert!(!failed_for_memory(&[(
        "error".to_string(),
        "lantern.7z needs 4.0 GiB of decode memory, 512.0 MiB allowed".to_string()
    )]));
    assert!(public_history_attributes(&marked).is_empty());
}

// An RSS job's item link and id are never a public attribute, in any
// case: they are carried apart, as stored, for an admin's view alone.
#[test]
fn an_rss_link_and_id_are_never_public_attributes() {
    let metadata = vec![
        ("rss.feed_name".into(), "feed".into()),
        ("rss.item_id".into(), "enc:v1:id".into()),
        ("rss.item_title".into(), "Item".into()),
        ("rss.item_url".into(), "enc:v1:url".into()),
        (
            " RSS.Item_URL ".into(),
            "https://x.invalid/?passkey=1".into(),
        ),
    ];
    assert_eq!(
        public_history_attributes(&metadata),
        vec![
            ("rss.feed_name".into(), "feed".into()),
            ("rss.item_title".into(), "Item".into()),
        ]
    );
    assert_eq!(
        rss_link_pairs(&metadata),
        vec![
            ("rss.item_id".into(), "enc:v1:id".into()),
            ("rss.item_url".into(), "enc:v1:url".into()),
        ]
    );
}

// No password a job holds is ever an attribute a client reads: neither
// the sealed validated password, nor the submitted mark, nor a password
// a client stored under the unpack-password parameter name.
#[test]
fn no_password_is_a_public_attribute() {
    let metadata = vec![
        (
            VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY.into(),
            "sealed".into(),
        ),
        (SUBMITTED_PASSWORD_ATTRIBUTE_KEY.into(), "1".into()),
        ("*Unpack:Password".into(), "hunter2".into()),
        (" *UNPACK:PASSWORD ".into(), "hunter2".into()),
        ("source".into(), "api".into()),
    ];
    assert_eq!(
        public_history_attributes(&metadata),
        vec![("source".into(), "api".into())]
    );
    assert_eq!(
        split_history_metadata(&metadata).1,
        vec![("source".into(), "api".into())]
    );
}
