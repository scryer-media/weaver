// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Each refusal carries the code of its kind beside an unchanged sentence.

mod common;

use async_graphql::Value;
use common::{TestHarness, assert_no_errors};

// The first error's sentence and its `extensions.code`.
fn refusal(resp: &async_graphql::Response) -> (String, Option<String>) {
    let error = resp.errors.first().expect("an error");
    let code = error
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get("code"))
        .and_then(|code| match code {
            Value::String(code) => Some(code.clone()),
            _ => None,
        });
    (error.message.clone(), code)
}

fn assert_refusal(resp: &async_graphql::Response, message: &str, code: &str) {
    assert_eq!(refusal(resp), (message.to_string(), Some(code.to_string())));
}

#[tokio::test]
async fn a_category_name_in_use_is_name_taken() {
    let h = TestHarness::new().await;
    let add = r#"mutation { addCategory(input: { name: "tv" }) { id } }"#;
    assert_no_errors(&h.execute(add).await);
    assert_refusal(
        &h.execute(add).await,
        "category 'tv' already exists",
        "NAME_TAKEN",
    );
}

#[tokio::test]
async fn an_unknown_category_is_not_found() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { updateCategory(id: 999, input: { name: "tv" }) { id } }"#)
        .await;
    assert_refusal(&resp, "category 999 not found", "NOT_FOUND");
}

#[tokio::test]
async fn an_unknown_rss_rule_is_not_found() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation { updateRssRule(id: 99, input: { sortOrder: 0, action: ACCEPT }) { id } }"#,
        )
        .await;
    assert_refusal(&resp, "database error: RSS rule 99 not found", "NOT_FOUND");
}

#[tokio::test]
async fn a_bad_feed_url_is_invalid_input() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation { addRssFeed(input: { name: "ftp", url: "ftp://example.com", enabled: true }) { id } }"#,
        )
        .await;
    assert_refusal(
        &resp,
        "unsupported RSS feed URL scheme: ftp",
        "INVALID_INPUT",
    );
}

#[tokio::test]
async fn a_bad_page_position_is_invalid_input() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"{ scriptRuns(limit: 2, before: "x") { total } }"#)
        .await;
    assert_refusal(
        &resp,
        "before is not a position in the list",
        "INVALID_INPUT",
    );
}

#[tokio::test]
async fn a_password_change_without_login_is_login_not_enabled() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation { changePassword(currentPassword: "old-password", newPassword: "long-enough-pass") }"#,
        )
        .await;
    assert_refusal(&resp, "login is not enabled", "LOGIN_NOT_ENABLED");
}

#[tokio::test]
async fn a_wrong_current_password_is_invalid_password() {
    let h = TestHarness::new().await;
    assert_no_errors(
        &h.execute(r#"mutation { enableLogin(username: "admin", password: "long-enough-pass") }"#)
            .await,
    );
    let resp = h
        .execute(
            r#"mutation { changePassword(currentPassword: "not-the-password", newPassword: "another-long-pass") }"#,
        )
        .await;
    assert_refusal(&resp, "current password is incorrect", "INVALID_PASSWORD");
}

#[tokio::test]
async fn a_rerun_of_a_job_without_history_is_a_conflict() {
    let h = TestHarness::new().await;
    let resp = h
        .execute("mutation { rerunPostProcessing(jobId: 1) }")
        .await;
    assert_refusal(
        &resp,
        "post-processing reruns require a terminal history job",
        "CONFLICT",
    );
}
