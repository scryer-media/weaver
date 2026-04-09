mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};

#[tokio::test]
async fn list_categories_empty() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ categories { id name } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let categories = data["categories"].as_array().unwrap();
    assert!(categories.is_empty());
}

#[tokio::test]
async fn add_category() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "tv" }) {
                    id
                    name
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let cat = &data["addCategory"];
    assert!(cat["id"].as_u64().unwrap() > 0);
    assert_eq!(cat["name"].as_str().unwrap(), "tv");
}

#[tokio::test]
async fn add_category_with_dest_dir() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "movies", destDir: "/data/movies" }) {
                    id
                    name
                    destDir
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let cat = &data["addCategory"];
    assert_eq!(cat["destDir"].as_str().unwrap(), "/data/movies");
}

#[tokio::test]
async fn add_category_with_aliases() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "tv", aliases: "television,shows" }) {
                    id
                    name
                    aliases
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let cat = &data["addCategory"];
    assert_eq!(cat["aliases"].as_str().unwrap(), "television,shows");
}

#[tokio::test]
async fn add_duplicate_category() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "tv" }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);

    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "tv" }) { id }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
    let err_msg = resp.errors[0].message.to_lowercase();
    assert!(
        err_msg.contains("already exists"),
        "expected 'already exists' in error: {err_msg}"
    );
}

#[tokio::test]
async fn add_duplicate_case_insensitive() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "TV" }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);

    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "tv" }) { id }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn add_category_empty_name() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "" }) { id }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
    let err_msg = resp.errors[0].message.to_lowercase();
    assert!(
        err_msg.contains("must not be empty") || err_msg.contains("empty"),
        "expected empty name error in: {err_msg}"
    );
}

#[tokio::test]
async fn add_category_whitespace_name() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "  " }) { id }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn update_category_name() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "tv" }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addCategory"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateCategory(id: {id}, input: {{ name: "television" }}) {{
                    id
                    name
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["updateCategory"]["name"].as_str().unwrap(),
        "television"
    );
}

#[tokio::test]
async fn update_category_name_conflict() {
    let h = TestHarness::new().await;
    h.execute(
        r#"mutation {
            addCategory(input: { name: "tv" }) { id }
        }"#,
    )
    .await;

    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "movies" }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let movies_id = response_data(&resp)["addCategory"]["id"].as_u64().unwrap();

    // Try to rename "movies" to "tv" which already exists.
    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateCategory(id: {movies_id}, input: {{ name: "tv" }}) {{
                    id
                    name
                }}
            }}"#
        ))
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn remove_category() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "tv" }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addCategory"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                removeCategory(id: {id}) {{
                    id
                    name
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let remaining = data["removeCategory"].as_array().unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn remove_nonexistent_category() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                removeCategory(id: 999) {
                    id
                }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
    let err_msg = resp.errors[0].message.to_lowercase();
    assert!(
        err_msg.contains("not found"),
        "expected 'not found' in error: {err_msg}"
    );
}

#[tokio::test]
async fn list_categories_multiple() {
    let h = TestHarness::new().await;
    for name in &["tv", "movies", "audio"] {
        let resp = h
            .execute(&format!(
                r#"mutation {{
                    addCategory(input: {{ name: "{name}" }}) {{ id }}
                }}"#
            ))
            .await;
        assert_no_errors(&resp);
    }

    let resp = h.execute("{ categories { id name } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let categories = data["categories"].as_array().unwrap();
    assert_eq!(categories.len(), 3);
}
