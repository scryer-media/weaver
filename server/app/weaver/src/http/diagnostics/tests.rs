use super::*;

#[tokio::test]
async fn config_collection_redacts_and_refuses_unparseable_files() {
    let dir = tempfile::tempdir().expect("temp data dir");
    tokio::fs::write(
        dir.path().join("weaver.toml"),
        "data_dir = \"/var/lib/weaver\"\nencryption_key = \"0123456789abcdef\"\n",
    )
    .await
    .expect("write config");
    tokio::fs::write(dir.path().join("broken.toml"), "not = valid = toml")
        .await
        .expect("write broken config");
    tokio::fs::write(dir.path().join("notes.txt"), "ignored")
        .await
        .expect("write unrelated file");

    let components = collect_config_files(dir.path()).await;
    let names: Vec<&str> = components
        .iter()
        .map(|component| component.name.as_str())
        .collect();
    assert_eq!(names, vec!["config/broken.toml", "config/weaver.toml"]);

    let broken = &components[0];
    assert!(broken.outcome.is_err(), "an unparseable config is left out");

    let good = components[1].outcome.as_ref().expect("redacted config");
    let text = String::from_utf8(good.clone()).expect("utf-8");
    assert!(text.contains("/var/lib/weaver"));
    assert!(!text.contains("0123456789abcdef"));
    assert!(text.contains(redact::REDACTED));
}

#[tokio::test]
async fn file_tail_reads_only_the_last_bytes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("weaver.log");
    tokio::fs::write(&path, b"0123456789")
        .await
        .expect("write log");

    let whole = read_file_tail(&path, 64).await.expect("whole file");
    assert_eq!(whole, b"0123456789");

    let tail = read_file_tail(&path, 4).await.expect("tail");
    assert_eq!(tail, b"6789");
}

#[test]
fn every_collected_root_field_is_selectable_within_the_schema_limits() {
    use weaver_server_api::context::{GRAPHQL_MAX_COMPLEXITY, GRAPHQL_MAX_DEPTH};

    let shape = SchemaShape::current().expect("the shipped schema parses");

    for group in GRAPHQL_COMPONENTS {
        for (field, arguments) in group.fields {
            let query = graphql::root_query(shape, field, *arguments)
                .unwrap_or_else(|error| panic!("{field}: {error}"));

            let fields = graphql::field_count(&query);
            assert!(
                fields < GRAPHQL_MAX_COMPLEXITY,
                "{field} resolves {fields} fields, at or past the schema's complexity limit \
                 of {GRAPHQL_MAX_COMPLEXITY}; split it across files"
            );

            let depth = query
                .chars()
                .scan(0usize, |open, character| {
                    match character {
                        '{' => *open += 1,
                        '}' => *open = open.saturating_sub(1),
                        _ => {}
                    }
                    Some(*open)
                })
                .max()
                .unwrap_or(0);
            assert!(
                depth <= GRAPHQL_MAX_DEPTH,
                "{field} nests {depth} deep, past the schema's limit of {GRAPHQL_MAX_DEPTH}"
            );
        }
    }
}

#[test]
fn service_log_payload_flattens_to_plain_lines() {
    let payload = serde_json::json!({
        "serviceLogs": { "lines": ["first line", "second line"], "count": 2 }
    });
    assert_eq!(service_log_text(&payload), "first line\nsecond line\n");
    assert_eq!(service_log_text(&serde_json::json!({})), "");
}
