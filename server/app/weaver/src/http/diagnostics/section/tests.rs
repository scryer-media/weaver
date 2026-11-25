// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::Write as _;
use std::path::Path;

use super::*;

const SECRET: &str = "fixture-secret-0d6f9b";

fn bare_schema() -> WeaverSchema {
    async_graphql::Schema::build(
        weaver_server_api::QueryRoot::default(),
        weaver_server_api::MutationRoot::default(),
        weaver_server_api::SubscriptionRoot::default(),
    )
    .finish()
}

// Sources over a database and a configuration file in `dir`, whose
// configuration holds `SECRET` as a key, and an exposition and a log that
// name `dir`.
fn seeded_sources(dir: &Path, log: &LogRingBuffer) -> DiagnosticsSources {
    let data_dir = dir.to_str().expect("UTF-8 temp dir");
    std::fs::write(
        dir.join("weaver.toml"),
        format!("data_dir = \"{data_dir}\"\napi_key = \"{SECRET}\"\n"),
    )
    .expect("write config");
    let exposition: &'static str = Box::leak(
        format!(
            "# HELP weaver_fixture_total Fixture.\n# TYPE weaver_fixture_total counter\n\
             weaver_fixture_total{{path=\"{data_dir}/incoming\"}} 3\n\
             weaver_other_total 1\n"
        )
        .into_boxed_str(),
    );
    let db = Database::open(&dir.join("weaver.db")).expect("database");
    DiagnosticsSources::fixture(
        bare_schema(),
        db,
        data_dir,
        log.clone(),
        MetricsRender::fixed(exposition),
    )
}

fn read_text(read: &SectionRead) -> String {
    read.files
        .iter()
        .map(|file| format!("{} {:?} {:?}", file.name, file.value, file.partial))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn section_names_are_distinct_and_all_parse() {
    let mut names: Vec<_> = DiagnosticsSection::ALL
        .iter()
        .map(|section| section.as_str())
        .collect();
    for name in &names {
        assert_eq!(
            DiagnosticsSection::parse(name).map(|s| s.as_str()),
            Some(*name)
        );
    }
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), DiagnosticsSection::ALL.len());
    assert_eq!(DiagnosticsSection::parse("nzb_reports"), None);
}

#[test]
fn metric_lines_keep_the_named_families_up_to_the_cap() {
    assert_eq!(
        metric_family("# HELP weaver_fixture_total Things"),
        Some("weaver_fixture_total")
    );
    assert_eq!(
        metric_family("weaver_fixture_bucket{le=\"1\"} 3"),
        Some("weaver_fixture_bucket")
    );
    assert!(belongs("weaver_fixture_bucket", "weaver_fixture"));
    assert!(!belongs("weaver_fixture_other", "weaver_fixture"));

    let exposition = (0..MAX_METRIC_LINES + 5)
        .map(|index| format!("weaver_fixture_total{{n=\"{index}\"}} 1"))
        .chain(std::iter::once("weaver_other 1".to_string()))
        .collect::<Vec<_>>()
        .join("\n");
    let (text, left_out) = metric_lines(&exposition, &["weaver_fixture_total".into()]);
    assert_eq!(text.lines().count(), MAX_METRIC_LINES);
    assert_eq!(left_out, 5);
    assert!(!text.contains("weaver_other"));
}

// Every section is served through the package's pass: the configuration
// key is redacted, and the data directory, which the stand-in table holds,
// appears in no section.
#[tokio::test]
async fn every_section_is_served_redacted_and_names_nothing() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let log = LogRingBuffer::new(4);
    let sources = seeded_sources(dir.path(), &log);
    let canonical = std::fs::canonicalize(dir.path()).expect("canonical dir");
    let spellings = [
        dir.path().to_string_lossy().into_owned(),
        canonical.to_string_lossy().into_owned(),
    ];
    for section in DiagnosticsSection::ALL {
        let read = sources
            .read_section(
                CallerScope::Admin,
                CallerIdentity::ApiKey([7; 32]),
                section,
                &["weaver_fixture".to_string()],
            )
            .await;
        assert!(!read.files.is_empty(), "{section:?} served nothing");
        let text = read_text(&read);
        assert!(!text.contains(SECRET), "{section:?}: {text}");
        for spelling in &spellings {
            assert!(!text.contains(spelling.as_str()), "{section:?}: {text}");
        }
        match section {
            DiagnosticsSection::Config => {
                assert!(text.contains("<redacted>"), "{text}");
                assert!(text.contains("<data>"), "{text}");
            }
            DiagnosticsSection::Metrics => {
                assert!(text.contains("weaver_fixture_total"), "{text}");
                assert!(text.contains("<data>/incoming"), "{text}");
                assert!(!text.contains("weaver_other_total"), "{text}");
                assert_eq!(read.metric_lines_left_out, 0);
            }
            DiagnosticsSection::Host => {
                assert!(
                    matches!(read.files[0].value, SectionValue::Json(_)),
                    "{text}"
                );
                assert!(text.contains("<data>"), "{text}");
            }
            DiagnosticsSection::Database => {
                assert!(
                    read.files
                        .iter()
                        .any(|file| matches!(file.value, SectionValue::Json(_))),
                    "{text}"
                );
            }
            // The bare schema has no runtime behind it: each GraphQL section
            // is its file, failed with a sanitized error.
            _ => assert_eq!(read.files.len(), 1, "{text}"),
        }
    }
}

// A service log line goes through the package's pass: the data directory
// becomes its stand-in.
#[tokio::test]
async fn service_log_lines_are_sanitized_as_the_package_writes_them() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let mut log = LogRingBuffer::new(4);
    let sources = seeded_sources(dir.path(), &log);
    let path = dir.path().join("incoming").display().to_string();
    writeln!(log, "{{\"timestamp\":\"t\",\"level\":\"INFO\",\"target\":\"weaver\",\"fields\":{{\"message\":\"wrote {path}\"}}}}").expect("write log");

    let service_log = sources.service_log().await;
    assert_eq!(service_log.first, 0);
    assert_eq!(service_log.records.len(), 1);
    let sanitizer = tokio::task::spawn_blocking(move || {
        let sanitizer = service_log.sanitizer().expect("table builds");
        let line = weaver_server_core::runtime::log_buffer::text_line(&service_log.records[0]);
        sanitizer.line(&line)
    })
    .await
    .expect("sanitizing task");
    assert!(!sanitizer.contains(&path), "{sanitizer}");
    assert!(sanitizer.contains("<data>/incoming"), "{sanitizer}");
}
