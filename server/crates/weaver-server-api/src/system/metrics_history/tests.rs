use super::*;

fn compress(payload: &str) -> Vec<u8> {
    zstd::bulk::compress(payload.as_bytes(), 3).unwrap()
}

#[test]
fn build_metrics_history_handles_labeled_and_unlabeled_series() {
    let rows = vec![
        MetricsScrapeRow {
            scraped_at_epoch_sec: 100,
            body_zstd: compress(
                "# TYPE weaver_pipeline_jobs gauge\n\
                     weaver_pipeline_jobs{status=\"queued\"} 3\n\
                     weaver_pipeline_current_download_speed_bytes_per_second 100\n",
            ),
        },
        MetricsScrapeRow {
            scraped_at_epoch_sec: 110,
            body_zstd: compress(
                "weaver_pipeline_jobs{status=\"queued\"} 4\n\
                     weaver_pipeline_jobs{status=\"downloading\"} 1\n\
                     weaver_pipeline_current_download_speed_bytes_per_second 200\n",
            ),
        },
    ];

    let history = build_metrics_history(
        rows,
        &[
            "weaver_pipeline_jobs".to_string(),
            "weaver_pipeline_current_download_speed_bytes_per_second".to_string(),
        ],
    )
    .unwrap();

    assert_eq!(history.timestamps, vec![100000.0, 110000.0]);
    assert_eq!(history.series.len(), 3);
    assert_eq!(history.series[0].metric, "weaver_pipeline_jobs");
    assert_eq!(
        history.series[0].labels,
        vec![MetricLabel {
            key: "status".to_string(),
            value: "downloading".to_string(),
        }]
    );
    assert_eq!(history.series[0].values, vec![0.0, 1.0]);
    assert_eq!(history.series[1].metric, "weaver_pipeline_jobs");
    assert_eq!(
        history.series[1].labels,
        vec![MetricLabel {
            key: "status".to_string(),
            value: "queued".to_string(),
        }]
    );
    assert_eq!(history.series[1].values, vec![3.0, 4.0]);
    assert_eq!(
        history.series[2].metric,
        "weaver_pipeline_current_download_speed_bytes_per_second"
    );
    assert!(history.series[2].labels.is_empty());
    assert_eq!(history.series[2].values, vec![100.0, 200.0]);
}

#[test]
fn build_metrics_history_returns_empty_for_empty_inputs() {
    let history = build_metrics_history(Vec::new(), &["metric".to_string()]).unwrap();
    assert!(history.timestamps.is_empty());
    assert!(history.series.is_empty());
}

#[test]
fn parse_prometheus_line_ignores_comments_and_bad_names() {
    let wanted = HashSet::from(["weaver_pipeline_jobs"]);
    assert!(parse_prometheus_line("# HELP ignored", &wanted).is_none());
    assert!(parse_prometheus_line("other_metric 1", &wanted).is_none());

    let sample = parse_prometheus_line(
        "weaver_pipeline_jobs{status=\"downloading\",server=\"news.example.com:563\"} 7",
        &wanted,
    )
    .unwrap();
    assert_eq!(sample.metric, "weaver_pipeline_jobs");
    assert_eq!(sample.value, 7.0);
    assert_eq!(
        sample.labels,
        vec![
            ("server".to_string(), "news.example.com:563".to_string()),
            ("status".to_string(), "downloading".to_string()),
        ]
    );
}
