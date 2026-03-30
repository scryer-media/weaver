use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::OnceLock;

use regex::Regex;
use weaver_state::MetricsScrapeRow;

use crate::types::{MetricLabel, MetricSeries, MetricsHistoryResult};

#[derive(Debug, Clone, PartialEq)]
struct ParsedMetricSample {
    metric: String,
    labels: Vec<(String, String)>,
    value: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricSeriesKey {
    metric: String,
    labels: Vec<(String, String)>,
}

pub(crate) fn build_metrics_history(
    rows: Vec<MetricsScrapeRow>,
    requested_metrics: &[String],
) -> Result<MetricsHistoryResult, String> {
    if requested_metrics.is_empty() || rows.is_empty() {
        return Ok(MetricsHistoryResult::default());
    }

    let wanted: HashSet<&str> = requested_metrics.iter().map(String::as_str).collect();
    let metric_rank: HashMap<&str, usize> = requested_metrics
        .iter()
        .enumerate()
        .map(|(idx, metric)| (metric.as_str(), idx))
        .collect();

    let mut timestamps = Vec::with_capacity(rows.len());
    let mut series_by_key: HashMap<MetricSeriesKey, Vec<f64>> = HashMap::new();

    for row in rows {
        timestamps.push(row.scraped_at_epoch_sec as f64 * 1000.0);
        let sample_index = timestamps.len() - 1;
        for values in series_by_key.values_mut() {
            values.push(0.0);
        }

        let decoded = zstd::stream::decode_all(Cursor::new(&row.body_zstd))
            .map_err(|error| format!("failed to decompress metrics scrape: {error}"))?;
        let body = std::str::from_utf8(&decoded)
            .map_err(|error| format!("metrics scrape was not valid UTF-8: {error}"))?;

        for line in body.lines() {
            let Some(sample) = parse_prometheus_line(line, &wanted) else {
                continue;
            };
            let key = MetricSeriesKey {
                metric: sample.metric,
                labels: sample.labels,
            };
            let values = series_by_key
                .entry(key)
                .or_insert_with(|| vec![0.0; timestamps.len()]);
            if values.len() < timestamps.len() {
                values.resize(timestamps.len(), 0.0);
            }
            values[sample_index] = sample.value;
        }
    }

    let mut series_entries: Vec<(MetricSeriesKey, Vec<f64>)> = series_by_key.into_iter().collect();
    series_entries.sort_by(|(left_key, _), (right_key, _)| {
        let left_rank = metric_rank
            .get(left_key.metric.as_str())
            .copied()
            .unwrap_or(usize::MAX);
        let right_rank = metric_rank
            .get(right_key.metric.as_str())
            .copied()
            .unwrap_or(usize::MAX);

        left_rank
            .cmp(&right_rank)
            .then_with(|| left_key.metric.cmp(&right_key.metric))
            .then_with(|| left_key.labels.cmp(&right_key.labels))
    });

    Ok(MetricsHistoryResult {
        timestamps,
        series: series_entries
            .into_iter()
            .map(|(key, values)| MetricSeries {
                metric: key.metric,
                labels: key
                    .labels
                    .into_iter()
                    .map(|(key, value)| MetricLabel { key, value })
                    .collect(),
                values,
            })
            .collect(),
    })
}

fn parse_prometheus_line(line: &str, wanted: &HashSet<&str>) -> Option<ParsedMetricSample> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let mut parts = trimmed.split_whitespace();
    let metric_part = parts.next()?;
    let value_part = parts.next()?;
    let (metric, labels) = parse_metric_and_labels(metric_part)?;
    if !wanted.contains(metric.as_str()) {
        return None;
    }

    Some(ParsedMetricSample {
        metric,
        labels,
        value: parse_metric_value(value_part)?,
    })
}

fn parse_metric_and_labels(metric_part: &str) -> Option<(String, Vec<(String, String)>)> {
    let Some(open_brace) = metric_part.find('{') else {
        return Some((metric_part.to_string(), Vec::new()));
    };
    let close_brace = metric_part.rfind('}')?;
    if close_brace <= open_brace {
        return None;
    }

    let metric = metric_part[..open_brace].to_string();
    let mut labels = parse_metric_labels(&metric_part[open_brace + 1..close_brace]);
    labels.sort();
    Some((metric, labels))
}

fn parse_metric_labels(labels_part: &str) -> Vec<(String, String)> {
    static LABEL_RE: OnceLock<Regex> = OnceLock::new();
    let regex = LABEL_RE.get_or_init(|| {
        Regex::new(r#"([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)""#)
            .expect("label regex should compile")
    });

    regex
        .captures_iter(labels_part)
        .filter_map(|captures| {
            let key = captures.get(1)?.as_str().to_string();
            let value = unescape_label_value(captures.get(2)?.as_str());
            Some((key, value))
        })
        .collect()
}

fn unescape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\\') => out.push('\\'),
            Some('"') => out.push('"'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }

    out
}

fn parse_metric_value(value: &str) -> Option<f64> {
    match value {
        "+Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => value.parse::<f64>().ok(),
    }
}

#[cfg(test)]
mod tests {
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
}
