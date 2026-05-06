use weaver_server_core::{
    COUNTER_METRIC_KEYS, GAUGE_METRIC_KEYS, JOB_STATUS_KEYS, MetricsHistoryQueryData,
    MetricsHistoryQueryResult, MetricsHistoryTier,
};

use crate::system::types::{
    MetricLabel, MetricSeries, MetricSeriesVariant, MetricsHistoryRangeGql, MetricsHistoryResult,
};

const JOB_STATUS_HISTORY_METRIC: &str = "weaver_pipeline_jobs";

pub(crate) fn tier_for_range(range: MetricsHistoryRangeGql) -> MetricsHistoryTier {
    match range {
        MetricsHistoryRangeGql::TenMinutes | MetricsHistoryRangeGql::OneHour => {
            MetricsHistoryTier::Raw10s
        }
        MetricsHistoryRangeGql::SixHours | MetricsHistoryRangeGql::TwentyFourHours => {
            MetricsHistoryTier::Rollup5m
        }
        MetricsHistoryRangeGql::SevenDays | MetricsHistoryRangeGql::ThirtyDays => {
            MetricsHistoryTier::Rollup1h
        }
    }
}

pub(crate) fn build_metrics_history(
    result: MetricsHistoryQueryResult,
) -> Result<MetricsHistoryResult, String> {
    match result.data {
        MetricsHistoryQueryData::Raw(points) => {
            Ok(build_raw_metrics_history(result.resolution_sec, &points))
        }
        MetricsHistoryQueryData::Rollup(points) => {
            Ok(build_rollup_metrics_history(result.resolution_sec, &points))
        }
    }
}

fn build_raw_metrics_history(
    resolution_sec: i64,
    points: &[weaver_server_core::RawMetricsHistoryPoint],
) -> MetricsHistoryResult {
    let timestamps = points
        .iter()
        .map(|point| point.timestamp_epoch_sec as f64 * 1000.0)
        .collect();
    let mut series = Vec::new();

    for (index, metric) in COUNTER_METRIC_KEYS.iter().enumerate() {
        series.push(MetricSeries {
            metric: (*metric).to_string(),
            labels: Vec::new(),
            variant: MetricSeriesVariant::Actual,
            values: points
                .iter()
                .map(|point| point.counter_values[index])
                .collect(),
        });
    }

    for (index, metric) in GAUGE_METRIC_KEYS.iter().enumerate() {
        series.push(MetricSeries {
            metric: (*metric).to_string(),
            labels: Vec::new(),
            variant: MetricSeriesVariant::Actual,
            values: points
                .iter()
                .map(|point| point.gauge_values[index])
                .collect(),
        });
    }

    for (index, status) in JOB_STATUS_KEYS.iter().enumerate() {
        series.push(MetricSeries {
            metric: JOB_STATUS_HISTORY_METRIC.to_string(),
            labels: vec![MetricLabel {
                key: "status".to_string(),
                value: (*status).to_string(),
            }],
            variant: MetricSeriesVariant::Actual,
            values: points
                .iter()
                .map(|point| point.job_status_values[index])
                .collect(),
        });
    }

    MetricsHistoryResult {
        timestamps,
        resolution_sec: resolution_sec as i32,
        series,
    }
}

fn build_rollup_metrics_history(
    resolution_sec: i64,
    points: &[weaver_server_core::RollupMetricsHistoryPoint],
) -> MetricsHistoryResult {
    let timestamps = points
        .iter()
        .map(|point| point.timestamp_epoch_sec as f64 * 1000.0)
        .collect();
    let mut series = Vec::new();

    for (index, metric) in COUNTER_METRIC_KEYS.iter().enumerate() {
        series.push(MetricSeries {
            metric: (*metric).to_string(),
            labels: Vec::new(),
            variant: MetricSeriesVariant::Avg,
            values: points
                .iter()
                .map(|point| point.counter_values[index].avg_rate)
                .collect(),
        });
        series.push(MetricSeries {
            metric: (*metric).to_string(),
            labels: Vec::new(),
            variant: MetricSeriesVariant::Peak,
            values: points
                .iter()
                .map(|point| point.counter_values[index].peak_rate)
                .collect(),
        });
    }

    for (index, metric) in GAUGE_METRIC_KEYS.iter().enumerate() {
        series.push(MetricSeries {
            metric: (*metric).to_string(),
            labels: Vec::new(),
            variant: MetricSeriesVariant::Avg,
            values: points
                .iter()
                .map(|point| point.gauge_values[index].avg)
                .collect(),
        });
        series.push(MetricSeries {
            metric: (*metric).to_string(),
            labels: Vec::new(),
            variant: MetricSeriesVariant::Peak,
            values: points
                .iter()
                .map(|point| point.gauge_values[index].peak)
                .collect(),
        });
    }

    for (index, status) in JOB_STATUS_KEYS.iter().enumerate() {
        let labels = vec![MetricLabel {
            key: "status".to_string(),
            value: (*status).to_string(),
        }];
        series.push(MetricSeries {
            metric: JOB_STATUS_HISTORY_METRIC.to_string(),
            labels: labels.clone(),
            variant: MetricSeriesVariant::Avg,
            values: points
                .iter()
                .map(|point| point.job_status_values[index].avg)
                .collect(),
        });
        series.push(MetricSeries {
            metric: JOB_STATUS_HISTORY_METRIC.to_string(),
            labels,
            variant: MetricSeriesVariant::Peak,
            values: points
                .iter()
                .map(|point| point.job_status_values[index].peak)
                .collect(),
        });
    }

    MetricsHistoryResult {
        timestamps,
        resolution_sec: resolution_sec as i32,
        series,
    }
}

#[cfg(test)]
mod tests;
