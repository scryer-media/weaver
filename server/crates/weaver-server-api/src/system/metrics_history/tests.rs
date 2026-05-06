use super::*;

use weaver_server_core::{
    CounterRollupValue, MetricsHistoryQueryData, MetricsHistoryQueryResult, RawMetricsHistoryPoint,
    RollupMetricsHistoryPoint,
};

#[test]
fn build_metrics_history_returns_raw_actual_series() {
    let result = build_metrics_history(MetricsHistoryQueryResult {
        resolution_sec: 10,
        data: MetricsHistoryQueryData::Raw(vec![
            RawMetricsHistoryPoint {
                timestamp_epoch_sec: 100,
                counter_values: [1.0; 11],
                gauge_values: [2.0; 13],
                job_status_values: [0.0; 12],
            },
            RawMetricsHistoryPoint {
                timestamp_epoch_sec: 110,
                counter_values: [3.0; 11],
                gauge_values: [4.0; 13],
                job_status_values: [1.0; 12],
            },
        ]),
    })
    .unwrap();

    assert_eq!(result.resolution_sec, 10);
    assert_eq!(result.timestamps, vec![100_000.0, 110_000.0]);
    assert_eq!(result.series[0].variant, MetricSeriesVariant::Actual);
    assert_eq!(result.series[0].values, vec![1.0, 3.0]);

    let job_series = result
        .series
        .iter()
        .find(|series| {
            series.metric == JOB_STATUS_HISTORY_METRIC
                && series.labels[0].value == JOB_STATUS_KEYS[0]
        })
        .unwrap();
    assert_eq!(job_series.variant, MetricSeriesVariant::Actual);
    assert_eq!(job_series.values, vec![0.0, 1.0]);
}

#[test]
fn build_metrics_history_returns_rollup_avg_and_peak_series() {
    let result = build_metrics_history(MetricsHistoryQueryResult {
        resolution_sec: 300,
        data: MetricsHistoryQueryData::Rollup(vec![RollupMetricsHistoryPoint {
            timestamp_epoch_sec: 300,
            counter_values: [CounterRollupValue {
                end: 10.0,
                avg_rate: 2.5,
                peak_rate: 4.0,
                avg_rate_weight_sec: 290.0,
            }; 11],
            gauge_values: [weaver_server_core::GaugeRollupValue {
                avg: 6.0,
                peak: 9.0,
                sample_count: 29,
            }; 13],
            job_status_values: [weaver_server_core::GaugeRollupValue {
                avg: 1.5,
                peak: 3.0,
                sample_count: 29,
            }; 12],
        }]),
    })
    .unwrap();

    assert_eq!(result.resolution_sec, 300);
    assert_eq!(result.timestamps, vec![300_000.0]);
    let counter_avg = result
        .series
        .iter()
        .find(|series| {
            series.metric == COUNTER_METRIC_KEYS[0] && series.variant == MetricSeriesVariant::Avg
        })
        .unwrap();
    assert_eq!(counter_avg.values, vec![2.5]);
    let counter_peak = result
        .series
        .iter()
        .find(|series| {
            series.metric == COUNTER_METRIC_KEYS[0] && series.variant == MetricSeriesVariant::Peak
        })
        .unwrap();
    assert_eq!(counter_peak.values, vec![4.0]);

    let gauge_peak = result
        .series
        .iter()
        .find(|series| {
            series.metric == GAUGE_METRIC_KEYS[0] && series.variant == MetricSeriesVariant::Peak
        })
        .unwrap();
    assert_eq!(gauge_peak.values, vec![9.0]);
}

#[test]
fn tier_for_range_matches_expected_resolution() {
    assert_eq!(
        tier_for_range(MetricsHistoryRangeGql::TenMinutes),
        MetricsHistoryTier::Raw10s
    );
    assert_eq!(
        tier_for_range(MetricsHistoryRangeGql::OneHour),
        MetricsHistoryTier::Raw10s
    );
    assert_eq!(
        tier_for_range(MetricsHistoryRangeGql::SixHours),
        MetricsHistoryTier::Rollup5m
    );
    assert_eq!(
        tier_for_range(MetricsHistoryRangeGql::TwentyFourHours),
        MetricsHistoryTier::Rollup5m
    );
    assert_eq!(
        tier_for_range(MetricsHistoryRangeGql::SevenDays),
        MetricsHistoryTier::Rollup1h
    );
    assert_eq!(
        tier_for_range(MetricsHistoryRangeGql::ThirtyDays),
        MetricsHistoryTier::Rollup1h
    );
}
