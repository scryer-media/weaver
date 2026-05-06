mod common;

use common::TestHarness;

const DIAGNOSTICS_ENABLED: bool = cfg!(weaver_diagnostics);

#[tokio::test]
async fn public_facade_schema_exposes_core_surface() {
    let h = TestHarness::new().await;
    let sdl = h.schema.sdl();

    assert!(
        sdl.contains("type QueueItem"),
        "QueueItem type should be present"
    );
    assert!(
        sdl.contains("type HistoryItem"),
        "HistoryItem type should be present"
    );
    assert!(
        sdl.contains(
            "queueItems(filter: QueueFilterInput, first: Int, after: String): [QueueItem!]!"
        ),
        "queueItems query should be present"
    );
    assert!(
        sdl.contains("queueSnapshot(filter: QueueFilterInput): QueueSnapshot!"),
        "queueSnapshot query should be present"
    );
    assert!(
        sdl.contains("latestQueueCursor: String!"),
        "latestQueueCursor query should be present"
    );
    assert!(
        sdl.contains(
            "historyItems(filter: QueueFilterInput, first: Int, after: String): [HistoryItem!]!"
        ),
        "historyItems query should be present"
    );
    assert!(
        sdl.contains("jobDetailSnapshot(jobId: Int!): JobDetailSnapshot!"),
        "jobDetailSnapshot query should be present"
    );
    assert!(
        sdl.contains("historyDeleteOperations(activeOnly: Boolean"),
        "historyDeleteOperations query should be present"
    );
    assert!(
        sdl.contains("metricsHistory(range: MetricsHistoryRangeGql!): MetricsHistoryResult!"),
        "metricsHistory query should be present"
    );
    assert!(
        sdl.contains("submitNzb(input: SubmitNzbInput!): SubmissionResult!"),
        "submitNzb mutation should use facade input"
    );
    assert!(
        sdl.contains(
            "acceptHistoryDelete(input: AcceptHistoryDeleteInput!): HistoryDeleteAcceptance!"
        ),
        "acceptHistoryDelete mutation should be present"
    );
    if DIAGNOSTICS_ENABLED {
        assert!(
            sdl.contains("startDiagnosticRedownload("),
            "diagnostic redownload mutation should be present when diagnostics are enabled"
        );
        assert!(
            sdl.contains("diagnosticRun: HistoryDiagnosticRun"),
            "history item should expose active diagnostic state when diagnostics are enabled"
        );
        assert!(
            sdl.contains("lastDiagnosticId: String"),
            "history item should expose the last diagnostic id when diagnostics are enabled"
        );
    } else {
        assert!(
            !sdl.contains("startDiagnosticRedownload("),
            "diagnostic redownload mutation should be hidden when diagnostics are disabled"
        );
        assert!(
            !sdl.contains("diagnosticRun: HistoryDiagnosticRun"),
            "history item should hide active diagnostic state when diagnostics are disabled"
        );
        assert!(
            !sdl.contains("lastDiagnosticId: String"),
            "history item should hide last diagnostic metadata when diagnostics are disabled"
        );
    }
    assert!(
        !sdl.contains("submitNzbLegacy"),
        "legacy submitNzbLegacy mutation should not be present"
    );
    assert!(
        sdl.contains("queueSnapshots(filter: QueueFilterInput): QueueSnapshot!"),
        "queueSnapshots subscription should be present"
    );
    assert!(
        sdl.contains("queueEvents(after: String, filter: QueueFilterInput): QueueEvent!"),
        "queueEvents subscription should be present"
    );
    assert!(
        sdl.contains("jobDetailUpdates(jobId: Int!): JobDetailSnapshot!"),
        "jobDetailUpdates subscription should be present"
    );
    assert!(
        sdl.contains("systemMetricsUpdates: SystemMetricsSnapshot!"),
        "systemMetricsUpdates subscription should be present"
    );
    assert!(
        sdl.contains("attributeEquals: AttributeInput"),
        "QueueFilterInput should expose exact attribute matching"
    );
}
