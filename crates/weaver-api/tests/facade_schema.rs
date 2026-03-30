mod common;

use common::TestHarness;

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
        sdl.contains(
            "historyItems(filter: QueueFilterInput, first: Int, after: String): [HistoryItem!]!"
        ),
        "historyItems query should be present"
    );
    assert!(
        sdl.contains("metricsHistory(minutes: Int!, metrics: [String!]!): MetricsHistoryResult!"),
        "metricsHistory query should be present"
    );
    assert!(
        sdl.contains("submitNzb(input: SubmitNzbInput!): SubmissionResult!"),
        "submitNzb mutation should use facade input"
    );
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
}
