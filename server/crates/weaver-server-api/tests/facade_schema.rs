mod common;

use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema};
use common::TestHarness;
use serde_json::Value;
use weaver_server_api::context::{
    GRAPHQL_MAX_COMPLEXITY, GRAPHQL_MAX_DEPTH, apply_graphql_query_guards,
};

struct DeepQuery;

#[Object]
impl DeepQuery {
    async fn child(&self) -> DeepQuery {
        DeepQuery
    }

    async fn value(&self) -> &str {
        "ok"
    }
}

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
    } else {
        assert!(
            !sdl.contains("startDiagnosticRedownload("),
            "diagnostic redownload mutation should be hidden when diagnostics are disabled"
        );
    }
    assert!(
        sdl.contains("diagnosticRun: HistoryDiagnosticRun"),
        "history item should always expose diagnostic run state, falling back to null when diagnostics are disabled"
    );
    assert!(
        sdl.contains("lastDiagnosticId: String"),
        "history item should always expose the last diagnostic id, falling back to null when diagnostics are disabled"
    );
    assert!(
        sdl.contains("lastDiagnosticUploadedAt: DateTime"),
        "history item should always expose the last diagnostic upload timestamp, falling back to null when diagnostics are disabled"
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

#[tokio::test]
async fn graphql_introspection_queries_are_disabled() {
    let h = TestHarness::new().await;

    let resp = h
        .execute("{ __schema { queryType { name } mutationType { name } } }")
        .await;

    assert!(
        resp.errors.is_empty(),
        "async-graphql disables schema introspection by returning null, got {:?}",
        resp.errors
    );
    let data = resp.data.into_json().unwrap();
    assert_eq!(data["__schema"], Value::Null);
}

#[tokio::test]
async fn graphql_query_complexity_is_limited() {
    let h = TestHarness::new().await;
    let fields = (0..=GRAPHQL_MAX_COMPLEXITY)
        .map(|index| format!("f{index}: latestQueueCursor"))
        .collect::<Vec<_>>()
        .join("\n");
    let query = format!("query TooComplex {{ {fields} }}");

    let resp = h.execute(&query).await;

    assert!(
        resp.errors
            .iter()
            .any(|error| error.message.contains("Query is too complex")),
        "expected complexity limit error, got {:?}",
        resp.errors
    );
}

#[tokio::test]
async fn graphql_query_depth_is_limited() {
    let schema =
        apply_graphql_query_guards(Schema::build(DeepQuery, EmptyMutation, EmptySubscription))
            .finish();
    let mut selection = "value".to_string();
    for _ in 0..=GRAPHQL_MAX_DEPTH {
        selection = format!("child {{ {selection} }}");
    }
    let query = format!("query TooDeep {{ {selection} }}");

    let resp = schema.execute(&query).await;

    assert!(
        resp.errors.iter().any(|error| {
            let message = error.message.to_ascii_lowercase();
            message.contains("depth") || message.contains("nested")
        }),
        "expected depth limit error, got {:?}",
        resp.errors
    );
}
