use super::*;

#[tokio::test]
async fn quota_metering_migration_preserves_existing_sqlite_usage() {
    use sqlx::Connection;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("legacy.db");
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE bandwidth_usage_minute_buckets (
            bucket_epoch_minute INTEGER PRIMARY KEY,
            payload_bytes INTEGER NOT NULL
         );
         INSERT INTO bandwidth_usage_minute_buckets VALUES (100, 10), (101, 20);",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../db/migrations/0051_quota_metering/sqlite.sql"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    let rows: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT bucket_epoch_minute, metered, payload_bytes
           FROM bandwidth_usage_minute_buckets ORDER BY bucket_epoch_minute",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert_eq!(rows, [(100, 1, 10), (101, 1, 20)]);
    sqlx::query("INSERT INTO bandwidth_usage_minute_buckets VALUES (100, 0, 40)")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO bandwidth_usage_minute_buckets VALUES (100, 1, 99)")
            .execute(&mut connection)
            .await
            .is_err()
    );
    connection.close().await.unwrap();
    let mut reopened = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    let totals: (i64, i64) = sqlx::query_as(
        "SELECT SUM(payload_bytes), SUM(CASE WHEN metered = 1 THEN payload_bytes ELSE 0 END)
           FROM bandwidth_usage_minute_buckets",
    )
    .fetch_one(&mut reopened)
    .await
    .unwrap();
    assert_eq!(totals, (70, 30));
    reopened.close().await.unwrap();
}

#[test]
fn bandwidth_usage_minute_buckets_roundtrip_and_prune() {
    let db = Database::open_in_memory().unwrap();

    db.add_bandwidth_usage_minute(100, 10).unwrap();
    db.add_bandwidth_usage_minute(100, 5).unwrap();
    db.add_bandwidth_usage_minute(101, 20).unwrap();

    assert_eq!(db.sum_bandwidth_usage_minutes(100, 101).unwrap(), 15);
    assert_eq!(db.sum_bandwidth_usage_minutes(100, 102).unwrap(), 35);
    assert_eq!(db.sum_bandwidth_usage_minutes(99, 100).unwrap(), 0);

    let deleted = db.prune_bandwidth_usage_before(101).unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(db.sum_bandwidth_usage_minutes(100, 102).unwrap(), 20);
}
