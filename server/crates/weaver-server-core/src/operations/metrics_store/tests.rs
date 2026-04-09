use super::*;
use std::io::Cursor;

fn decompress_blob(blob: &[u8]) -> String {
    let decoded = zstd::stream::decode_all(Cursor::new(blob)).unwrap();
    String::from_utf8(decoded).unwrap()
}

#[test]
fn record_metrics_scrape_round_trips_and_orders() {
    let db = Database::open_in_memory().unwrap();
    db.record_metrics_scrape(100, "metric_one 1\n").unwrap();
    db.record_metrics_scrape(110, "metric_one 2\n").unwrap();

    let rows = db.list_metrics_scrapes_since(0).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].scraped_at_epoch_sec, 100);
    assert_eq!(rows[1].scraped_at_epoch_sec, 110);
    assert_eq!(decompress_blob(&rows[0].body_zstd), "metric_one 1\n");
    assert_eq!(decompress_blob(&rows[1].body_zstd), "metric_one 2\n");
}

#[test]
fn record_metrics_scrape_prunes_rows_older_than_24_hours() {
    let db = Database::open_in_memory().unwrap();
    let retention = METRICS_RETENTION_SECS;

    db.record_metrics_scrape(100, "old 1\n").unwrap();
    db.record_metrics_scrape(100 + retention, "boundary 1\n")
        .unwrap();
    db.record_metrics_scrape(100 + retention + 1, "new 1\n")
        .unwrap();

    let rows = db.list_metrics_scrapes_since(0).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].scraped_at_epoch_sec, 100 + retention);
    assert_eq!(rows[1].scraped_at_epoch_sec, 100 + retention + 1);
}
