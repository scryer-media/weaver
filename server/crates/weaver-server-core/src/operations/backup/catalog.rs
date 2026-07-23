use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackupTableClassification {
    Export,
    ResetOnRestore,
    Rebuild,
    Ignore,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BackupTableCatalogEntry {
    pub table: &'static str,
    pub classification: BackupTableClassification,
}

macro_rules! table {
    ($name:literal, $classification:ident) => {
        BackupTableCatalogEntry {
            table: $name,
            classification: BackupTableClassification::$classification,
        }
    };
}

/// Exhaustive policy for every application-owned table. Database engine tables
/// such as `sqlite_sequence` are filtered before catalog validation.
pub(crate) const BACKUP_TABLE_CATALOG: &[BackupTableCatalogEntry] = &[
    table!("_sqlx_migrations", Ignore),
    table!("schema_version", Ignore),
    table!("weaver_internal_metadata", Ignore),
    table!("postgres_only_marker", Ignore),
    table!("settings", Export),
    table!("servers", Export),
    table!("server_download_usage", Export),
    table!("categories", Export),
    table!("api_keys", Export),
    table!("auth_credentials", Export),
    table!("bandwidth_usage_minute_buckets", Export),
    table!("rss_feeds", Export),
    table!("rss_rules", Export),
    table!("rss_seen_items", Export),
    table!("job_history", Export),
    table!("job_history_attributes", Export),
    table!("job_events", Export),
    table!("post_processing_extension_revisions", Export),
    table!("post_processing_profiles", Export),
    table!("post_processing_profile_steps", Export),
    table!("post_processing_profile_assignments", Export),
    table!("post_processing_job_plans", Export),
    table!("post_processing_runs", Export),
    table!("post_processing_attempts", Export),
    table!("post_processing_log_chunks", Export),
    table!("duplicate_job_snapshots", Export),
    table!("job_fingerprints", Export),
    table!("duplicate_admission_claims", Export),
    table!("submission_idempotency", Export),
    table!("forgotten_duplicate_identities", Export),
    table!("semantic_duplicate_groups", Export),
    table!("semantic_duplicate_candidates", Export),
    table!("duplicate_backfill_state", Rebuild),
    table!("active_jobs", ResetOnRestore),
    table!("active_segments", ResetOnRestore),
    table!("active_file_progress", ResetOnRestore),
    table!("active_files", ResetOnRestore),
    table!("active_par2", ResetOnRestore),
    table!("active_par2_files", ResetOnRestore),
    table!("active_extracted", ResetOnRestore),
    table!("active_failed_extractions", ResetOnRestore),
    table!("active_extraction_chunks", ResetOnRestore),
    table!("active_archive_headers", ResetOnRestore),
    table!("active_rar_volume_facts", ResetOnRestore),
    table!("active_detected_archives", ResetOnRestore),
    table!("active_file_identities", ResetOnRestore),
    table!("active_volume_status", ResetOnRestore),
    table!("active_rar_verified_suspect", ResetOnRestore),
    table!("integration_events", ResetOnRestore),
    table!("metrics_scrapes", ResetOnRestore),
    table!("metrics_history_chunks", ResetOnRestore),
    table!("diagnostic_runs", ResetOnRestore),
    table!("async_operations", ResetOnRestore),
    table!("async_operation_targets", ResetOnRestore),
];

pub(crate) fn catalog_tables(classifications: &[BackupTableClassification]) -> BTreeSet<String> {
    BACKUP_TABLE_CATALOG
        .iter()
        .filter(|entry| classifications.contains(&entry.classification))
        .map(|entry| entry.table.to_string())
        .collect()
}

pub(crate) fn export_query(table: &str) -> String {
    let quoted = quote_identifier(table);
    let predicate = match table {
        "settings" => "key <> 'nzbget.scheduled_resume_at'",
        "job_history_attributes" | "job_events" | "post_processing_job_plans" => {
            "job_id IN (SELECT job_id FROM job_history)"
        }
        "post_processing_runs" => {
            "job_id IN (SELECT job_id FROM job_history)
             AND status IN ('succeeded', 'failed', 'skipped', 'cancelled', 'interrupted')"
        }
        "post_processing_attempts" => {
            "run_id IN (
                SELECT run_id FROM post_processing_runs
                 WHERE job_id IN (SELECT job_id FROM job_history)
                   AND status IN ('succeeded', 'failed', 'skipped', 'cancelled', 'interrupted')
             )"
        }
        "post_processing_log_chunks" => {
            "attempt_id IN (
                SELECT attempt_id FROM post_processing_attempts
                 WHERE run_id IN (
                    SELECT run_id FROM post_processing_runs
                     WHERE job_id IN (SELECT job_id FROM job_history)
                       AND status IN ('succeeded', 'failed', 'skipped', 'cancelled', 'interrupted')
                 )
             )"
        }
        "duplicate_job_snapshots" => "lifecycle IN ('succeeded', 'failed', 'cancelled')",
        "job_fingerprints" | "duplicate_admission_claims" | "submission_idempotency" => {
            "job_id IN (
                SELECT job_id FROM duplicate_job_snapshots
                 WHERE lifecycle IN ('succeeded', 'failed', 'cancelled')
             )"
        }
        "semantic_duplicate_candidates" => {
            "candidate_state IN ('nonblocking', 'suppressed')
             AND job_id IN (
                SELECT job_id FROM duplicate_job_snapshots
                 WHERE lifecycle IN ('succeeded', 'failed', 'cancelled')
             )"
        }
        "semantic_duplicate_groups" => {
            "group_id IN (
                SELECT group_id FROM semantic_duplicate_candidates
                 WHERE candidate_state IN ('nonblocking', 'suppressed')
                   AND job_id IN (
                    SELECT job_id FROM duplicate_job_snapshots
                     WHERE lifecycle IN ('succeeded', 'failed', 'cancelled')
                 )
             )"
        }
        _ => return format!("SELECT * FROM {quoted}"),
    };
    format!("SELECT * FROM {quoted} WHERE {predicate}")
}

pub(crate) fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn is_engine_internal_table(table: &str) -> bool {
    table.starts_with("sqlite_")
}
