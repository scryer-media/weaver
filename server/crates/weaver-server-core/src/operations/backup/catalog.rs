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
    pub restore_target_policy: RestoreTargetPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RestoreTargetPolicy {
    Replace,
    RequireEmpty,
    RequireZeroUsage,
}

macro_rules! table {
    ($name:literal, $classification:ident, $restore_target_policy:ident) => {
        BackupTableCatalogEntry {
            table: $name,
            classification: BackupTableClassification::$classification,
            restore_target_policy: RestoreTargetPolicy::$restore_target_policy,
        }
    };
}

/// Exhaustive policy for every application-owned table. Database engine tables
/// such as `sqlite_sequence` are filtered before catalog validation.
pub(crate) const BACKUP_TABLE_CATALOG: &[BackupTableCatalogEntry] = &[
    table!("_sqlx_migrations", Ignore, Replace),
    table!("schema_version", Ignore, Replace),
    table!("weaver_internal_metadata", Ignore, Replace),
    table!("settings", Export, Replace),
    table!("servers", Export, Replace),
    table!("server_download_usage", Export, RequireZeroUsage),
    table!("categories", Export, Replace),
    table!("api_keys", Export, RequireEmpty),
    table!("auth_credentials", Export, Replace),
    table!("bandwidth_usage_minute_buckets", Export, RequireEmpty),
    table!("rss_feeds", Export, RequireEmpty),
    table!("rss_rules", Export, RequireEmpty),
    table!("rss_seen_items", Export, RequireEmpty),
    table!("job_history", Export, RequireEmpty),
    table!("job_history_attributes", Export, RequireEmpty),
    table!("job_events", Export, RequireEmpty),
    table!("post_processing_extension_revisions", Export, RequireEmpty),
    table!("post_processing_profiles", Export, RequireEmpty),
    table!("post_processing_profile_steps", Export, RequireEmpty),
    table!("post_processing_profile_assignments", Export, RequireEmpty),
    table!("post_processing_job_plans", Export, RequireEmpty),
    table!("post_processing_runs", Export, RequireEmpty),
    table!("post_processing_attempts", Export, RequireEmpty),
    table!("post_processing_log_chunks", Export, RequireEmpty),
    table!("duplicate_job_snapshots", Export, RequireEmpty),
    table!("job_fingerprints", Export, RequireEmpty),
    table!("duplicate_admission_claims", Export, RequireEmpty),
    table!("submission_idempotency", Export, RequireEmpty),
    table!("forgotten_duplicate_identities", Export, RequireEmpty),
    table!("semantic_duplicate_groups", Export, RequireEmpty),
    table!("semantic_duplicate_candidates", Export, RequireEmpty),
    table!("duplicate_backfill_state", Rebuild, Replace),
    table!("active_jobs", ResetOnRestore, RequireEmpty),
    table!("active_file_progress", ResetOnRestore, RequireEmpty),
    table!("active_files", ResetOnRestore, RequireEmpty),
    table!("active_par2", ResetOnRestore, RequireEmpty),
    table!("active_par2_files", ResetOnRestore, RequireEmpty),
    table!("active_extracted", ResetOnRestore, RequireEmpty),
    table!("active_failed_extractions", ResetOnRestore, RequireEmpty),
    table!("active_extraction_chunks", ResetOnRestore, RequireEmpty),
    table!("active_archive_headers", ResetOnRestore, RequireEmpty),
    table!("active_rar_volume_facts", ResetOnRestore, RequireEmpty),
    table!("active_detected_archives", ResetOnRestore, RequireEmpty),
    table!("active_file_identities", ResetOnRestore, RequireEmpty),
    table!("active_volume_status", ResetOnRestore, RequireEmpty),
    table!("active_rar_verified_suspect", ResetOnRestore, RequireEmpty),
    table!("integration_events", ResetOnRestore, Replace),
    table!("metrics_history_chunks", ResetOnRestore, Replace),
    table!("async_operations", ResetOnRestore, Replace),
    table!("async_operation_targets", ResetOnRestore, Replace),
];

pub(crate) fn is_optional_catalog_table(table: &str) -> bool {
    table == "weaver_internal_metadata"
}

pub(crate) fn catalog_tables(classifications: &[BackupTableClassification]) -> BTreeSet<String> {
    BACKUP_TABLE_CATALOG
        .iter()
        .filter(|entry| classifications.contains(&entry.classification))
        .map(|entry| entry.table.to_string())
        .collect()
}

const TERMINAL_RUN_IDS: &str = "SELECT run_id FROM post_processing_runs
 WHERE job_id IN (SELECT job_id FROM job_history)
   AND status IN ('succeeded', 'failed', 'skipped', 'cancelled', 'interrupted')";

pub(crate) fn export_query(table: &str, columns: &[String]) -> String {
    let quoted = quote_identifier(table);
    let projection = columns
        .iter()
        .map(|column| export_column(table, column))
        .collect::<Vec<_>>()
        .join(", ");
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
            "candidate_state IN ('active', 'nonblocking', 'suppressed')
             AND job_id IN (
                SELECT job_id FROM duplicate_job_snapshots
                 WHERE lifecycle IN ('succeeded', 'failed', 'cancelled')
             )"
        }
        "semantic_duplicate_groups" => {
            "group_id IN (
                SELECT group_id FROM semantic_duplicate_candidates
                 WHERE candidate_state IN ('active', 'nonblocking', 'suppressed')
                   AND job_id IN (
                    SELECT job_id FROM duplicate_job_snapshots
                     WHERE lifecycle IN ('succeeded', 'failed', 'cancelled')
                 )
             )"
        }
        _ => return format!("SELECT {projection} FROM {quoted}"),
    };
    format!("SELECT {projection} FROM {quoted} WHERE {predicate}")
}

fn export_column(table: &str, column: &str) -> String {
    let quoted = quote_identifier(column);
    match (table, column) {
        ("job_history", "post_processing_run_id") => format!(
            "CASE WHEN {quoted} IN ({TERMINAL_RUN_IDS})
                  THEN {quoted} ELSE NULL END AS {quoted}"
        ),
        ("job_history", "post_processing_summary") => format!(
            "CASE WHEN post_processing_run_id IN ({TERMINAL_RUN_IDS})
                  THEN {quoted} ELSE 'not_run' END AS {quoted}"
        ),
        ("post_processing_runs", "rerun_of_run_id") => format!(
            "CASE WHEN {quoted} IN ({TERMINAL_RUN_IDS})
                  THEN {quoted} ELSE NULL END AS {quoted}"
        ),
        ("rss_seen_items", "job_id") => format!(
            "CASE WHEN {quoted} IN (SELECT job_id FROM job_history)
                  THEN {quoted} ELSE NULL END AS {quoted}"
        ),
        ("post_processing_extension_revisions", "discovered_source_path") => {
            format!("CAST(NULL AS TEXT) AS {quoted}")
        }
        _ => quoted,
    }
}

pub(crate) fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn is_engine_internal_table(table: &str) -> bool {
    table.starts_with("sqlite_")
}
