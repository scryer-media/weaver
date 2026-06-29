pub(crate) fn is_known_migration_hook_id(hook_id: &str) -> bool {
    matches!(
        hook_id,
        "adopt_legacy_schema_to_21"
            | "upgrade_to_schema_22"
            | "upgrade_to_schema_23"
            | "upgrade_to_schema_25"
            | "restart_active_jobs_drop_active_segments_v28"
    )
}

pub(crate) fn validate_migration_hook_id(hook_id: &str) -> Result<(), String> {
    if is_known_migration_hook_id(hook_id) {
        Ok(())
    } else {
        Err(format!("unknown migration hook id '{hook_id}'"))
    }
}
