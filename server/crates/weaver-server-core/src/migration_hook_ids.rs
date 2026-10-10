pub(crate) fn is_known_migration_hook_id(hook_id: &str) -> bool {
    matches!(
        hook_id,
        "adopt_legacy_schema_to_21"
            | "upgrade_to_schema_22"
            | "upgrade_to_schema_23"
            | "upgrade_to_schema_25"
            | "restart_active_jobs_drop_active_segments_v28"
            | "move_isp_cap_to_system_egress_v53"
            | "move_script_wiring_to_instances_v55"
            | "default_unwanted_extensions_v56"
            | "raise_script_concurrency_v58"
    )
}

pub(crate) fn validate_migration_hook_id(hook_id: &str) -> Result<(), String> {
    if is_known_migration_hook_id(hook_id) {
        Ok(())
    } else {
        Err(format!("unknown migration hook id '{hook_id}'"))
    }
}
