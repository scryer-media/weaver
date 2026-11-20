// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod migration_assets;
pub mod migration_hook_ids;
pub mod schema_upgrade;

pub const EMBEDDED_MIGRATION_CATALOG: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/migration_catalog.json.zst"));
pub const EMBEDDED_MIGRATION_PAYLOAD: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/migration_payload.bin.zst"));
pub const MIGRATION_21_BASE_SCHEMA_SQL: &str =
    include_str!("db/migrations/0021_legacy_schema_parity/fresh.sql");
pub const MIGRATION_22_SCHEMA_SQL: &str =
    include_str!("db/migrations/0022_diagnostic_and_async_state/schema.sql");
