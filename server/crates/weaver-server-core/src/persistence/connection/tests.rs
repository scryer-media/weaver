use super::*;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::bandwidth::{IspBandwidthCapConfig, IspBandwidthCapPeriod, IspBandwidthCapWeekday};
use crate::categories::CategoryConfig;
use crate::persistence::database_target::DatabaseTarget;
use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRuntime, StoreDatastore};
use crate::rss::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenItemRow};
use crate::servers::ServerConfig;
use crate::settings::{BufferPoolOverrides, Config, RetryOverrides, TunerOverrides};

fn fetch_i64(db: &Database, sql: &'static str, args: Vec<SqlArg>) -> i64 {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), sql, &args)
            .await?
            .ok_or_else(|| StateError::Database(format!("query returned no rows: {sql}")))?;
        row.i64_at(0)
    })
    .unwrap()
}

fn fetch_text(db: &Database, sql: &'static str, args: Vec<SqlArg>) -> String {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), sql, &args)
            .await?
            .ok_or_else(|| StateError::Database(format!("query returned no rows: {sql}")))?;
        row.text("value")
    })
    .unwrap()
}

fn execute(db: &Database, sql: &'static str, args: Vec<SqlArg>) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.read_exec(), sql, &args).await?;
        Ok(())
    })
    .unwrap()
}

fn current_schema_version() -> i64 {
    crate::schema_migrations::embedded_catalog()
        .unwrap()
        .migrations
        .iter()
        .map(|migration| migration.version)
        .max()
        .unwrap()
}

fn postgres_sample_job(job_id: crate::jobs::ids::JobId) -> ActiveJob {
    ActiveJob {
        job_id,
        nzb_hash: [0xA5; 32],
        nzb_path: PathBuf::from(format!("/tmp/postgres-{}.nzb", job_id.0)),
        nzb_zstd: crate::ingest::compress_nzb_bytes(
            br#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"/>"#,
        )
        .unwrap(),
        output_dir: PathBuf::from(format!("/tmp/postgres-{}", job_id.0)),
        created_at: 1_700_000_000 + job_id.0,
        category: Some("postgres".to_string()),
        metadata: vec![("engine".to_string(), "postgres".to_string())],
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    }
}

fn postgres_sample_history(job_id: crate::jobs::ids::JobId) -> JobHistoryRow {
    JobHistoryRow {
        job_id: job_id.0,
        job_hash: Some(vec![0xA5; 32]),
        name: format!("postgres-{}", job_id.0),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 123,
        downloaded_bytes: 123,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: Some("postgres".to_string()),
        output_dir: Some(format!("/tmp/postgres-{}", job_id.0)),
        nzb_path: Some(format!("/tmp/postgres-{}.nzb", job_id.0)),
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: Some("[[\"engine\",\"postgres\"]]".to_string()),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct CanonicalSchema {
    tables: BTreeMap<String, CanonicalTable>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct CanonicalTable {
    columns: BTreeMap<String, CanonicalColumn>,
    primary_key: Vec<String>,
    foreign_keys: BTreeSet<CanonicalForeignKey>,
    unique_indexes: BTreeSet<CanonicalIndex>,
    indexes: BTreeSet<CanonicalIndex>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalColumn {
    data_type: String,
    not_null: bool,
    default_value: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CanonicalForeignKey {
    columns: Vec<String>,
    foreign_table: String,
    foreign_columns: Vec<String>,
    on_delete: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CanonicalIndex {
    expressions: Vec<String>,
}

type ForeignKeyGroup = (String, String, Vec<(i64, String, String)>);

fn extract_canonical_schema(db: &Database) -> CanonicalSchema {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        match datastore.engine() {
            SqlEngine::Sqlite => extract_sqlite_schema(&datastore).await,
            SqlEngine::Postgres => extract_postgres_schema(&datastore).await,
        }
    })
    .unwrap()
}

async fn extract_sqlite_schema(datastore: &StoreDatastore) -> Result<CanonicalSchema, StateError> {
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        "SELECT name
           FROM sqlite_master
          WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
            AND name NOT LIKE '_sqlx_%'
          ORDER BY name",
        &[],
    )
    .await?;
    let mut schema = CanonicalSchema::default();

    for row in rows {
        let table_name = row.text("name")?;
        let mut table = CanonicalTable::default();
        let column_rows = SqlRuntime::fetch_all(
            datastore.read_exec(),
            &format!(
                "PRAGMA table_info({})",
                quote_sqlite_identifier(&table_name)
            ),
            &[],
        )
        .await?;
        let mut pk_columns = Vec::new();
        for column_row in column_rows {
            let column_name = column_row.text("name")?;
            let raw_type = column_row.text("type")?;
            let pk_position = column_row.i64("pk")?;
            let data_type = normalize_column_type(&table_name, &column_name, &raw_type);
            let default_value = normalize_default(
                &table_name,
                &column_name,
                &data_type,
                column_row.opt_text("dflt_value")?,
            );
            table.columns.insert(
                column_name.clone(),
                CanonicalColumn {
                    data_type,
                    not_null: column_row.i64("notnull")? != 0 || pk_position > 0,
                    default_value,
                },
            );
            if pk_position > 0 {
                pk_columns.push((pk_position, column_name));
            }
        }
        pk_columns.sort_by_key(|(position, _)| *position);
        table.primary_key = pk_columns
            .into_iter()
            .map(|(_, column_name)| column_name)
            .collect();

        table.foreign_keys = extract_sqlite_foreign_keys(datastore, &table_name).await?;
        let (unique_indexes, indexes) = extract_sqlite_indexes(datastore, &table_name).await?;
        table.unique_indexes = unique_indexes;
        table.indexes = indexes;
        schema.tables.insert(table_name, table);
    }

    Ok(schema)
}

async fn extract_sqlite_foreign_keys(
    datastore: &StoreDatastore,
    table_name: &str,
) -> Result<BTreeSet<CanonicalForeignKey>, StateError> {
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        &format!(
            "PRAGMA foreign_key_list({})",
            quote_sqlite_identifier(table_name)
        ),
        &[],
    )
    .await?;
    let mut grouped: BTreeMap<i64, ForeignKeyGroup> = BTreeMap::new();

    for row in rows {
        let id = row.i64("id")?;
        let seq = row.i64("seq")?;
        let entry = grouped.entry(id).or_insert_with(|| {
            (
                row.text("table").unwrap_or_default(),
                normalize_referential_action(&row.text("on_delete").unwrap_or_default()),
                Vec::new(),
            )
        });
        entry
            .2
            .push((seq, row.text("from")?, row.text("to")?.to_ascii_lowercase()));
    }

    Ok(grouped
        .into_values()
        .map(|(foreign_table, on_delete, mut parts)| {
            parts.sort_by_key(|(seq, _, _)| *seq);
            CanonicalForeignKey {
                columns: parts
                    .iter()
                    .map(|(_, column, _)| column.to_ascii_lowercase())
                    .collect(),
                foreign_table: foreign_table.to_ascii_lowercase(),
                foreign_columns: parts
                    .into_iter()
                    .map(|(_, _, column)| column.to_ascii_lowercase())
                    .collect(),
                on_delete,
            }
        })
        .collect())
}

async fn extract_sqlite_indexes(
    datastore: &StoreDatastore,
    table_name: &str,
) -> Result<(BTreeSet<CanonicalIndex>, BTreeSet<CanonicalIndex>), StateError> {
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        &format!("PRAGMA index_list({})", quote_sqlite_identifier(table_name)),
        &[],
    )
    .await?;
    let mut unique_indexes = BTreeSet::new();
    let mut indexes = BTreeSet::new();

    for row in rows {
        if row.opt_text("origin")?.as_deref() == Some("pk") {
            continue;
        }
        let index_name = row.text("name")?;
        let expressions =
            extract_sqlite_index_expressions(datastore, table_name, &index_name).await?;
        if expressions.is_empty() {
            continue;
        }
        let index = CanonicalIndex { expressions };
        if row.i64("unique")? != 0 {
            unique_indexes.insert(index);
        } else {
            indexes.insert(index);
        }
    }

    Ok((unique_indexes, indexes))
}

async fn extract_sqlite_index_expressions(
    datastore: &StoreDatastore,
    table_name: &str,
    index_name: &str,
) -> Result<Vec<String>, StateError> {
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        &format!(
            "PRAGMA index_xinfo({})",
            quote_sqlite_identifier(index_name)
        ),
        &[],
    )
    .await?;
    let mut parts = Vec::new();
    for row in rows {
        if row.i64("key")? == 0 {
            continue;
        }
        let seqno = row.i64("seqno")?;
        let Some(column_name) = row.opt_text("name")? else {
            parts.push((seqno, format!("sqlite-expression-{seqno}")));
            continue;
        };
        let collation = row.opt_text("coll")?.unwrap_or_default();
        parts.push((
            seqno,
            normalize_sqlite_index_expression(table_name, &column_name, &collation),
        ));
    }
    parts.sort_by_key(|(seqno, _)| *seqno);
    Ok(parts
        .into_iter()
        .map(|(_, expression)| expression)
        .collect())
}

async fn extract_postgres_schema(
    datastore: &StoreDatastore,
) -> Result<CanonicalSchema, StateError> {
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        "SELECT table_name,
                column_name,
                data_type,
                is_nullable,
                column_default
           FROM information_schema.columns
          WHERE table_schema = current_schema()
            AND table_name NOT LIKE '_sqlx_%'
          ORDER BY table_name, ordinal_position",
        &[],
    )
    .await?;
    let mut schema = CanonicalSchema::default();

    for row in rows {
        let table_name = row.text("table_name")?.to_ascii_lowercase();
        let column_name = row.text("column_name")?.to_ascii_lowercase();
        let raw_type = row.text("data_type")?;
        let data_type = normalize_column_type(&table_name, &column_name, &raw_type);
        let table = schema.tables.entry(table_name.clone()).or_default();
        table.columns.insert(
            column_name.clone(),
            CanonicalColumn {
                data_type: data_type.clone(),
                not_null: row.text("is_nullable")? == "NO",
                default_value: normalize_default(
                    &table_name,
                    &column_name,
                    &data_type,
                    row.opt_text("column_default")?,
                ),
            },
        );
    }

    apply_postgres_primary_keys(datastore, &mut schema).await?;
    apply_postgres_foreign_keys(datastore, &mut schema).await?;
    apply_postgres_indexes(datastore, &mut schema).await?;
    Ok(schema)
}

async fn apply_postgres_primary_keys(
    datastore: &StoreDatastore,
    schema: &mut CanonicalSchema,
) -> Result<(), StateError> {
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        "SELECT tc.table_name,
                kcu.column_name,
                kcu.ordinal_position::BIGINT AS ordinal_position
           FROM information_schema.table_constraints tc
           JOIN information_schema.key_column_usage kcu
             ON tc.constraint_catalog = kcu.constraint_catalog
            AND tc.constraint_schema = kcu.constraint_schema
            AND tc.constraint_name = kcu.constraint_name
          WHERE tc.table_schema = current_schema()
            AND tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name NOT LIKE '_sqlx_%'
          ORDER BY tc.table_name, kcu.ordinal_position",
        &[],
    )
    .await?;
    let mut grouped: BTreeMap<String, Vec<(i64, String)>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry(row.text("table_name")?.to_ascii_lowercase())
            .or_default()
            .push((
                row.i64("ordinal_position")?,
                row.text("column_name")?.to_ascii_lowercase(),
            ));
    }

    for (table_name, mut columns) in grouped {
        if let Some(table) = schema.tables.get_mut(&table_name) {
            columns.sort_by_key(|(position, _)| *position);
            table.primary_key = columns
                .into_iter()
                .map(|(_, column_name)| column_name)
                .collect();
            for column_name in &table.primary_key {
                if let Some(column) = table.columns.get_mut(column_name) {
                    column.not_null = true;
                }
            }
        }
    }
    Ok(())
}

async fn apply_postgres_foreign_keys(
    datastore: &StoreDatastore,
    schema: &mut CanonicalSchema,
) -> Result<(), StateError> {
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        "SELECT con.conname AS constraint_name,
                rel.relname AS table_name,
                src.attname AS column_name,
                ord.ordinality::BIGINT AS ordinal_position,
                foreign_rel.relname AS foreign_table_name,
                dst.attname AS foreign_column_name,
                con.confdeltype::TEXT AS delete_action
           FROM pg_constraint con
           JOIN pg_class rel ON rel.oid = con.conrelid
           JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
           JOIN pg_class foreign_rel ON foreign_rel.oid = con.confrelid
           JOIN unnest(con.conkey, con.confkey) WITH ORDINALITY AS ord(attnum, refattnum, ordinality)
             ON TRUE
           JOIN pg_attribute src ON src.attrelid = con.conrelid AND src.attnum = ord.attnum
           JOIN pg_attribute dst ON dst.attrelid = con.confrelid AND dst.attnum = ord.refattnum
          WHERE nsp.nspname = current_schema()
            AND con.contype = 'f'
            AND rel.relname NOT LIKE '_sqlx_%'
          ORDER BY rel.relname, con.conname, ord.ordinality",
        &[],
    )
    .await?;
    let mut grouped: BTreeMap<(String, String), ForeignKeyGroup> = BTreeMap::new();

    for row in rows {
        let table_name = row.text("table_name")?.to_ascii_lowercase();
        let constraint_name = row.text("constraint_name")?;
        let entry = grouped
            .entry((table_name, constraint_name))
            .or_insert_with(|| {
                (
                    row.text("foreign_table_name")
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    normalize_postgres_referential_action(
                        &row.text("delete_action").unwrap_or_default(),
                    ),
                    Vec::new(),
                )
            });
        entry.2.push((
            row.i64("ordinal_position")?,
            row.text("column_name")?.to_ascii_lowercase(),
            row.text("foreign_column_name")?.to_ascii_lowercase(),
        ));
    }

    for ((table_name, _), (foreign_table, on_delete, mut parts)) in grouped {
        if let Some(table) = schema.tables.get_mut(&table_name) {
            parts.sort_by_key(|(position, _, _)| *position);
            table.foreign_keys.insert(CanonicalForeignKey {
                columns: parts
                    .iter()
                    .map(|(_, column, _)| column.to_string())
                    .collect(),
                foreign_table,
                foreign_columns: parts.into_iter().map(|(_, _, column)| column).collect(),
                on_delete,
            });
        }
    }

    Ok(())
}

async fn apply_postgres_indexes(
    datastore: &StoreDatastore,
    schema: &mut CanonicalSchema,
) -> Result<(), StateError> {
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        "SELECT tablename AS table_name,
                indexname,
                indexdef
           FROM pg_indexes
          WHERE schemaname = current_schema()
            AND tablename NOT LIKE '_sqlx_%'
          ORDER BY tablename, indexname",
        &[],
    )
    .await?;

    for row in rows {
        let table_name = row.text("table_name")?.to_ascii_lowercase();
        let index_name = row.text("indexname")?.to_ascii_lowercase();
        let indexdef = row.text("indexdef")?;
        let normalized = indexdef.to_ascii_lowercase();
        if index_name.ends_with("_pkey") {
            continue;
        }
        let expressions = postgres_index_expressions(&indexdef);
        if expressions.is_empty() {
            continue;
        }
        let index = CanonicalIndex { expressions };
        if let Some(table) = schema.tables.get_mut(&table_name) {
            if normalized.starts_with("create unique index") {
                if index.expressions == table.primary_key {
                    continue;
                }
                table.unique_indexes.insert(index);
            } else {
                table.indexes.insert(index);
            }
        }
    }

    Ok(())
}

fn quote_sqlite_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn normalize_column_type(table: &str, column: &str, raw_type: &str) -> String {
    if is_boolean_column(table, column) {
        return "bool".to_string();
    }

    let raw = raw_type.trim().to_ascii_lowercase();
    if raw.contains("int") {
        "integer".to_string()
    } else if raw == "blob" || raw == "bytea" {
        "bytes".to_string()
    } else if raw == "real" || raw == "double precision" {
        "float".to_string()
    } else if raw == "bool" || raw == "boolean" {
        "bool".to_string()
    } else {
        raw
    }
}

fn normalize_default(
    _table: &str,
    _column: &str,
    data_type: &str,
    default_value: Option<String>,
) -> Option<String> {
    let mut value = default_value?.trim().to_string();
    if value.is_empty() || value.eq_ignore_ascii_case("null") {
        return None;
    }
    if value.to_ascii_lowercase().starts_with("nextval(") {
        return None;
    }
    value = strip_outer_parentheses(&value);
    value = strip_unquoted_pg_cast(&value);

    if data_type == "bool" {
        return match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some("true".to_string()),
            "0" | "false" => Some("false".to_string()),
            _ => Some(value.trim().to_ascii_lowercase()),
        };
    }

    let value = value.trim();
    if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
        let unquoted = value[1..value.len() - 1].replace("''", "'");
        if data_type == "integer" && unquoted.parse::<i64>().is_ok() {
            Some(unquoted)
        } else if data_type == "float" && unquoted.parse::<f64>().is_ok() {
            Some(unquoted.to_ascii_lowercase())
        } else {
            Some(format!("text:{unquoted}"))
        }
    } else {
        Some(value.to_ascii_lowercase())
    }
}

fn is_boolean_column(table: &str, column: &str) -> bool {
    matches!(
        (table, column),
        ("servers", "tls")
            | ("servers", "active")
            | ("servers", "supports_pipelining")
            | ("active_jobs", "normalization_retried")
            | ("active_par2_files", "promoted")
            | ("active_extraction_chunks", "verified")
            | ("active_extraction_chunks", "appended")
            | ("active_volume_status", "extracted")
            | ("active_volume_status", "par2_clean")
            | ("active_volume_status", "deleted")
            | ("rss_feeds", "enabled")
            | ("rss_rules", "enabled")
    )
}

fn normalize_referential_action(action: &str) -> String {
    action.trim().replace(' ', "_").to_ascii_lowercase()
}

fn normalize_postgres_referential_action(action: &str) -> String {
    match action {
        "a" => "no_action".to_string(),
        "r" => "restrict".to_string(),
        "c" => "cascade".to_string(),
        "n" => "set_null".to_string(),
        "d" => "set_default".to_string(),
        other => normalize_referential_action(other),
    }
}

fn normalize_sqlite_index_expression(_table: &str, column: &str, collation: &str) -> String {
    let column = column.trim_matches('"').to_ascii_lowercase();
    if collation.eq_ignore_ascii_case("NOCASE") {
        format!("lower({column})")
    } else {
        column
    }
}

fn postgres_index_expressions(indexdef: &str) -> Vec<String> {
    let Some(using_pos) = indexdef.to_ascii_lowercase().find(" using ") else {
        return Vec::new();
    };
    let after_using = &indexdef[using_pos..];
    let Some(open_rel) = after_using.find('(') else {
        return Vec::new();
    };
    let open = using_pos + open_rel;
    let Some(close) = matching_closing_paren(indexdef, open) else {
        return Vec::new();
    };
    split_top_level_csv(&indexdef[open + 1..close])
        .into_iter()
        .map(|expression| normalize_postgres_index_expression(&expression))
        .collect()
}

fn normalize_postgres_index_expression(expression: &str) -> String {
    let expression = expression
        .trim()
        .trim_matches('"')
        .replace('"', "")
        .to_ascii_lowercase();
    let expression = strip_outer_parentheses(&expression);
    if expression.starts_with("lower(") && expression.ends_with(')') {
        let inner = &expression[6..expression.len() - 1];
        return format!("lower({})", normalize_postgres_expression_inner(inner));
    }
    normalize_postgres_expression_inner(&expression)
}

fn normalize_postgres_expression_inner(expression: &str) -> String {
    let expression = strip_outer_parentheses(expression.trim());
    let expression = strip_unquoted_pg_cast(&expression);
    strip_outer_parentheses(&expression)
        .trim()
        .trim_matches('"')
        .to_ascii_lowercase()
}

fn advance_sql_quote(bytes: &[u8], i: usize, in_quote: &mut bool) -> usize {
    if *in_quote && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
        i + 1
    } else {
        *in_quote = !*in_quote;
        i
    }
}

fn split_top_level_csv(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut in_quote = false;
    let bytes = input.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = advance_sql_quote(bytes, i, &mut in_quote),
            b'(' if !in_quote => depth += 1,
            b')' if !in_quote => depth -= 1,
            b',' if !in_quote && depth == 0 => {
                parts.push(input[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail.to_string());
    }
    parts
}

fn strip_outer_parentheses(value: &str) -> String {
    let mut current = value.trim().to_string();
    loop {
        let trimmed = current.trim();
        if trimmed.starts_with('(')
            && trimmed.ends_with(')')
            && matching_closing_paren(trimmed, 0) == Some(trimmed.len() - 1)
        {
            current = trimmed[1..trimmed.len() - 1].trim().to_string();
        } else {
            return trimmed.to_string();
        }
    }
}

fn matching_closing_paren(value: &str, open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_quote = false;
    let bytes = value.as_bytes();
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = advance_sql_quote(bytes, i, &mut in_quote),
            b'(' if !in_quote => depth += 1,
            b')' if !in_quote => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn strip_unquoted_pg_cast(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut in_quote = false;
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        match bytes[i] {
            b'\'' => i = advance_sql_quote(bytes, i, &mut in_quote),
            b':' if !in_quote && bytes[i + 1] == b':' => return value[..i].trim().to_string(),
            _ => {}
        }
        i += 1;
    }
    value.trim().to_string()
}

#[test]
fn normalizes_quoted_numeric_defaults_as_numeric() {
    assert_eq!(
        normalize_default(
            "active_extracted",
            "output_size",
            "integer",
            Some("-1".into())
        ),
        Some("-1".to_string())
    );
    assert_eq!(
        normalize_default(
            "active_extracted",
            "output_size",
            "integer",
            Some("'-1'::bigint".into())
        ),
        Some("-1".to_string())
    );
}

fn assert_json_arg_binds_as_text(db: &Database) {
    execute(
        db,
        "CREATE TABLE json_text_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
        vec![],
    );
    let value = serde_json::json!({
        "kind": "probe",
        "nested": [true, null, 7],
    });
    let expected = value.to_string();
    execute(
        db,
        "INSERT INTO json_text_probe (id, value) VALUES ({}, {})",
        vec![SqlArg::I32(1), SqlArg::Json(value)],
    );
    assert_eq!(
        fetch_text(
            db,
            "SELECT value FROM json_text_probe WHERE id = {}",
            vec![SqlArg::I32(1)],
        ),
        expected
    );
}

#[test]
fn open_in_memory_creates_schema() {
    let db = Database::open_in_memory().unwrap();
    assert!(db.is_empty().unwrap());
}

#[test]
fn sqlite_json_arg_binds_as_text() {
    let db = Database::open_in_memory().unwrap();
    assert_json_arg_binds_as_text(&db);
}

#[test]
fn open_in_memory_stamps_sqlx_migration_ledger() {
    let db = Database::open_in_memory().unwrap();
    let latest_version = fetch_i64(
        &db,
        "SELECT MAX(version) AS value FROM _sqlx_migrations",
        vec![],
    );
    assert_eq!(latest_version, current_schema_version());
}

#[test]
fn schema_version_is_set() {
    let db = Database::open_in_memory().unwrap();
    let version = fetch_i64(&db, "SELECT version FROM schema_version", vec![]);
    assert_eq!(version, current_schema_version());
}

#[test]
fn open_applies_sqlite_file_pragmas() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("weaver.db")).unwrap();

    let cache_size = fetch_i64(&db, "PRAGMA cache_size", vec![]);
    assert_eq!(cache_size, -16000);

    let mmap_size = fetch_i64(&db, "PRAGMA mmap_size", vec![]);
    assert_eq!(mmap_size, 16 * 1024 * 1024);
}

#[test]
fn open_file_database_stamps_sqlx_migration_ledger() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("weaver.db")).unwrap();
    let catalog = crate::schema_migrations::embedded_catalog().unwrap();

    let latest_version = fetch_i64(
        &db,
        "SELECT MAX(version) AS value FROM _sqlx_migrations",
        vec![],
    );
    assert_eq!(latest_version, current_schema_version());

    let migration_count = fetch_i64(
        &db,
        "SELECT COUNT(*) AS value FROM _sqlx_migrations",
        vec![],
    );
    assert_eq!(migration_count, catalog.migrations.len() as i64);

    let checksum_algo = fetch_text(
        &db,
        "SELECT checksum_algo AS value FROM _sqlx_migrations WHERE version = {}",
        vec![SqlArg::I64(current_schema_version())],
    );
    assert_eq!(checksum_algo, "blake3");
}

#[test]
fn reserve_next_job_id_remains_monotonic_across_reopen_without_history_rows() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("weaver.db");

    {
        let db = Database::open(&db_path).unwrap();
        assert_eq!(db.reserve_next_job_id().unwrap().0, 10_000);

        execute(&db, "DELETE FROM active_jobs", vec![]);
        execute(&db, "DELETE FROM job_history", vec![]);
    }

    let db = Database::open(&db_path).unwrap();
    assert_eq!(db.reserve_next_job_id().unwrap().0, 10_001);

    let persisted_next = fetch_text(
        &db,
        "SELECT value FROM settings WHERE key = 'next_job_id'",
        vec![],
    );
    assert_eq!(persisted_next, "10002");
}

#[tokio::test]
async fn flush_write_queue_succeeds_without_raw_sqlite_connection() {
    let db = Database::open_in_memory().unwrap();
    db.flush_write_queue().await.unwrap();
}

#[test]
fn writer_task_does_not_retain_database_sender() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(db.writer_tx.strong_count(), 1);
}

#[test]
fn canonical_schema_comparison_rejects_intentional_drift() {
    let mut left = CanonicalSchema::default();
    let mut table = CanonicalTable::default();
    table.columns.insert(
        "key".to_string(),
        CanonicalColumn {
            data_type: "text".to_string(),
            not_null: true,
            default_value: None,
        },
    );
    table.primary_key = vec!["key".to_string()];
    left.tables.insert("settings".to_string(), table);

    let mut right = left.clone();
    right.tables.get_mut("settings").unwrap().columns.insert(
        "value".to_string(),
        CanonicalColumn {
            data_type: "text".to_string(),
            not_null: true,
            default_value: None,
        },
    );

    assert_ne!(left, right);
}

#[tokio::test]
async fn postgres_bulk_hot_paths_when_configured() {
    let Some((admin_pool, schema, target_url)) = create_postgres_test_schema("postgres_bulk").await
    else {
        return;
    };

    let db = Database::open_target(DatabaseTarget::PostgresUrl(target_url)).unwrap();
    let job_id = crate::jobs::ids::JobId(314);
    let file_identities = vec![
        crate::jobs::record::ActiveFileIdentity {
            file_index: 0,
            source_filename: "archive.rar".to_string(),
            current_filename: "archive.rar".to_string(),
            canonical_filename: None,
            classification: None,
            classification_source: crate::jobs::record::FileIdentitySource::Declared,
        },
        crate::jobs::record::ActiveFileIdentity {
            file_index: 1,
            source_filename: "archive.r00".to_string(),
            current_filename: "archive.r00".to_string(),
            canonical_filename: Some("archive.part02.rar".to_string()),
            classification: None,
            classification_source: crate::jobs::record::FileIdentitySource::Probe,
        },
    ];
    db.create_active_job_with_file_identities(&postgres_sample_job(job_id), &file_identities)
        .unwrap();

    let high_progress = (0..325)
        .map(|file_index| ActiveFileProgress {
            job_id,
            file_index,
            contiguous_bytes_written: 2048 + u64::from(file_index),
        })
        .collect::<Vec<_>>();
    let low_progress = (0..325)
        .map(|file_index| ActiveFileProgress {
            job_id,
            file_index,
            contiguous_bytes_written: 1024,
        })
        .collect::<Vec<_>>();
    db.upsert_file_progress_batch(&high_progress).unwrap();
    db.upsert_file_progress_batch(&low_progress).unwrap();
    db.complete_file(job_id, 0, "archive.rar", &[0x44; 16])
        .unwrap();

    let job_events = (0..250)
        .map(|index| JobEvent {
            job_id: job_id.0,
            timestamp: 1000 + index,
            kind: format!("EVENT_{index:03}"),
            message: format!("message {index}"),
            file_id: Some(format!("{}:{index}", job_id.0)),
        })
        .collect::<Vec<_>>();
    db.insert_job_events(&job_events).unwrap();

    let integration_events = (0..250)
        .map(|index| IntegrationEventRow {
            id: 0,
            timestamp: 2000 + index,
            kind: format!("ITEM_PROGRESS_{index:03}"),
            item_id: Some(index as u64),
            payload_json: format!("{{\"index\":{index}}}"),
        })
        .collect::<Vec<_>>();
    db.insert_integration_events(&integration_events).unwrap();

    db.replace_failed_extractions(
        job_id,
        &HashSet::from(["bad-a.mkv".to_string(), "bad-b.mkv".to_string()]),
    )
    .unwrap();
    db.replace_verified_suspect_volumes(job_id, "set", &HashSet::from([37_u32, 38_u32]))
        .unwrap();
    db.replace_member_chunks(
        job_id,
        "set",
        "movie.mkv",
        &[
            ExtractionChunk {
                member_name: "movie.mkv".to_string(),
                volume_index: 0,
                bytes_written: 111,
                temp_path: "/tmp/chunk0".to_string(),
                start_offset: 0,
                end_offset: 111,
                verified: true,
                appended: false,
            },
            ExtractionChunk {
                member_name: "movie.mkv".to_string(),
                volume_index: 1,
                bytes_written: 222,
                temp_path: "/tmp/chunk1".to_string(),
                start_offset: 111,
                end_offset: 333,
                verified: true,
                appended: false,
            },
        ],
    )
    .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    let job = jobs.get(&job_id).unwrap();
    assert_eq!(job.file_identities.len(), 2);
    assert_eq!(job.file_identities.get(&0), Some(&file_identities[0]));
    assert_eq!(job.file_identities.get(&1), Some(&file_identities[1]));
    assert_eq!(job.complete_files.len(), 1);
    assert_eq!(job.file_progress.len(), 324);
    assert_eq!(job.file_progress.get(&0).copied(), None);
    assert_eq!(job.file_progress.get(&324).copied(), Some(2048 + 324));
    assert_eq!(db.get_job_events(job_id.0).unwrap().len(), 250);
    assert_eq!(
        db.list_integration_events_after(None, None, Some(300))
            .unwrap()
            .len(),
        250
    );
    assert_eq!(
        db.load_failed_extractions(job_id).unwrap(),
        HashSet::from(["bad-a.mkv".to_string(), "bad-b.mkv".to_string()])
    );
    assert_eq!(
        db.load_verified_suspect_volumes(job_id)
            .unwrap()
            .get("set")
            .cloned(),
        Some(HashSet::from([37_u32, 38_u32]))
    );
    assert_eq!(db.get_extraction_chunks(job_id, "set").unwrap().len(), 2);

    drop(db);
    execute_schema_ddl(&admin_pool, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin_pool.close().await;
}

#[tokio::test]
async fn postgres_waiting_active_write_noops_after_delete_when_configured() {
    let Some((admin_pool, schema, target_url)) =
        create_postgres_test_schema("postgres_active_write_lock").await
    else {
        return;
    };

    let db = Database::open_target(DatabaseTarget::PostgresUrl(target_url.clone())).unwrap();
    let job_id = crate::jobs::ids::JobId(501);
    db.create_active_job(&postgres_sample_job(job_id)).unwrap();

    let lock_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&target_url)
        .await
        .unwrap();
    let mut lock_tx = lock_pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM active_jobs WHERE job_id = $1 FOR UPDATE")
        .bind(job_id.0 as i64)
        .execute(&mut *lock_tx)
        .await
        .unwrap();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let writer_db = db.clone();
    std::thread::spawn(move || {
        let result = writer_db.upsert_file_progress_batch(&[ActiveFileProgress {
            job_id,
            file_index: 0,
            contiguous_bytes_written: 123,
        }]);
        let _ = done_tx.send(result);
    });

    std::thread::sleep(Duration::from_millis(200));
    assert!(
        done_rx.try_recv().is_err(),
        "active-state writer should wait for the parent row lock"
    );

    sqlx::query("DELETE FROM active_file_progress WHERE job_id = $1")
        .bind(job_id.0 as i64)
        .execute(&mut *lock_tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM active_jobs WHERE job_id = $1")
        .bind(job_id.0 as i64)
        .execute(&mut *lock_tx)
        .await
        .unwrap();
    lock_tx.commit().await.unwrap();
    lock_pool.close().await;

    done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        fetch_i64(
            &db,
            "SELECT COUNT(*) AS value FROM active_file_progress WHERE job_id = {}",
            vec![SqlArg::I64(job_id.0 as i64)],
        ),
        0
    );

    drop(db);
    execute_schema_ddl(&admin_pool, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin_pool.close().await;
}

#[tokio::test]
async fn postgres_archive_and_delete_wait_on_active_job_lock_when_configured() {
    let Some((admin_pool, schema, target_url)) =
        create_postgres_test_schema("postgres_archive_delete_lock").await
    else {
        return;
    };

    let db = Database::open_target(DatabaseTarget::PostgresUrl(target_url.clone())).unwrap();

    for (job_id, operation) in [
        (crate::jobs::ids::JobId(601), "archive"),
        (crate::jobs::ids::JobId(602), "delete"),
    ] {
        db.create_active_job(&postgres_sample_job(job_id)).unwrap();
        db.upsert_file_progress_batch(&[ActiveFileProgress {
            job_id,
            file_index: 0,
            contiguous_bytes_written: 1,
        }])
        .unwrap();

        let lock_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&target_url)
            .await
            .unwrap();
        let mut lock_tx = lock_pool.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM active_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id.0 as i64)
            .execute(&mut *lock_tx)
            .await
            .unwrap();

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let operation_db = db.clone();
        let history = postgres_sample_history(job_id);
        std::thread::spawn(move || {
            let result = if operation == "archive" {
                operation_db.archive_job(job_id, &history)
            } else {
                operation_db.delete_active_job(job_id)
            };
            let _ = done_tx.send(result);
        });

        std::thread::sleep(Duration::from_millis(200));
        assert!(
            done_rx.try_recv().is_err(),
            "{operation} should wait for the parent row lock"
        );

        lock_tx.commit().await.unwrap();
        lock_pool.close().await;
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();

        assert_eq!(
            fetch_i64(
                &db,
                "SELECT COUNT(*) AS value FROM active_jobs WHERE job_id = {}",
                vec![SqlArg::I64(job_id.0 as i64)],
            ),
            0
        );
        assert_eq!(
            fetch_i64(
                &db,
                "SELECT COUNT(*) AS value FROM active_file_progress WHERE job_id = {}",
                vec![SqlArg::I64(job_id.0 as i64)],
            ),
            0
        );
    }

    drop(db);
    execute_schema_ddl(&admin_pool, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin_pool.close().await;
}

#[tokio::test]
async fn schema_parity_when_postgres_configured() {
    let Some((admin_pool, schema, target_url)) = create_postgres_test_schema("schema_parity").await
    else {
        return;
    };

    let sqlite_db = Database::open_in_memory().unwrap();
    let postgres_db = Database::open_target(DatabaseTarget::PostgresUrl(target_url)).unwrap();
    let sqlite_schema = extract_canonical_schema(&sqlite_db);
    let postgres_schema = extract_canonical_schema(&postgres_db);

    assert_eq!(
        sqlite_schema, postgres_schema,
        "SQLite/Postgres canonical schema drift:\nSQLite:\n{sqlite_schema:#?}\nPostgres:\n{postgres_schema:#?}"
    );

    drop(postgres_db);
    execute_schema_ddl(&admin_pool, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin_pool.close().await;
}

#[tokio::test]
async fn postgres_executor_runs_sync_calls_concurrently_when_configured() {
    let Some((admin_pool, schema, target_url)) =
        create_postgres_test_schema("postgres_executor_concurrency").await
    else {
        return;
    };

    if postgres_db_concurrency_from_env() < 4 {
        eprintln!(
            "skipping postgres executor concurrency test; WEAVER_POSTGRES_DB_CONCURRENCY is below 4"
        );
        execute_schema_ddl(&admin_pool, format!("DROP SCHEMA {schema} CASCADE")).await;
        admin_pool.close().await;
        return;
    }

    let db = Database::open_target(DatabaseTarget::PostgresUrl(target_url)).unwrap();
    let started = Instant::now();
    let handles = (0..4)
        .map(|_| {
            let db = db.clone();
            std::thread::spawn(move || {
                let datastore = db.datastore();
                db.run_sql_blocking(async move {
                    SqlRuntime::fetch_optional(
                        datastore.read_exec(),
                        "SELECT pg_sleep({})",
                        &[SqlArg::F64(0.5)],
                    )
                    .await?;
                    Ok(())
                })
                .unwrap();
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = started.elapsed();

    drop(db);
    execute_schema_ddl(&admin_pool, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin_pool.close().await;

    assert!(
        elapsed < Duration::from_millis(1500),
        "Postgres DB calls appear serialized; elapsed = {elapsed:?}"
    );
}

#[tokio::test]
async fn postgres_runtime_smoke_when_configured() {
    let Some((admin_pool, schema, target_url)) =
        create_postgres_test_schema("postgres_smoke").await
    else {
        return;
    };

    let db = Database::open_target(DatabaseTarget::PostgresUrl(target_url)).unwrap();
    assert_json_arg_binds_as_text(&db);

    let latest_version = fetch_i64(
        &db,
        "SELECT MAX(version) AS value FROM _sqlx_migrations",
        vec![],
    );
    assert_eq!(latest_version, current_schema_version());
    assert_eq!(db.schema_version().unwrap(), current_schema_version());
    assert_eq!(
        fetch_text(
            &db,
            "SELECT runtime_version AS value FROM _sqlx_migrations WHERE version = {}",
            vec![SqlArg::I64(current_schema_version())],
        ),
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(
        fetch_i64(
            &db,
            "SELECT COUNT(*) AS value FROM _sqlx_migrations
              WHERE version = {} AND error_message IS NULL",
            vec![SqlArg::I64(current_schema_version())],
        ),
        1
    );

    db.set_setting("postgres_smoke", "ok").unwrap();
    assert_eq!(
        db.get_setting("postgres_smoke").unwrap(),
        Some("ok".to_string())
    );

    let config = Config {
        data_dir: "/tmp/weaver-pg".to_string(),
        intermediate_dir: Some("/tmp/weaver-pg/intermediate".to_string()),
        complete_dir: Some("/tmp/weaver-pg/complete".to_string()),
        buffer_pool: Some(BufferPoolOverrides {
            small_count: Some(4),
            medium_count: Some(3),
            large_count: Some(2),
        }),
        tuner: Some(TunerOverrides {
            max_concurrent_downloads: Some(8),
            decode_thread_count: Some(2),
            extract_thread_count: Some(1),
        }),
        servers: vec![ServerConfig {
            id: 7,
            host: "news.example.com".to_string(),
            port: 563,
            tls: true,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            connections: 12,
            active: true,
            supports_pipelining: true,
            priority: 2,
            tls_ca_cert: Some(PathBuf::from("/tmp/ca.pem")),
        }],
        categories: vec![CategoryConfig {
            id: 3,
            name: "TV".to_string(),
            dest_dir: Some("/media/tv".to_string()),
            aliases: "television,tv-*".to_string(),
        }],
        retry: Some(RetryOverrides {
            max_retries: Some(9),
            base_delay_secs: Some(4.0),
            multiplier: Some(1.5),
        }),
        max_download_speed: Some(12_345),
        cleanup_after_extract: Some(false),
        isp_bandwidth_cap: Some(IspBandwidthCapConfig {
            enabled: true,
            period: IspBandwidthCapPeriod::Weekly,
            limit_bytes: 9_999_999,
            reset_time_minutes_local: 6 * 60,
            weekly_reset_weekday: IspBandwidthCapWeekday::Mon,
            monthly_reset_day: 7,
        }),
        ip_replacement_trial_extra_connections: Some(1),
        config_path: None,
    };
    db.save_config(&config).unwrap();
    let loaded_config = db.load_config().unwrap();
    assert_eq!(loaded_config.data_dir, config.data_dir);
    assert_eq!(loaded_config.intermediate_dir, config.intermediate_dir);
    assert_eq!(loaded_config.complete_dir, config.complete_dir);
    assert_eq!(loaded_config.max_download_speed, config.max_download_speed);
    assert_eq!(loaded_config.cleanup_after_extract, Some(false));
    assert_eq!(
        loaded_config
            .isp_bandwidth_cap
            .as_ref()
            .map(|cap| cap.period),
        Some(IspBandwidthCapPeriod::Weekly)
    );
    assert_eq!(loaded_config.servers.len(), 1);
    assert_eq!(loaded_config.servers[0].host, "news.example.com");
    assert_eq!(loaded_config.servers[0].password.as_deref(), Some("pass"));
    assert_eq!(
        loaded_config.servers[0].tls_ca_cert.as_deref(),
        Some(PathBuf::from("/tmp/ca.pem").as_path())
    );
    assert_eq!(loaded_config.categories.len(), 1);
    assert_eq!(loaded_config.categories[0].name, "TV");
    assert_eq!(db.next_server_id().unwrap(), 8);
    assert_eq!(db.next_category_id().unwrap(), 4);

    let rss_feed = RssFeedRow {
        id: 1,
        name: "Feed 1".to_string(),
        url: "https://example.com/feed.xml".to_string(),
        enabled: true,
        poll_interval_secs: 900,
        username: Some("rss-user".to_string()),
        password: Some("rss-pass".to_string()),
        default_category: Some("TV".to_string()),
        default_metadata: vec![("source".to_string(), "rss".to_string())],
        etag: Some("etag".to_string()),
        last_modified: Some("Mon, 01 Jan 2024 00:00:00 GMT".to_string()),
        last_polled_at: Some(100),
        last_success_at: Some(90),
        last_error: None,
        consecutive_failures: 0,
    };
    db.insert_rss_feed(&rss_feed).unwrap();
    assert_eq!(db.get_rss_feed(1).unwrap(), Some(rss_feed.clone()));

    let rss_rule = RssRuleRow {
        id: 10,
        feed_id: 1,
        sort_order: 0,
        enabled: true,
        action: RssRuleAction::Accept,
        title_regex: Some("Example".to_string()),
        item_categories: vec!["tv".to_string()],
        min_size_bytes: Some(100),
        max_size_bytes: Some(1000),
        category_override: Some("TV".to_string()),
        metadata: vec![("quality".to_string(), "hd".to_string())],
    };
    db.insert_rss_rule(&rss_rule).unwrap();
    assert_eq!(db.list_rss_rules(1).unwrap(), vec![rss_rule]);

    let rss_seen_item = RssSeenItemRow {
        feed_id: 1,
        item_id: "guid-1".to_string(),
        item_title: "Example".to_string(),
        published_at: Some(123),
        size_bytes: Some(456),
        decision: "accepted".to_string(),
        seen_at: 1000,
        job_id: Some(42),
        item_url: Some("https://example.com/item.nzb".to_string()),
        error: None,
    };
    db.insert_rss_seen_item(&rss_seen_item).unwrap();
    assert!(db.rss_seen_item_exists(1, "guid-1").unwrap());
    assert_eq!(
        db.list_rss_seen_items(Some(1), Some(10)).unwrap(),
        vec![rss_seen_item]
    );

    db.add_bandwidth_usage_minute(100, 10).unwrap();
    db.add_bandwidth_usage_minute(100, 5).unwrap();
    db.add_bandwidth_usage_minute(101, 20).unwrap();
    assert_eq!(db.sum_bandwidth_usage_minutes(100, 102).unwrap(), 35);
    assert_eq!(db.sum_bandwidth_usage_minutes(102, 103).unwrap(), 0);
    assert_eq!(db.reserve_next_job_id().unwrap().0, 10_000);

    let api_key_id = db
        .insert_api_key("integration", &[0x31_u8; 32], "integration")
        .unwrap();
    assert!(api_key_id > 0);

    let job_id = crate::jobs::ids::JobId(42);
    db.create_active_job(&ActiveJob {
        job_id,
        nzb_hash: [0xA5; 32],
        nzb_path: PathBuf::from("/tmp/postgres-smoke.nzb"),
        nzb_zstd: crate::ingest::compress_nzb_bytes(
            br#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"/>"#,
        )
        .unwrap(),
        output_dir: PathBuf::from("/tmp/postgres-smoke"),
        created_at: 1_700_000_000,
        category: Some("smoke".to_string()),
        metadata: vec![("engine".to_string(), "postgres".to_string())],
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    })
    .unwrap();
    db.upsert_file_progress_batch(&[ActiveFileProgress {
        job_id,
        file_index: 0,
        contiguous_bytes_written: 123,
    }])
    .unwrap();
    assert_eq!(db.load_active_jobs().unwrap().len(), 1);

    db.archive_job(
        job_id,
        &JobHistoryRow {
            job_id: job_id.0,
            job_hash: Some(vec![0xA5; 32]),
            name: "postgres-smoke".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 123,
            downloaded_bytes: 123,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: Some("smoke".to_string()),
            output_dir: Some("/tmp/postgres-smoke".to_string()),
            nzb_path: Some("/tmp/postgres-smoke.nzb".to_string()),
            created_at: 1_700_000_000,
            completed_at: 1_700_000_100,
            metadata: Some("[[\"engine\",\"postgres\"]]".to_string()),
        },
    )
    .unwrap();
    assert!(db.load_active_jobs().unwrap().is_empty());
    assert!(db.get_job_history(job_id.0).unwrap().is_some());

    let backup_path = tempfile::NamedTempFile::new().unwrap();
    let backup_error = db.export_stable_state(backup_path.path()).unwrap_err();
    assert!(
        backup_error
            .to_string()
            .contains("requires sqlite datastore")
    );

    drop(db);
    execute_schema_ddl(&admin_pool, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin_pool.close().await;
}

#[tokio::test]
async fn postgres_reserve_next_job_id_is_unique_under_concurrency_when_configured() {
    let Some((admin_pool, schema, target_url)) =
        create_postgres_test_schema("postgres_reserve_next_job_id").await
    else {
        return;
    };

    let db = Database::open_target(DatabaseTarget::PostgresUrl(target_url)).unwrap();
    let handles = (0..16)
        .map(|_| {
            let db = db.clone();
            std::thread::spawn(move || db.reserve_next_job_id().unwrap().0)
        })
        .collect::<Vec<_>>();

    let mut ids = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    ids.sort_unstable();

    assert_eq!(ids, (10_000..10_016).collect::<Vec<_>>());
    assert_eq!(
        db.get_setting("next_job_id").unwrap(),
        Some("10016".to_string())
    );

    drop(db);
    execute_schema_ddl(&admin_pool, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin_pool.close().await;
}

async fn create_postgres_test_schema(test_name: &str) -> Option<(sqlx::PgPool, String, String)> {
    let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is not set");
        return None;
    };
    if base_url.trim().is_empty() {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is empty");
        return None;
    }

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("weaver_test_{test_name}_{}_{}", std::process::id(), suffix);
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    execute_schema_ddl(
        &admin_pool,
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
    )
    .await;
    execute_schema_ddl(&admin_pool, format!("CREATE SCHEMA {schema}")).await;

    let target_url = postgres_url_for_schema(&base_url, &schema);
    Some((admin_pool, schema, target_url))
}

async fn execute_schema_ddl(pool: &sqlx::PgPool, sql: String) {
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap();
}

fn postgres_url_for_schema(base_url: &str, schema: &str) -> String {
    let separator = if base_url.contains('?') { '&' } else { '?' };
    format!("{base_url}{separator}options=-csearch_path%3D{schema}")
}
