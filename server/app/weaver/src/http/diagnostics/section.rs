// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One section of the diagnostics package at a time, for the MCP tools.
//
// A section is collected by the function that collects it for the package,
// alone, and leaves through the package's own pass: the stand-in table is
// read for each call as the package reads it, beside the collection and up
// to the same deadline, and the section is sanitized and redacted exactly as
// its file would be. Nothing here builds the whole package.
//
// Left out, because they cannot be built that way: the pipeline internals
// (an answer from the pipeline actor, which a request handler does not wait
// on), the pipeline turn phases (sampled across the package's ten-second
// collection), the log files (up to 32 MiB each; the service log is searched
// instead), and the NZB reports, which read stored NZBs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;
use weaver_server_api::WeaverSchema;
use weaver_server_api::auth::CallerIdentity;
use weaver_server_core::auth::CallerScope;
use weaver_server_core::operations::disk::StorageCapacity;
use weaver_server_core::runtime::log_buffer::LogRingBuffer;
use weaver_server_core::settings::model::SharedConfig;
use weaver_server_core::{Database, SchedulerHandle};

use super::archive::{Component, Form};
use super::graphql::SchemaShape;
use super::{
    ConfiguredDirs, GRAPHQL_COMPONENTS, GRAPHQL_WAIT, cached_stored_names, collect_config_files,
    collect_graphql_within, collect_host, database_facts, graphql, names_cache, sanitize,
    sanitize_components, table_over,
};

// Lines of the Prometheus exposition one `metrics` section holds.
pub(crate) const MAX_METRIC_LINES: usize = 60;

// Renders the Prometheus exposition the package samples.
#[derive(Clone)]
pub(crate) struct MetricsRender(
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>,
);

impl MetricsRender {
    pub(crate) fn exporter(
        exporter: crate::http::PrometheusMetricsExporter,
        disk_space: Arc<StorageCapacity>,
        http_metrics: crate::http::HttpMetricsHandle,
    ) -> Self {
        Self(Arc::new(move || {
            let exporter = exporter.clone();
            let disk_space = Arc::clone(&disk_space);
            let http_metrics = http_metrics.clone();
            Box::pin(async move { exporter.render(&disk_space, &http_metrics).await })
        }))
    }

    // A fixed exposition.
    #[cfg(test)]
    pub(crate) fn fixed(text: &'static str) -> Self {
        Self(Arc::new(move || Box::pin(async move { text.to_string() })))
    }
}

// What the package reads, as the router holds it.
#[derive(Clone)]
pub(crate) struct DiagnosticsSources {
    pub schema: WeaverSchema,
    pub handle: SchedulerHandle,
    pub config: SharedConfig,
    pub db: Database,
    // The disk owner; without one every directory's space is unknown.
    pub disk_space: Option<Arc<StorageCapacity>>,
    pub log_buffer: LogRingBuffer,
    pub metrics: MetricsRender,
    // The stored names the MCP tools share for five minutes; the package
    // download reads its own.
    pub stored_names: Arc<names_cache::StoredNamesCache>,
}

// The sections one call can read, each the package file it is named after.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DiagnosticsSection {
    // `system-info.json`
    SystemInfo,
    // `metrics.json`: the pipeline's metrics and the global queue state.
    PipelineMetrics,
    // `server-health.json`
    ServerHealth,
    // `queue.json`
    Queue,
    // `servers.json`
    Servers,
    // `settings.json`
    Settings,
    // `schedules.json`
    Schedules,
    // `database.json`
    Database,
    // `host.json`
    Host,
    // `config/config-N.toml`
    Config,
    // The Prometheus exposition, filtered to named families.
    Metrics,
}

impl DiagnosticsSection {
    pub(crate) const ALL: [Self; 11] = [
        Self::SystemInfo,
        Self::PipelineMetrics,
        Self::ServerHealth,
        Self::Queue,
        Self::Servers,
        Self::Settings,
        Self::Schedules,
        Self::Database,
        Self::Host,
        Self::Config,
        Self::Metrics,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SystemInfo => "system_info",
            Self::PipelineMetrics => "pipeline_metrics",
            Self::ServerHealth => "server_health",
            Self::Queue => "queue",
            Self::Servers => "servers",
            Self::Settings => "settings",
            Self::Schedules => "schedules",
            Self::Database => "database",
            Self::Host => "host",
            Self::Config => "config",
            Self::Metrics => "metrics",
        }
    }

    pub(crate) fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|section| section.as_str() == name)
    }

    // The package file a GraphQL section is.
    fn graphql_file(self) -> Option<&'static str> {
        Some(match self {
            Self::SystemInfo => "system-info.json",
            Self::PipelineMetrics => "metrics.json",
            Self::ServerHealth => "server-health.json",
            Self::Queue => "queue.json",
            Self::Servers => "servers.json",
            Self::Settings => "settings.json",
            Self::Schedules => "schedules.json",
            Self::Database | Self::Host | Self::Config | Self::Metrics => return None,
        })
    }
}

// One sanitized file of a section.
#[derive(Debug)]
pub(crate) struct SectionFile {
    pub name: String,
    pub value: SectionValue,
    // The stable code for why it is missing or partial, where there is one.
    pub reason: Option<&'static str>,
    // What a file that was served is short of.
    pub partial: Option<String>,
}

#[derive(Debug)]
pub(crate) enum SectionValue {
    Json(Value),
    Text(String),
    // Why there is no file, sanitized.
    Failed(String),
}

#[derive(Debug)]
pub(crate) struct SectionRead {
    pub files: Vec<SectionFile>,
    // Whether the stand-in table read everything; when it did not, what it
    // could not replace was left out in place.
    pub table_complete: bool,
    // Matching exposition lines past `MAX_METRIC_LINES`, left out.
    pub metric_lines_left_out: usize,
}

impl DiagnosticsSources {
    // One section, collected and sanitized as the package does it. `families`
    // filters the `metrics` section and is ignored by the rest.
    pub(crate) async fn read_section(
        &self,
        scope: CallerScope,
        identity: CallerIdentity,
        section: DiagnosticsSection,
        families: &[String],
    ) -> SectionRead {
        let dirs = ConfiguredDirs::current(&self.config).await;
        // The stored names, kept or read now, beside the collection, as the
        // package reads its own beside its collection.
        let stored = cached_stored_names(&self.stored_names, &self.handle, &self.db);
        let collect = async {
            match section {
                DiagnosticsSection::Database => {
                    (database_facts::collect_database_facts(&self.db).await, 0)
                }
                DiagnosticsSection::Host => (
                    vec![collect_host(self.disk_space.as_deref(), &dirs).await],
                    0,
                ),
                DiagnosticsSection::Config => (collect_config_files(&dirs.data).await, 0),
                DiagnosticsSection::Metrics => {
                    let exposition = (self.metrics.0)().await;
                    let (text, left_out) = metric_lines(&exposition, families);
                    (vec![Component::text("metrics.txt", text)], left_out)
                }
                graphql_section => (
                    self.graphql_section(scope, identity, graphql_section).await,
                    0,
                ),
            }
        };
        let (stored, (components, metric_lines_left_out)) = tokio::join!(stored, collect);
        let volumes = self
            .disk_space
            .as_ref()
            .map(|disk_space| disk_space.volume_labels())
            .unwrap_or_default();
        let local = dirs.local_names(volumes);
        // Memory is read once the collection is in, so a job it names that
        // joined since the stored read is still named.
        let names = table_over(&stored, &self.handle, &self.config).await;
        let (components, sanitized) = sanitize_components(components, names, local).await;
        SectionRead {
            files: components.into_iter().map(section_file).collect(),
            table_complete: sanitized.table.complete,
            metric_lines_left_out,
        }
    }

    async fn graphql_section(
        &self,
        scope: CallerScope,
        identity: CallerIdentity,
        section: DiagnosticsSection,
    ) -> Vec<Component> {
        let Some(group) = section
            .graphql_file()
            .and_then(|file| GRAPHQL_COMPONENTS.iter().find(|group| group.file == file))
        else {
            return Vec::new();
        };
        let shape = match SchemaShape::current() {
            Ok(shape) => shape,
            Err(error) => return vec![Component::failed(group.file, error)],
        };
        collect_graphql_within(
            std::slice::from_ref(group),
            tokio::time::Instant::now() + GRAPHQL_WAIT,
            |field, arguments| {
                graphql::run_root_field(
                    &self.schema,
                    shape,
                    scope,
                    identity.clone(),
                    field,
                    arguments,
                )
            },
        )
        .await
    }

    // The service log as it stands, with the stand-in table read for it as
    // the package reads it.
    pub(crate) async fn service_log(&self) -> ServiceLog {
        let stored = cached_stored_names(&self.stored_names, &self.handle, &self.db).await;
        let (first, records) = self.log_buffer.numbered_snapshot();
        let volumes = self
            .disk_space
            .as_ref()
            .map(|disk_space| disk_space.volume_labels())
            .unwrap_or_default();
        let local = ConfiguredDirs::current(&self.config)
            .await
            .local_names(volumes);
        let names = table_over(&stored, &self.handle, &self.config).await;
        ServiceLog {
            first,
            records,
            names,
            local,
        }
    }
}

#[cfg(test)]
impl DiagnosticsSources {
    // Sources over `db` and `schema` with an empty queue and no pipeline
    // behind it, a configuration naming only `data_dir`, and no disk owner.
    pub(crate) fn fixture(
        schema: WeaverSchema,
        db: Database,
        data_dir: &str,
        log_buffer: LogRingBuffer,
        metrics: MetricsRender,
    ) -> Self {
        use weaver_server_core::{PipelineMetrics, SharedPipelineState};
        let (event_tx, _) = tokio::sync::broadcast::channel(4);
        let state = SharedPipelineState::new(PipelineMetrics::new(), Vec::new());
        let config = weaver_server_core::settings::model::Config {
            data_dir: data_dir.to_string(),
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            bandwidth: Default::default(),
            seeding: None,
            torrent: Default::default(),
            upload_cap: None,
            cleanup_after_extract: None,
            delete_failed_job_files: None,
            history_retention_days: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        };
        Self {
            schema,
            handle: SchedulerHandle::new(event_tx, state),
            config: Arc::new(tokio::sync::RwLock::new(config)),
            db,
            disk_space: None,
            log_buffer,
            metrics,
            stored_names: Default::default(),
        }
    }
}

// The service log's held records, oldest first, the number of the first,
// and what its lines are sanitized with.
pub(crate) struct ServiceLog {
    pub first: u64,
    pub records: Vec<String>,
    names: super::stored_read::TableNames,
    local: sanitize::LocalNames,
}

impl ServiceLog {
    // The package's pass over `service-log.txt`, one line at a time. A table
    // that cannot be built from what was read is built from the host's own
    // names alone with every job under the guard, as the package does; an
    // error only when that cannot be built either. Blocking.
    pub(crate) fn sanitizer(&self) -> Result<LogLineSanitizer, String> {
        let schema_keys = SchemaShape::current()
            .map(SchemaShape::field_names)
            .unwrap_or_default();
        sanitize::LineSanitizer::build(
            &self.local,
            &self.names.stored,
            schema_keys.clone(),
            self.names.guard.clone(),
        )
        .or_else(|_| {
            sanitize::LineSanitizer::build(
                &self.local,
                &sanitize::StoredNames::default(),
                schema_keys,
                Some(sanitize::Guard { jobs: None }),
            )
        })
        .map(LogLineSanitizer)
    }

    pub(crate) fn table_complete(&self) -> bool {
        self.names.manifest.complete
    }
}

pub(crate) struct LogLineSanitizer(sanitize::LineSanitizer);

impl LogLineSanitizer {
    pub(crate) fn line(&self, line: &str) -> String {
        self.0.line(line)
    }
}

fn section_file(component: Component) -> SectionFile {
    let value = match component.outcome {
        Err(error) => SectionValue::Failed(error),
        Ok(bytes) if component.form == Form::Json => match serde_json::from_slice(&bytes) {
            Ok(value) => SectionValue::Json(value),
            Err(_) => SectionValue::Failed("the sanitized file is not JSON".to_string()),
        },
        Ok(bytes) => SectionValue::Text(String::from_utf8_lossy(&bytes).into_owned()),
    };
    SectionFile {
        name: component.name,
        value,
        reason: component.reason,
        partial: component.partial,
    }
}

// The exposition's lines about `families`, at most `MAX_METRIC_LINES`, and
// how many more matched.
fn metric_lines(exposition: &str, families: &[String]) -> (String, usize) {
    let mut kept = exposition.lines().filter(|line| {
        metric_family(line).is_some_and(|name| families.iter().any(|family| belongs(name, family)))
    });
    let text = kept
        .by_ref()
        .take(MAX_METRIC_LINES)
        .collect::<Vec<_>>()
        .join("\n");
    (text, kept.count())
}

// The metric name a line is about: the name in `# HELP` and `# TYPE`, or the
// sample's name before its labels or value.
fn metric_family(line: &str) -> Option<&str> {
    let line = line.trim();
    if let Some(rest) = line
        .strip_prefix("# HELP ")
        .or_else(|| line.strip_prefix("# TYPE "))
    {
        return rest.split_whitespace().next();
    }
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    line.split(['{', ' ']).next()
}

// Whether `name` is `family` or one of its histogram or summary samples.
fn belongs(name: &str, family: &str) -> bool {
    name == family
        || name.strip_prefix(family).is_some_and(|suffix| {
            matches!(
                suffix,
                "_bucket" | "_sum" | "_count" | "_created" | "_total"
            )
        })
}

#[cfg(test)]
mod tests;
