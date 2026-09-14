//! The admin-only diagnostics package endpoint.
//!
//! One request produces one `weaver-diagnostics-<timestamp>.tar.zst` holding
//! everything a stall investigation asks for: two Prometheus samples ten
//! seconds apart, the GraphQL surfaces a maintainer would otherwise ask the
//! reporter to paste one at a time, the real log files, the redacted
//! configuration, and the pipeline internals that are reachable from nowhere
//! else.
//!
//! Two rules govern everything here:
//!
//! 1. **No credential leaves the host.** Every GraphQL payload passes through
//!    [`redact`], every configuration file is parsed and re-serialized with its
//!    secrets removed, and a configuration file that cannot be parsed is left
//!    out rather than copied verbatim.
//! 2. **A failed component never fails the package.** Each piece records
//!    either its bytes or its error, and the error is written into the manifest.
//!    A report that is missing one section is still a report; a 500 is not.

mod archive;
mod graphql;
mod redact;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{ConnectInfo, Extension};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use weaver_server_api::WeaverSchema;
use weaver_server_core::SchedulerHandle;
use weaver_server_core::operations::disk::DiskSpaceCollector;
use weaver_server_core::settings::model::SharedConfig;

use self::archive::Component;
use self::graphql::SchemaShape;
use self::redact::{is_sensitive_json_key, redact_json};

/// Gap between the two Prometheus samples. Long enough for a rate to be
/// meaningful, short enough that the browser request does not look hung.
const METRICS_SAMPLE_GAP: Duration = Duration::from_secs(10);

/// Per-file ceiling on copied log text. The tail is what matters, so an
/// oversized log contributes its last 32 MiB.
const MAX_LOG_BYTES: u64 = 32 * 1024 * 1024;

/// Ceiling on a single copied configuration file. A config this large is not a
/// config, and reading it into memory to redact it should stay bounded.
const MAX_CONFIG_BYTES: u64 = 4 * 1024 * 1024;

/// Arguments for the two bounded root fields. The ring buffer holds far fewer
/// lines than the service-log limit, so that one asks for all of them; the job
/// limit is a real ceiling on a very long history.
const SERVICE_LOG_ARGUMENTS: &str = "limit: 100000";
const JOBS_ARGUMENTS: &str = "limit: 2000";

/// Per-directory host facts.
#[derive(Debug, Serialize)]
struct HostStorage {
    role: &'static str,
    path: String,
    storage_class: Option<String>,
    filesystem: Option<String>,
    total_bytes: Option<u64>,
    used_bytes: Option<u64>,
    free_bytes: Option<u64>,
    error: Option<String>,
}

/// `host.json`: the facts about the machine that the rest of the package is
/// only meaningful against.
#[derive(Debug, Serialize)]
struct HostFacts {
    operating_system: &'static str,
    architecture: &'static str,
    deployment: String,
    physical_cores: usize,
    logical_cores: usize,
    cpu_cgroup_limit: Option<f64>,
    memory_total_bytes: u64,
    memory_available_bytes: u64,
    memory_cgroup_limit_bytes: Option<u64>,
    storage: Vec<HostStorage>,
}

#[allow(
    clippy::too_many_arguments,
    reason = "axum extractors: each runtime surface the package collects arrives as its own layer"
)]
pub(super) async fn diagnostics_package_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(handle): Extension<SchedulerHandle>,
    Extension(exporter): Extension<super::PrometheusMetricsExporter>,
    Extension(disk_space): Extension<Arc<DiskSpaceCollector>>,
    Extension(http_metrics): Extension<super::HttpMetricsHandle>,
    Extension(config): Extension<SharedConfig>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Response {
    let caller = match super::auth::resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        super::auth::BrowserSessionPolicy::TrustedPeer(
            peer.map(|Extension(ConnectInfo(peer))| peer),
        ),
        &headers,
    )
    .await
    {
        Ok(caller) => caller,
        Err(status) => return status.into_response(),
    };
    if !caller.scope.is_admin() {
        return StatusCode::FORBIDDEN.into_response();
    }

    let generated_at_utc = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let filename = archive::archive_filename(&generated_at_utc);

    let components = collect_components(
        &schema,
        &handle,
        &exporter,
        &disk_space,
        &http_metrics,
        &config,
        caller,
    )
    .await;

    let archive =
        match archive::build_archive(components, env!("CARGO_PKG_VERSION"), &generated_at_utc) {
            Ok(archive) => archive,
            Err(error) => {
                return super::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("failed to write diagnostics archive: {error}"),
                );
            }
        };

    (
        [
            (header::CONTENT_TYPE, "application/zstd".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        Body::from(archive),
    )
        .into_response()
}

#[allow(
    clippy::too_many_arguments,
    reason = "one parameter per collected runtime surface; grouping them would only move the list"
)]
async fn collect_components(
    schema: &WeaverSchema,
    handle: &SchedulerHandle,
    exporter: &super::PrometheusMetricsExporter,
    disk_space: &Arc<DiskSpaceCollector>,
    http_metrics: &super::HttpMetricsHandle,
    config: &SharedConfig,
    caller: super::auth::ResolvedCaller,
) -> Vec<Component> {
    let started = Instant::now();
    let mut components = Vec::new();

    components.push(Component::text(
        "metrics-1.txt",
        exporter.render(disk_space, http_metrics).await,
    ));

    components.extend(collect_graphql_components(schema, &caller).await);
    components.push(collect_pipeline_internals(handle).await);

    let (data_dir, intermediate_dir, complete_dir) = {
        let config = config.read().await;
        (
            PathBuf::from(&config.data_dir),
            PathBuf::from(config.intermediate_dir()),
            PathBuf::from(config.complete_dir()),
        )
    };

    let host_dirs = (data_dir.clone(), intermediate_dir, complete_dir);
    let host = tokio::task::spawn_blocking(move || collect_host_facts(host_dirs)).await;
    components.push(match host {
        Ok(facts) => Component::json("host.json", &facts),
        Err(error) => Component::failed("host.json", error),
    });

    components.extend(collect_log_files().await);
    components.extend(collect_config_files(&data_dir).await);

    // The second Prometheus sample is the last thing collected, so the gap
    // between the two is at least the configured one no matter how long the
    // rest of the collection took.
    let elapsed = started.elapsed();
    if let Some(remaining) = METRICS_SAMPLE_GAP.checked_sub(elapsed) {
        tokio::time::sleep(remaining).await;
    }
    components.push(Component::text(
        "metrics-2.txt",
        exporter.render(disk_space, http_metrics).await,
    ));

    components
}

/// Everything reachable over GraphQL, each root group in its own file.
///
/// Every root field is executed as its own request. One combined query would be
/// simpler, but the schema caps query complexity, and the whole point of a
/// generated selection is that it grows: a shared budget would mean a new field
/// on `Metrics` silently costing `queueSnapshot` its place in the package.
async fn collect_graphql_components(
    schema: &WeaverSchema,
    caller: &super::auth::ResolvedCaller,
) -> Vec<Component> {
    let shape = match SchemaShape::current() {
        Ok(shape) => shape,
        Err(error) => {
            return GRAPHQL_COMPONENTS
                .iter()
                .map(|group| Component::failed(group.file, &error))
                .collect();
        }
    };

    let mut components = Vec::new();
    for group in GRAPHQL_COMPONENTS {
        let mut data = serde_json::Map::new();
        let mut errors = Vec::new();
        for (field, arguments) in group.fields {
            match graphql::run_root_field(
                schema,
                shape,
                caller.scope,
                caller.identity.clone(),
                field,
                *arguments,
            )
            .await
            {
                Ok((value, field_errors)) => {
                    data.insert((*field).to_string(), value);
                    errors.extend(field_errors);
                }
                Err(error) => errors.push(error),
            }
        }

        let mut data = serde_json::Value::Object(data);
        if group.redact {
            redact_json(&mut data, is_sensitive_json_key);
        }

        components.push(if group.file == SERVICE_LOG_FILE {
            Component::text(group.file, service_log_text(&data))
        } else if data.as_object().is_some_and(serde_json::Map::is_empty) {
            Component::failed(group.file, errors.join("; "))
        } else if errors.is_empty() {
            Component::json(group.file, &data)
        } else {
            // Partial data is worth keeping, but a reader has to be able to see
            // that it is partial.
            Component::json(
                group.file,
                &serde_json::json!({ "data": data, "errors": errors }),
            )
        });
    }
    components
}

const SERVICE_LOG_FILE: &str = "service-log.txt";

/// One output file and the root fields that fill it.
struct GraphqlComponent {
    file: &'static str,
    fields: &'static [(&'static str, Option<&'static str>)],
    redact: bool,
}

const GRAPHQL_COMPONENTS: &[GraphqlComponent] = &[
    GraphqlComponent {
        file: "system-info.json",
        fields: &[("systemInfo", None), ("version", None)],
        redact: true,
    },
    GraphqlComponent {
        file: "metrics.json",
        fields: &[("metrics", None), ("globalQueueState", None)],
        redact: true,
    },
    GraphqlComponent {
        file: "server-health.json",
        fields: &[("serverHealth", None)],
        redact: true,
    },
    GraphqlComponent {
        file: "queue.json",
        fields: &[("queueSnapshot", None), ("jobs", Some(JOBS_ARGUMENTS))],
        redact: true,
    },
    GraphqlComponent {
        file: "servers.json",
        fields: &[("servers", None)],
        redact: true,
    },
    GraphqlComponent {
        file: "settings.json",
        fields: &[("settings", None)],
        redact: true,
    },
    GraphqlComponent {
        file: "schedules.json",
        fields: &[("schedules", None)],
        redact: true,
    },
    GraphqlComponent {
        file: SERVICE_LOG_FILE,
        fields: &[("serviceLogs", Some(SERVICE_LOG_ARGUMENTS))],
        redact: false,
    },
];

/// Flattens the `serviceLogs` payload into the plain text a reader wants.
fn service_log_text(data: &serde_json::Value) -> String {
    let Some(lines) = data
        .get("serviceLogs")
        .and_then(|payload| payload.get("lines"))
        .and_then(serde_json::Value::as_array)
    else {
        return String::new();
    };
    let mut text = String::new();
    for line in lines {
        if let Some(line) = line.as_str() {
            text.push_str(line);
            text.push('\n');
        }
    }
    text
}

/// The read-only pipeline snapshot, which exists nowhere else.
async fn collect_pipeline_internals(handle: &SchedulerHandle) -> Component {
    const FILE: &str = "pipeline-internals.json";
    // A wedged pipeline is exactly the case this endpoint is for, so the
    // request must not inherit the wedge: if the actor cannot answer promptly,
    // that fact is the diagnostic.
    match tokio::time::timeout(Duration::from_secs(3), handle.pipeline_diagnostics()).await {
        Ok(Ok(diagnostics)) => Component::json(FILE, &diagnostics),
        Ok(Err(error)) => Component::failed(FILE, error),
        Err(_) => Component::failed(
            FILE,
            "the pipeline did not answer the diagnostics command within 3s",
        ),
    }
}

/// Host facts, including a storage classification per configured directory.
fn collect_host_facts(dirs: (PathBuf, PathBuf, PathBuf)) -> HostFacts {
    use weaver_server_core::runtime::{environment, system_probe};

    let (data_dir, intermediate_dir, complete_dir) = dirs;
    // The startup profile is the cheap half of the probe: it classifies the
    // filesystem behind the path it is given and reads CPU and memory, with no
    // benchmark attached.
    let profile = system_probe::detect_startup_profile(&data_dir);
    let deployment = environment::detect_runtime_environment();

    let storage = [
        ("data", data_dir),
        ("intermediate", intermediate_dir),
        ("complete", complete_dir),
    ]
    .into_iter()
    .map(|(role, path)| {
        let (storage_class, filesystem) = system_probe::classify_storage(&path);
        let mut entry = HostStorage {
            role,
            path: path.display().to_string(),
            storage_class: Some(format!("{storage_class:?}")),
            filesystem: Some(format!("{filesystem:?}")),
            total_bytes: None,
            used_bytes: None,
            free_bytes: None,
            error: None,
        };
        match weaver_server_core::operations::probe_disk_space(&path) {
            Ok(space) => {
                entry.total_bytes = Some(space.total_bytes);
                entry.used_bytes = Some(space.used_bytes());
                entry.free_bytes = Some(space.available_bytes);
            }
            Err(error) => entry.error = Some(error.to_string()),
        }
        entry
    })
    .collect();

    HostFacts {
        operating_system: std::env::consts::OS,
        architecture: std::env::consts::ARCH,
        deployment: format!("{:?}", deployment.deployment),
        physical_cores: profile.cpu.physical_cores,
        logical_cores: profile.cpu.logical_cores,
        cpu_cgroup_limit: profile.cpu.cgroup_limit,
        memory_total_bytes: profile.memory.total_bytes,
        memory_available_bytes: profile.memory.available_bytes,
        memory_cgroup_limit_bytes: profile.memory.cgroup_limit,
        storage,
    }
}

/// The rolling log files, as the running process resolved their location.
async fn collect_log_files() -> Vec<Component> {
    let paths = weaver_server_core::runtime::log_buffer::existing_log_files();
    if paths.is_empty() {
        return vec![Component::failed(
            "weaver.log",
            "this process writes no log file, so none could be collected",
        )];
    }

    let mut components = Vec::new();
    for path in paths {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "weaver.log".to_string());
        // A rotated log is gzipped, and the tail of a gzip stream decompresses
        // to nothing. An oversized rotated file is therefore reported rather
        // than truncated into an unreadable member.
        if name.ends_with(".gz") {
            components.push(match tokio::fs::metadata(&path).await {
                Ok(metadata) if metadata.len() > MAX_LOG_BYTES => Component::failed(
                    name,
                    format!(
                        "left out because the compressed file is {} bytes, past the {MAX_LOG_BYTES} \
                         byte ceiling, and a partial gzip stream cannot be read",
                        metadata.len()
                    ),
                ),
                Ok(_) => match read_file_tail(&path, MAX_LOG_BYTES).await {
                    Ok(data) => Component::bytes(name, data),
                    Err(error) => Component::failed(name, error),
                },
                Err(error) => {
                    Component::failed(name, format!("could not stat {}: {error}", path.display()))
                }
            });
            continue;
        }

        components.push(match read_file_tail(&path, MAX_LOG_BYTES).await {
            Ok(data) => Component::bytes(name, data),
            Err(error) => Component::failed(name, error),
        });
    }
    components
}

/// Every TOML file sitting directly in the data directory, redacted.
async fn collect_config_files(data_dir: &Path) -> Vec<Component> {
    let mut entries = match tokio::fs::read_dir(data_dir).await {
        Ok(entries) => entries,
        Err(error) => {
            return vec![Component::failed(
                "config",
                format!("could not list {}: {error}", data_dir.display()),
            )];
        }
    };

    let mut paths = Vec::new();
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let path = entry.path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "toml")
                {
                    paths.push(path);
                }
            }
            Ok(None) => break,
            Err(error) => {
                return vec![Component::failed(
                    "config",
                    format!("could not walk {}: {error}", data_dir.display()),
                )];
            }
        }
    }
    paths.sort();

    let mut components = Vec::with_capacity(paths.len());
    for path in paths {
        let name = format!(
            "config/{}",
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| "config.toml".to_string())
        );
        components.push(match read_file_tail(&path, MAX_CONFIG_BYTES).await {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(contents) => match redact::redact_toml_document(&contents) {
                    Ok(redacted) => Component::text(name, redacted),
                    Err(error) => Component::failed(
                        name,
                        format!("left out because it could not be parsed for redaction: {error}"),
                    ),
                },
                Err(_) => Component::failed(name, "left out because it is not valid UTF-8"),
            },
            Err(error) => Component::failed(name, error),
        });
    }
    components
}

/// Reads at most `max_bytes` from the end of a file.
async fn read_file_tail(path: &Path, max_bytes: u64) -> Result<Vec<u8>, String> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| format!("could not open {}: {error}", path.display()))?;
    let length = file
        .metadata()
        .await
        .map_err(|error| format!("could not stat {}: {error}", path.display()))?
        .len();
    if length > max_bytes {
        file.seek(std::io::SeekFrom::Start(length - max_bytes))
            .await
            .map_err(|error| format!("could not seek {}: {error}", path.display()))?;
    }
    let mut data = Vec::with_capacity(usize::try_from(length.min(max_bytes)).unwrap_or_default());
    file.take(max_bytes)
        .read_to_end(&mut data)
        .await
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    Ok(data)
}

#[cfg(test)]
mod tests;
