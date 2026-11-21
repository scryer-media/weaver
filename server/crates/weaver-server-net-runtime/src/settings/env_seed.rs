// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::proxies::ProxiesNetworkPersistenceQueries;
use crate::servers::service_queries::ServersServiceInternalQueries;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::task::Poll;
use std::time::Duration;
use weaver_server_db::persistence::connection::Database as SqlDatabase;
use weaver_server_settings::settings::persistence::SettingsPersistenceQueries;
use weaver_server_settings::settings::queries::SettingsQueries;

use crate::servers::{
    ServerConfig, ServerConnectivityResult, ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod,
};
use weaver_server_db::persistence::StateError;
use weaver_server_net::bandwidth::QuotaWeekday;
use weaver_server_settings::settings::model::Config;

pub const ENV_DATA_DIR: &str = "WEAVER_DATA_DIR";
pub const ENV_SCRIPTS_DIR: &str = "WEAVER_SCRIPTS_DIR";
pub const ENV_INTERMEDIATE_DIR: &str = "WEAVER_INTERMEDIATE_DIR";
pub const ENV_COMPLETE_DIR: &str = "WEAVER_COMPLETE_DIR";
pub const ENV_MAX_DOWNLOAD_SPEED: &str = "WEAVER_MAX_DOWNLOAD_SPEED";
pub const ENV_CLEANUP_AFTER_EXTRACT: &str = "WEAVER_CLEANUP_AFTER_EXTRACT";

const SERVER_PREFIX: &str = "WEAVER_SERVER_";
const SERVER_FIELD_HOSTNAME: &str = "HOSTNAME";
const SERVER_FIELD_PORT: &str = "PORT";
const SERVER_FIELD_TLS: &str = "TLS";
const SERVER_FIELD_USERNAME: &str = "USERNAME";
const SERVER_FIELD_PASSWORD: &str = "PASSWORD";
const SERVER_FIELD_CONNECTIONS: &str = "CONNECTIONS";
const SERVER_FIELD_ACTIVE: &str = "ACTIVE";
const SERVER_FIELD_PRIORITY: &str = "PRIORITY";
const SERVER_FIELD_BACKFILL: &str = "BACKFILL";
const SERVER_FIELD_RETENTION_DAYS: &str = "RETENTION_DAYS";
const SERVER_FIELD_MAX_DOWNLOAD_SPEED: &str = "MAX_DOWNLOAD_SPEED";
const SERVER_FIELD_DOWNLOAD_QUOTA_ENABLED: &str = "DOWNLOAD_QUOTA_ENABLED";
const SERVER_FIELD_DOWNLOAD_QUOTA_LIMIT_BYTES: &str = "DOWNLOAD_QUOTA_LIMIT_BYTES";
const SERVER_FIELD_DOWNLOAD_QUOTA_PERIOD: &str = "DOWNLOAD_QUOTA_PERIOD";
const SERVER_FIELD_DOWNLOAD_QUOTA_RESET_TIME_MINUTES_LOCAL: &str =
    "DOWNLOAD_QUOTA_RESET_TIME_MINUTES_LOCAL";
const SERVER_FIELD_DOWNLOAD_QUOTA_WEEKLY_RESET_WEEKDAY: &str =
    "DOWNLOAD_QUOTA_WEEKLY_RESET_WEEKDAY";
const SERVER_FIELD_DOWNLOAD_QUOTA_MONTHLY_RESET_DAY: &str = "DOWNLOAD_QUOTA_MONTHLY_RESET_DAY";
const SERVER_FIELD_TLS_CA_CERT: &str = "TLS_CA_CERT";
const SERVER_FIELD_PIPELINING: &str = "PIPELINING";

#[derive(Debug, Clone, Default)]
pub struct EnvSeedConfig {
    pub core: EnvCoreSeed,
    pub servers: Vec<EnvSeedServer>,
}

// One seeded server, plus whether the environment stated its pipelining flag
// outright. A stated flag is a pin and is taken at its word; every other
// seeded server is asked, the way a server saved through the server form is.
#[derive(Debug, Clone)]
pub struct EnvSeedServer {
    pub config: ServerConfig,
    pub pipelining_pinned: bool,
}

// What asking one seeded server about pipelining came back with. A server
// that could not be reached reports the reason and keeps the sequential
// default, so a provider that is down at boot cannot stop the seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeededServerProbeOutcome {
    pub server_id: u32,
    pub supports_pipelining: bool,
    pub first_byte_latency_ms: Option<u64>,
    pub failure: Option<String>,
}

impl EnvSeedConfig {
    pub fn is_empty(&self) -> bool {
        self.core.is_empty() && self.servers.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvCoreSeed {
    pub data_dir: Option<String>,
    pub scripts_dir: Option<String>,
    pub intermediate_dir: Option<String>,
    pub complete_dir: Option<String>,
    pub max_download_speed: Option<u64>,
    pub cleanup_after_extract: Option<bool>,
}

impl EnvCoreSeed {
    pub fn is_empty(&self) -> bool {
        self.data_dir.is_none()
            && self.scripts_dir.is_none()
            && self.intermediate_dir.is_none()
            && self.complete_dir.is_none()
            && self.max_download_speed.is_none()
            && self.cleanup_after_extract.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvSeedApplyReport {
    pub core_settings_seeded: usize,
    pub servers_seeded: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvSeedError {
    message: String,
}

impl EnvSeedError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for EnvSeedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for EnvSeedError {}

#[derive(Default)]
struct PartialServerSeed {
    hostname: Option<String>,
    port: Option<u16>,
    tls: Option<bool>,
    username: Option<String>,
    password: Option<String>,
    connections: Option<u16>,
    active: Option<bool>,
    priority: Option<u32>,
    backfill: Option<bool>,
    retention_days: Option<u32>,
    max_download_speed: Option<u64>,
    download_quota_enabled: Option<bool>,
    download_quota_limit_bytes: Option<u64>,
    download_quota_period: Option<ServerDownloadQuotaPeriod>,
    download_quota_reset_time_minutes_local: Option<u16>,
    download_quota_weekly_reset_weekday: Option<QuotaWeekday>,
    download_quota_monthly_reset_day: Option<u8>,
    tls_ca_cert: Option<PathBuf>,
    pipelining: Option<bool>,
}

pub fn parse_env_seed<I, K, V>(vars: I) -> Result<EnvSeedConfig, EnvSeedError>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let vars: HashMap<String, String> = vars
        .into_iter()
        .map(|(key, value)| (key.as_ref().to_string(), value.as_ref().to_string()))
        .collect();

    let core = EnvCoreSeed {
        data_dir: parse_required_string(&vars, ENV_DATA_DIR)?,
        scripts_dir: parse_required_string(&vars, ENV_SCRIPTS_DIR)?,
        intermediate_dir: parse_required_string(&vars, ENV_INTERMEDIATE_DIR)?,
        complete_dir: parse_required_string(&vars, ENV_COMPLETE_DIR)?,
        max_download_speed: parse_optional_u64(&vars, ENV_MAX_DOWNLOAD_SPEED)?,
        cleanup_after_extract: parse_optional_bool(&vars, ENV_CLEANUP_AFTER_EXTRACT)?,
    };
    let servers = parse_servers(&vars)?;

    Ok(EnvSeedConfig { core, servers })
}

pub fn apply_core_seed(
    db: &SqlDatabase,
    config: &mut Config,
    seed: &EnvSeedConfig,
) -> Result<usize, StateError> {
    let mut seeded = 0;

    // The data folder is the one setting the process may already have resolved
    // before this runs: a config with nothing stored is given a folder derived
    // from the config path, in memory only. Every reader that loads the
    // settings afresh would still see an empty data_dir, so the stored row is
    // the test here, not the in-memory value, and whatever the process resolved
    // is written back when the store has none.
    let stored_data_dir = db
        .get_setting("data_dir")?
        .filter(|value| !value.trim().is_empty());
    if stored_data_dir.is_none() {
        if let Some(value) = &seed.core.data_dir {
            db.set_setting("data_dir", value)?;
            config.data_dir = value.clone();
            seeded += 1;
        } else if !config.data_dir.trim().is_empty() {
            db.set_setting("data_dir", &config.data_dir)?;
        }
    }

    if config
        .intermediate_dir
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
        && let Some(value) = &seed.core.intermediate_dir
    {
        db.set_setting("intermediate_dir", value)?;
        config.intermediate_dir = Some(value.clone());
        seeded += 1;
    }

    if config
        .complete_dir
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
        && let Some(value) = &seed.core.complete_dir
    {
        db.set_setting("complete_dir", value)?;
        config.complete_dir = Some(value.clone());
        seeded += 1;
    }

    // A seeded limit of 0 is no limit, as the setting it seeds always was.
    if config.bandwidth.download_bps.is_none()
        && let Some(value) = seed.core.max_download_speed.filter(|&value| value > 0)
    {
        let bandwidth = crate::proxies::Bandwidth {
            download_bps: Some(value),
            ..config.bandwidth
        };
        db.set_network_bandwidth(&bandwidth)?;
        config.bandwidth = bandwidth;
        seeded += 1;
    }

    if config.cleanup_after_extract.is_none()
        && let Some(value) = seed.core.cleanup_after_extract
    {
        db.set_setting("cleanup_after_extract", &value.to_string())?;
        config.cleanup_after_extract = Some(value);
        seeded += 1;
    }

    Ok(seeded)
}

// Whether the seeded servers are the ones this install will actually run on.
// Nothing is asked of a server the seed is not going to store.
pub fn server_seed_applies(config: &Config, seed: &EnvSeedConfig) -> bool {
    config.servers.is_empty() && !seed.servers.is_empty()
}

// How long boot waits, in all, for the seeded servers to answer. The probes
// run before the seed is stored and before the listener opens, so a server
// that never answers must not hold startup for its full connect timeouts.
pub const SEED_PROBE_DEADLINE: Duration = Duration::from_secs(10);

// Ask every seeded server whose pipelining flag the environment left unstated
// whether it pipelines, and keep what came back. The caller supplies the
// question so the seed can be exercised without a server to reach.
//
// Every server is asked at once, and all of them share one
// [`SEED_PROBE_DEADLINE`]. A server with no answer by then is treated like
// one that could not be reached: it keeps the sequential default.
pub async fn probe_seeded_server_pipelining<P, F>(
    seed: &mut EnvSeedConfig,
    probe: P,
) -> Vec<SeededServerProbeOutcome>
where
    P: Fn(ServerConfig) -> F,
    F: Future<Output = ServerConnectivityResult>,
{
    let deadline = tokio::time::Instant::now() + SEED_PROBE_DEADLINE;
    let mut probes: Vec<_> = seed
        .servers
        .iter()
        .enumerate()
        .filter(|(_, server)| !server.pipelining_pinned && server.config.active)
        .map(|(index, server)| {
            (
                index,
                Box::pin(tokio::time::timeout_at(
                    deadline,
                    probe(server.config.clone()),
                )),
                None,
            )
        })
        .collect();
    std::future::poll_fn(|cx| {
        let mut waiting = false;
        for (_, probe, answer) in &mut probes {
            if answer.is_none() {
                match probe.as_mut().poll(cx) {
                    Poll::Ready(result) => *answer = Some(result),
                    Poll::Pending => waiting = true,
                }
            }
        }
        if waiting {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;

    let mut outcomes = Vec::new();
    for (index, _, answer) in probes {
        let server = &mut seed.servers[index];
        let result =
            answer
                .expect("every probe finished")
                .unwrap_or_else(|_| ServerConnectivityResult {
                    success: false,
                    message: format!("no answer within {} seconds", SEED_PROBE_DEADLINE.as_secs()),
                    latency_ms: None,
                    first_byte_latency_ms: None,
                    first_byte_latency_band: None,
                    supports_pipelining: false,
                    adoptable_tls_name_mismatch_certificate_der: None,
                    tls_cipher_suite: None,
                    tls_honors_client_cipher_order: None,
                });
        if result.success {
            server.config.supports_pipelining = result.supports_pipelining;
            outcomes.push(SeededServerProbeOutcome {
                server_id: server.config.id,
                supports_pipelining: result.supports_pipelining,
                first_byte_latency_ms: result.first_byte_latency_ms,
                failure: None,
            });
        } else {
            outcomes.push(SeededServerProbeOutcome {
                server_id: server.config.id,
                supports_pipelining: false,
                first_byte_latency_ms: None,
                failure: Some(result.message),
            });
        }
    }
    outcomes
}

pub fn apply_server_seed(
    db: &SqlDatabase,
    config: &mut Config,
    seed: &EnvSeedConfig,
) -> Result<usize, StateError> {
    if !server_seed_applies(config, seed) {
        return Ok(0);
    }

    let servers: Vec<ServerConfig> = seed
        .servers
        .iter()
        .map(|server| server.config.clone())
        .collect();
    db.replace_servers(&servers)?;
    config.servers = servers;
    Ok(seed.servers.len())
}

pub fn apply_seed(
    db: &SqlDatabase,
    config: &mut Config,
    seed: &EnvSeedConfig,
) -> Result<EnvSeedApplyReport, StateError> {
    let core_settings_seeded = apply_core_seed(db, config, seed)?;
    let servers_seeded = apply_server_seed(db, config, seed)?;
    Ok(EnvSeedApplyReport {
        core_settings_seeded,
        servers_seeded,
    })
}

fn parse_servers(vars: &HashMap<String, String>) -> Result<Vec<EnvSeedServer>, EnvSeedError> {
    let mut partials = BTreeMap::<u32, PartialServerSeed>::new();
    for (key, value) in vars {
        let Some(rest) = key.strip_prefix(SERVER_PREFIX) else {
            continue;
        };
        let Some((raw_index, field)) = rest.split_once('_') else {
            return Err(EnvSeedError::new(format!(
                "{key} must use WEAVER_SERVER_<N>_<FIELD>"
            )));
        };
        let index = raw_index.parse::<u32>().map_err(|_| {
            EnvSeedError::new(format!(
                "{key} must use a positive integer server index after {SERVER_PREFIX}"
            ))
        })?;
        if index == 0 {
            return Err(EnvSeedError::new(format!(
                "{key} must use a positive integer server index"
            )));
        }

        let partial = partials.entry(index).or_default();
        match field {
            SERVER_FIELD_HOSTNAME => {
                partial.hostname = Some(parse_required_value(key, value)?);
            }
            SERVER_FIELD_PORT => {
                partial.port = Some(parse_u16_value(key, value)?);
            }
            SERVER_FIELD_TLS => {
                partial.tls = Some(parse_bool_value(key, value)?);
            }
            SERVER_FIELD_USERNAME => {
                partial.username = normalize_optional_string(value);
            }
            SERVER_FIELD_PASSWORD => {
                partial.password = normalize_optional_string(value);
            }
            SERVER_FIELD_CONNECTIONS => {
                partial.connections = Some(parse_u16_value(key, value)?);
            }
            SERVER_FIELD_ACTIVE => {
                partial.active = Some(parse_bool_value(key, value)?);
            }
            SERVER_FIELD_PRIORITY => {
                partial.priority = Some(parse_u32_value(key, value)?);
            }
            SERVER_FIELD_BACKFILL => {
                partial.backfill = Some(parse_bool_value(key, value)?);
            }
            SERVER_FIELD_RETENTION_DAYS => {
                partial.retention_days = Some(parse_u32_value(key, value)?);
            }
            SERVER_FIELD_MAX_DOWNLOAD_SPEED => {
                partial.max_download_speed = Some(parse_u64_value(key, value)?);
            }
            SERVER_FIELD_DOWNLOAD_QUOTA_ENABLED => {
                partial.download_quota_enabled = Some(parse_bool_value(key, value)?);
            }
            SERVER_FIELD_DOWNLOAD_QUOTA_LIMIT_BYTES => {
                partial.download_quota_limit_bytes = Some(parse_u64_value(key, value)?);
            }
            SERVER_FIELD_DOWNLOAD_QUOTA_PERIOD => {
                partial.download_quota_period = Some(parse_quota_period_value(key, value)?);
            }
            SERVER_FIELD_DOWNLOAD_QUOTA_RESET_TIME_MINUTES_LOCAL => {
                partial.download_quota_reset_time_minutes_local =
                    Some(parse_reset_minutes_value(key, value)?);
            }
            SERVER_FIELD_DOWNLOAD_QUOTA_WEEKLY_RESET_WEEKDAY => {
                partial.download_quota_weekly_reset_weekday =
                    Some(parse_quota_weekday_value(key, value)?);
            }
            SERVER_FIELD_DOWNLOAD_QUOTA_MONTHLY_RESET_DAY => {
                partial.download_quota_monthly_reset_day =
                    Some(parse_monthly_reset_day_value(key, value)?);
            }
            SERVER_FIELD_TLS_CA_CERT => {
                partial.tls_ca_cert = normalize_optional_string(value).map(PathBuf::from);
            }
            SERVER_FIELD_PIPELINING => {
                partial.pipelining = Some(parse_bool_value(key, value)?);
            }
            _ => {
                return Err(EnvSeedError::new(format!(
                    "unknown {SERVER_PREFIX} field {field:?} in {key}"
                )));
            }
        }
    }

    let mut servers = Vec::with_capacity(partials.len());
    for (expected, (index, partial)) in (1u32..).zip(partials) {
        if index != expected {
            return Err(EnvSeedError::new(format!(
                "WEAVER_SERVER indexes must be contiguous from 1; missing WEAVER_SERVER_{expected}_{SERVER_FIELD_HOSTNAME}"
            )));
        }

        let Some(host) = partial.hostname else {
            return Err(EnvSeedError::new(format!(
                "WEAVER_SERVER_{index}_{SERVER_FIELD_HOSTNAME} is required"
            )));
        };
        let tls = partial.tls.unwrap_or(true);
        let port = partial.port.unwrap_or(if tls { 563 } else { 119 });
        let connections = partial.connections.unwrap_or(10);
        let download_quota = ServerDownloadQuotaConfig {
            enabled: partial.download_quota_enabled.unwrap_or(false),
            limit_bytes: partial.download_quota_limit_bytes.unwrap_or(0),
            period: partial.download_quota_period.unwrap_or_default(),
            reset_time_minutes_local: partial.download_quota_reset_time_minutes_local.unwrap_or(0),
            weekly_reset_weekday: partial
                .download_quota_weekly_reset_weekday
                .unwrap_or(QuotaWeekday::Mon),
            monthly_reset_day: partial.download_quota_monthly_reset_day.unwrap_or(1),
        };
        let server = ServerConfig {
            id: index,
            host,
            port,
            tls,
            username: partial.username,
            password: partial.password,
            connections,
            active: partial.active.unwrap_or(true),
            // A stated pipelining flag is a pin and stands as given. Left
            // unstated, this is only the value the seed starts from: the
            // server is asked for its capabilities before it is stored.
            supports_pipelining: partial.pipelining.unwrap_or(false),
            pipelining_depth: None,
            priority: partial.priority.unwrap_or(0),
            backfill: partial.backfill.unwrap_or(false),
            retention_days: partial.retention_days.unwrap_or(0),
            max_download_speed: partial.max_download_speed.unwrap_or(0),
            download_quota,
            tls_ca_cert: partial.tls_ca_cert,
            tls_name_mismatch_certificate_der: None,
        };
        server.validate_download_limits().map_err(|error| {
            EnvSeedError::new(format!("WEAVER_SERVER_{index} download limits: {error}"))
        })?;
        servers.push(EnvSeedServer {
            config: server,
            pipelining_pinned: partial.pipelining.is_some(),
        });
    }

    Ok(servers)
}

fn parse_required_string(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<String>, EnvSeedError> {
    vars.get(name)
        .map(|value| parse_required_value(name, value))
        .transpose()
}

fn parse_optional_bool(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<bool>, EnvSeedError> {
    vars.get(name)
        .map(|value| parse_bool_value(name, value))
        .transpose()
}

fn parse_optional_u64(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<u64>, EnvSeedError> {
    vars.get(name)
        .map(|value| parse_u64_value(name, value))
        .transpose()
}

fn parse_required_value(name: &str, value: &str) -> Result<String, EnvSeedError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(EnvSeedError::new(format!("{name} must not be empty")));
    }
    Ok(trimmed.to_string())
}

fn normalize_optional_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn parse_bool_value(name: &str, value: &str) -> Result<bool, EnvSeedError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(EnvSeedError::new(format!(
            "{name} must be a boolean value (true/false, 1/0, yes/no, on/off)"
        ))),
    }
}

fn parse_u16_value(name: &str, value: &str) -> Result<u16, EnvSeedError> {
    let parsed = value
        .trim()
        .parse::<u16>()
        .map_err(|_| EnvSeedError::new(format!("{name} must be an integer from 1 to 65535")))?;
    if parsed == 0 {
        return Err(EnvSeedError::new(format!(
            "{name} must be an integer from 1 to 65535"
        )));
    }
    Ok(parsed)
}

fn parse_u32_value(name: &str, value: &str) -> Result<u32, EnvSeedError> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| EnvSeedError::new(format!("{name} must be an unsigned integer")))
}

fn parse_u64_value(name: &str, value: &str) -> Result<u64, EnvSeedError> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| EnvSeedError::new(format!("{name} must be an unsigned integer")))
}

fn parse_quota_period_value(
    name: &str,
    value: &str,
) -> Result<ServerDownloadQuotaPeriod, EnvSeedError> {
    let value = value.trim().to_ascii_lowercase();
    ServerDownloadQuotaPeriod::parse(&value).ok_or_else(|| {
        EnvSeedError::new(format!(
            "{name} must be one_time, daily, weekly, or monthly"
        ))
    })
}

fn parse_quota_weekday_value(name: &str, value: &str) -> Result<QuotaWeekday, EnvSeedError> {
    let value = value.trim().to_ascii_lowercase();
    crate::servers::record::parse_quota_weekday(&value).ok_or_else(|| {
        EnvSeedError::new(format!(
            "{name} must be mon, tue, wed, thu, fri, sat, or sun"
        ))
    })
}

fn parse_reset_minutes_value(name: &str, value: &str) -> Result<u16, EnvSeedError> {
    let value = value
        .trim()
        .parse::<u16>()
        .map_err(|_| EnvSeedError::new(format!("{name} must be an integer from 0 to 1439")))?;
    if value >= 24 * 60 {
        return Err(EnvSeedError::new(format!(
            "{name} must be an integer from 0 to 1439"
        )));
    }
    Ok(value)
}

fn parse_monthly_reset_day_value(name: &str, value: &str) -> Result<u8, EnvSeedError> {
    let value = value
        .trim()
        .parse::<u8>()
        .map_err(|_| EnvSeedError::new(format!("{name} must be an integer from 1 to 31")))?;
    if !(1..=31).contains(&value) {
        return Err(EnvSeedError::new(format!(
            "{name} must be an integer from 1 to 31"
        )));
    }
    Ok(value)
}
