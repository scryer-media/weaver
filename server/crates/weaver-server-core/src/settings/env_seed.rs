use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::task::Poll;
use std::time::Duration;

use crate::StateError;
use crate::bandwidth::IspBandwidthCapWeekday;
use crate::persistence::Database;
use crate::servers::{
    ServerConfig, ServerConnectivityResult, ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod,
};
use crate::settings::Config;

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

/// One seeded server, plus whether the environment stated its pipelining flag
/// outright. A stated flag is a pin and is taken at its word; every other
/// seeded server is asked, the way a server saved through the server form is.
#[derive(Debug, Clone)]
pub struct EnvSeedServer {
    pub config: ServerConfig,
    pub pipelining_pinned: bool,
}

/// What asking one seeded server about pipelining came back with. A server
/// that could not be reached reports the reason and keeps the sequential
/// default, so a provider that is down at boot cannot stop the seed.
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
    download_quota_weekly_reset_weekday: Option<IspBandwidthCapWeekday>,
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
    db: &Database,
    config: &mut Config,
    seed: &EnvSeedConfig,
) -> Result<usize, StateError> {
    let mut seeded = 0;

    if config.data_dir.trim().is_empty()
        && let Some(value) = &seed.core.data_dir
    {
        db.set_setting("data_dir", value)?;
        config.data_dir = value.clone();
        seeded += 1;
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

    if config.max_download_speed.is_none()
        && let Some(value) = seed.core.max_download_speed
    {
        db.set_setting("max_download_speed", &value.to_string())?;
        config.max_download_speed = Some(value);
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

/// Whether the seeded servers are the ones this install will actually run on.
/// Nothing is asked of a server the seed is not going to store.
pub fn server_seed_applies(config: &Config, seed: &EnvSeedConfig) -> bool {
    config.servers.is_empty() && !seed.servers.is_empty()
}

/// How long boot waits, in all, for the seeded servers to answer. The probes
/// run before the seed is stored and before the listener opens, so a server
/// that never answers must not hold startup for its full connect timeouts.
pub const SEED_PROBE_DEADLINE: Duration = Duration::from_secs(10);

/// Ask every seeded server whose pipelining flag the environment left unstated
/// whether it pipelines, and keep what came back. The caller supplies the
/// question so the seed can be exercised without a server to reach.
///
/// Every server is asked at once, and all of them share one
/// [`SEED_PROBE_DEADLINE`]. A server with no answer by then is treated like
/// one that could not be reached: it keeps the sequential default.
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
    db: &Database,
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
    db: &Database,
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
                .unwrap_or(IspBandwidthCapWeekday::Mon),
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

fn parse_quota_weekday_value(
    name: &str,
    value: &str,
) -> Result<IspBandwidthCapWeekday, EnvSeedError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::categories::CategoryConfig;

    fn parse(vars: &[(&str, &str)]) -> Result<EnvSeedConfig, EnvSeedError> {
        parse_env_seed(vars.iter().copied())
    }

    #[test]
    fn parses_single_server_defaults() {
        let seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "news.example.com")]).unwrap();

        assert_eq!(seed.servers.len(), 1);
        let server = &seed.servers[0].config;
        assert_eq!(server.id, 1);
        assert_eq!(server.host, "news.example.com");
        assert_eq!(server.port, 563);
        assert!(server.tls);
        assert_eq!(server.connections, 10);
        assert!(server.active);
        assert_eq!(server.priority, 0);
        assert!(!server.supports_pipelining);
        assert!(!seed.servers[0].pipelining_pinned);
        assert_eq!(server.max_download_speed, 0);
        assert_eq!(server.download_quota, ServerDownloadQuotaConfig::default());
    }

    #[test]
    fn parses_server_download_limits() {
        let seed = parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
            ("WEAVER_SERVER_1_MAX_DOWNLOAD_SPEED", "2500000"),
            ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_ENABLED", "true"),
            ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_LIMIT_BYTES", "9000000"),
            ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_PERIOD", "weekly"),
            (
                "WEAVER_SERVER_1_DOWNLOAD_QUOTA_RESET_TIME_MINUTES_LOCAL",
                "375",
            ),
            ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_WEEKLY_RESET_WEEKDAY", "thu"),
            ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_MONTHLY_RESET_DAY", "31"),
        ])
        .unwrap();

        let server = &seed.servers[0].config;
        assert_eq!(server.max_download_speed, 2_500_000);
        assert!(server.download_quota.enabled);
        assert_eq!(server.download_quota.limit_bytes, 9_000_000);
        assert_eq!(
            server.download_quota.period,
            ServerDownloadQuotaPeriod::Weekly
        );
        assert_eq!(server.download_quota.reset_time_minutes_local, 375);
        assert_eq!(
            server.download_quota.weekly_reset_weekday,
            IspBandwidthCapWeekday::Thu
        );
        assert_eq!(server.download_quota.monthly_reset_day, 31);
    }

    #[test]
    fn parses_multiple_servers_and_plain_port_default() {
        let seed = parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "plain.example.com"),
            ("WEAVER_SERVER_1_TLS", "false"),
            ("WEAVER_SERVER_1_CONNECTIONS", "4"),
            ("WEAVER_SERVER_2_HOSTNAME", "tls.example.com"),
            ("WEAVER_SERVER_2_PORT", "443"),
            ("WEAVER_SERVER_2_USERNAME", " user "),
            ("WEAVER_SERVER_2_PASSWORD", " pass "),
            ("WEAVER_SERVER_2_PRIORITY", "1"),
            ("WEAVER_SERVER_2_TLS_CA_CERT", "/etc/ssl/custom.pem"),
            ("WEAVER_SERVER_2_PIPELINING", "true"),
        ])
        .unwrap();

        assert_eq!(seed.servers.len(), 2);
        // Unstated is not the same answer as stated false: only the second
        // server carries a pin the probe must leave alone.
        assert!(!seed.servers[0].config.supports_pipelining);
        assert!(!seed.servers[0].pipelining_pinned);
        assert!(seed.servers[1].config.supports_pipelining);
        assert!(seed.servers[1].pipelining_pinned);
        assert_eq!(seed.servers[1].config.pipelining_depth, None);
        assert_eq!(seed.servers[0].config.port, 119);
        assert!(!seed.servers[0].config.tls);
        assert_eq!(seed.servers[0].config.connections, 4);
        assert_eq!(seed.servers[1].config.port, 443);
        assert_eq!(seed.servers[1].config.username.as_deref(), Some("user"));
        assert_eq!(seed.servers[1].config.password.as_deref(), Some("pass"));
        assert_eq!(seed.servers[1].config.priority, 1);
        assert_eq!(
            seed.servers[1].config.tls_ca_cert.as_deref(),
            Some(std::path::Path::new("/etc/ssl/custom.pem"))
        );
    }

    #[test]
    fn records_an_explicitly_disabled_pipelining_flag_as_a_pin() {
        let seed = parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
            ("WEAVER_SERVER_1_PIPELINING", "false"),
        ])
        .unwrap();

        assert!(!seed.servers[0].config.supports_pipelining);
        assert!(seed.servers[0].pipelining_pinned);
    }

    #[test]
    fn rejects_non_contiguous_server_indexes() {
        let error = parse(&[("WEAVER_SERVER_2_HOSTNAME", "news.example.com")]).unwrap_err();

        assert!(error.to_string().contains("contiguous from 1"));
    }

    #[test]
    fn rejects_server_without_hostname() {
        let error = parse(&[("WEAVER_SERVER_1_PORT", "563")]).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("WEAVER_SERVER_1_HOSTNAME is required")
        );
    }

    #[test]
    fn rejects_invalid_values() {
        assert!(
            parse(&[("WEAVER_CLEANUP_AFTER_EXTRACT", "maybe")])
                .unwrap_err()
                .to_string()
                .contains("boolean")
        );
        assert!(
            parse(&[
                ("WEAVER_SERVER_1_HOSTNAME", "news"),
                ("WEAVER_SERVER_1_PORT", "0")
            ])
            .unwrap_err()
            .to_string()
            .contains("1 to 65535")
        );
        assert!(
            parse(&[("WEAVER_MAX_DOWNLOAD_SPEED", "fast")])
                .unwrap_err()
                .to_string()
                .contains("unsigned integer")
        );
        assert!(
            parse(&[
                ("WEAVER_SERVER_1_HOSTNAME", "news"),
                ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_ENABLED", "true")
            ])
            .unwrap_err()
            .to_string()
            .contains("greater than zero")
        );
        assert!(
            parse(&[
                ("WEAVER_SERVER_1_HOSTNAME", "news"),
                (
                    "WEAVER_SERVER_1_DOWNLOAD_QUOTA_RESET_TIME_MINUTES_LOCAL",
                    "1440"
                )
            ])
            .unwrap_err()
            .to_string()
            .contains("0 to 1439")
        );

        let outside_database_range = ((i64::MAX as u64) + 1).to_string();
        assert!(
            parse(&[
                ("WEAVER_SERVER_1_HOSTNAME", "news"),
                (
                    "WEAVER_SERVER_1_MAX_DOWNLOAD_SPEED",
                    outside_database_range.as_str()
                )
            ])
            .unwrap_err()
            .to_string()
            .contains("max download speed exceeds database range")
        );
        assert!(
            parse(&[
                ("WEAVER_SERVER_1_HOSTNAME", "news"),
                (
                    "WEAVER_SERVER_1_DOWNLOAD_QUOTA_LIMIT_BYTES",
                    outside_database_range.as_str()
                )
            ])
            .unwrap_err()
            .to_string()
            .contains("download quota limit exceeds database range")
        );
    }

    #[test]
    fn seeds_missing_core_settings() {
        let db = Database::open_in_memory().unwrap();
        let mut config = db.load_config().unwrap();
        let seed = parse(&[
            ("WEAVER_DATA_DIR", "/config"),
            ("WEAVER_INTERMEDIATE_DIR", "/downloads/incomplete"),
            ("WEAVER_COMPLETE_DIR", "/downloads/complete"),
            ("WEAVER_MAX_DOWNLOAD_SPEED", "1000"),
            ("WEAVER_CLEANUP_AFTER_EXTRACT", "false"),
        ])
        .unwrap();

        let seeded = apply_core_seed(&db, &mut config, &seed).unwrap();

        assert_eq!(seeded, 5);
        assert_eq!(config.data_dir, "/config");
        assert_eq!(
            config.intermediate_dir.as_deref(),
            Some("/downloads/incomplete")
        );
        assert_eq!(config.complete_dir.as_deref(), Some("/downloads/complete"));
        assert_eq!(config.max_download_speed, Some(1000));
        assert_eq!(config.cleanup_after_extract, Some(false));
        assert_eq!(
            db.get_setting("data_dir").unwrap().as_deref(),
            Some("/config")
        );
        assert_eq!(
            db.get_setting("max_download_speed").unwrap().as_deref(),
            Some("1000")
        );
    }

    #[test]
    fn parses_scripts_directory_seed_without_applying_it_as_a_general_setting() {
        let seed = parse(&[("WEAVER_SCRIPTS_DIR", "/scripts")]).unwrap();
        assert_eq!(seed.core.scripts_dir.as_deref(), Some("/scripts"));
    }

    #[test]
    fn preserves_existing_core_settings() {
        let db = Database::open_in_memory().unwrap();
        db.set_setting("data_dir", "/db").unwrap();
        db.set_setting("max_download_speed", "2000").unwrap();
        let mut config = db.load_config().unwrap();
        let seed = parse(&[
            ("WEAVER_DATA_DIR", "/env"),
            ("WEAVER_MAX_DOWNLOAD_SPEED", "1000"),
        ])
        .unwrap();

        let seeded = apply_core_seed(&db, &mut config, &seed).unwrap();

        assert_eq!(seeded, 0);
        assert_eq!(config.data_dir, "/db");
        assert_eq!(config.max_download_speed, Some(2000));
        assert_eq!(db.get_setting("data_dir").unwrap().as_deref(), Some("/db"));
    }

    #[test]
    fn seeds_servers_only_when_database_has_no_servers() {
        let db = Database::open_in_memory().unwrap();
        let mut config = db.load_config().unwrap();
        let seed = parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
            ("WEAVER_SERVER_1_USERNAME", "user"),
        ])
        .unwrap();

        let seeded = apply_server_seed(&db, &mut config, &seed).unwrap();

        assert_eq!(seeded, 1);
        assert_eq!(config.servers.len(), 1);
        let persisted = db.list_servers().unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].host, "news.example.com");
        assert_eq!(persisted[0].username.as_deref(), Some("user"));
    }

    fn probe_answer(success: bool, supports_pipelining: bool) -> ServerConnectivityResult {
        ServerConnectivityResult {
            success,
            message: if success {
                "Connected successfully".to_string()
            } else {
                "connection refused".to_string()
            },
            latency_ms: success.then_some(12),
            first_byte_latency_ms: success.then_some(34),
            first_byte_latency_band: None,
            supports_pipelining: success && supports_pipelining,
            adoptable_tls_name_mismatch_certificate_der: None,
            tls_cipher_suite: None,
            tls_honors_client_cipher_order: None,
        }
    }

    #[tokio::test]
    async fn stores_what_the_probe_found_for_a_server_left_unstated() {
        let db = Database::open_in_memory().unwrap();
        let mut config = db.load_config().unwrap();
        let mut seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "news.example.com")]).unwrap();

        let outcomes =
            probe_seeded_server_pipelining(&mut seed, |_| async { probe_answer(true, true) }).await;
        apply_server_seed(&db, &mut config, &seed).unwrap();

        assert_eq!(
            outcomes,
            vec![SeededServerProbeOutcome {
                server_id: 1,
                supports_pipelining: true,
                first_byte_latency_ms: Some(34),
                failure: None,
            }]
        );
        assert!(config.servers[0].supports_pipelining);
        assert!(db.list_servers().unwrap()[0].supports_pipelining);
    }

    #[tokio::test]
    async fn keeps_the_sequential_default_when_a_seeded_server_cannot_be_reached() {
        let db = Database::open_in_memory().unwrap();
        let mut config = db.load_config().unwrap();
        let mut seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "news.example.com")]).unwrap();

        let outcomes =
            probe_seeded_server_pipelining(&mut seed, |_| async { probe_answer(false, true) })
                .await;
        let seeded = apply_server_seed(&db, &mut config, &seed).unwrap();

        assert_eq!(seeded, 1);
        assert_eq!(outcomes[0].failure.as_deref(), Some("connection refused"));
        assert!(!outcomes[0].supports_pipelining);
        assert!(!config.servers[0].supports_pipelining);
        assert!(!db.list_servers().unwrap()[0].supports_pipelining);
    }

    #[tokio::test]
    async fn leaves_a_stated_pipelining_flag_alone() {
        let db = Database::open_in_memory().unwrap();
        let mut config = db.load_config().unwrap();
        let mut seed = parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
            ("WEAVER_SERVER_1_PIPELINING", "false"),
        ])
        .unwrap();

        let outcomes =
            probe_seeded_server_pipelining(&mut seed, |_| async { probe_answer(true, true) }).await;
        apply_server_seed(&db, &mut config, &seed).unwrap();

        assert!(outcomes.is_empty());
        assert!(!config.servers[0].supports_pipelining);
        assert!(!db.list_servers().unwrap()[0].supports_pipelining);
    }

    #[tokio::test(start_paused = true)]
    async fn asks_every_seeded_server_at_once_under_one_deadline() {
        let mut seed = parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "slow.example.com"),
            ("WEAVER_SERVER_2_HOSTNAME", "slower.example.com"),
            ("WEAVER_SERVER_3_HOSTNAME", "silent.example.com"),
        ])
        .unwrap();
        let started = tokio::time::Instant::now();

        // Asked one after another, the first two would not both fit inside the
        // deadline, and the silent one would hold boot forever.
        let outcomes = probe_seeded_server_pipelining(&mut seed, |server| async move {
            match server.id {
                1 | 2 => {
                    tokio::time::sleep(SEED_PROBE_DEADLINE * 3 / 5).await;
                    probe_answer(true, true)
                }
                _ => std::future::pending().await,
            }
        })
        .await;

        assert_eq!(started.elapsed(), SEED_PROBE_DEADLINE);
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| (outcome.server_id, outcome.failure.is_none()))
                .collect::<Vec<_>>(),
            vec![(1, true), (2, true), (3, false)]
        );
        assert!(seed.servers[0].config.supports_pipelining);
        assert!(seed.servers[1].config.supports_pipelining);
        assert!(!seed.servers[2].config.supports_pipelining);
        assert!(
            outcomes[2]
                .failure
                .as_deref()
                .is_some_and(|reason| reason.contains("no answer"))
        );
    }

    #[tokio::test]
    async fn does_not_ask_an_inactive_seeded_server() {
        let mut seed = parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
            ("WEAVER_SERVER_1_ACTIVE", "false"),
        ])
        .unwrap();

        let outcomes =
            probe_seeded_server_pipelining(&mut seed, |_| async { probe_answer(true, true) }).await;

        assert!(outcomes.is_empty());
        assert!(!seed.servers[0].config.supports_pipelining);
    }

    #[test]
    fn asks_nothing_when_the_database_already_has_servers() {
        let db = Database::open_in_memory().unwrap();
        let mut config = db.load_config().unwrap();
        config.servers = db.list_servers().unwrap();
        let seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "news.example.com")]).unwrap();
        assert!(server_seed_applies(&config, &seed));

        config.servers = vec![existing_server()];

        assert!(!server_seed_applies(&config, &seed));
    }

    fn existing_server() -> ServerConfig {
        ServerConfig {
            id: 1,
            host: "ui.example.com".to_string(),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 5,
            active: true,
            supports_pipelining: false,
            pipelining_depth: None,
            tls_name_mismatch_certificate_der: None,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: ServerDownloadQuotaConfig::default(),
            tls_ca_cert: None,
        }
    }

    #[test]
    fn preserves_existing_servers() {
        let db = Database::open_in_memory().unwrap();
        let existing = Config {
            data_dir: "/db".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            servers: vec![existing_server()],
            categories: vec![CategoryConfig {
                id: 1,
                name: "Movies".to_string(),
                dest_dir: None,
                aliases: String::new(),
            }],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            propagation_delay_secs: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
            direct_unpack: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        };
        db.save_config(&existing).unwrap();
        let mut config = db.load_config().unwrap();
        let seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "env.example.com")]).unwrap();

        let seeded = apply_server_seed(&db, &mut config, &seed).unwrap();

        assert_eq!(seeded, 0);
        assert_eq!(config.servers[0].host, "ui.example.com");
        assert_eq!(db.list_servers().unwrap()[0].host, "ui.example.com");
    }
}
