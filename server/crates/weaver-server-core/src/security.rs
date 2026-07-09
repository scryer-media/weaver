use std::env;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use reqwest::Url;

pub const ENV_HTTP_BIND_ADDRESS: &str = "WEAVER_HTTP_BIND_ADDRESS";
pub const ENV_METRICS_AUTH_REQUIRED: &str = "WEAVER_METRICS_AUTH_REQUIRED";
pub const ENV_CORS_ALLOWED_ORIGINS: &str = "WEAVER_CORS_ALLOWED_ORIGINS";
pub const ENV_SECURE_COOKIES: &str = "WEAVER_SECURE_COOKIES";
pub const ENV_BACKUP_UPLOAD_LIMIT_BYTES: &str = "WEAVER_BACKUP_UPLOAD_LIMIT_BYTES";
pub const ENV_NZB_UPLOAD_LIMIT_BYTES: &str = "WEAVER_NZB_UPLOAD_LIMIT_BYTES";
pub const ENV_NZB_DECOMPRESSED_LIMIT_BYTES: &str = "WEAVER_NZB_DECOMPRESSED_LIMIT_BYTES";
pub const ENV_RSS_ALLOW_PRIVATE_NETWORK: &str = "WEAVER_RSS_ALLOW_PRIVATE_NETWORK";
pub const ENV_STRICT_SECURITY: &str = "WEAVER_STRICT_SECURITY";

// Keep the default bind broad for Docker and homelab deployments. Production-style
// guardrails live in WEAVER_STRICT_SECURITY rather than silently narrowing this.
pub const DEFAULT_HTTP_BIND_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
pub const DEFAULT_BACKUP_UPLOAD_LIMIT_BYTES: u64 = 2_147_483_648;
pub const DEFAULT_NZB_UPLOAD_LIMIT_BYTES: u64 = 268_435_456;
pub const DEFAULT_NZB_DECOMPRESSED_LIMIT_BYTES: u64 = 536_870_912;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSecurityConfig {
    pub http_bind_address: IpAddr,
    pub metrics_auth_required: bool,
    pub cors_allowed_origins: Vec<String>,
    pub secure_cookies: bool,
    pub backup_upload_limit_bytes: u64,
    pub nzb_upload_limit_bytes: u64,
    pub nzb_decompressed_limit_bytes: u64,
    pub rss_allow_private_network: bool,
    pub strict_security: bool,
}

impl RuntimeSecurityConfig {
    pub fn from_env() -> Result<Self, SecurityConfigError> {
        Ok(Self {
            http_bind_address: parse_ip_env(ENV_HTTP_BIND_ADDRESS, DEFAULT_HTTP_BIND_ADDRESS)?,
            metrics_auth_required: parse_bool_env(ENV_METRICS_AUTH_REQUIRED, true)?,
            cors_allowed_origins: parse_origin_list_env(ENV_CORS_ALLOWED_ORIGINS)?,
            secure_cookies: parse_bool_env(ENV_SECURE_COOKIES, false)?,
            backup_upload_limit_bytes: parse_u64_env(
                ENV_BACKUP_UPLOAD_LIMIT_BYTES,
                DEFAULT_BACKUP_UPLOAD_LIMIT_BYTES,
            )?,
            nzb_upload_limit_bytes: parse_u64_env(
                ENV_NZB_UPLOAD_LIMIT_BYTES,
                DEFAULT_NZB_UPLOAD_LIMIT_BYTES,
            )?,
            nzb_decompressed_limit_bytes: parse_u64_env(
                ENV_NZB_DECOMPRESSED_LIMIT_BYTES,
                DEFAULT_NZB_DECOMPRESSED_LIMIT_BYTES,
            )?,
            rss_allow_private_network: parse_bool_env(ENV_RSS_ALLOW_PRIVATE_NETWORK, false)?,
            strict_security: parse_bool_env(ENV_STRICT_SECURITY, false)?,
        })
    }

    pub fn from_env_or_default_for_tests() -> Self {
        Self::from_env().unwrap_or_default()
    }

    pub fn exposes_admin_without_login(&self, login_enabled: bool) -> bool {
        !login_enabled && !self.http_bind_address.is_loopback()
    }

    pub fn strict_security_violation(&self, login_enabled: bool) -> Option<String> {
        if self.strict_security && self.exposes_admin_without_login(login_enabled) {
            return Some(format!(
                "{ENV_STRICT_SECURITY}=1 refuses {ENV_HTTP_BIND_ADDRESS}={} without login auth enabled",
                self.http_bind_address
            ));
        }
        None
    }
}

impl Default for RuntimeSecurityConfig {
    fn default() -> Self {
        Self {
            http_bind_address: DEFAULT_HTTP_BIND_ADDRESS,
            metrics_auth_required: true,
            cors_allowed_origins: Vec::new(),
            secure_cookies: false,
            backup_upload_limit_bytes: DEFAULT_BACKUP_UPLOAD_LIMIT_BYTES,
            nzb_upload_limit_bytes: DEFAULT_NZB_UPLOAD_LIMIT_BYTES,
            nzb_decompressed_limit_bytes: DEFAULT_NZB_DECOMPRESSED_LIMIT_BYTES,
            rss_allow_private_network: false,
            strict_security: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityConfigError {
    message: String,
}

impl SecurityConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SecurityConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SecurityConfigError {}

pub fn parse_bool_env(name: &str, default: bool) -> Result<bool, SecurityConfigError> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" => Ok(default),
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(SecurityConfigError::new(format!(
            "{name} must be a boolean value (true/false, 1/0, yes/no, on/off)"
        ))),
    }
}

fn parse_ip_env(name: &str, default: IpAddr) -> Result<IpAddr, SecurityConfigError> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(default);
    }
    trimmed
        .parse::<IpAddr>()
        .map_err(|_| SecurityConfigError::new(format!("{name} must be an IPv4 or IPv6 address")))
}

fn parse_u64_env(name: &str, default: u64) -> Result<u64, SecurityConfigError> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(default);
    }
    trimmed
        .parse::<u64>()
        .map_err(|_| SecurityConfigError::new(format!("{name} must be an unsigned integer")))
}

fn parse_origin_list_env(name: &str) -> Result<Vec<String>, SecurityConfigError> {
    let Ok(value) = env::var(name) else {
        return Ok(Vec::new());
    };

    value
        .split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .map(validate_exact_origin)
        .collect()
}

fn validate_exact_origin(origin: &str) -> Result<String, SecurityConfigError> {
    let url = Url::parse(origin).map_err(|_| {
        SecurityConfigError::new(format!(
            "{ENV_CORS_ALLOWED_ORIGINS} contains invalid origin {origin:?}"
        ))
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || origin.ends_with('/')
    {
        return Err(SecurityConfigError::new(format!(
            "{ENV_CORS_ALLOWED_ORIGINS} entries must be exact http(s) origins like http://localhost:5173"
        )));
    }
    Ok(origin.to_string())
}

pub fn is_blocked_egress_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_unspecified()
        }
        IpAddr::V6(ip) => {
            if let Some(ipv4) = ip.to_ipv4_mapped() {
                return is_blocked_egress_ip(IpAddr::V4(ipv4));
            }
            ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast()
                || ip.is_unspecified()
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedFetchTarget {
    pub url: Url,
    pub host: String,
    pub addrs: Vec<SocketAddr>,
}

impl ResolvedFetchTarget {
    pub fn apply_dns_override(&self, builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        if self.host.parse::<IpAddr>().is_ok() {
            builder
        } else {
            builder.resolve_to_addrs(&self.host, &self.addrs)
        }
    }
}

pub async fn resolve_fetch_target(
    url: &Url,
    allow_private_network: bool,
) -> Result<ResolvedFetchTarget, String> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err("URL must use http or https".to_string());
    }
    let host = url
        .host_str()
        .ok_or_else(|| "URL must include a host".to_string())?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| "URL must include a valid port".to_string())?;

    if let Ok(ip) = host.parse::<IpAddr>() {
        if !allow_private_network && is_blocked_egress_ip(ip) {
            return Err(format!("URL destination {ip} is not allowed"));
        }
        return Ok(ResolvedFetchTarget {
            url: url.clone(),
            host: host.to_string(),
            addrs: vec![SocketAddr::new(ip, port)],
        });
    }

    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| format!("failed to resolve {host}: {error}"))?
        .collect();
    if addrs.is_empty() {
        return Err(format!("failed to resolve {host}: no addresses returned"));
    }
    if !allow_private_network {
        for addr in &addrs {
            if is_blocked_egress_ip(addr.ip()) {
                return Err(format!("URL destination {} is not allowed", addr.ip()));
            }
        }
    }

    Ok(ResolvedFetchTarget {
        url: url.clone(),
        host: host.to_string(),
        addrs,
    })
}

pub async fn validate_fetch_egress(url: &Url, allow_private_network: bool) -> Result<(), String> {
    resolve_fetch_target(url, allow_private_network)
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn clear_env() {
        for name in [
            ENV_HTTP_BIND_ADDRESS,
            ENV_METRICS_AUTH_REQUIRED,
            ENV_CORS_ALLOWED_ORIGINS,
            ENV_SECURE_COOKIES,
            ENV_BACKUP_UPLOAD_LIMIT_BYTES,
            ENV_NZB_UPLOAD_LIMIT_BYTES,
            ENV_NZB_DECOMPRESSED_LIMIT_BYTES,
            ENV_RSS_ALLOW_PRIVATE_NETWORK,
            ENV_STRICT_SECURITY,
        ] {
            unsafe { env::remove_var(name) };
        }
    }

    #[test]
    fn security_env_defaults_match_plan() {
        let _guard = env_lock();
        clear_env();

        let config = RuntimeSecurityConfig::from_env().unwrap();

        assert_eq!(config.http_bind_address, DEFAULT_HTTP_BIND_ADDRESS);
        assert_eq!(config.http_bind_address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert!(config.metrics_auth_required);
        assert!(config.cors_allowed_origins.is_empty());
        assert!(!config.secure_cookies);
        assert_eq!(
            config.backup_upload_limit_bytes,
            DEFAULT_BACKUP_UPLOAD_LIMIT_BYTES
        );
        assert_eq!(
            config.nzb_upload_limit_bytes,
            DEFAULT_NZB_UPLOAD_LIMIT_BYTES
        );
        assert_eq!(
            config.nzb_decompressed_limit_bytes,
            DEFAULT_NZB_DECOMPRESSED_LIMIT_BYTES
        );
        assert!(!config.rss_allow_private_network);
        assert!(!config.strict_security);
    }

    #[test]
    fn security_env_accepts_bool_aliases_and_origins() {
        let _guard = env_lock();
        clear_env();
        unsafe {
            env::set_var(ENV_HTTP_BIND_ADDRESS, "127.0.0.1");
            env::set_var(ENV_METRICS_AUTH_REQUIRED, "off");
            env::set_var(
                ENV_CORS_ALLOWED_ORIGINS,
                "http://localhost:5173,https://dev.example.test",
            );
            env::set_var(ENV_SECURE_COOKIES, "yes");
            env::set_var(ENV_BACKUP_UPLOAD_LIMIT_BYTES, "10");
            env::set_var(ENV_NZB_UPLOAD_LIMIT_BYTES, "11");
            env::set_var(ENV_NZB_DECOMPRESSED_LIMIT_BYTES, "12");
            env::set_var(ENV_RSS_ALLOW_PRIVATE_NETWORK, "1");
            env::set_var(ENV_STRICT_SECURITY, "on");
        }

        let config = RuntimeSecurityConfig::from_env().unwrap();

        assert_eq!(config.http_bind_address, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(!config.metrics_auth_required);
        assert_eq!(
            config.cors_allowed_origins,
            vec!["http://localhost:5173", "https://dev.example.test"]
        );
        assert!(config.secure_cookies);
        assert_eq!(config.backup_upload_limit_bytes, 10);
        assert_eq!(config.nzb_upload_limit_bytes, 11);
        assert_eq!(config.nzb_decompressed_limit_bytes, 12);
        assert!(config.rss_allow_private_network);
        assert!(config.strict_security);

        clear_env();
    }

    #[test]
    fn security_env_allows_explicit_all_interfaces_bind() {
        let _guard = env_lock();
        clear_env();
        unsafe {
            env::set_var(ENV_HTTP_BIND_ADDRESS, "0.0.0.0");
        }

        let config = RuntimeSecurityConfig::from_env().unwrap();

        assert_eq!(config.http_bind_address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        clear_env();
    }

    #[test]
    fn open_admin_warning_predicate_tracks_bind_and_login() {
        let default = RuntimeSecurityConfig::default();
        assert!(default.exposes_admin_without_login(false));
        assert!(!default.exposes_admin_without_login(true));

        let loopback = RuntimeSecurityConfig {
            http_bind_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            ..RuntimeSecurityConfig::default()
        };
        assert!(!loopback.exposes_admin_without_login(false));
    }

    #[test]
    fn strict_security_refuses_exposed_admin_without_login() {
        let exposed = RuntimeSecurityConfig {
            http_bind_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            strict_security: true,
            ..RuntimeSecurityConfig::default()
        };

        assert!(exposed.strict_security_violation(false).is_some());
        assert!(exposed.strict_security_violation(true).is_none());

        let loopback = RuntimeSecurityConfig {
            http_bind_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            strict_security: true,
            ..RuntimeSecurityConfig::default()
        };
        assert!(loopback.strict_security_violation(false).is_none());
    }

    #[test]
    fn security_env_rejects_invalid_values() {
        let _guard = env_lock();
        clear_env();

        unsafe { env::set_var(ENV_METRICS_AUTH_REQUIRED, "maybe") };
        assert!(RuntimeSecurityConfig::from_env().is_err());
        clear_env();

        unsafe { env::set_var(ENV_BACKUP_UPLOAD_LIMIT_BYTES, "-1") };
        assert!(RuntimeSecurityConfig::from_env().is_err());
        clear_env();

        unsafe { env::set_var(ENV_HTTP_BIND_ADDRESS, "localhost") };
        assert!(RuntimeSecurityConfig::from_env().is_err());
        clear_env();

        unsafe { env::set_var(ENV_CORS_ALLOWED_ORIGINS, "http://localhost:5173/path") };
        assert!(RuntimeSecurityConfig::from_env().is_err());
        clear_env();
    }

    #[test]
    fn egress_ip_blocklist_covers_local_networks() {
        assert!(is_blocked_egress_ip("127.0.0.1".parse().unwrap()));
        assert!(is_blocked_egress_ip("10.0.0.1".parse().unwrap()));
        assert!(is_blocked_egress_ip("172.16.0.1".parse().unwrap()));
        assert!(is_blocked_egress_ip("192.168.1.2".parse().unwrap()));
        assert!(is_blocked_egress_ip("169.254.1.1".parse().unwrap()));
        assert!(is_blocked_egress_ip("::1".parse().unwrap()));
        assert!(is_blocked_egress_ip("fd00::1".parse().unwrap()));
        assert!(is_blocked_egress_ip("::ffff:127.0.0.1".parse().unwrap()));
        assert!(is_blocked_egress_ip("::ffff:10.0.0.1".parse().unwrap()));
        assert!(is_blocked_egress_ip("::ffff:169.254.1.1".parse().unwrap()));
        assert!(!is_blocked_egress_ip("1.1.1.1".parse().unwrap()));
        assert!(!is_blocked_egress_ip(
            "2606:4700:4700::1111".parse().unwrap()
        ));
    }

    #[tokio::test]
    async fn fetch_egress_rejects_private_literal_unless_allowed() {
        let url = Url::parse("http://127.0.0.1:8080/feed.xml").unwrap();
        assert!(validate_fetch_egress(&url, false).await.is_err());
        assert!(validate_fetch_egress(&url, true).await.is_ok());

        let bad_scheme = Url::parse("file:///tmp/feed.xml").unwrap();
        assert!(validate_fetch_egress(&bad_scheme, true).await.is_err());
    }

    #[tokio::test]
    async fn fetch_target_accepts_and_pins_public_literal() {
        let url = Url::parse("http://1.1.1.1/feed.xml").unwrap();
        let target = resolve_fetch_target(&url, false).await.unwrap();

        assert_eq!(target.host, "1.1.1.1");
        assert_eq!(target.addrs.len(), 1);
        assert_eq!(target.addrs[0].ip(), "1.1.1.1".parse::<IpAddr>().unwrap());
        assert_eq!(target.addrs[0].port(), 80);
    }

    #[tokio::test]
    async fn redirect_to_private_target_is_rejected_by_policy() {
        let public_url = Url::parse("http://1.1.1.1/feed.xml").unwrap();
        assert!(resolve_fetch_target(&public_url, false).await.is_ok());

        let redirect_url = public_url.join("http://127.0.0.1/private.nzb").unwrap();
        assert!(resolve_fetch_target(&redirect_url, false).await.is_err());
    }
}
