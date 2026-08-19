use std::env;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use http::uri::Authority;
use ipnet::IpNet;
use reqwest::Url;

pub const ENV_HTTP_BIND_ADDRESS: &str = "WEAVER_HTTP_BIND_ADDRESS";
pub const ENV_HTTP_ALLOWED_HOSTS: &str = "WEAVER_HTTP_ALLOWED_HOSTS";
pub const ENV_METRICS_AUTH_REQUIRED: &str = "WEAVER_METRICS_AUTH_REQUIRED";
pub const ENV_CORS_ALLOWED_ORIGINS: &str = "WEAVER_CORS_ALLOWED_ORIGINS";
pub const ENV_SECURE_COOKIES: &str = "WEAVER_SECURE_COOKIES";
pub const ENV_BACKUP_UPLOAD_LIMIT_BYTES: &str = "WEAVER_BACKUP_UPLOAD_LIMIT_BYTES";
pub const ENV_NZB_UPLOAD_LIMIT_BYTES: &str = "WEAVER_NZB_UPLOAD_LIMIT_BYTES";
pub const ENV_NZB_DECOMPRESSED_LIMIT_BYTES: &str = "WEAVER_NZB_DECOMPRESSED_LIMIT_BYTES";
pub const ENV_RSS_ALLOW_PRIVATE_NETWORK: &str = "WEAVER_RSS_ALLOW_PRIVATE_NETWORK";
pub const ENV_STRICT_SECURITY: &str = "WEAVER_STRICT_SECURITY";
pub const ENV_TRUSTED_CIDRS: &str = "WEAVER_TRUSTED_CIDRS";

pub const DEFAULT_HTTP_BIND_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Settings-table key holding the operator's chosen bind address.
///
/// The address has to be editable somewhere the operator can always reach,
/// which the environment is not: a desktop install is launched by a shortcut
/// and a service by a unit file, neither of which a user edits to answer "let
/// my other machine see this". The loopback default makes the UI itself that
/// place — it is always reachable from the machine Weaver runs on, so the
/// setting that widens the binding can be changed from inside the thing it
/// configures.
pub const SETTING_HTTP_BIND_ADDRESS: &str = "http_bind_address";

/// Where the running bind address came from, so the UI can explain itself
/// rather than silently ignoring an edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindAddressSource {
    /// `WEAVER_HTTP_BIND_ADDRESS` was set. It wins outright: a container image
    /// or a service unit that pins the address is describing its deployment,
    /// and a stored setting must not quietly override the environment the
    /// operator handed to the process.
    Environment,
    /// Persisted in the settings table, editable in the UI.
    Setting,
    /// Nothing configured anywhere; loopback.
    Default,
}

impl BindAddressSource {
    /// Whether the UI may offer this as an editable field.
    pub fn is_editable(self) -> bool {
        !matches!(self, BindAddressSource::Environment)
    }
}

/// Settings-table key holding the access mode the operator chose at setup.
pub const SETTING_ACCESS_MODE: &str = "access_mode";
/// Settings-table key holding the trusted-network list (JSON array of CIDRs)
/// backing [`AccessMode::LoginExceptLocal`].
pub const SETTING_TRUSTED_NETWORKS: &str = "trusted_networks";

/// How browsers are admitted, chosen in the first-run wizard and editable in
/// Settings → Security afterwards.
///
/// The mode is sugar over one mechanism: a trusted-CIDR list consulted by
/// [`RuntimeSecurityConfig::is_trusted_peer`]. `LoginRequired` trusts nothing,
/// `LoginExceptLocal` trusts the stored network list, and `NoLogin` trusts
/// loopback — which is what makes a credential-less install reach its own UI
/// while remaining unreachable from anywhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    LoginRequired,
    LoginExceptLocal,
    NoLogin,
}

impl AccessMode {
    pub fn as_setting_value(self) -> &'static str {
        match self {
            AccessMode::LoginRequired => "login_required",
            AccessMode::LoginExceptLocal => "login_except_local",
            AccessMode::NoLogin => "no_login",
        }
    }

    pub fn parse_setting_value(value: &str) -> Option<Self> {
        match value.trim() {
            "login_required" => Some(AccessMode::LoginRequired),
            "login_except_local" => Some(AccessMode::LoginExceptLocal),
            "no_login" => Some(AccessMode::NoLogin),
            _ => None,
        }
    }
}

/// The private-network preset offered by the wizard for
/// [`AccessMode::LoginExceptLocal`]: loopback, RFC 1918, link-local, and ULA.
/// A preset rather than "everything", so trusting the local network never
/// quietly extends to a routable address.
pub const LOCAL_NETWORK_PRESETS: [&str; 8] = [
    "127.0.0.0/8",
    "::1/128",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "169.254.0.0/16",
    "fe80::/10",
    "fc00::/7",
];

/// The trust list [`AccessMode::NoLogin`] resolves to.
pub const LOOPBACK_NETWORKS: [&str; 2] = ["127.0.0.0/8", "::1/128"];

fn parse_cidr_list(values: &[&str]) -> Vec<IpNet> {
    values
        .iter()
        .filter_map(|value| value.parse::<IpNet>().ok())
        .collect()
}

/// Parse the stored trusted-network list: a JSON array of CIDR strings.
/// All-or-nothing — one bad entry rejects the list, because trust is the one
/// place a partial parse must not quietly admit less (or more) than intended.
pub fn parse_trusted_networks_json(json: &str) -> Result<Vec<IpNet>, SecurityConfigError> {
    let entries: Vec<String> = serde_json::from_str(json).map_err(|error| {
        SecurityConfigError::new(format!(
            "{SETTING_TRUSTED_NETWORKS} must be a JSON array of CIDR strings: {error}"
        ))
    })?;
    entries
        .iter()
        .map(|entry| {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                return Err(SecurityConfigError::new(format!(
                    "{SETTING_TRUSTED_NETWORKS} must not contain empty entries"
                )));
            }
            trimmed.parse::<IpNet>().map_err(|_| {
                SecurityConfigError::new(format!(
                    "{SETTING_TRUSTED_NETWORKS} contains invalid CIDR {trimmed:?}"
                ))
            })
        })
        .collect()
}

/// Collapse an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) to the IPv4 address
/// it carries; every other address is returned unchanged.
///
/// A dual-stack listener reports IPv4 peers in the mapped form, and the mapped
/// form answers `false` to every IPv4 classification predicate on `IpAddr` —
/// `::ffff:127.0.0.1` is not `is_loopback()`. Classification must therefore
/// happen on the canonical form, or the same host reads as loopback on one
/// socket and as exposed on another.
pub fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    }
}

/// Loopback test that sees through the IPv4-mapped form. Use this everywhere a
/// decision turns on "is this address only reachable from this machine".
pub fn ip_is_loopback(ip: IpAddr) -> bool {
    canonical_ip(ip).is_loopback()
}

/// Resolve the bind address from the environment value and the stored setting.
///
/// Pure so the precedence can be tested without touching process environment
/// or a database. Precedence is environment, then setting, then loopback; an
/// empty or whitespace-only value at either layer means "not configured"
/// rather than an error, matching how the rest of the environment parsing
/// treats blanks.
pub fn resolve_bind_address(
    env_value: Option<&str>,
    setting_value: Option<&str>,
) -> Result<(IpAddr, BindAddressSource), SecurityConfigError> {
    if let Some(value) = env_value.map(str::trim).filter(|value| !value.is_empty()) {
        let address = value.parse::<IpAddr>().map_err(|_| {
            SecurityConfigError::new(format!(
                "{ENV_HTTP_BIND_ADDRESS} must be an IPv4 or IPv6 address"
            ))
        })?;
        return Ok((address, BindAddressSource::Environment));
    }

    if let Some(value) = setting_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let address = value.parse::<IpAddr>().map_err(|_| {
            SecurityConfigError::new(format!(
                "stored {SETTING_HTTP_BIND_ADDRESS} setting {value:?} is not an IPv4 or IPv6 address"
            ))
        })?;
        return Ok((address, BindAddressSource::Setting));
    }

    Ok((DEFAULT_HTTP_BIND_ADDRESS, BindAddressSource::Default))
}
pub const DEFAULT_BACKUP_UPLOAD_LIMIT_BYTES: u64 = 2_147_483_648;
pub const DEFAULT_NZB_UPLOAD_LIMIT_BYTES: u64 = 268_435_456;
pub const DEFAULT_NZB_DECOMPRESSED_LIMIT_BYTES: u64 = 536_870_912;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpAuthority {
    host: String,
    port: Option<u16>,
    ip_literal: bool,
}

impl HttpAuthority {
    pub fn parse(value: &str) -> Result<Self, HttpAuthorityError> {
        let value = value.trim();
        if value.is_empty()
            || value.contains(['/', '@', '*', '?', '#'])
            || value.contains("://")
            || (value.contains(':') && !value.starts_with('[') && value.matches(':').count() > 1)
        {
            return Err(HttpAuthorityError);
        }

        let port = if value.starts_with('[') {
            let closing_bracket = value.find(']').ok_or(HttpAuthorityError)?;
            match &value[closing_bracket + 1..] {
                "" => None,
                suffix => Some(
                    suffix
                        .strip_prefix(':')
                        .filter(|port| !port.is_empty())
                        .ok_or(HttpAuthorityError)?
                        .parse::<u16>()
                        .map_err(|_| HttpAuthorityError)?,
                ),
            }
        } else {
            value
                .rsplit_once(':')
                .map(|(_, port)| {
                    if port.is_empty() {
                        return Err(HttpAuthorityError);
                    }
                    port.parse::<u16>().map_err(|_| HttpAuthorityError)
                })
                .transpose()?
        };

        let authority = value.parse::<Authority>().map_err(|_| HttpAuthorityError)?;
        let raw_host = authority.host();
        let unbracketed = raw_host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(raw_host);
        let ip = unbracketed.parse::<IpAddr>().ok();
        if unbracketed.contains(':') && ip.is_none() {
            return Err(HttpAuthorityError);
        }

        let host = if let Some(ip) = ip {
            ip.to_string()
        } else {
            if !unbracketed.is_ascii()
                || unbracketed
                    .chars()
                    .any(|ch| ch.is_ascii_control() || ch.is_ascii_whitespace())
            {
                return Err(HttpAuthorityError);
            }
            unbracketed
                .strip_suffix('.')
                .unwrap_or(unbracketed)
                .to_ascii_lowercase()
        };
        if host.is_empty() || host.ends_with('.') {
            return Err(HttpAuthorityError);
        }

        Ok(Self {
            host,
            port,
            ip_literal: ip.is_some(),
        })
    }

    pub fn matches(&self, other: &Self) -> bool {
        self.host == other.host && self.port == other.port
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpAuthorityError;

impl fmt::Display for HttpAuthorityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid HTTP authority")
    }
}

impl std::error::Error for HttpAuthorityError {}

// No PartialEq: the trusted-network list lives behind a shared lock, and no
// caller compared configs anyway (verified before dropping the derive).
#[derive(Debug, Clone)]
pub struct RuntimeSecurityConfig {
    pub http_bind_address: IpAddr,
    /// Which layer supplied [`Self::http_bind_address`]. Starts as the
    /// environment's answer and is settled by [`Self::apply_stored_bind_address`]
    /// once the database is open.
    pub bind_address_source: BindAddressSource,
    pub http_allowed_hosts: Vec<HttpAuthority>,
    pub metrics_auth_required: bool,
    pub cors_allowed_origins: Vec<String>,
    pub secure_cookies: bool,
    pub backup_upload_limit_bytes: u64,
    pub nzb_upload_limit_bytes: u64,
    pub nzb_decompressed_limit_bytes: u64,
    pub rss_allow_private_network: bool,
    pub strict_security: bool,
    /// Behind a shared lock so a setup or policy change grants (or revokes)
    /// trust immediately in every clone spread through the router layers — a
    /// wizard that picks "no login" must be able to admit the very next
    /// request, not the next restart.
    trusted_cidrs: Arc<RwLock<Vec<IpNet>>>,
    /// True once the operator's browser-access policy is settled, shared across
    /// clones for the same reason the trust list is: the answer decides whether
    /// a credential-less visitor is shown the first-run wizard, and a wizard
    /// that just finished must stop being offered on the very next request.
    ///
    /// It is deliberately NOT "has trust" or "has credentials": a configured
    /// no-login instance has neither from an outside browser's point of view,
    /// and that is exactly the install that would otherwise be handed an
    /// uncompletable wizard forever.
    security_configured: Arc<AtomicBool>,
    /// True when `WEAVER_TRUSTED_CIDRS` supplied the list, which pins it: the
    /// stored access mode is ignored and the UI shows the policy read-only.
    pub trust_env_pinned: bool,
    /// Set when the process could not honor the configured bind address and
    /// fell back to loopback instead of refusing to start. Shown as a banner
    /// so the deviation is impossible to miss from the UI that still works.
    pub bind_fallback: Option<String>,
}

impl RuntimeSecurityConfig {
    pub fn from_env() -> Result<Self, SecurityConfigError> {
        let strict_security = parse_bool_env(ENV_STRICT_SECURITY, false)?;
        let trusted_cidrs = parse_trusted_cidrs_env()?;
        let trust_env_pinned = !trusted_cidrs.is_empty();
        if strict_security && trust_env_pinned {
            return Err(SecurityConfigError::new(format!(
                "{ENV_STRICT_SECURITY}=1 refuses non-empty {ENV_TRUSTED_CIDRS}"
            )));
        }

        // Resolved against the environment alone here, because the database is
        // not open yet. `apply_stored_bind_address` settles it afterwards.
        let (http_bind_address, bind_address_source) =
            resolve_bind_address(env::var(ENV_HTTP_BIND_ADDRESS).ok().as_deref(), None)?;

        Ok(Self {
            http_bind_address,
            bind_address_source,
            http_allowed_hosts: parse_http_allowed_hosts_env()?,
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
            strict_security,
            trusted_cidrs: Arc::new(RwLock::new(trusted_cidrs)),
            // An env-pinned deployment has already declared its policy, so it
            // is configured before the database is even open. Everything else
            // waits for `apply_stored_trust` to read the stored mode.
            security_configured: Arc::new(AtomicBool::new(trust_env_pinned)),
            trust_env_pinned,
            bind_fallback: None,
        })
    }

    pub fn from_env_or_default_for_tests() -> Self {
        Self::from_env().unwrap_or_default()
    }

    /// Settle the bind address against the stored setting, now that the
    /// database is readable.
    ///
    /// A no-op when the environment supplied an address: that layer wins, and
    /// the stored value is left untouched rather than reconciled, so removing
    /// the environment variable later restores whatever the operator last
    /// chose in the UI instead of silently inheriting the deployment's value.
    ///
    /// Infallible by design: an unparsable stored value — a hand-edited
    /// database, a restore from a different version — must never cost the
    /// operator their process, because the process is the only thing that can
    /// fix the value. It falls back to loopback and records the deviation in
    /// [`Self::bind_fallback`] for the UI banner.
    pub fn apply_stored_bind_address(&mut self, stored: Option<&str>) {
        if matches!(self.bind_address_source, BindAddressSource::Environment) {
            return;
        }
        match resolve_bind_address(None, stored) {
            Ok((address, source)) => {
                self.http_bind_address = address;
                self.bind_address_source = source;
            }
            Err(error) => {
                self.http_bind_address = DEFAULT_HTTP_BIND_ADDRESS;
                self.bind_address_source = BindAddressSource::Default;
                self.bind_fallback = Some(format!(
                    "{error}; listening on {DEFAULT_HTTP_BIND_ADDRESS} instead — \
                     fix or clear the address in Settings → Security"
                ));
            }
        }
    }

    /// Settle the trusted networks against the stored access mode, now that
    /// the database is readable.
    ///
    /// `WEAVER_TRUSTED_CIDRS` pins the list exactly like the bind variable
    /// pins the address. Otherwise the mode is sugar over the list: trust
    /// nothing, trust the stored networks, or trust loopback. Trust fails
    /// CLOSED — an unparsable mode or network list yields an empty list plus a
    /// warning, never accidental admission — the deliberate opposite of the
    /// bind address's fail-open-to-loopback, because a bad bind value costs
    /// reachability while a bad trust value would cost authentication.
    pub fn apply_stored_trust(&self, mode: Option<&str>, stored_networks: Option<&str>) {
        if self.trust_env_pinned {
            // An environment-managed deployment declared its policy in the
            // deployment itself; its outside visitors must never be offered a
            // wizard they could not submit anyway.
            self.mark_security_configured();
            return;
        }
        let mode = mode.and_then(AccessMode::parse_setting_value);
        // An unparsable stored mode leaves the install unconfigured on purpose:
        // trust already failed closed above, and re-asking is the only way the
        // operator gets to answer again.
        if mode.is_some() {
            self.mark_security_configured();
        }
        let networks: Vec<IpNet> = match mode {
            None | Some(AccessMode::LoginRequired) => Vec::new(),
            Some(AccessMode::NoLogin) => parse_cidr_list(&LOOPBACK_NETWORKS),
            Some(AccessMode::LoginExceptLocal) => match stored_networks {
                None => parse_cidr_list(&LOCAL_NETWORK_PRESETS),
                Some(json) => match parse_trusted_networks_json(json) {
                    Ok(networks) => networks,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "stored {SETTING_TRUSTED_NETWORKS} is invalid; trusting nothing"
                        );
                        Vec::new()
                    }
                },
            },
        };
        self.set_trusted_cidrs(networks);
    }

    /// Replace the live trusted-network list, visible to every clone of this
    /// config immediately.
    pub fn set_trusted_cidrs(&self, networks: Vec<IpNet>) {
        *self
            .trusted_cidrs
            .write()
            .expect("trusted-network lock poisoned") = networks;
    }

    pub fn trusted_cidrs(&self) -> Vec<IpNet> {
        self.trusted_cidrs
            .read()
            .expect("trusted-network lock poisoned")
            .clone()
    }

    /// Whether the operator's browser-access policy has been settled — by a
    /// stored access mode, or by an environment that pins the trust list.
    ///
    /// The gate that keeps a configured install from re-offering the first-run
    /// wizard to browsers it does not trust. Without it, "no credentials and
    /// not a trusted peer" reads identically for a never-configured instance
    /// and for a configured no-login one, and the second is then stuck: the
    /// wizard renders on every visit and its endpoint refuses every submit.
    pub fn security_configured(&self) -> bool {
        self.security_configured.load(Ordering::Relaxed)
    }

    /// Record that the browser-access policy is settled, visible to every clone
    /// of this config immediately. Called by the two writers that can settle it
    /// at runtime: the first-run wizard and the access-policy mutation.
    pub fn mark_security_configured(&self) {
        self.security_configured.store(true, Ordering::Relaxed);
    }

    pub fn has_trusted_cidrs(&self) -> bool {
        !self
            .trusted_cidrs
            .read()
            .expect("trusted-network lock poisoned")
            .is_empty()
    }

    /// Record that the configured address could not be bound and the process
    /// is serving on loopback instead.
    pub fn note_bind_fallback(&mut self, reason: String) {
        self.http_bind_address = DEFAULT_HTTP_BIND_ADDRESS;
        self.bind_fallback = Some(reason);
    }

    pub fn exposes_admin_without_login(&self, login_enabled: bool) -> bool {
        !login_enabled && !ip_is_loopback(self.http_bind_address)
    }

    pub fn is_http_authority_allowed(&self, authority: &HttpAuthority) -> bool {
        if authority.ip_literal || authority.host == "localhost" {
            return true;
        }

        self.http_allowed_hosts.iter().any(|allowed| {
            allowed.host == authority.host
                && allowed.port.is_none_or(|port| authority.port == Some(port))
        })
    }

    pub fn strict_security_violation(&self, login_enabled: bool) -> Option<String> {
        if self.strict_security && self.has_trusted_cidrs() {
            // Names the layer the trust actually came from, so the operator
            // debugs the thing they touched rather than a variable they never
            // set.
            let origin = if self.trust_env_pinned {
                format!("non-empty {ENV_TRUSTED_CIDRS}")
            } else {
                format!("a trusting stored {SETTING_ACCESS_MODE} setting")
            };
            return Some(format!("{ENV_STRICT_SECURITY}=1 refuses {origin}"));
        }
        if self.strict_security && self.exposes_admin_without_login(login_enabled) {
            let origin = match self.bind_address_source {
                BindAddressSource::Environment => format!("{ENV_HTTP_BIND_ADDRESS}="),
                BindAddressSource::Setting => {
                    format!("the stored {SETTING_HTTP_BIND_ADDRESS} setting ")
                }
                BindAddressSource::Default => String::new(),
            };
            return Some(format!(
                "{ENV_STRICT_SECURITY}=1 refuses binding {origin}{} without login auth enabled",
                self.http_bind_address
            ));
        }
        None
    }

    /// Returns whether the immediate socket peer is inside an explicitly trusted network.
    /// Proxy forwarding headers are intentionally not considered here.
    pub fn is_trusted_peer(&self, peer: Option<SocketAddr>) -> bool {
        let Some(peer) = peer else {
            return false;
        };
        let ip = canonical_ip(peer.ip());
        self.trusted_cidrs
            .read()
            .expect("trusted-network lock poisoned")
            .iter()
            .any(|network| network.contains(&ip))
    }
}

impl Default for RuntimeSecurityConfig {
    fn default() -> Self {
        Self {
            http_bind_address: DEFAULT_HTTP_BIND_ADDRESS,
            bind_address_source: BindAddressSource::Default,
            http_allowed_hosts: Vec::new(),
            metrics_auth_required: true,
            cors_allowed_origins: Vec::new(),
            secure_cookies: false,
            backup_upload_limit_bytes: DEFAULT_BACKUP_UPLOAD_LIMIT_BYTES,
            nzb_upload_limit_bytes: DEFAULT_NZB_UPLOAD_LIMIT_BYTES,
            nzb_decompressed_limit_bytes: DEFAULT_NZB_DECOMPRESSED_LIMIT_BYTES,
            rss_allow_private_network: false,
            strict_security: false,
            trusted_cidrs: Arc::new(RwLock::new(Vec::new())),
            security_configured: Arc::new(AtomicBool::new(false)),
            trust_env_pinned: false,
            bind_fallback: None,
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

fn parse_http_allowed_hosts_env() -> Result<Vec<HttpAuthority>, SecurityConfigError> {
    let Ok(value) = env::var(ENV_HTTP_ALLOWED_HOSTS) else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    value
        .split(',')
        .map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return Err(SecurityConfigError::new(format!(
                    "{ENV_HTTP_ALLOWED_HOSTS} must not contain empty entries"
                )));
            }
            HttpAuthority::parse(entry).map_err(|_| {
                SecurityConfigError::new(format!(
                    "{ENV_HTTP_ALLOWED_HOSTS} contains invalid authority {entry:?}; use exact hostnames or host:port entries"
                ))
            })
        })
        .collect()
}

fn parse_trusted_cidrs_env() -> Result<Vec<IpNet>, SecurityConfigError> {
    let Ok(value) = env::var(ENV_TRUSTED_CIDRS) else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    value
        .split(',')
        .map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return Err(SecurityConfigError::new(format!(
                    "{ENV_TRUSTED_CIDRS} must not contain empty entries"
                )));
            }
            entry.parse::<IpNet>().map_err(|_| {
                SecurityConfigError::new(format!(
                    "{ENV_TRUSTED_CIDRS} contains invalid CIDR {entry:?}"
                ))
            })
        })
        .collect()
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
            ENV_HTTP_ALLOWED_HOSTS,
            ENV_METRICS_AUTH_REQUIRED,
            ENV_CORS_ALLOWED_ORIGINS,
            ENV_SECURE_COOKIES,
            ENV_BACKUP_UPLOAD_LIMIT_BYTES,
            ENV_NZB_UPLOAD_LIMIT_BYTES,
            ENV_NZB_DECOMPRESSED_LIMIT_BYTES,
            ENV_RSS_ALLOW_PRIVATE_NETWORK,
            ENV_STRICT_SECURITY,
            ENV_TRUSTED_CIDRS,
        ] {
            unsafe { env::remove_var(name) };
        }
    }

    #[test]
    fn bind_address_precedence_is_environment_then_setting_then_loopback() {
        // No environment, no setting: the safe default, and the UI may offer it.
        let (address, source) = resolve_bind_address(None, None).unwrap();
        assert_eq!(address, DEFAULT_HTTP_BIND_ADDRESS);
        assert_eq!(source, BindAddressSource::Default);
        assert!(source.is_editable());

        // A stored setting widens the binding without any environment help —
        // this is the path a desktop or Windows operator takes through the UI.
        let (address, source) = resolve_bind_address(None, Some("0.0.0.0")).unwrap();
        assert_eq!(address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(source, BindAddressSource::Setting);
        assert!(source.is_editable());

        // The environment wins over a stored setting, and the field goes
        // read-only: a container image pinning the address is describing its
        // deployment, and a stale stored value must not override it.
        let (address, source) = resolve_bind_address(Some("0.0.0.0"), Some("127.0.0.1")).unwrap();
        assert_eq!(address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(source, BindAddressSource::Environment);
        assert!(!source.is_editable());

        // Blank at either layer means "not configured", never an error.
        let (address, source) = resolve_bind_address(Some("   "), Some("")).unwrap();
        assert_eq!(address, DEFAULT_HTTP_BIND_ADDRESS);
        assert_eq!(source, BindAddressSource::Default);
    }

    #[test]
    fn an_unparsable_bind_address_names_the_layer_it_came_from() {
        let env_error = resolve_bind_address(Some("not-an-ip"), None).unwrap_err();
        assert!(
            env_error.to_string().contains(ENV_HTTP_BIND_ADDRESS),
            "an environment failure must name the variable: {env_error}"
        );

        let setting_error = resolve_bind_address(None, Some("not-an-ip")).unwrap_err();
        assert!(
            setting_error
                .to_string()
                .contains(SETTING_HTTP_BIND_ADDRESS),
            "a stored-setting failure must name the setting, not the variable \
             the operator never touched: {setting_error}"
        );
    }

    #[test]
    fn a_pinned_environment_address_ignores_the_stored_setting() {
        let mut config = RuntimeSecurityConfig {
            http_bind_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            bind_address_source: BindAddressSource::Environment,
            ..RuntimeSecurityConfig::default()
        };

        config.apply_stored_bind_address(Some("127.0.0.1"));

        assert_eq!(config.http_bind_address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(config.bind_address_source, BindAddressSource::Environment);

        // And a stored value only takes effect where the environment is silent.
        let mut config = RuntimeSecurityConfig::default();
        config.apply_stored_bind_address(Some("0.0.0.0"));
        assert_eq!(config.http_bind_address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(config.bind_address_source, BindAddressSource::Setting);
    }

    #[test]
    fn access_modes_resolve_to_their_trust_lists() {
        let config = RuntimeSecurityConfig::default();

        // Unset and login_required trust nothing.
        config.apply_stored_trust(None, None);
        assert!(config.trusted_cidrs().is_empty());
        config.apply_stored_trust(Some("login_required"), None);
        assert!(config.trusted_cidrs().is_empty());

        // no_login trusts loopback only — the credential-less install can
        // reach itself and nothing else can reach it.
        config.apply_stored_trust(Some("no_login"), None);
        let loopback_peer = "127.0.0.1:1".parse().unwrap();
        let lan_peer = "192.168.1.20:1".parse().unwrap();
        assert!(config.is_trusted_peer(Some(loopback_peer)));
        assert!(!config.is_trusted_peer(Some(lan_peer)));

        // except-local with no stored list gets the private-network preset.
        config.apply_stored_trust(Some("login_except_local"), None);
        assert!(config.is_trusted_peer(Some(lan_peer)));
        assert!(!config.is_trusted_peer(Some("203.0.113.9:1".parse().unwrap())));

        // A stored list narrows the preset.
        config.apply_stored_trust(Some("login_except_local"), Some(r#"["10.1.0.0/16"]"#));
        assert!(config.is_trusted_peer(Some("10.1.4.4:1".parse().unwrap())));
        assert!(!config.is_trusted_peer(Some(lan_peer)));

        // Trust fails CLOSED: garbage mode or garbage list trusts nothing.
        config.apply_stored_trust(Some("everyone"), None);
        assert!(config.trusted_cidrs().is_empty());
        config.apply_stored_trust(Some("login_except_local"), Some("not json"));
        assert!(config.trusted_cidrs().is_empty());
    }

    #[test]
    fn env_pinned_trust_ignores_stored_policy() {
        let config = RuntimeSecurityConfig {
            trust_env_pinned: true,
            ..RuntimeSecurityConfig::default()
        };
        config.set_trusted_cidrs(vec!["198.51.100.0/24".parse().unwrap()]);

        config.apply_stored_trust(Some("no_login"), None);

        // The env list survives; the stored mode changed nothing.
        assert!(config.is_trusted_peer(Some("198.51.100.7:1".parse().unwrap())));
        assert!(!config.is_trusted_peer(Some("127.0.0.1:1".parse().unwrap())));
    }

    #[test]
    fn a_parsed_stored_mode_marks_the_install_configured() {
        // The Loop-1 signal: "configured" must be answerable without asking
        // whether credentials exist or whether THIS peer is trusted, because a
        // no-login instance answers no to both from every outside browser.
        let config = RuntimeSecurityConfig::default();
        assert!(!config.security_configured());

        config.apply_stored_trust(None, None);
        assert!(
            !config.security_configured(),
            "a fresh install with no stored mode is not configured"
        );

        config.apply_stored_trust(Some("no_login"), None);
        assert!(config.security_configured());

        // An unparsable mode stays unconfigured, so the operator gets to
        // answer again instead of being locked out of the question.
        let config = RuntimeSecurityConfig::default();
        config.apply_stored_trust(Some("everyone"), None);
        assert!(!config.security_configured());
        assert!(config.trusted_cidrs().is_empty());
    }

    #[test]
    fn env_pinned_trust_counts_as_configured() {
        // `apply_stored_trust` early-returns for an env pin, and that path must
        // still settle the question: an environment-managed deployment declared
        // its policy in the deployment, so its visitors must never be offered a
        // wizard the endpoint would refuse.
        let config = RuntimeSecurityConfig {
            trust_env_pinned: true,
            ..RuntimeSecurityConfig::default()
        };
        assert!(!config.security_configured(), "premise: not yet settled");

        config.apply_stored_trust(None, None);

        assert!(config.security_configured());
    }

    #[test]
    fn configured_state_is_live_across_clones() {
        // Same requirement as the trust list: the wizard writes through one
        // clone and the next request arrives through another.
        let config = RuntimeSecurityConfig::default();
        let clone = config.clone();
        assert!(!clone.security_configured());

        config.mark_security_configured();

        assert!(clone.security_configured());
    }

    #[test]
    fn trust_changes_are_live_across_clones() {
        // The wizard's whole mechanism: a policy write in one clone must admit
        // the very next request arriving through another clone.
        let config = RuntimeSecurityConfig::default();
        let clone = config.clone();
        let peer = "127.0.0.1:1".parse().unwrap();
        assert!(!clone.is_trusted_peer(Some(peer)));

        config.apply_stored_trust(Some("no_login"), None);

        assert!(clone.is_trusted_peer(Some(peer)));
    }

    #[test]
    fn an_unbindable_stored_address_falls_back_instead_of_failing() {
        // The never-brick rule for the settlement half: an unparsable stored
        // value (hand-edited database, restore from another version) yields
        // loopback plus a recorded reason, never an error.
        let mut config = RuntimeSecurityConfig::default();
        config.apply_stored_bind_address(Some("not-an-address"));

        assert_eq!(config.http_bind_address, DEFAULT_HTTP_BIND_ADDRESS);
        assert_eq!(config.bind_address_source, BindAddressSource::Default);
        let reason = config.bind_fallback.as_deref().expect("fallback recorded");
        assert!(reason.contains(SETTING_HTTP_BIND_ADDRESS));
    }

    #[test]
    fn strict_security_messages_name_the_layer_that_caused_them() {
        // The S4 shape: a stored setting tripping strict security must not
        // blame an environment variable the operator never touched.
        let mut config = RuntimeSecurityConfig {
            strict_security: true,
            http_bind_address: "0.0.0.0".parse().unwrap(),
            bind_address_source: BindAddressSource::Setting,
            ..RuntimeSecurityConfig::default()
        };
        let message = config.strict_security_violation(false).expect("violation");
        assert!(
            message.contains(SETTING_HTTP_BIND_ADDRESS) && !message.contains("WEAVER_HTTP_BIND"),
            "stored-setting violation must name the setting: {message}"
        );

        config.bind_address_source = BindAddressSource::Environment;
        let message = config.strict_security_violation(false).expect("violation");
        assert!(
            message.contains(ENV_HTTP_BIND_ADDRESS),
            "environment violation must name the variable: {message}"
        );

        // And a trusting stored mode is named as such.
        config.bind_address_source = BindAddressSource::Default;
        config.http_bind_address = DEFAULT_HTTP_BIND_ADDRESS;
        config.set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
        let message = config.strict_security_violation(true).expect("violation");
        assert!(
            message.contains(SETTING_ACCESS_MODE),
            "stored-trust violation must name the access-mode setting: {message}"
        );
    }

    #[test]
    fn security_env_defaults_match_plan() {
        let _guard = env_lock();
        clear_env();

        let config = RuntimeSecurityConfig::from_env().unwrap();

        assert_eq!(config.http_bind_address, DEFAULT_HTTP_BIND_ADDRESS);
        assert_eq!(config.http_bind_address, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(config.http_allowed_hosts.is_empty());
        assert!(config.trusted_cidrs().is_empty());
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
        assert!(!config.security_configured());
    }

    #[test]
    fn security_env_accepts_bool_aliases_and_origins() {
        let _guard = env_lock();
        clear_env();
        unsafe {
            env::set_var(ENV_HTTP_BIND_ADDRESS, "127.0.0.1");
            env::set_var(
                ENV_HTTP_ALLOWED_HOSTS,
                "Weaver.Example.Test.,proxy.internal:8443",
            );
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
        assert_eq!(
            config.http_allowed_hosts,
            vec![
                HttpAuthority::parse("weaver.example.test").unwrap(),
                HttpAuthority::parse("proxy.internal:8443").unwrap(),
            ]
        );
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
        assert!(!default.exposes_admin_without_login(false));
        assert!(!default.exposes_admin_without_login(true));

        let exposed = RuntimeSecurityConfig {
            http_bind_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            ..RuntimeSecurityConfig::default()
        };
        assert!(exposed.exposes_admin_without_login(false));
    }

    #[test]
    fn ipv4_mapped_loopback_classifies_as_this_machine() {
        // The S7 shape: a dual-stack listener reports the machine's own
        // browser as ::ffff:127.0.0.1, which IpAddr::is_loopback() denies.
        let mapped_loopback: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        assert!(!mapped_loopback.is_loopback(), "premise of this test");
        assert_eq!(
            canonical_ip(mapped_loopback),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
        assert!(ip_is_loopback(mapped_loopback));

        // Everything else passes through untouched.
        for value in ["127.0.0.1", "::1", "2001:db8::1", "192.168.1.5"] {
            let ip: IpAddr = value.parse().unwrap();
            assert_eq!(canonical_ip(ip), ip, "rewrote {value}");
        }
        assert!(ip_is_loopback("::1".parse().unwrap()));
        assert!(!ip_is_loopback("::ffff:192.168.1.5".parse().unwrap()));

        // A mapped loopback bind is neither exposed nor a strict violation…
        let mapped = RuntimeSecurityConfig {
            http_bind_address: mapped_loopback,
            strict_security: true,
            ..RuntimeSecurityConfig::default()
        };
        assert!(!mapped.exposes_admin_without_login(false));
        assert!(mapped.strict_security_violation(false).is_none());

        // …while a mapped ROUTABLE address is still both.
        let mapped_lan = RuntimeSecurityConfig {
            http_bind_address: "::ffff:192.168.1.5".parse().unwrap(),
            strict_security: true,
            ..RuntimeSecurityConfig::default()
        };
        assert!(mapped_lan.exposes_admin_without_login(false));
        assert!(mapped_lan.strict_security_violation(false).is_some());
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
    fn trusted_cidrs_parse_and_match_immediate_peer_only() {
        let _guard = env_lock();
        clear_env();
        unsafe {
            env::set_var(
                ENV_TRUSTED_CIDRS,
                " 10.0.0.0/8 , 2001:db8::/32 , 192.168.0.0/16 ",
            );
        }

        let config = RuntimeSecurityConfig::from_env().unwrap();
        assert_eq!(config.trusted_cidrs().len(), 3);
        // An env pin is a declared policy from the first instant, before the
        // database is even open.
        assert!(config.trust_env_pinned && config.security_configured());
        assert!(config.is_trusted_peer(Some("10.42.0.1:1234".parse().unwrap())));
        assert!(config.is_trusted_peer(Some("[2001:db8::42]:1234".parse().unwrap())));
        assert!(config.is_trusted_peer(Some("[::ffff:192.168.3.4]:1234".parse().unwrap())));
        assert!(!config.is_trusted_peer(Some("172.16.0.1:1234".parse().unwrap())));
        assert!(!config.is_trusted_peer(None));

        clear_env();
    }

    #[test]
    fn trusted_cidrs_reject_empty_invalid_and_non_cidr_entries() {
        let _guard = env_lock();
        clear_env();

        for value in ["10.0.0.0/8,,192.168.0.0/16", "not-a-cidr", "10.0.0.1"] {
            unsafe { env::set_var(ENV_TRUSTED_CIDRS, value) };
            assert!(
                RuntimeSecurityConfig::from_env().is_err(),
                "accepted {value}"
            );
            clear_env();
        }
    }

    #[test]
    fn strict_security_rejects_trusted_cidrs() {
        let _guard = env_lock();
        clear_env();
        unsafe {
            env::set_var(ENV_STRICT_SECURITY, "1");
            env::set_var(ENV_TRUSTED_CIDRS, "127.0.0.0/8");
        }
        assert!(RuntimeSecurityConfig::from_env().is_err());
        clear_env();
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

        for value in [
            "example.test,,proxy.test",
            "https://example.test",
            "*.example.test",
            "user@example.test",
            "[::1",
            "example.test:not-a-port",
            "example.test:65536",
            "[::1]:not-a-port",
        ] {
            unsafe { env::set_var(ENV_HTTP_ALLOWED_HOSTS, value) };
            assert!(
                RuntimeSecurityConfig::from_env().is_err(),
                "accepted {value}"
            );
            clear_env();
        }
    }

    #[test]
    fn http_authority_policy_is_exact_and_port_aware() {
        let config = RuntimeSecurityConfig {
            http_allowed_hosts: vec![
                HttpAuthority::parse("weaver.example.test").unwrap(),
                HttpAuthority::parse("proxy.example.test:8443").unwrap(),
            ],
            ..RuntimeSecurityConfig::default()
        };

        for allowed in [
            "localhost",
            "LOCALHOST.:9090",
            "127.0.0.1:9090",
            "[::1]:9090",
            "weaver.example.test:9090",
            "WEAVER.EXAMPLE.TEST.",
            "proxy.example.test:8443",
        ] {
            let authority = HttpAuthority::parse(allowed).unwrap();
            assert!(
                config.is_http_authority_allowed(&authority),
                "denied {allowed}"
            );
        }

        for denied in [
            "localhost.example.test",
            "127.0.0.1.example.test",
            "attacker.example.test",
            "proxy.example.test",
            "proxy.example.test:9090",
        ] {
            let authority = HttpAuthority::parse(denied).unwrap();
            assert!(
                !config.is_http_authority_allowed(&authority),
                "allowed {denied}"
            );
        }

        assert!(HttpAuthority::parse("2001:db8::1").is_err());
        assert!(HttpAuthority::parse("[2001:db8::1]:9090").is_ok());
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
