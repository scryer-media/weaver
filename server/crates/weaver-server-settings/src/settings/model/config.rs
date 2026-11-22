// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[cfg(test)]
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::categories::CategoryConfig;
#[cfg(test)]
use crate::torrents::identity::IDENTITY;
use crate::watch_folder::WatchFolderConfig;
use weaver_server_base::runtime::hardware_profile::HardwareProfile;
use weaver_server_jobs::jobs::duplicate::DuplicatePolicy;
use weaver_server_net::servers::ServerConfig;
#[cfg(test)]
use weaver_torrent::tracker::qbittorrent;

use super::*;

// The NZB admission limit when none is saved, for new and upgraded installations.
pub const DEFAULT_MAX_CONCURRENT_NZBS: std::num::NonZeroU32 =
    std::num::NonZeroU32::new(10).unwrap();

// Shared config handle for runtime reads and writes.
pub type SharedConfig = Arc<RwLock<Config>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: String,
    // Directory for active downloads (per-job subdirectories).
    // Defaults to `{data_dir}/intermediate`.
    pub intermediate_dir: Option<String>,
    // Directory for completed downloads (category subdirectories).
    // Defaults to `{data_dir}/complete`.
    pub complete_dir: Option<String>,
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
    #[serde(default)]
    pub categories: Vec<CategoryConfig>,
    #[serde(default)]
    pub retry: Option<RetryOverrides>,
    // The global bandwidth limits, kept in networking.
    #[serde(default)]
    pub bandwidth: weaver_server_net::proxies::Bandwidth,
    // Minimum post age before downloading. Zero disables the hold.
    #[serde(default)]
    pub propagation_delay_secs: Option<u32>,
    // NZBs admitted through terminal completion, across every Usenet provider.
    #[serde(default)]
    pub max_concurrent_nzbs: Option<std::num::NonZeroU32>,
    // Whether to delete intermediate files (NZB articles, PAR2, RAR volumes)
    // after successful extraction. Defaults to true.
    #[serde(default)]
    pub cleanup_after_extract: Option<bool>,
    // Whether housekeeping deletes what a failed job left in the intermediate
    // and staging directories a day after it failed. Defaults to true.
    #[serde(default)]
    pub delete_failed_job_files: Option<bool>,
    // How many days a history entry is kept after its job finished, before
    // housekeeping deletes the entry. Its delivered files stay. 0 keeps
    // history forever. Defaults to 90.
    #[serde(default)]
    pub history_retention_days: Option<u32>,
    // Watched-folder NZB intake settings.
    #[serde(default)]
    pub watch_folder: WatchFolderConfig,
    // Duplicate admission handling policy.
    #[serde(default)]
    pub duplicate_policy: DuplicatePolicy,
    // Limits on the bytes the graph engine holds while it waits to place
    // them. Absent means "every default".
    #[serde(default)]
    pub direct_store: Option<DirectStoreOverrides>,
    // Naming policy for the files a finished job delivers. Absent means
    // "every default".
    #[serde(default)]
    pub delivery_naming: Option<DeliveryNamingOverrides>,
    // How hard Weaver leans on this machine. Absent means the operator has
    // never chosen one, and the machine's recommendation stands in.
    #[serde(default)]
    pub hardware_profile: Option<HardwareProfile>,
    // Prometheus exposition knobs.
    #[serde(default)]
    pub metrics: MetricsConfig,
    // The global seeding policy. Absent means no goal: a torrent seeds until
    // it is stopped.
    #[serde(default)]
    pub seeding: Option<SeedingPolicy>,
    // The upload cap each torrent takes unless its category or tracker rule
    // sets its own; absent is none.
    #[serde(default)]
    pub upload_cap: Option<u64>,
    #[serde(default)]
    pub torrent: TorrentConfig,
    // Path to the config file on disk. Not serialized to TOML.
    #[serde(skip)]
    pub config_path: Option<PathBuf>,
}

// The history retention, in days, when none is configured.
pub const DEFAULT_HISTORY_RETENTION_DAYS: u32 = 90;
// The longest history retention, in days, a setting may ask for.
pub const MAX_HISTORY_RETENTION_DAYS: u32 = 36_500;

impl Config {
    // Returns the intermediate directory for active downloads.
    // Defaults to `{data_dir}/intermediate`.
    pub fn intermediate_dir(&self) -> String {
        self.intermediate_dir
            .clone()
            .unwrap_or_else(|| format!("{}/intermediate", self.data_dir))
    }

    // Returns the complete directory for finished downloads.
    // Defaults to `{data_dir}/complete`.
    pub fn complete_dir(&self) -> String {
        self.complete_dir
            .clone()
            .unwrap_or_else(|| format!("{}/complete", self.data_dir))
    }

    // Whether to clean up intermediate files after successful extraction.
    // Defaults to `true` when not explicitly configured.
    pub fn cleanup_after_extract(&self) -> bool {
        self.cleanup_after_extract.unwrap_or(true)
    }

    // Whether housekeeping deletes a failed job's files. Defaults to `true`
    // when not explicitly configured.
    pub fn delete_failed_job_files(&self) -> bool {
        self.delete_failed_job_files.unwrap_or(true)
    }

    // How many days history is kept. 0 keeps it forever. Defaults to 90 when
    // not explicitly configured.
    pub fn history_retention_days(&self) -> u32 {
        self.history_retention_days
            .unwrap_or(DEFAULT_HISTORY_RETENTION_DAYS)
    }

    // Whether a finished job may rename a delivered member that still wears an
    // obfuscated name. Defaults to `true`: an obfuscated member is unusable to
    // every downstream tool, and the pass refuses itself whenever the job's own
    // name is no better.
    pub fn deobfuscate_delivered_members(&self) -> bool {
        self.delivery_naming
            .as_ref()
            .and_then(|naming| naming.deobfuscate_delivered_members)
            .unwrap_or(true)
    }

    // Whether an obfuscated member may be looked up by CRC32 in the public
    // srrdb release index before falling back to the job's own name.
    //
    // Defaults to `false`. This is the only part of completion that leaves the
    // operator's network, so it stays an explicit opt-in even though the
    // request carries nothing but a checksum.
    //
    pub fn enable_srrdb_lookup(&self) -> bool {
        self.delivery_naming
            .as_ref()
            .and_then(|naming| naming.enable_srrdb_lookup)
            .unwrap_or(false)
    }

    pub fn max_concurrent_nzbs(&self) -> u32 {
        self.max_concurrent_nzbs
            .unwrap_or(DEFAULT_MAX_CONCURRENT_NZBS)
            .get()
    }

    // A saved setting takes precedence over the legacy environment override.
    pub fn propagation_delay_secs(&self) -> u32 {
        self.propagation_delay_secs
            .or_else(|| {
                std::env::var("WEAVER_PROPAGATION_DELAY_SECS")
                    .ok()
                    .and_then(|value| value.trim().parse().ok())
            })
            .unwrap_or(0)
    }

    // Validate the configuration, returning any issues found.
    // Empty server list is allowed (users add servers via UI).
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for (i, server) in self.servers.iter().enumerate() {
            if server.port == 0 {
                errors.push(format!("server[{i}] ({}) port must be > 0", server.host));
            }
            if server.connections == 0 {
                errors.push(format!(
                    "server[{i}] ({}) connections must be > 0",
                    server.host
                ));
            }
            if let Err(error) = server.validate_download_limits() {
                errors.push(format!("server[{i}] ({}) {error}", server.host));
            }
        }

        if self.data_dir.is_empty() {
            errors.push("data_dir must not be empty".to_string());
        }

        if let Err(error) = self.watch_folder.validate() {
            errors.push(error);
        }

        if let Err(torrent) = self.torrent.validate() {
            errors.extend(torrent);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    // Save the config back to disk.
    pub fn save(&self) -> std::io::Result<()> {
        let path = self
            .config_path
            .as_ref()
            .ok_or_else(|| std::io::Error::other("no config path set"))?;
        let toml_str = toml::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(path, toml_str)
    }

    // Return the next available server ID.
    pub fn next_server_id(&self) -> u32 {
        self.servers.iter().map(|s| s.id).max().unwrap_or(0) + 1
    }

    // Assign IDs to any servers that have id == 0 (backward compat with old TOML files).
    pub fn assign_server_ids(&mut self) {
        let mut next = self.next_server_id();
        for server in &mut self.servers {
            if server.id == 0 {
                server.id = next;
                next += 1;
            }
        }
    }

    // Return the next available category ID.
    pub fn next_category_id(&self) -> u32 {
        self.categories.iter().map(|c| c.id).max().unwrap_or(0) + 1
    }

    // Assign IDs to any categories that have id == 0.
    pub fn assign_category_ids(&mut self) {
        let mut next = self.next_category_id();
        for cat in &mut self.categories {
            if cat.id == 0 {
                cat.id = next;
                next += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_server_net::servers::ServerDownloadQuotaConfig;
    use weaver_server_net::servers::model::MAX_PERSISTED_SERVER_DOWNLOAD_BYTES;

    fn config_with_server() -> Config {
        Config {
            data_dir: "/tmp/weaver".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            servers: vec![ServerConfig {
                id: 1,
                host: "news.example.com".to_string(),
                port: 563,
                tls: true,
                username: None,
                password: None,
                connections: 4,
                active: false,
                supports_pipelining: false,
                pipelining_depth: None,
                tls_name_mismatch_certificate_der: None,
                priority: 0,
                backfill: false,
                retention_days: 0,
                max_download_speed: 0,
                download_quota: ServerDownloadQuotaConfig::default(),
                tls_ca_cert: None,
            }],
            categories: vec![],
            retry: None,
            bandwidth: Default::default(),
            cleanup_after_extract: None,
            delete_failed_job_files: None,
            history_retention_days: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            watch_folder: WatchFolderConfig::default(),
            duplicate_policy: DuplicatePolicy::default(),
            direct_store: None,
            delivery_naming: None,
            hardware_profile: None,
            metrics: Default::default(),
            seeding: None,
            upload_cap: None,
            torrent: TorrentConfig::default(),
            config_path: None,
        }
    }

    fn policy(ratio_goal: Option<f64>, seed_time_goal_minutes: Option<u32>) -> SeedingPolicy {
        SeedingPolicy {
            ratio_goal: Leaf::of(ratio_goal),
            seed_time_goal_minutes: Leaf::of(seed_time_goal_minutes),
            ..SeedingPolicy::default()
        }
    }

    // A policy kept before its fields were leaves reads as one: a goal it
    // left unset inherits, and its mode and action are set.
    #[test]
    fn a_policy_kept_before_leaves_reads_as_leaves() {
        let kept: SeedingPolicy = serde_json::from_str(
            r#"{"ratio_goal":2.0,"seed_time_goal_minutes":null,"inactive_goal_minutes":null,"mode":"all","action":"remove"}"#,
        )
        .unwrap();
        assert_eq!(
            kept,
            SeedingPolicy {
                ratio_goal: Leaf::Set(2.0),
                mode: Leaf::Set(SeedingGoalMode::All),
                action: Leaf::Set(SeedingAction::Remove),
                ..SeedingPolicy::default()
            }
        );
        let off = SeedingPolicy {
            seed_time_goal_minutes: Leaf::Off,
            ..kept
        };
        let json = serde_json::to_string(&off).unwrap();
        assert_eq!(
            json,
            r#"{"ratio_goal":2.0,"seed_time_goal_minutes":"off","mode":"all","action":"remove"}"#
        );
        assert_eq!(serde_json::from_str::<SeedingPolicy>(&json).unwrap(), off);
        assert_eq!(SeedingPolicy::kept(Some(SeedingPolicy::default())), None);
        assert!(
            SeedingPolicy {
                mode: Leaf::Off,
                ..SeedingPolicy::default()
            }
            .validate()
            .is_err()
        );
    }

    fn rule(id: u32, host_pattern: &str) -> TrackerRule {
        TrackerRule {
            transports: TransportRule::default(),
            archive_passwords: Default::default(),
            bound: None,
            id,
            host_pattern: TrackerRule::pattern(host_pattern).unwrap(),
            seeding: None,
            never_pause: false,
            unpack: None,
            client: None,
            torrent_route: false,
            dht: None,
            pex: None,
            lsd: None,
            encryption: None,
            upload_cap: Leaf::Inherit,
            forced: false,
            ssl: None,
        }
    }

    // A certificate for `subject` until the start of 2031, and its key.
    fn tracker_certificate(subject: &str) -> TrackerCertificate {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, subject);
        params.not_after = rcgen::date_time_ymd(2031, 1, 1);
        TrackerCertificate {
            certificate_pem: params.self_signed(&key).unwrap().pem(),
            private_key_pem: key.serialize_pem(),
        }
    }

    // A rule keeps a certificate whose key is its own, and says whom it
    // names and until when; a PEM that does not parse, or a key of another
    // certificate, is refused.
    #[test]
    fn a_rules_certificate_is_checked_against_its_key() {
        let issued = tracker_certificate("user-1");
        let with = |ssl: TrackerCertificate| {
            TrackerRule {
                ssl: Some(ssl),
                ..rule(0, "tracker.example")
            }
            .checked()
        };
        assert_eq!(with(issued.clone()).unwrap().ssl.as_ref(), Some(&issued));
        let summary = issued.summary().unwrap();
        assert_eq!(summary.subject, "CN=user-1");
        assert_eq!(summary.not_after_ms, 1_924_992_000_000);

        let other = tracker_certificate("user-2");
        let refused = [
            TrackerCertificate {
                private_key_pem: other.private_key_pem.clone(),
                ..issued.clone()
            },
            TrackerCertificate {
                certificate_pem: "not a certificate".to_owned(),
                ..issued.clone()
            },
            TrackerCertificate {
                private_key_pem: "not a key".to_owned(),
                ..issued.clone()
            },
        ];
        for ssl in refused {
            let error = with(ssl).unwrap_err();
            assert!(
                error.starts_with("the SSL certificate is refused"),
                "{error}"
            );
        }
        // The key never shows in what is logged.
        assert!(!format!("{issued:?}").contains(&issued.private_key_pem));
    }

    // Each row's prefix, User-Agent and `v` name one version, encoded as
    // that client encodes it.
    #[test]
    fn each_client_names_one_version_its_own_way() {
        fn version(text: &str, separator: char) -> Vec<u32> {
            let (_, version) = text.rsplit_once(separator).unwrap();
            version
                .split('.')
                .map(|part| part.parse().unwrap())
                .collect()
        }
        // libtorrent: a digit below ten, then `A` onwards.
        fn libtorrent(code: &[u8; 2], parts: &[u32]) -> [u8; 8] {
            let digit = |v: u32| {
                if v < 10 {
                    b'0' + v as u8
                } else {
                    b'A' + (v - 10) as u8
                }
            };
            let mut parts = parts.to_vec();
            parts.resize(4, 0);
            [
                b'-',
                code[0],
                code[1],
                digit(parts[0]),
                digit(parts[1]),
                digit(parts[2]),
                digit(parts[3]),
                b'-',
            ]
        }
        let qb = Client::QBittorrent.name();
        assert_eq!(
            qb.prefix(),
            &libtorrent(b"qB", &version(qb.user_agent(), '/'))
        );
        assert_eq!(qb.prefix(), b"-qB5300-");
        assert_eq!(qb.version(), qb.user_agent());
        assert_eq!(qb.identity().shape, &qbittorrent::SHAPE);
        assert_eq!(&libtorrent(b"qB", &[5, 1, 2]), b"-qB5120-");
        assert_eq!(&libtorrent(b"qB", &[5, 10, 0]), b"-qB5A00-");

        let weaver = Client::Weaver.name();
        assert_eq!(weaver, IDENTITY);
        assert_eq!(weaver.version(), weaver.user_agent());
        assert_eq!(
            weaver.identity().shape,
            &weaver_torrent::tracker::AnnounceShape::WEAVER
        );

        for client in Client::ALL {
            assert_eq!(Client::parse(client.as_str()), Some(client));
        }
        assert_eq!(Client::parse("deluge"), None);
        assert_eq!(Client::parse("transmission"), Some(Client::Weaver));
    }

    #[test]
    fn a_tracker_rule_names_a_host_or_every_host_under_one() {
        assert_eq!(
            TrackerRule::pattern(" *.Example.ORG. ").as_deref(),
            Ok("*.example.org")
        );
        for bad in [
            "",
            "*.",
            "http://example.org",
            "example.org/announce",
            "a..b",
            "*",
        ] {
            assert!(TrackerRule::pattern(bad).is_err(), "{bad}");
        }
        let rules = [
            rule(1, "*.example.org"),
            rule(2, "tracker.example.org"),
            rule(3, "*.tracker.example.org"),
            rule(4, "other.invalid"),
        ];
        let id = |trackers: &[&str]| {
            tracker_rule_for(&rules, trackers.iter().copied()).map(|rule| rule.id)
        };
        assert_eq!(id(&["https://tracker.example.org/announce"]), Some(2));
        assert_eq!(id(&["udp://Tracker.Example.org.:1337"]), Some(2));
        assert_eq!(id(&["http://a.tracker.example.org/x"]), Some(3));
        assert_eq!(id(&["http://example.org/x"]), Some(1));
        assert_eq!(id(&["http://cdn.example.org/x"]), Some(1));
        assert_eq!(id(&["http://notexample.org/x"]), None);
        assert_eq!(id(&["http://public.invalid/x", "not a url"]), None);
        // The closest pattern over every tracker wins, wherever it is listed.
        assert_eq!(
            id(&["http://cdn.example.org/x", "http://other.invalid/x"]),
            Some(4)
        );
        assert_eq!(id(&[]), None);
        // On a tie, the lower id.
        let tied = [rule(9, "*.example.org"), rule(5, "*.example.org")];
        assert_eq!(
            tracker_rule_for(&tied, ["http://a.example.org/"]).map(|rule| rule.id),
            Some(5)
        );
    }

    // A rule's handshake and dial order overrides are keyed by host alone: an
    // announce URL carrying a passkey finds them by its host, and a rule
    // cannot be given the URL, so its passkey is never kept with them.
    #[test]
    fn handshake_and_dial_order_overrides_follow_the_host_never_the_passkey() {
        let passkeyed = "https://Private.Example.ORG/0123456789abcdef/announce?passkey=secret";
        assert!(TrackerRule::pattern(passkeyed).is_err());
        let private = TrackerRule {
            encryption: Some(TorrentEncryption::Disabled),
            transports: TransportRule {
                utp: Some(false),
                ..TransportRule::default()
            },
            ..rule(1, "private.example.org")
        };
        let public = TrackerRule {
            encryption: Some(TorrentEncryption::Require),
            ..rule(2, "*.public.test")
        };
        let rules = [private, public];
        let ruled = tracker_rule_for(&rules, [passkeyed]).unwrap();
        assert_eq!(ruled.host_pattern, "private.example.org");
        assert_eq!(
            (ruled.encryption, ruled.transports.utp),
            (Some(TorrentEncryption::Disabled), Some(false))
        );
        let open = tracker_rule_for(&rules, ["udp://tracker.public.test:1337/announce"]).unwrap();
        assert_eq!(
            (open.encryption, open.transports),
            (Some(TorrentEncryption::Require), TransportRule::default())
        );
        assert!(!format!("{rules:?}").contains("secret"));
    }

    fn transports(
        tcp: bool,
        utp: bool,
        downloading: PeerTransport,
        seeding: PeerTransport,
    ) -> PeerTransports {
        PeerTransports {
            tcp,
            utp,
            prefer_downloading: downloading,
            prefer_seeding: seeding,
        }
    }

    // Each combination of the switches and preferences gives the orders
    // libtorrent's `enable_outgoing_*` and `torrent::connect_to_peer` would:
    // with both on the preferred one first, with one on that one alone.
    #[test]
    fn each_combination_of_transports_gives_its_dial_orders() {
        use PeerTransport::{Tcp, Utp};
        use weaver_torrent::DialOrder::{TcpFirst, TcpOnly, UtpFirst, UtpOnly};
        let orders = |downloading, seeding| weaver_torrent::DialOrders {
            downloading,
            seeding,
        };
        for (set, expected) in [
            (transports(true, true, Tcp, Tcp), orders(TcpFirst, TcpFirst)),
            (transports(true, true, Tcp, Utp), orders(TcpFirst, UtpFirst)),
            (transports(true, true, Utp, Tcp), orders(UtpFirst, TcpFirst)),
            (transports(true, true, Utp, Utp), orders(UtpFirst, UtpFirst)),
            (transports(true, false, Utp, Utp), orders(TcpOnly, TcpOnly)),
            (transports(false, true, Tcp, Tcp), orders(UtpOnly, UtpOnly)),
        ] {
            assert_eq!(set.checked().unwrap().dial_orders(), expected, "{set:?}");
        }
    }

    // A save keeps both preferences as set whatever the switches say, and
    // refuses both off; with one off, the effective preferences are the one
    // left on, and the kept ones come back when both are on again.
    #[test]
    fn a_save_keeps_the_preferences_and_refuses_both_off() {
        use PeerTransport::{Tcp, Utp};
        for set in [
            transports(true, false, Utp, Utp),
            transports(false, true, Tcp, Utp),
            transports(true, true, Utp, Tcp),
        ] {
            assert_eq!(set.checked(), Ok(set));
        }
        assert!(transports(false, false, Tcp, Utp).checked().is_err());
        assert_eq!(
            transports(true, false, Utp, Utp).effective(),
            transports(true, false, Tcp, Tcp)
        );
        assert_eq!(
            transports(false, true, Tcp, Utp).effective(),
            transports(false, true, Utp, Utp)
        );
        assert_eq!(
            transports(true, true, Utp, Tcp).effective(),
            transports(true, true, Utp, Tcp)
        );

        let saved = TorrentConfig {
            transports: transports(false, true, Tcp, Tcp),
            ..TorrentConfig::default()
        }
        .checked()
        .unwrap();
        assert_eq!(saved.transports, transports(false, true, Tcp, Tcp));
        assert_eq!(
            saved.transports.dial_orders(),
            weaver_torrent::DialOrders::both(weaver_torrent::DialOrder::UtpOnly)
        );
        let again = PeerTransports {
            tcp: true,
            ..saved.transports
        };
        assert_eq!(
            again.dial_orders(),
            weaver_torrent::DialOrders::both(weaver_torrent::DialOrder::TcpFirst)
        );
        let refused = TorrentConfig {
            transports: transports(false, false, Tcp, Utp),
            ..TorrentConfig::default()
        };
        assert_eq!(
            refused.clone().checked(),
            Err(vec![
                "torrent.transports: TCP and uTP cannot both be off".to_owned()
            ])
        );
        assert!(refused.validate().is_err());
    }

    // A rule's fields each override the global one, so a rule may broaden
    // or narrow the global transports; its save keeps its preferences and
    // is refused when both end off over the global ones, and a rule a later
    // global change leaves with both off is not applied.
    #[test]
    fn a_rules_transports_broaden_or_narrow_the_global_ones() {
        use PeerTransport::{Tcp, Utp};
        use weaver_torrent::DialOrder::{TcpFirst, TcpOnly, UtpFirst, UtpOnly};
        let utp_only = transports(false, true, Utp, Utp);
        let tcp_only = transports(true, false, Tcp, Tcp);
        let both = PeerTransports::default();
        let rule = |tcp, utp, downloading, seeding| TransportRule {
            tcp,
            utp,
            prefer_downloading: downloading,
            prefer_seeding: seeding,
        };
        // Broadened: TCP on over uTP alone, TCP first while downloading.
        let tcp_first = rule(Some(true), None, Some(Tcp), None);
        let resolved = tcp_first.resolve(utp_only);
        assert_eq!(
            (
                resolved.dial_orders().downloading,
                resolved.dial_orders().seeding
            ),
            (TcpFirst, UtpFirst)
        );
        // Broadened: uTP on over TCP alone.
        let resolved = rule(None, Some(true), None, Some(Utp)).resolve(tcp_only);
        assert_eq!(
            (
                resolved.dial_orders().downloading,
                resolved.dial_orders().seeding
            ),
            (TcpFirst, UtpFirst)
        );
        // Narrowed: uTP alone, or TCP alone, over both.
        let utp_alone = rule(Some(false), None, None, None);
        assert_eq!(
            utp_alone.resolve(both).dial_orders(),
            weaver_torrent::DialOrders::both(UtpOnly)
        );
        assert_eq!(
            rule(None, Some(false), None, None)
                .resolve(both)
                .dial_orders(),
            weaver_torrent::DialOrders::both(TcpOnly)
        );
        // Each preference on its own.
        assert_eq!(
            rule(None, None, Some(Utp), Some(Tcp))
                .resolve(both)
                .dial_orders(),
            weaver_torrent::DialOrders {
                downloading: UtpFirst,
                seeding: TcpFirst,
            }
        );

        // Saved: a set preference for a transport left off is kept, and
        // the effective one is the transport left on.
        let kept = rule(Some(false), None, Some(Tcp), None);
        assert_eq!(kept.checked(both), Ok(kept));
        assert_eq!(kept.resolve(both).prefer_downloading, Tcp);
        assert_eq!(kept.resolve(both).effective().prefer_downloading, Utp);
        let kept = rule(None, None, Some(Utp), Some(Utp));
        assert_eq!(kept.checked(tcp_only), Ok(kept));
        assert_eq!(
            kept.resolve(tcp_only).dial_orders(),
            weaver_torrent::DialOrders::both(TcpOnly)
        );
        // Refused when both end off, on its own or over the global ones.
        assert!(
            rule(Some(false), Some(false), None, None)
                .checked(both)
                .is_err()
        );
        assert!(utp_alone.checked(tcp_only).is_err());
        // Saved over both, then left with both off by a global change: the
        // torrents take the global transports.
        assert_eq!(utp_alone.resolve(tcp_only), tcp_only);
    }

    #[test]
    fn a_config_without_torrent_keys_loads_the_defaults_and_round_trips() {
        let config: Config = toml::from_str(
            r#"
data_dir = "/tmp/weaver-data"

[[servers]]
id = 1
host = "news.example.com"
port = 443
tls = true
username = "user"
password = "pass"
connections = 10
active = true

[[categories]]
id = 1
name = "tv"
"#,
        )
        .unwrap();

        assert_eq!(config.seeding, None);
        assert_eq!(config.torrent, TorrentConfig::default());
        assert!(!config.torrent.enabled);
        assert_eq!(config.torrent.max_peers_total, 100);
        assert_eq!(config.categories[0].seeding, None);
        assert!(!config.categories[0].torrent_route);

        let reparsed: Config = toml::from_str(&toml::to_string_pretty(&config).unwrap()).unwrap();
        assert_eq!(reparsed.seeding, None);
        assert_eq!(reparsed.torrent, TorrentConfig::default());
        assert_eq!(reparsed.categories[0].seeding, None);
        assert!(!reparsed.categories[0].torrent_route);
    }

    #[test]
    fn torrent_and_seeding_tables_round_trip_through_toml() {
        let mut config = config_with_server();
        config.seeding = Some(SeedingPolicy {
            inactive_goal_minutes: Leaf::Off,
            mode: Leaf::Set(SeedingGoalMode::All),
            action: Leaf::Set(SeedingAction::RemoveWithData),
            ..policy(Some(2.0), Some(1_440))
        });
        config.upload_cap = Some(65_536);
        config.torrent = TorrentConfig {
            enabled: true,
            base_port: 51_413,
            announce_port: Some(40_000),
            upnp: false,
            encryption: TorrentEncryption::Require,
            max_active_downloads: 5,
            max_peers_total: 200,
            session_dir: Some(PathBuf::from("/var/lib/weaver/torrents")),
            unpack: false,
            connections_limit: TorrentLimit::Unlimited,
            unchoke_slots_limit: TorrentLimit::Count(NonZeroU32::new(20).unwrap()),
            connection_speed: NonZeroU32::new(50).unwrap(),
            allow_multiple_connections_per_ip: true,
            transports: PeerTransports {
                tcp: true,
                utp: false,
                prefer_downloading: PeerTransport::Tcp,
                prefer_seeding: PeerTransport::Tcp,
            },
            dht_bootstrap_nodes: "router.example:6881".to_owned(),
            blocklist_enabled: true,
            blocklist: Some("https://lists.example/level1.p2p".to_owned()),
            stop_tracker_timeout_secs: 0,
            banned_ips: vec!["192.0.2.7".to_owned(), "10.0.0.0 - 10.0.0.255".to_owned()],
            block_privileged_ports: true,
            filter_trackers: true,
            ssl_enabled: true,
            ssl_port: Some(4_433),
        };

        let reparsed: Config = toml::from_str(&toml::to_string_pretty(&config).unwrap()).unwrap();

        assert_eq!(reparsed.seeding, config.seeding);
        assert_eq!(reparsed.upload_cap, config.upload_cap);
        assert_eq!(reparsed.torrent, config.torrent);
    }

    #[test]
    fn torrent_session_settings_default_to_libtorrents() {
        // libtorrent settings_pack.cpp: TCP first, uTP raced behind it.
        let count = |count| TorrentLimit::Count(NonZeroU32::new(count).unwrap());
        let torrent = TorrentConfig::default();
        assert_eq!(torrent.connections_limit, count(200));
        assert_eq!(torrent.unchoke_slots_limit, count(8));
        assert_eq!(torrent.connection_speed.get(), 30);
        assert!(!torrent.allow_multiple_connections_per_ip);
        // Both transports on, TCP first while downloading and uTP first
        // while seeding.
        assert_eq!(
            torrent.transports,
            PeerTransports {
                tcp: true,
                utp: true,
                prefer_downloading: PeerTransport::Tcp,
                prefer_seeding: PeerTransport::Utp,
            }
        );
        assert_eq!(
            torrent.transports.dial_orders(),
            weaver_torrent::DialOrders {
                downloading: weaver_torrent::DialOrder::TcpFirst,
                seeding: weaver_torrent::DialOrder::UtpFirst,
            }
        );
        // The engine's own routers, as before the setting.
        assert_eq!(
            torrent.dht_bootstrap_nodes,
            "dht.transmissionbt.com:6881,dht.libtorrent.org:25401"
        );
        assert!(!torrent.blocklist_enabled);
        assert_eq!(torrent.blocklist, None);
        assert_eq!(torrent.stop_tracker_timeout_secs, 5);
        // qBittorrent's: none banned, privileged ports and trackers left
        // alone.
        assert!(torrent.banned_ips.is_empty());
        assert!(!torrent.block_privileged_ports);
        assert!(!torrent.filter_trackers);
        assert_eq!(torrent.validate(), Ok(()));

        let config: Config = toml::from_str(
            r#"
data_dir = "/tmp/weaver-data"

[torrent]
enabled = true
connections_limit = 80
unchoke_slots_limit = "unlimited"

[torrent.transports]
tcp = false
prefer_seeding = "tcp"
"#,
        )
        .unwrap();
        assert_eq!(config.torrent.connections_limit, count(80));
        // A key left out is its default.
        assert_eq!(
            config.torrent.transports,
            PeerTransports {
                tcp: false,
                utp: true,
                prefer_downloading: PeerTransport::Tcp,
                prefer_seeding: PeerTransport::Tcp,
            }
        );
        assert_eq!(config.torrent.unchoke_slots_limit, TorrentLimit::Unlimited);
        assert_eq!(config.torrent.connection_speed.get(), 30);
        assert_eq!(config.torrent.stop_tracker_timeout_secs, 5);

        // A count is never zero.
        let zero = toml::from_str::<Config>(
            r#"
data_dir = "/tmp/weaver-data"

[torrent]
connections_limit = 0
"#,
        );
        assert!(zero.is_err());
    }

    #[test]
    fn a_blocklist_is_a_url_or_a_path_and_applies_while_on() {
        let source = |blocklist: &str| BlocklistSource::of(blocklist);
        assert!(matches!(
            source("https://lists.example/level1.p2p"),
            BlocklistSource::Http(_)
        ));
        // A file URL is the platform's path it names.
        #[cfg(not(windows))]
        let (file_url, file_path) = ("file:///var/lib/weaver/level1.p2p", "/var/lib/weaver/level1.p2p");
        #[cfg(windows)]
        let (file_url, file_path) = ("file:///C:/lists/level1.p2p", r"C:\lists\level1.p2p");
        assert_eq!(source(file_url), BlocklistSource::File(file_path.into()));
        // Every other scheme is a path, a Windows drive letter among them.
        for path in [
            r"C:\lists\level1.p2p",
            "/var/lib/weaver/level1.p2p",
            "lists/level1.p2p",
        ] {
            assert_eq!(source(path), BlocklistSource::File(path.into()), "{path}");
        }

        let named = TorrentConfig {
            blocklist: Some("/var/lib/weaver/level1.p2p".to_owned()),
            ..TorrentConfig::default()
        };
        assert_eq!(named.blocklist_source(), None, "off");
        let on = TorrentConfig {
            blocklist_enabled: true,
            ..named
        };
        assert_eq!(
            on.blocklist_source(),
            Some(BlocklistSource::File("/var/lib/weaver/level1.p2p".into()))
        );
        let unnamed = TorrentConfig {
            blocklist: None,
            ..on
        };
        assert_eq!(unnamed.blocklist_source(), None, "on, naming none");
    }

    #[test]
    fn checked_settings_keep_one_bootstrap_list_and_no_blank_blocklist() {
        let checked = TorrentConfig {
            dht_bootstrap_nodes: " a.example:6881 ,, b.example:25401 ".to_owned(),
            blocklist: Some("  ".to_owned()),
            ..TorrentConfig::default()
        }
        .checked()
        .unwrap();
        assert_eq!(
            checked.dht_bootstrap_nodes,
            "a.example:6881,b.example:25401"
        );
        assert_eq!(checked.blocklist, None);

        let trimmed = TorrentConfig {
            blocklist: Some(" /var/lib/weaver/level1.p2p ".to_owned()),
            ..TorrentConfig::default()
        }
        .checked()
        .unwrap();
        assert_eq!(
            trimmed.blocklist.as_deref(),
            Some("/var/lib/weaver/level1.p2p")
        );
    }

    // qBittorrent `SessionImpl::setBannedIPs`: each entry an address, a
    // range or a subnet, kept in one form, sorted and once.
    #[test]
    fn banned_entries_are_kept_in_one_form_sorted_and_once() {
        let checked = TorrentConfig {
            banned_ips: [
                " 192.0.2.7 ",
                "",
                "2001:0db8:0000::1",
                "10.0.0.0/24",
                "10.0.0.0-10.0.0.255",
                "192.0.2.7",
                "2001:db8::/120",
                "172.16.0.9 - 172.16.0.9",
            ]
            .map(str::to_owned)
            .to_vec(),
            ..TorrentConfig::default()
        }
        .checked()
        .unwrap();
        assert_eq!(
            checked.banned_ips,
            [
                "10.0.0.0 - 10.0.0.255",
                "172.16.0.9",
                "192.0.2.7",
                "2001:db8:: - 2001:db8::ff",
                "2001:db8::1",
            ]
        );
        let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
        assert_eq!(
            checked.banned_ranges()[0],
            (ip("10.0.0.0"), ip("10.0.0.255"))
        );
        assert_eq!(
            banned_range("0.0.0.0/0"),
            Some((ip("0.0.0.0"), ip("255.255.255.255")))
        );
        assert_eq!(banned_range("::1/128"), Some((ip("::1"), ip("::1"))));

        for refused in [
            "192.0.2.300",
            "10.0.0.9 - 10.0.0.1",
            "10.0.0.1 - ::1",
            "10.0.0.0/33",
            "10.0.0.1 - 10.0.0.2 - 10.0.0.3",
            "example.org",
        ] {
            let errors = TorrentConfig {
                banned_ips: vec![refused.to_owned()],
                ..TorrentConfig::default()
            }
            .checked()
            .unwrap_err();
            assert_eq!(errors.len(), 1, "{refused}");
        }
    }

    #[test]
    fn torrent_session_settings_out_of_range_are_refused() {
        let refused = |torrent: TorrentConfig| torrent.validate().unwrap_err();
        for torrent in [
            TorrentConfig {
                dht_bootstrap_nodes: "dht.libtorrent.org".to_owned(),
                ..TorrentConfig::default()
            },
            TorrentConfig {
                dht_bootstrap_nodes: "a.example:6881,:6881".to_owned(),
                ..TorrentConfig::default()
            },
            TorrentConfig {
                dht_bootstrap_nodes: "a.example:70000".to_owned(),
                ..TorrentConfig::default()
            },
        ] {
            assert_eq!(refused(torrent.clone()).len(), 1, "{torrent:?}");
        }

        let mut config = config_with_server();
        config.torrent.dht_bootstrap_nodes = "dht.libtorrent.org".to_owned();
        assert!(config.validate().is_err());

        // No bootstrap nodes, no stopped announces, no limits and a
        // blocklist path are all settings libtorrent takes.
        let accepted = TorrentConfig {
            dht_bootstrap_nodes: String::new(),
            stop_tracker_timeout_secs: 0,
            connections_limit: TorrentLimit::Unlimited,
            unchoke_slots_limit: TorrentLimit::Unlimited,
            blocklist_enabled: true,
            blocklist: Some("lists/level1.p2p".to_owned()),
            ..TorrentConfig::default()
        };
        assert_eq!(accepted.validate(), Ok(()));
        assert!(accepted.dht_bootstrap_nodes().is_empty());
    }

    #[test]
    fn a_partial_seeding_table_fills_the_rest_with_defaults() {
        let config: Config = toml::from_str(
            r#"
data_dir = "/tmp/weaver-data"

[seeding]
ratio_goal = 1.0

[torrent]
enabled = true
"#,
        )
        .unwrap();

        assert_eq!(config.seeding, Some(policy(Some(1.0), None)));
        assert_eq!(
            config.torrent,
            TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            }
        );
    }

    #[test]
    fn per_job_series_parses_persisted_values_and_defaults_safely() {
        assert_eq!(PerJobSeries::from_str_or_default("all"), PerJobSeries::All);
        assert_eq!(
            PerJobSeries::from_str_or_default(" OFF "),
            PerJobSeries::Off
        );
        assert_eq!(
            PerJobSeries::from_str_or_default("active"),
            PerJobSeries::Active
        );
        // A typo must not fail startup or silently pick the highest-cardinality
        // setting; it falls back to the default.
        assert_eq!(
            PerJobSeries::from_str_or_default("evrything"),
            PerJobSeries::Active
        );
        assert_eq!(PerJobSeries::default(), PerJobSeries::Active);
        for mode in PerJobSeries::ALL {
            assert_eq!(PerJobSeries::from_str_or_default(mode.as_str()), mode);
        }
        assert_eq!(
            config_with_server().metrics.per_job_series,
            PerJobSeries::Active
        );
    }

    #[test]
    fn validate_rejects_server_download_limits_outside_database_range() {
        let mut config = config_with_server();
        config.servers[0].max_download_speed = MAX_PERSISTED_SERVER_DOWNLOAD_BYTES + 1;
        let errors = config.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("max download speed exceeds database range"))
        );

        config.servers[0].max_download_speed = 0;
        config.servers[0].download_quota.limit_bytes = MAX_PERSISTED_SERVER_DOWNLOAD_BYTES + 1;
        let errors = config.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("download quota limit exceeds database range"))
        );
    }
}
