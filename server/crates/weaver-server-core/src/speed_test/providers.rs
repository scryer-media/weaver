// Public endpoints a bandwidth run can pull bytes from. Every one serves
// arbitrary bytes over a plain HTTPS GET with no token.

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderInfo {
    pub id: &'static str,
    pub name: &'static str,
    // The only sizes the provider serves; empty when it serves any size.
    pub fixed_sizes: &'static [u64],
    pub any_size: bool,
    // The largest single GET an any-size provider serves.
    pub max_size: Option<u64>,
    // Whether the operator picks one server from the provider's list.
    pub needs_server: bool,
}

pub const PROVIDERS: &[ProviderInfo] = &[
    ProviderInfo {
        id: "cloudflare",
        name: "Cloudflare",
        fixed_sizes: &[],
        any_size: true,
        // A larger request is refused with HTTP 403.
        max_size: Some(99_999_999),
        needs_server: false,
    },
    ProviderInfo {
        id: "sabnzbd",
        name: "SABnzbd",
        fixed_sizes: &[100 * MIB],
        any_size: false,
        max_size: None,
        needs_server: false,
    },
    ProviderInfo {
        id: "ovh",
        name: "OVH",
        fixed_sizes: &[100 * MIB, GIB, 10 * GIB],
        any_size: false,
        max_size: None,
        needs_server: false,
    },
    ProviderInfo {
        id: "hetzner",
        name: "Hetzner",
        fixed_sizes: &[100 * MIB, GIB, 10 * GIB],
        any_size: false,
        max_size: None,
        needs_server: false,
    },
    ProviderInfo {
        id: "librespeed",
        name: "LibreSpeed",
        fixed_sizes: &[],
        any_size: true,
        max_size: Some(LIBRESPEED_MAX_CHUNKS * MIB),
        needs_server: true,
    },
];

// The LibreSpeed public server list.
pub const LIBRESPEED_SERVER_LIST: &str = "https://librespeed.org/backend-servers/servers.php";
// LibreSpeed serves its garbage in 1 MiB chunks, at most this many per GET.
const LIBRESPEED_MAX_CHUNKS: u64 = 1024;

pub fn provider(id: &str) -> Option<&'static ProviderInfo> {
    PROVIDERS.iter().find(|provider| provider.id == id)
}

fn size_name(bytes: u64) -> &'static str {
    match bytes {
        b if b == 100 * MIB => "100",
        b if b == GIB => "1",
        _ => "10",
    }
}

// The URL one stream of a run on a fixed-size or any-size provider GETs.
// List providers resolve through `list_server_url`.
pub fn provider_url(provider: &ProviderInfo, bytes: u64) -> Result<reqwest::Url, String> {
    if !provider.any_size && !provider.fixed_sizes.contains(&bytes) {
        let sizes = provider
            .fixed_sizes
            .iter()
            .map(|size| format!("{size}"))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "{} serves only these sizes in bytes: {sizes}",
            provider.name
        ));
    }
    if bytes == 0 {
        return Err("a bandwidth run must ask for at least one byte".into());
    }
    if let Some(max) = provider.max_size
        && bytes > max
    {
        return Err(format!(
            "{} serves at most {max} bytes per stream",
            provider.name
        ));
    }
    let url = match provider.id {
        "cloudflare" => format!("https://speed.cloudflare.com/__down?bytes={bytes}"),
        "sabnzbd" => "https://sabnzbd.org/tests/internetspeed/100MB.bin".into(),
        "ovh" => {
            let name = match size_name(bytes) {
                "100" => "100Mb",
                "1" => "1Gb",
                _ => "10Gb",
            };
            format!("https://proof.ovh.net/files/{name}.dat")
        }
        "hetzner" => {
            let name = match size_name(bytes) {
                "100" => "100MB",
                "1" => "1GB",
                _ => "10GB",
            };
            format!("https://nbg1-speed.hetzner.com/{name}.bin")
        }
        other => return Err(format!("provider {other} needs a server from its list")),
    };
    reqwest::Url::parse(&url).map_err(|error| error.to_string())
}

// One server of a list provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderServer {
    pub id: String,
    pub name: String,
    pub location: Option<String>,
    pub host: String,
    // Where the server's download endpoint lives.
    pub download_url: String,
}

#[derive(serde::Deserialize)]
struct LibreSpeedEntry {
    id: serde_json::Value,
    name: String,
    server: String,
    #[serde(rename = "dlURL")]
    dl_url: String,
    #[serde(default, rename = "sponsorName")]
    sponsor_name: Option<String>,
}

// Parse the LibreSpeed server list. Entries that do not name an HTTPS
// endpoint are left out: a run only fetches over HTTPS.
pub fn parse_librespeed_servers(body: &[u8]) -> Result<Vec<ProviderServer>, String> {
    let entries: Vec<LibreSpeedEntry> = serde_json::from_slice(body)
        .map_err(|error| format!("LibreSpeed server list is not readable: {error}"))?;
    Ok(entries
        .into_iter()
        .filter_map(|entry| {
            let base = if let Some(rest) = entry.server.strip_prefix("//") {
                format!("https://{rest}")
            } else {
                entry.server.clone()
            };
            // LibreSpeed appends dlURL to the server string, so a server
            // without a trailing slash still names a directory.
            let base = if base.ends_with('/') {
                base
            } else {
                format!("{base}/")
            };
            let base = reqwest::Url::parse(&base).ok()?;
            if base.scheme() != "https" {
                return None;
            }
            let download = base.join(entry.dl_url.trim_start_matches('/')).ok()?;
            let id = match entry.id {
                serde_json::Value::String(id) => id,
                other => other.to_string(),
            };
            Some(ProviderServer {
                id,
                name: entry.name,
                location: entry.sponsor_name.filter(|name| !name.is_empty()),
                host: base.host_str()?.to_owned(),
                download_url: download.to_string(),
            })
        })
        .collect())
}

// The URL one stream GETs from a LibreSpeed server, rounded up to whole
// MiB chunks.
pub fn list_server_url(server: &ProviderServer, bytes: u64) -> Result<reqwest::Url, String> {
    let chunks = bytes.div_ceil(MIB).max(1);
    if chunks > LIBRESPEED_MAX_CHUNKS {
        return Err(format!(
            "LibreSpeed serves at most {} bytes per stream",
            LIBRESPEED_MAX_CHUNKS * MIB
        ));
    }
    let mut url = reqwest::Url::parse(&server.download_url).map_err(|e| e.to_string())?;
    url.query_pairs_mut()
        .append_pair("ckSize", &chunks.to_string());
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_size_providers_refuse_other_sizes() {
        let ovh = provider("ovh").unwrap();
        assert_eq!(
            provider_url(ovh, GIB).unwrap().as_str(),
            "https://proof.ovh.net/files/1Gb.dat"
        );
        assert!(provider_url(ovh, 5 * MIB).is_err());
        let cloudflare = provider("cloudflare").unwrap();
        assert_eq!(
            provider_url(cloudflare, 12345).unwrap().as_str(),
            "https://speed.cloudflare.com/__down?bytes=12345"
        );
        assert!(provider_url(cloudflare, 99_999_999).is_ok());
        assert!(provider_url(cloudflare, 100_000_000).is_err());
        assert!(provider_url(provider("librespeed").unwrap(), MIB).is_err());
    }

    #[test]
    fn librespeed_list_entries_resolve_to_https_download_urls() {
        let body = br#"[
            {"id":3,"name":"Somewhere","server":"//speed.example.test/","dlURL":"backend/garbage.php","ulURL":"","pingURL":"","getIpURL":"","sponsorName":"Example"},
            {"id":"4","name":"Plain","server":"http://plain.example.test/","dlURL":"garbage.php"},
            {"id":5,"name":"Nested","server":"https://nested.example.test/backend","dlURL":"garbage.php"}
        ]"#;
        let servers = parse_librespeed_servers(body).unwrap();
        assert_eq!(
            servers,
            vec![
                ProviderServer {
                    id: "3".into(),
                    name: "Somewhere".into(),
                    location: Some("Example".into()),
                    host: "speed.example.test".into(),
                    download_url: "https://speed.example.test/backend/garbage.php".into(),
                },
                ProviderServer {
                    id: "5".into(),
                    name: "Nested".into(),
                    location: None,
                    host: "nested.example.test".into(),
                    download_url: "https://nested.example.test/backend/garbage.php".into(),
                }
            ]
        );
        assert_eq!(
            list_server_url(&servers[0], 3 * MIB + 1).unwrap().as_str(),
            "https://speed.example.test/backend/garbage.php?ckSize=4"
        );
        assert!(list_server_url(&servers[0], 1024 * MIB).is_ok());
        assert!(list_server_url(&servers[0], 1024 * MIB + 1).is_err());
    }
}

#[cfg(test)]
#[path = "providers_live.rs"]
mod live;
