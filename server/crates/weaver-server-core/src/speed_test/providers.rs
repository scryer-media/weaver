// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Public endpoints a bandwidth run can pull bytes from. Every one serves
// arbitrary bytes over a plain HTTPS GET with no token.

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderInfo {
    pub id: &'static str,
    pub name: &'static str,
    // The host a run downloads from; `None` for a list provider, whose
    // servers each carry their own.
    pub host: Option<&'static str>,
    // Where the provider's largest single download lives on `host`.
    path: &'static str,
    // Bytes one GET of that download moves.
    pub download_bytes: u64,
    // Whether the operator picks one server from the provider's list.
    pub needs_server: bool,
}

pub const PROVIDERS: &[ProviderInfo] = &[
    ProviderInfo {
        id: "cloudflare",
        name: "Cloudflare",
        host: Some("speed.cloudflare.com"),
        // A larger request is refused with HTTP 403.
        path: "/__down?bytes=99999999",
        download_bytes: 99_999_999,
        needs_server: false,
    },
    ProviderInfo {
        id: "sabnzbd",
        name: "SABnzbd",
        host: Some("sabnzbd.org"),
        path: "/tests/internetspeed/100MB.bin",
        download_bytes: 100 * MIB,
        needs_server: false,
    },
    ProviderInfo {
        id: "ovh",
        name: "OVH",
        host: Some("proof.ovh.net"),
        path: "/files/10Gb.dat",
        download_bytes: 10 * GIB,
        needs_server: false,
    },
    ProviderInfo {
        id: "hetzner",
        name: "Hetzner",
        host: Some("nbg1-speed.hetzner.com"),
        path: "/10GB.bin",
        download_bytes: 10 * GIB,
        needs_server: false,
    },
    ProviderInfo {
        id: "librespeed",
        name: "LibreSpeed",
        host: None,
        path: "",
        download_bytes: LIBRESPEED_MAX_CHUNKS * MIB,
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

// The URL every GET of a run on a fixed provider asks for. List providers
// resolve through `list_server_url`.
pub fn provider_url(provider: &ProviderInfo) -> Result<reqwest::Url, String> {
    let Some(host) = provider.host else {
        return Err(format!("{} takes its server from a list", provider.name));
    };
    reqwest::Url::parse(&format!("https://{host}{}", provider.path))
        .map_err(|error| error.to_string())
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
    // Where the server's ping endpoint lives; `None` for a server that names
    // none, which is never pinged.
    pub ping_url: Option<String>,
}

#[derive(serde::Deserialize)]
struct LibreSpeedEntry {
    id: serde_json::Value,
    name: String,
    server: String,
    #[serde(rename = "dlURL")]
    dl_url: String,
    #[serde(default, rename = "pingURL")]
    ping_url: Option<String>,
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
            // The ping endpoint sits beside the download one, on the same
            // base; a server that names none, or one that does not resolve,
            // is listed but never pinged.
            let ping = entry
                .ping_url
                .as_deref()
                .map(str::trim)
                .filter(|ping| !ping.is_empty())
                .and_then(|ping| base.join(ping.trim_start_matches('/')).ok())
                .map(|ping| ping.to_string());
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
                ping_url: ping,
            })
        })
        .collect())
}

// The URL every GET of a run on a LibreSpeed server asks for: the most
// chunks the server serves at once.
pub fn list_server_url(server: &ProviderServer) -> Result<reqwest::Url, String> {
    chunked_url(server, LIBRESPEED_MAX_CHUNKS)
}

fn chunked_url(server: &ProviderServer, chunks: u64) -> Result<reqwest::Url, String> {
    let mut url = reqwest::Url::parse(&server.download_url).map_err(|e| e.to_string())?;
    url.query_pairs_mut()
        .append_pair("ckSize", &chunks.to_string());
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fixed_provider_downloads_from_its_host() {
        for info in PROVIDERS.iter().filter(|info| !info.needs_server) {
            let url = provider_url(info).unwrap();
            assert_eq!(url.scheme(), "https");
            assert_eq!(url.host_str(), info.host, "{}", info.id);
        }
        let cloudflare = provider("cloudflare").unwrap();
        assert_eq!(
            provider_url(cloudflare).unwrap().as_str(),
            "https://speed.cloudflare.com/__down?bytes=99999999"
        );
        let asked = provider_url(cloudflare)
            .unwrap()
            .query_pairs()
            .find(|(name, _)| name == "bytes")
            .map(|(_, value)| value.parse::<u64>().unwrap());
        assert_eq!(asked, Some(cloudflare.download_bytes));
        assert_eq!(
            provider_url(provider("ovh").unwrap()).unwrap().as_str(),
            "https://proof.ovh.net/files/10Gb.dat"
        );
        let librespeed = provider("librespeed").unwrap();
        assert_eq!(librespeed.host, None);
        assert!(provider_url(librespeed).is_err());
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
                    ping_url: None,
                },
                ProviderServer {
                    id: "5".into(),
                    name: "Nested".into(),
                    location: None,
                    host: "nested.example.test".into(),
                    download_url: "https://nested.example.test/backend/garbage.php".into(),
                    ping_url: None,
                }
            ]
        );
        assert_eq!(
            list_server_url(&servers[0]).unwrap().as_str(),
            "https://speed.example.test/backend/garbage.php?ckSize=1024"
        );
    }

    #[test]
    fn librespeed_ping_urls_resolve_against_the_server_base() {
        let body = br#"[
            {"id":1,"name":"Root","server":"//root.example.test/","dlURL":"garbage.php","pingURL":"empty.php"},
            {"id":2,"name":"Backend","server":"https://backend.example.test/speed","dlURL":"backend/garbage.php","pingURL":"backend/empty.php"},
            {"id":3,"name":"Rooted","server":"https://rooted.example.test/speed/","dlURL":"garbage.php","pingURL":"/empty.php"},
            {"id":4,"name":"Blank","server":"https://blank.example.test/","dlURL":"garbage.php","pingURL":"  "},
            {"id":5,"name":"Absent","server":"https://absent.example.test/","dlURL":"garbage.php"},
            {"id":6,"name":"Null","server":"https://null.example.test/","dlURL":"garbage.php","pingURL":null}
        ]"#;
        let pings: Vec<(String, Option<String>)> = parse_librespeed_servers(body)
            .unwrap()
            .into_iter()
            .map(|server| (server.id, server.ping_url))
            .collect();
        assert_eq!(
            pings,
            vec![
                (
                    "1".into(),
                    Some("https://root.example.test/empty.php".into())
                ),
                (
                    "2".into(),
                    Some("https://backend.example.test/speed/backend/empty.php".into())
                ),
                (
                    "3".into(),
                    Some("https://rooted.example.test/speed/empty.php".into())
                ),
                ("4".into(), None),
                ("5".into(), None),
                ("6".into(), None),
            ]
        );
    }
}

#[cfg(test)]
#[path = "providers_live.rs"]
mod live;
