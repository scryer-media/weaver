// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Public endpoints a bandwidth run can pull bytes from and post bytes to.
// Every one serves arbitrary bytes over plain HTTPS; fast.com hands out a
// token its own page carries.


pub const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderInfo {
    pub id: &'static str,
    pub name: &'static str,
    // The host a run reaches first; `None` for a list provider, whose
    // servers each carry their own.
    pub host: Option<&'static str>,
    source: Source,
}

// Where a provider's endpoints come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    // One download on `host`, of a known size, and the path on `host` that
    // takes a post, if any.
    Fixed {
        download: &'static str,
        bytes: u64,
        upload: Option<&'static str>,
    },
    // A server the operator picks from the provider's list, or the nearest.
    List,
    // The targets fast.com hands out for a token read from its own page.
    FastCom,
}

pub const PROVIDERS: &[ProviderInfo] = &[
    ProviderInfo {
        id: "cloudflare",
        name: "Cloudflare",
        host: Some("speed.cloudflare.com"),
        source: Source::Fixed {
            // A larger request is refused with HTTP 403.
            download: "/__down?bytes=99999999",
            bytes: 99_999_999,
            upload: Some("/__up"),
        },
    },
    ProviderInfo {
        id: "sabnzbd",
        name: "SABnzbd",
        host: Some("sabnzbd.org"),
        source: Source::Fixed {
            download: "/tests/internetspeed/100MB.bin",
            bytes: 100 * MIB,
            upload: None,
        },
    },
    ProviderInfo {
        id: "ovh",
        name: "OVH",
        host: Some("proof.ovh.net"),
        source: Source::Fixed {
            download: "/files/10Gb.dat",
            bytes: 10 * GIB,
            upload: None,
        },
    },
    ProviderInfo {
        id: "hetzner",
        name: "Hetzner",
        host: Some("nbg1-speed.hetzner.com"),
        source: Source::Fixed {
            download: "/10GB.bin",
            bytes: 10 * GIB,
            upload: None,
        },
    },
    ProviderInfo {
        id: "librespeed",
        name: "LibreSpeed",
        host: None,
        source: Source::List,
    },
    ProviderInfo {
        id: "fast",
        name: "fast.com",
        host: Some("fast.com"),
        source: Source::FastCom,
    },
];

// The LibreSpeed public server list.
pub const LIBRESPEED_SERVER_LIST: &str = "https://librespeed.org/backend-servers/servers.php";
// LibreSpeed serves its garbage in 1 MiB chunks, at most this many per GET.
const LIBRESPEED_MAX_CHUNKS: u64 = 1024;
// The fast.com page whose script carries the token.
pub const FAST_COM_PAGE: &str = "https://fast.com/";
// The fast.com endpoint that hands out targets for a token.
pub const FAST_COM_API: &str = "https://api.fast.com/netflix/speedtest/v2";
// Targets asked of fast.com.
const FAST_COM_TARGETS: u32 = 5;
// The most of a page, script or answer read while finding the endpoints.

impl ProviderInfo {
    // Whether the operator picks one server from the provider's list.
    pub fn needs_server(&self) -> bool {
        self.source == Source::List
    }

    // Bytes one GET of the provider's download moves, when it is known
    // before the GET.
    pub fn download_bytes(&self) -> Option<u64> {
        match self.source {
            Source::Fixed { bytes, .. } => Some(bytes),
            Source::List => Some(LIBRESPEED_MAX_CHUNKS * MIB),
            Source::FastCom => None,
        }
    }

    // Whether a run finds its endpoints through the route before it holds
    // the queue.
    pub fn discovered(&self) -> bool {
        !matches!(self.source, Source::Fixed { .. })
    }
}

pub fn provider(id: &str) -> Option<&'static ProviderInfo> {
    PROVIDERS.iter().find(|provider| provider.id == id)
}

// Where a run pulls from and posts to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    // Each new connection takes the next, in turn.
    pub download: Vec<url::Url>,
    // What one GET moves; `None` when only the response says.
    pub size: Option<u64>,
    // `None` for a provider that takes no post.
    pub upload: Option<url::Url>,
}

impl Endpoints {
    // A custom URL: pulled from, never posted to.
    pub fn custom(url: url::Url) -> Self {
        Self {
            download: vec![url],
            size: None,
            upload: None,
        }
    }
}

// The endpoints of a fixed provider. List providers resolve through
// `list_server_endpoints`, fast.com through `fast_com_endpoints`.
pub fn fixed_endpoints(provider: &ProviderInfo) -> Result<Endpoints, String> {
    let (
        Some(host),
        Source::Fixed {
            download,
            bytes,
            upload,
        },
    ) = (provider.host, provider.source)
    else {
        return Err(format!(
            "{} finds its endpoints through the route",
            provider.name
        ));
    };
    let on_host = |path: &str| {
        url::Url::parse(&format!("https://{host}{path}")).map_err(|error| error.to_string())
    };
    Ok(Endpoints {
        download: vec![on_host(download)?],
        size: Some(bytes),
        upload: upload.map(on_host).transpose()?,
    })
}

// The URL every GET of a run on a fixed provider asks for.
pub fn provider_url(provider: &ProviderInfo) -> Result<url::Url, String> {
    fixed_endpoints(provider).map(|endpoints| endpoints.download[0].clone())
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
    // Where the server takes a post; `None` for a server that names none.
    pub upload_url: Option<String>,
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
    #[serde(default, rename = "ulURL")]
    ul_url: Option<String>,
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
            let base = url::Url::parse(&base).ok()?;
            if base.scheme() != "https" {
                return None;
            }
            // An absolute URL in the entry would replace the base outright, so
            // every endpoint must stay on the base's own HTTPS origin.
            let on_base = |path: &str| {
                base.join(path.trim_start_matches('/'))
                    .ok()
                    .filter(|url| url.origin() == base.origin())
            };
            let download = on_base(&entry.dl_url)?;
            // The ping and upload endpoints sit beside the download one, on
            // the same base; one a server names none of, or that does not
            // resolve there, is never asked.
            let beside = |path: Option<&str>| {
                path.map(str::trim)
                    .filter(|path| !path.is_empty())
                    .and_then(on_base)
                    .map(|url| url.to_string())
            };
            let ping = beside(entry.ping_url.as_deref());
            let upload = beside(entry.ul_url.as_deref());
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
                upload_url: upload,
                ping_url: ping,
            })
        })
        .collect())
}

// The URL every GET of a run on a LibreSpeed server asks for: the most
// chunks the server serves at once.
pub fn list_server_url(server: &ProviderServer) -> Result<url::Url, String> {
    chunked_url(server, LIBRESPEED_MAX_CHUNKS)
}

// The endpoints of a run on a LibreSpeed server.
pub fn list_server_endpoints(server: &ProviderServer) -> Result<Endpoints, String> {
    Ok(Endpoints {
        download: vec![list_server_url(server)?],
        size: Some(LIBRESPEED_MAX_CHUNKS * MIB),
        upload: server
            .upload_url
            .as_deref()
            .map(url::Url::parse)
            .transpose()
            .map_err(|error| error.to_string())?,
    })
}

// The app script a fast.com page loads: the first script whose file is
// named `app-<hash>.js`, on the page's own origin.
pub fn fast_com_script(page: &url::Url, html: &str) -> Result<url::Url, String> {
    html.split("src=")
        .skip(1)
        .filter_map(|rest| {
            let quote = rest.chars().next().filter(|c| *c == '"' || *c == '\'')?;
            let rest = &rest[1..];
            Some(&rest[..rest.find(quote)?])
        })
        .filter_map(|src| page.join(src).ok())
        .find(|url| {
            url.origin() == page.origin()
                && url
                    .path_segments()
                    .and_then(|mut segments| segments.next_back())
                    .is_some_and(|file| file.starts_with("app-") && file.ends_with(".js"))
        })
        .ok_or_else(|| "the fast.com page loads no app script".to_owned())
}

// The token a fast.com app script carries, as its `token:"..."` literal.
pub fn fast_com_token(script: &str) -> Result<String, String> {
    let rest = script
        .split_once("token:\"")
        .map(|(_, rest)| rest)
        .ok_or("the fast.com script carries no token")?;
    let token = &rest[..rest.find('"').ok_or("the fast.com token is not closed")?];
    if token.is_empty() || !token.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err("the fast.com token is not readable".into());
    }
    Ok(token.to_owned())
}

// Where fast.com hands out targets for `token`.
pub fn fast_com_api_url(api: &url::Url, token: &str) -> url::Url {
    let mut url = api.clone();
    url.query_pairs_mut()
        .append_pair("https", "true")
        .append_pair("token", token)
        .append_pair("urlCount", &FAST_COM_TARGETS.to_string());
    url
}

#[derive(serde::Deserialize)]
struct FastComAnswer {
    targets: Vec<FastComTarget>,
}

#[derive(serde::Deserialize)]
struct FastComTarget {
    url: String,
}

// The targets fast.com handed out. Each is pulled from and posted to; one
// that is not HTTPS is left out.
pub fn fast_com_endpoints(body: &[u8]) -> Result<Endpoints, String> {
    let answer: FastComAnswer = serde_json::from_slice(body)
        .map_err(|error| format!("the fast.com answer is not readable: {error}"))?;
    let download: Vec<url::Url> = answer
        .targets
        .into_iter()
        .filter_map(|target| url::Url::parse(&target.url).ok())
        .filter(|url| url.scheme() == "https")
        .collect();
    let upload = download
        .first()
        .cloned()
        .ok_or("fast.com handed out no HTTPS target")?;
    Ok(Endpoints {
        download,
        size: None,
        upload: Some(upload),
    })
}

pub fn chunked_url(server: &ProviderServer, chunks: u64) -> Result<url::Url, String> {
    let mut url = url::Url::parse(&server.download_url).map_err(|e| e.to_string())?;
    url.query_pairs_mut()
        .append_pair("ckSize", &chunks.to_string());
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fixed_provider_downloads_from_its_host() {
        for info in PROVIDERS.iter().filter(|info| !info.discovered()) {
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
        assert_eq!(asked, cloudflare.download_bytes());
        assert_eq!(
            provider_url(provider("ovh").unwrap()).unwrap().as_str(),
            "https://proof.ovh.net/files/10Gb.dat"
        );
        let librespeed = provider("librespeed").unwrap();
        assert_eq!(librespeed.host, None);
        assert!(provider_url(librespeed).is_err());
        assert!(provider_url(provider("fast").unwrap()).is_err());
    }

    #[test]
    fn each_fixed_provider_names_its_own_upload_endpoint() {
        let uploads: Vec<(&str, Option<String>)> = PROVIDERS
            .iter()
            .filter(|info| !info.discovered())
            .map(|info| {
                let upload = fixed_endpoints(info).unwrap().upload;
                (info.id, upload.map(|url| url.to_string()))
            })
            .collect();
        assert_eq!(
            uploads,
            vec![
                (
                    "cloudflare",
                    Some("https://speed.cloudflare.com/__up".into())
                ),
                ("sabnzbd", None),
                ("ovh", None),
                ("hetzner", None),
            ]
        );
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
                    upload_url: None,
                    ping_url: None,
                },
                ProviderServer {
                    id: "5".into(),
                    name: "Nested".into(),
                    location: None,
                    host: "nested.example.test".into(),
                    download_url: "https://nested.example.test/backend/garbage.php".into(),
                    upload_url: None,
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

    #[test]
    fn librespeed_endpoints_stay_on_the_server_origin() {
        let body = br#"[
            {"id":1,"name":"Cleartext","server":"https://one.example.test/","dlURL":"http://one.example.test/garbage.php"},
            {"id":2,"name":"Elsewhere","server":"https://two.example.test/","dlURL":"https://other.example.test/garbage.php"},
            {"id":3,"name":"Port","server":"https://three.example.test/","dlURL":"https://three.example.test:8443/garbage.php"},
            {"id":4,"name":"Absolute","server":"https://four.example.test/speed/","dlURL":"https://four.example.test/garbage.php","pingURL":"https://four.example.test/empty.php"},
            {"id":5,"name":"PingCleartext","server":"https://five.example.test/","dlURL":"garbage.php","pingURL":"http://five.example.test/empty.php"},
            {"id":6,"name":"PingElsewhere","server":"https://six.example.test/","dlURL":"garbage.php","pingURL":"https://other.example.test/empty.php"},
            {"id":7,"name":"SchemeRelative","server":"https://seven.example.test/","dlURL":"//other.example.test/garbage.php","pingURL":"//other.example.test/empty.php"}
        ]"#;
        let servers: Vec<(String, String, Option<String>)> = parse_librespeed_servers(body)
            .unwrap()
            .into_iter()
            .map(|server| (server.id, server.download_url, server.ping_url))
            .collect();
        assert_eq!(
            servers,
            vec![
                (
                    "4".into(),
                    "https://four.example.test/garbage.php".into(),
                    Some("https://four.example.test/empty.php".into())
                ),
                (
                    "5".into(),
                    "https://five.example.test/garbage.php".into(),
                    None
                ),
                (
                    "6".into(),
                    "https://six.example.test/garbage.php".into(),
                    None
                ),
                (
                    "7".into(),
                    "https://seven.example.test/other.example.test/garbage.php".into(),
                    Some("https://seven.example.test/other.example.test/empty.php".into())
                ),
            ]
        );
    }

    #[test]
    fn a_librespeed_server_posts_beside_its_download() {
        let body = br#"[
            {"id":1,"name":"Backend","server":"https://one.example.test/","dlURL":"backend/garbage.php","ulURL":"backend/empty.php"},
            {"id":2,"name":"Elsewhere","server":"https://two.example.test/","dlURL":"garbage.php","ulURL":"https://other.example.test/empty.php"},
            {"id":3,"name":"Blank","server":"https://three.example.test/","dlURL":"garbage.php","ulURL":""}
        ]"#;
        let servers = parse_librespeed_servers(body).unwrap();
        let uploads: Vec<Option<String>> = servers
            .iter()
            .map(|server| {
                list_server_endpoints(server)
                    .unwrap()
                    .upload
                    .map(|url| url.to_string())
            })
            .collect();
        assert_eq!(
            uploads,
            vec![
                Some("https://one.example.test/backend/empty.php".into()),
                None,
                None
            ]
        );
    }

    #[test]
    fn the_fast_com_page_names_its_app_script_on_its_own_origin() {
        let page = url::Url::parse("https://fast.com/").unwrap();
        let html = r#"<html><head><script src="https://elsewhere.example.test/app-1.js"></script>
            <link rel="stylesheet" href="/style.css"><script src="/vendor-2.js"></script>
            <script src='/app-9f8e7d.js'></script></head></html>"#;
        assert_eq!(
            fast_com_script(&page, html).unwrap().as_str(),
            "https://fast.com/app-9f8e7d.js"
        );
        assert!(fast_com_script(&page, "<script src=\"/main.js\"></script>").is_err());
    }

    #[test]
    fn the_fast_com_token_is_read_from_its_literal() {
        let script = r#"var a=1;e.exports={apiEndpoint:"api.fast.com/netflix/speedtest/v2",token:"YXNkZmFzZGxmbnNk",urlCount:5}"#;
        assert_eq!(fast_com_token(script).unwrap(), "YXNkZmFzZGxmbnNk");
        assert!(fast_com_token("token:\"\"").is_err());
        assert!(fast_com_token("token:\"a b\"").is_err());
        assert!(fast_com_token("nothing here").is_err());
        assert_eq!(
            fast_com_api_url(&url::Url::parse(FAST_COM_API).unwrap(), "abc").as_str(),
            "https://api.fast.com/netflix/speedtest/v2?https=true&token=abc&urlCount=5"
        );
    }

    #[test]
    fn fast_com_targets_are_pulled_from_and_posted_to() {
        let body = br#"{"client":{"ip":"192.0.2.1"},"targets":[
            {"name":"https://one.example.test/speedtest","url":"https://one.example.test/speedtest?c=a&e=1","location":{"city":"X"}},
            {"name":"cleartext","url":"http://two.example.test/speedtest"},
            {"name":"https://three.example.test/speedtest","url":"https://three.example.test/speedtest?c=b"}
        ]}"#;
        let endpoints = fast_com_endpoints(body).unwrap();
        assert_eq!(
            endpoints
                .download
                .iter()
                .map(url::Url::as_str)
                .collect::<Vec<_>>(),
            vec![
                "https://one.example.test/speedtest?c=a&e=1",
                "https://three.example.test/speedtest?c=b"
            ]
        );
        assert_eq!(endpoints.size, None);
        assert_eq!(
            endpoints.upload.as_ref().map(url::Url::as_str),
            Some("https://one.example.test/speedtest?c=a&e=1")
        );
        assert!(fast_com_endpoints(br#"{"targets":[]}"#).is_err());
        assert!(fast_com_endpoints(b"not json").is_err());
    }
}
