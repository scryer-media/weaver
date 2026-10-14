// Live probe of every public endpoint a speed test reaches. It talks to the
// internet, so it is ignored and runs only when asked for by name:
// cargo nextest run -p weaver-server-core -E 'test(/providers::live/)' --run-ignored all

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;
use crate::speed_test::SabTestSize;
use crate::speed_test::http::{HttpLeg, HttpOptions, fetch_test_nzb, read_capped, routed_get};

// Bytes read from a large body before the probe stops.
const PREFIX: u64 = 4 * MIB;
// LibreSpeed servers probed from the head of the list.
const LIBRESPEED_SERVERS: usize = 3;

struct Row {
    provider: String,
    url: String,
    status: String,
    length: String,
    elapsed: Duration,
    error: Option<String>,
}

fn direct_leg() -> HttpLeg {
    HttpLeg {
        stage: Arc::new(weaver_tunnel::pipe::Egress {
            id: 0,
            binding: weaver_tunnel::egress::SocketEgress::System,
            timeout: Duration::from_secs(30),
        }),
        dns_servers: Vec::new(),
    }
}

// GET the URL the way a bandwidth stream does and read at most `read` bytes.
// `expected` is the size the provider advertises for this URL.
async fn probe(leg: &HttpLeg, provider: &str, url: &reqwest::Url, expected: u64, read: u64) -> Row {
    let started = Instant::now();
    let mut row = Row {
        provider: provider.into(),
        url: url.to_string(),
        status: "-".into(),
        length: "-".into(),
        elapsed: Duration::ZERO,
        error: None,
    };
    let mut routed = match routed_get(leg, url, &HttpOptions::default()).await {
        Ok(routed) => routed,
        Err(error) => {
            row.elapsed = started.elapsed();
            row.error = Some(error);
            return row;
        }
    };
    row.status = routed.response.status().as_u16().to_string();
    let declared = routed.response.content_length();
    // With no declared length, a body of at most 100 MiB is read whole so
    // its size can be checked; a larger one never is.
    let read = if declared.is_none() && expected <= 100 * MIB {
        expected + MIB
    } else {
        read
    };
    let mut observed = 0u64;
    let mut ended = false;
    while observed < read {
        match routed.response.chunk().await {
            Ok(Some(chunk)) => observed += chunk.len() as u64,
            Ok(None) => {
                ended = true;
                break;
            }
            Err(error) => {
                row.error = Some(format!("reading the body failed: {error}"));
                break;
            }
        }
    }
    routed.close().await;
    row.elapsed = started.elapsed();
    row.length = match declared {
        Some(length) => format!("declared {length}, read {observed}"),
        None => format!("no length, read {observed}"),
    };
    if row.error.is_none() {
        let total = if ended { Some(observed) } else { declared };
        row.error = match total {
            Some(total) if total == expected => None,
            Some(total) => Some(format!("advertised {expected} bytes, got {total}")),
            None if observed == 0 => Some("the body is empty".into()),
            None => Some(format!(
                "no declared length; {observed} bytes streamed before the probe stopped"
            )),
        };
    }
    row
}

fn print(rows: &[Row]) {
    println!("| provider | URL | status | length | elapsed | result |");
    println!("|---|---|---|---|---|---|");
    for row in rows {
        println!(
            "| {} | {} | {} | {} | {:.2}s | {} |",
            row.provider,
            row.url,
            row.status,
            row.length,
            row.elapsed.as_secs_f64(),
            row.error
                .as_deref()
                .map_or("pass".into(), |e| format!("FAIL: {e}"))
        );
    }
}

#[tokio::test]
#[ignore = "reaches public speed test endpoints"]
async fn every_public_endpoint_answers() {
    let leg = direct_leg();
    let mut rows = Vec::new();

    for info in PROVIDERS.iter().filter(|info| !info.needs_server) {
        let url = provider_url(info).expect("provider URL");
        rows.push(probe(&leg, info.id, &url, info.download_bytes, PREFIX).await);
    }

    let list = reqwest::Url::parse(LIBRESPEED_SERVER_LIST).unwrap();
    let started = Instant::now();
    let servers = match routed_get(&leg, &list, &HttpOptions::default()).await {
        Ok(routed) => {
            let status = routed.response.status().as_u16().to_string();
            match read_capped(routed, 4 * 1024 * 1024).await {
                Ok(body) => {
                    let parsed = parse_librespeed_servers(&body);
                    rows.push(Row {
                        provider: "librespeed list".into(),
                        url: list.to_string(),
                        status,
                        length: format!(
                            "read {}, {} https servers",
                            body.len(),
                            parsed.as_ref().map_or(0, Vec::len)
                        ),
                        elapsed: started.elapsed(),
                        error: parsed.as_ref().err().cloned(),
                    });
                    parsed.unwrap_or_default()
                }
                Err(error) => {
                    rows.push(Row {
                        provider: "librespeed list".into(),
                        url: list.to_string(),
                        status,
                        length: "-".into(),
                        elapsed: started.elapsed(),
                        error: Some(error),
                    });
                    Vec::new()
                }
            }
        }
        Err(error) => {
            rows.push(Row {
                provider: "librespeed list".into(),
                url: list.to_string(),
                status: "-".into(),
                length: "-".into(),
                elapsed: started.elapsed(),
                error: Some(error),
            });
            Vec::new()
        }
    };
    for server in servers.iter().take(LIBRESPEED_SERVERS) {
        let url = chunked_url(server, 1).expect("LibreSpeed URL");
        let label = format!("librespeed {} ({})", server.id, server.name);
        rows.push(probe(&leg, &label, &url, MIB, 2 * MIB).await);
    }

    for size in [SabTestSize::Mb100, SabTestSize::Gb1, SabTestSize::Gb10] {
        let url = reqwest::Url::parse(size.url()).unwrap();
        let started = Instant::now();
        let fetched = fetch_test_nzb(&leg, &url, &HttpOptions::default()).await;
        rows.push(match fetched {
            Ok(articles) => Row {
                provider: "sabnzbd nzb".into(),
                url: url.to_string(),
                status: "200".into(),
                length: format!(
                    "{} articles, {} declared bytes",
                    articles.len(),
                    articles.iter().map(|a| u64::from(a.bytes)).sum::<u64>()
                ),
                elapsed: started.elapsed(),
                error: None,
            },
            Err(error) => Row {
                provider: "sabnzbd nzb".into(),
                url: url.to_string(),
                status: "-".into(),
                length: "-".into(),
                elapsed: started.elapsed(),
                error: Some(error),
            },
        });
    }

    print(&rows);
    let failed: Vec<_> = rows.iter().filter(|row| row.error.is_some()).collect();
    assert!(failed.is_empty(), "{} endpoint(s) failed", failed.len());
}
