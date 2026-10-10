use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::args::{DEFAULT_REPORT_URL, NzbCommand};

// Largest NZB, after decompression, the analyzer will read.
const MAX_NZB_BYTES: u64 = 512 * 1024 * 1024;

const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];
const ZSTD_MAGIC: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd];

const URL_ENV: &str = "WEAVER_URL";
const API_KEY_ENV: &str = "WEAVER_API_KEY";
const API_KEY_FILE_ENV: &str = "WEAVER_API_KEY_FILE";

// The job report query. It asks for the redacted report only, so nothing
// else about the job crosses the wire.
const JOB_REPORT_QUERY: &str =
    "query JobSupportReport($jobId: Int!) { jobSupportReport(jobId: $jobId) { text json } }";

// Exit code 0 when a report was printed, 1 when the file could not be read
// or is not an NZB, or the server could not give the job's report.
pub(crate) async fn run(command: NzbCommand) -> i32 {
    match command {
        NzbCommand::Analyze { file, json } => match analyze_file(&file, json) {
            Ok(report) => {
                println!("{report}");
                0
            }
            Err(error) => {
                eprintln!("weaver nzb analyze: {}: {error}", file.display());
                1
            }
        },
        NzbCommand::Report { job_id, url, json } => {
            let base = url
                .or_else(|| {
                    std::env::var(URL_ENV)
                        .ok()
                        .filter(|value| !value.is_empty())
                })
                .unwrap_or_else(|| DEFAULT_REPORT_URL.to_string());
            let result = match api_key_from_env() {
                Ok(api_key) => fetch_job_report(&base, api_key.as_deref(), job_id, json).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(report) => {
                    println!("{report}");
                    0
                }
                Err(error) => {
                    eprintln!("weaver nzb report: job {job_id}: {error}");
                    1
                }
            }
        }
    }
}

fn api_key_from_env() -> Result<Option<String>, String> {
    if let Some(key) = std::env::var(API_KEY_ENV)
        .ok()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
    {
        return Ok(Some(key));
    }
    let Some(path) = std::env::var_os(API_KEY_FILE_ENV).filter(|path| !path.is_empty()) else {
        return Ok(None);
    };
    let key =
        std::fs::read_to_string(&path).map_err(|error| format!("{API_KEY_FILE_ENV}: {error}"))?;
    let key = key.trim();
    Ok((!key.is_empty()).then(|| key.to_string()))
}

// The GraphQL endpoint under a server address that may carry a base path.
fn graphql_endpoint(base: &str) -> String {
    format!("{}/graphql", base.trim_end_matches('/'))
}

async fn fetch_job_report(
    base: &str,
    api_key: Option<&str>,
    job_id: u32,
    json: bool,
) -> Result<String, String> {
    let endpoint = graphql_endpoint(base);
    let mut request = reqwest::Client::new()
        .post(&endpoint)
        .json(&serde_json::json!({
            "query": JOB_REPORT_QUERY,
            "variables": { "jobId": job_id },
        }));
    if let Some(key) = api_key {
        request = request.bearer_auth(key);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("{endpoint}: {error}"))?;
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return Err(format!(
            "{endpoint} refused the request ({status}); set {API_KEY_ENV} or {API_KEY_FILE_ENV}"
        ));
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|error| format!("{endpoint} ({status}): {error}"))?;
    report_from_response(&body, json)
}

// The report out of a GraphQL response, or the server's errors.
fn report_from_response(body: &serde_json::Value, json: bool) -> Result<String, String> {
    if let Some(errors) = body["errors"]
        .as_array()
        .filter(|errors| !errors.is_empty())
    {
        let messages: Vec<&str> = errors
            .iter()
            .map(|error| error["message"].as_str().unwrap_or("unknown error"))
            .collect();
        return Err(messages.join("; "));
    }
    let report = &body["data"]["jobSupportReport"];
    let field = if json { "json" } else { "text" };
    report[field]
        .as_str()
        .map(|value| value.trim_end().to_string())
        .ok_or_else(|| "the server's response carried no report".to_string())
}

fn analyze_file(path: &Path, json: bool) -> Result<String, String> {
    let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let xml = read_nzb(file, MAX_NZB_BYTES)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    render_xml(&xml, json, now)
}

fn too_large(limit: u64) -> String {
    format!("NZB is larger than {limit} bytes")
}

// Reads an NZB as a stream, holding no more than `limit` bytes of the file
// or of its decompressed XML. The compression is read from the bytes, so a
// renamed file still opens.
fn read_nzb(source: impl Read, limit: u64) -> Result<Vec<u8>, String> {
    let mut raw = BufReader::new(source.take(limit + 1));
    let decoded = decompress(&mut raw, limit);
    // A cut-off compressed stream fails to decode; the size is the real cause.
    if raw.get_ref().limit() == 0 {
        return Err(too_large(limit));
    }
    decoded
}

fn decompress(raw: &mut impl BufRead, limit: u64) -> Result<Vec<u8>, String> {
    let head = raw.fill_buf().map_err(|error| error.to_string())?;
    let (gzip, zstd) = (head.starts_with(GZIP_MAGIC), head.starts_with(ZSTD_MAGIC));
    let decoded: Box<dyn Read + '_> = if gzip {
        Box::new(flate2::bufread::GzDecoder::new(raw))
    } else if zstd {
        Box::new(zstd::stream::read::Decoder::with_buffer(raw).map_err(|error| error.to_string())?)
    } else {
        Box::new(raw)
    };
    let mut xml = Vec::new();
    decoded
        .take(limit + 1)
        .read_to_end(&mut xml)
        .map_err(|error| error.to_string())?;
    if xml.len() as u64 > limit {
        return Err(too_large(limit));
    }
    Ok(xml)
}

#[cfg(test)]
fn render(bytes: Vec<u8>, json: bool, now_epoch_secs: u64) -> Result<String, String> {
    let xml = read_nzb(bytes.as_slice(), MAX_NZB_BYTES)?;
    render_xml(&xml, json, now_epoch_secs)
}

fn render_xml(xml: &[u8], json: bool, now_epoch_secs: u64) -> Result<String, String> {
    let (nzb, diagnostics) =
        weaver_nzb::parse_nzb_with_diagnostics(xml).map_err(|error| error.to_string())?;
    let report = weaver_nzb::analysis::analyze(&nzb, &diagnostics, now_epoch_secs)
        .with_weaver_version(env!("CARGO_PKG_VERSION"));
    Ok(if json {
        report.to_json()
    } else {
        report.to_text().trim_end().to_string()
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::{fetch_job_report, graphql_endpoint, read_nzb, render, report_from_response};

    const NOW: u64 = 1_790_000_000;

    fn sample_nzb() -> Vec<u8> {
        let mut xml = String::from(
            "<?xml version=\"1.0\"?>\n<nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n",
        );
        for (index, name) in ["Quiet.Harbor.part1.rar", "Quiet.Harbor.part2.rar"]
            .iter()
            .enumerate()
        {
            xml.push_str(&format!(
                "<file poster=\"someone@cli.invalid\" date=\"1789000000\" \
                 subject=\"[{n}/2] &quot;{name}&quot; yEnc (1/1)\">\
                 <groups><group>alt.binaries.cli</group></groups>\
                 <segments><segment bytes=\"700000\" number=\"1\">p{n}@cli.invalid</segment>\
                 </segments></file>\n",
                n = index + 1,
            ));
        }
        xml.push_str("</nzb>\n");
        xml.into_bytes()
    }

    #[test]
    fn text_report_names_nothing_from_the_nzb() {
        let text = render(sample_nzb(), false, NOW).unwrap();
        assert!(text.contains(env!("CARGO_PKG_VERSION")));
        for piece in ["Quiet", "Harbor", "cli.invalid", "alt.binaries"] {
            assert!(!text.contains(piece), "{piece} leaked into:\n{text}");
        }
    }

    #[test]
    fn gzipped_and_plain_nzbs_give_the_same_json() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&sample_nzb()).unwrap();
        let gzipped = encoder.finish().unwrap();
        let plain = render(sample_nzb(), true, NOW).unwrap();
        assert_eq!(render(gzipped, true, NOW).unwrap(), plain);
        assert!(serde_json::from_str::<serde_json::Value>(&plain).is_ok());
    }

    #[test]
    fn an_oversized_nzb_is_refused_while_it_is_read() {
        const LIMIT: u64 = 1024;
        let too_large = Err(super::too_large(LIMIT));

        // An endless source ends at the cap instead of being buffered whole.
        assert_eq!(read_nzb(std::io::repeat(b'<'), LIMIT), too_large);

        let exact = vec![b'<'; LIMIT as usize];
        assert_eq!(read_nzb(exact.as_slice(), LIMIT), Ok(exact.clone()));
        let over = vec![b'<'; LIMIT as usize + 1];
        assert_eq!(read_nzb(over.as_slice(), LIMIT), too_large);

        // A small compressed file whose XML is over the cap.
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        encoder.write_all(&vec![b'<'; 64 * LIMIT as usize]).unwrap();
        let bomb = encoder.finish().unwrap();
        assert!((bomb.len() as u64) < LIMIT);
        assert_eq!(read_nzb(bomb.as_slice(), LIMIT), too_large);
        let bomb = zstd::encode_all(vec![b'<'; 64 * LIMIT as usize].as_slice(), 19).unwrap();
        assert!((bomb.len() as u64) < LIMIT);
        assert_eq!(read_nzb(bomb.as_slice(), LIMIT), too_large);

        // A compressed file that is itself over the cap.
        let mut state = 0x9e37_79b9_u32;
        let noise: Vec<u8> = (0..4 * LIMIT)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&noise).unwrap();
        let large = encoder.finish().unwrap();
        assert!(large.len() as u64 > LIMIT);
        assert_eq!(read_nzb(large.as_slice(), LIMIT), too_large);
    }

    #[test]
    fn a_file_that_is_not_an_nzb_is_an_error() {
        assert!(render(b"not an nzb".to_vec(), false, NOW).is_err());
    }

    #[test]
    fn the_endpoint_keeps_a_base_path() {
        assert_eq!(
            graphql_endpoint("http://127.0.0.1:9090"),
            "http://127.0.0.1:9090/graphql"
        );
        assert_eq!(
            graphql_endpoint("http://192.0.2.10:9090/weaver/"),
            "http://192.0.2.10:9090/weaver/graphql"
        );
    }

    #[test]
    fn a_response_gives_the_report_or_the_server_errors() {
        let body = serde_json::json!({
            "data": { "jobSupportReport": { "text": "report text\n", "json": "{}" } }
        });
        assert_eq!(report_from_response(&body, false).unwrap(), "report text");
        assert_eq!(report_from_response(&body, true).unwrap(), "{}");
        let body = serde_json::json!({
            "data": null,
            "errors": [{ "message": "job 9 not found" }]
        });
        assert_eq!(
            report_from_response(&body, false).unwrap_err(),
            "job 9 not found"
        );
        assert!(report_from_response(&serde_json::json!({}), false).is_err());
    }

    // A one-route server stands in for weaver: the request must carry the
    // key as a bearer token and the job ID as a variable.
    #[tokio::test]
    async fn the_report_is_fetched_with_the_key_and_the_job_id() {
        use axum::{Json, Router, http::HeaderMap, routing::post};

        async fn graphql(
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            let authorized = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                == Some("Bearer test-key");
            let job_id = body["variables"]["jobId"].as_u64();
            Json(if authorized && job_id == Some(42) {
                serde_json::json!({
                    "data": { "jobSupportReport": { "text": "job (history)\n", "json": "{\"job\":{}}" } }
                })
            } else {
                serde_json::json!({ "errors": [{ "message": "rejected" }] })
            })
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/base/graphql", post(graphql)),
            )
            .await
            .unwrap();
        });
        let base = format!("http://{address}/base/");

        assert_eq!(
            fetch_job_report(&base, Some("test-key"), 42, false)
                .await
                .unwrap(),
            "job (history)"
        );
        assert_eq!(
            fetch_job_report(&base, Some("test-key"), 42, true)
                .await
                .unwrap(),
            "{\"job\":{}}"
        );
        assert_eq!(
            fetch_job_report(&base, Some("wrong"), 42, false)
                .await
                .unwrap_err(),
            "rejected"
        );
        server.abort();
    }
}
