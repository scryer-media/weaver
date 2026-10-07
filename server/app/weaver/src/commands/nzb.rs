use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::args::{DEFAULT_REPORT_URL, NzbCommand};

/// Largest NZB, after decompression, the analyzer will read.
const MAX_NZB_BYTES: u64 = 512 * 1024 * 1024;

const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];
const ZSTD_MAGIC: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd];

const URL_ENV: &str = "WEAVER_URL";
const API_KEY_ENV: &str = "WEAVER_API_KEY";
const API_KEY_FILE_ENV: &str = "WEAVER_API_KEY_FILE";

/// The job report query. It asks for the redacted report only, so nothing
/// else about the job crosses the wire.
const JOB_REPORT_QUERY: &str =
    "query JobSupportReport($jobId: Int!) { jobSupportReport(jobId: $jobId) { text json } }";

/// Exit code 0 when a report was printed, 1 when the file could not be read
/// or is not an NZB, or the server could not give the job's report.
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

/// The GraphQL endpoint under a server address that may carry a base path.
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

/// The report out of a GraphQL response, or the server's errors.
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
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    render(bytes, json, now)
}

/// The compression is read from the bytes, so a renamed file still opens.
fn decompress(bytes: Vec<u8>) -> Result<Vec<u8>, String> {
    let source = std::io::Cursor::new(bytes);
    let decoded: Box<dyn Read> = if source.get_ref().starts_with(GZIP_MAGIC) {
        Box::new(flate2::read::GzDecoder::new(source))
    } else if source.get_ref().starts_with(ZSTD_MAGIC) {
        Box::new(zstd::stream::read::Decoder::new(source).map_err(|error| error.to_string())?)
    } else {
        Box::new(source)
    };
    let mut xml = Vec::new();
    decoded
        .take(MAX_NZB_BYTES + 1)
        .read_to_end(&mut xml)
        .map_err(|error| error.to_string())?;
    if xml.len() as u64 > MAX_NZB_BYTES {
        return Err(format!("NZB is larger than {MAX_NZB_BYTES} bytes"));
    }
    Ok(xml)
}

fn render(bytes: Vec<u8>, json: bool, now_epoch_secs: u64) -> Result<String, String> {
    let xml = decompress(bytes)?;
    let (nzb, diagnostics) =
        weaver_nzb::parse_nzb_with_diagnostics(&xml).map_err(|error| error.to_string())?;
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

    use super::{fetch_job_report, graphql_endpoint, render, report_from_response};

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

    /// A one-route server stands in for weaver: the request must carry the
    /// key as a bearer token and the job ID as a variable.
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
