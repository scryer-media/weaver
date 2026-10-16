// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;
use async_graphql::{Request, UploadValue, Variables};
use common::{TestHarness, assert_no_errors, response_data};
use std::sync::atomic::Ordering;
use weaver_server_api::auth::{CallerIdentity, CallerScope};
use weaver_server_core::proxies::ProxyRuntime;
use weaver_server_core::speed_test::fixture::{
    FixtureArticle, HttpFixture, NntpFixture, loopback_tls, nzb,
};
use weaver_server_core::speed_test::{SpeedTestService, SpeedTestStatus};

async fn harness() -> TestHarness {
    let h = TestHarness::new().await;
    h.handle.set_proxy_runtime(
        ProxyRuntime::new(h.db.clone(), tokio::runtime::Handle::current()).unwrap(),
    );
    h
}

// The one error a refused mutation answers with, as (message, code).
fn refusal(response: &async_graphql::Response) -> (String, String) {
    assert_eq!(response.errors.len(), 1, "{:?}", response.errors);
    let error = serde_json::to_value(&response.errors[0]).unwrap();
    (
        error["message"].as_str().unwrap().to_owned(),
        error["extensions"]["code"]
            .as_str()
            .unwrap_or("")
            .to_owned(),
    )
}

async fn add_server(h: &TestHarness, nntp: &NntpFixture) -> u64 {
    let response = h
        .execute(&format!(
            r#"mutation{{addServer(input:{{host:"127.0.0.1",port:{},tls:false,connections:2,active:true}}){{id}}}}"#,
            nntp.addr.port()
        ))
        .await;
    assert_no_errors(&response);
    let id = response_data(&response)["addServer"]["id"]
        .as_u64()
        .unwrap();
    // The mock scheduler does not install a download pool. Supply the live
    // membership and loan budget required by a speed test explicitly.
    h.handle
        .set_nntp_pool(std::sync::Arc::new(weaver_nntp::pool::NntpPool::new(
            weaver_nntp::pool::PoolConfig {
                servers: vec![weaver_nntp::pool::ServerPoolConfig {
                    stable_id: weaver_nntp::transfer::StableServerId(id as u32),
                    max_connections: 2,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )));
    id
}

const DIRECT: &str = "{legs:[{egressId:0,weight:100,path:{direct:true}}]}";

const RUN_SHAPE: &str = "id consumer consumerName kind request{bandwidth{providerId server customUrl} usenet{sabSize nzbUrl nzbFilename nzbRunId connections maxDurationSeconds}} status phase startedAt finishedAt failure{code message params} tested{legs{position egressId weight down downReason target path{kind}} failover overrides{downLegs downRungs{leg rung} downProxies downEgresses} names{kind id name}} result{bytes elapsedMs averageBps peakBps legs{position bytes connections sourceAddresses} connections{leg connectMs bytes error sourceAddress path{egressId rung poolId memberId proxyIds}} usenet{articlesRequested delivered missing failed declaredBytes decodedBytes pipelining tls tlsCipherSuite} bandwidth{providerId server url connections} samples{atMs leg bytes decodedBytes}}";

async fn start(h: &TestHarness, input: &str) -> serde_json::Value {
    let response = h
        .execute(&format!(
            "mutation{{startSpeedTest(input:{input}){{{RUN_SHAPE}}}}}"
        ))
        .await;
    assert_no_errors(&response);
    response_data(&response)["startSpeedTest"].clone()
}

// Start a run whose `usenet` input carries `upload` as its `nzbUpload`,
// beside `usenet`'s other fields.
async fn start_with_upload(
    h: &TestHarness,
    server: u64,
    usenet: serde_json::Value,
    upload: &[u8],
) -> async_graphql::Response {
    let mut usenet = usenet;
    usenet["nzbUpload"] = serde_json::Value::Null;
    let mut request = Request::new(format!(
        "mutation Start($input: SpeedTestInput!){{startSpeedTest(input:$input){{{RUN_SHAPE}}}}}"
    ))
    .data(CallerScope::Local)
    .data(CallerIdentity::Local([7; 32]))
    .variables(Variables::from_json(serde_json::json!({
        "input": {
            "kind": "SERVER",
            "id": server,
            "route": {"legs": [{"egressId": 0, "weight": 100, "path": {"direct": true}}]},
            "usenet": usenet,
        }
    })));
    request.set_upload(
        "variables.input.usenet.nzbUpload",
        UploadValue {
            filename: "test.nzb".into(),
            content_type: Some("application/x-nzb".into()),
            content: upload.to_vec().into(),
        },
    );
    h.schema.execute(request).await
}

async fn run(h: &TestHarness, id: u64) -> serde_json::Value {
    let response = h
        .execute(&format!("{{speedTestRun(id:{id}){{{RUN_SHAPE}}}}}"))
        .await;
    assert_no_errors(&response);
    response_data(&response)["speedTestRun"].clone()
}

async fn wait_until(
    h: &TestHarness,
    id: u64,
    done: impl Fn(&weaver_server_core::speed_test::SpeedTestRun) -> bool,
) {
    let service = h
        .handle
        .speed_test_service()
        .expect("the schema holds the service");
    let mut revision = service.subscribe();
    while !done(&service.run(id as u32).unwrap()) {
        revision.changed().await.unwrap();
    }
}

async fn finished(h: &TestHarness, id: u64) -> serde_json::Value {
    wait_until(h, id, |run| run.status != SpeedTestStatus::Running).await;
    run(h, id).await
}

#[tokio::test]
async fn providers_are_listed_with_their_hosts() {
    let h = harness().await;
    let response = h
        .execute("{bandwidthProviders{id name host needsServer}}")
        .await;
    assert_no_errors(&response);
    let providers = response_data(&response)["bandwidthProviders"].clone();
    let ids: Vec<&str> = providers
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        ["cloudflare", "sabnzbd", "ovh", "hetzner", "librespeed"]
    );
    let hosts: serde_json::Value = providers
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["host"].clone())
        .collect();
    assert_eq!(
        hosts,
        serde_json::json!([
            "speed.cloudflare.com",
            "sabnzbd.org",
            "proof.ovh.net",
            "nbg1-speed.hetzner.com",
            null
        ])
    );
    assert_eq!(providers[4]["needsServer"], true);
    assert_eq!(providers[0]["needsServer"], false);
}

#[tokio::test]
async fn the_removed_size_stream_and_duration_fields_are_refused_by_the_schema() {
    let h = harness().await;
    for (body, field) in [
        (
            r#"bandwidth:{customUrl:"https://127.0.0.1/blob",bytes:1000}"#,
            "bytes",
        ),
        (
            r#"bandwidth:{customUrl:"https://127.0.0.1/blob",streams:3}"#,
            "streams",
        ),
        (
            r#"maxDurationSeconds:30,bandwidth:{customUrl:"https://127.0.0.1/blob"}"#,
            "maxDurationSeconds",
        ),
    ] {
        let response = h
            .execute(&format!(
                "mutation{{startSpeedTest(input:{{kind:RSS,id:1,route:{DIRECT},{body}}}){{id}}}}"
            ))
            .await;
        assert_eq!(response.errors.len(), 1, "{body}");
        assert!(
            response.errors[0]
                .message
                .contains(&format!("unknown field \"{field}\"")),
            "{}",
            response.errors[0].message
        );
    }
    for query in [
        "{bandwidthProviders{fixedSizes}}",
        "{bandwidthProviders{anySize}}",
        "{bandwidthProviders{maxBytes}}",
        "{speedTestRuns{request{maxDurationSeconds}}}",
        "{speedTestRuns{request{bandwidth{bytes}}}}",
        "{speedTestRuns{result{bandwidth{streams}}}}",
    ] {
        let response = h.execute(query).await;
        assert_eq!(response.errors.len(), 1, "{query}");
        assert!(
            response.errors[0].message.starts_with("Unknown field"),
            "{}",
            response.errors[0].message
        );
    }
}

#[tokio::test]
async fn a_run_takes_exactly_one_of_bandwidth_and_usenet() {
    let h = harness().await;
    for body in [
        "",
        r#"bandwidth:{customUrl:"https://127.0.0.1/blob"},usenet:{sabSize:MB_100}"#,
    ] {
        let response = h
            .execute(&format!(
                "mutation{{startSpeedTest(input:{{kind:SERVER,id:1,route:{DIRECT}{}{body}}}){{id}}}}",
                if body.is_empty() { "" } else { "," }
            ))
            .await;
        let (message, _) = refusal(&response);
        assert_eq!(
            message,
            "a speed test takes exactly one of bandwidth and usenet"
        );
    }
}

#[tokio::test]
async fn usenet_on_a_feed_and_an_all_down_route_are_refused_by_code() {
    let h = harness().await;
    let response = h
        .execute(&format!(
            "mutation{{startSpeedTest(input:{{kind:RSS,id:1,route:{DIRECT},usenet:{{sabSize:MB_100}}}}){{id}}}}"
        ))
        .await;
    assert_eq!(
        refusal(&response),
        (
            "a Usenet speed test runs on a server; a feed can run a bandwidth test only".into(),
            "SPEED_TEST_USENET_NEEDS_SERVER".into()
        )
    );
    let response = h
        .execute(&format!(
            r#"mutation{{startSpeedTest(input:{{kind:RSS,id:1,route:{DIRECT},overrides:{{downLegs:[0]}},bandwidth:{{customUrl:"https://127.0.0.1/blob"}}}}){{id}}}}"#
        ))
        .await;
    assert_eq!(refusal(&response).1, "SPEED_TEST_LEG_DOWN_ALL");
}

#[tokio::test]
async fn a_usenet_run_books_raw_bytes_counts_missing_articles_and_gives_the_queue_back() {
    let h = harness().await;
    let nntp = NntpFixture::start(
        vec![
            FixtureArticle::patterned("one@fixture", 30_000),
            FixtureArticle::patterned("two@fixture", 30_000),
        ],
        None,
        None,
    )
    .await;
    let http = HttpFixture::start(
        nzb(&[
            ("one@fixture", 31_000),
            ("gone@fixture", 31_000),
            ("two@fixture", 31_000),
        ]),
        0,
        None,
    )
    .await;
    let server = add_server(&h, &nntp).await;
    let before = h.metrics.bytes_downloaded.load(Ordering::Acquire);

    let started = start(
        &h,
        &format!(
            r#"{{kind:SERVER,id:{server},route:{DIRECT},usenet:{{nzbUrl:"http://{}/test.nzb"}}}}"#,
            http.addr
        ),
    )
    .await;
    assert_eq!(started["status"], "RUNNING");
    assert_eq!(started["consumer"], format!("server:{server}"));
    assert_eq!(started["kind"], "USENET");
    let id = started["id"].as_u64().unwrap();
    let done = finished(&h, id).await;

    assert_eq!(done["status"], "COMPLETED", "{done}");
    assert_eq!(done["phase"], serde_json::Value::Null);
    assert!(done["finishedAt"].as_f64().unwrap() >= done["startedAt"].as_f64().unwrap());
    assert_eq!(done["tested"]["legs"][0]["target"], 2);
    assert_eq!(
        done["request"],
        serde_json::json!({
            "bandwidth": null,
            "usenet": {
                "sabSize": null,
                "nzbUrl": format!("http://{}/test.nzb", http.addr),
                "nzbFilename": null,
                "nzbRunId": null,
                "connections": null,
                "maxDurationSeconds": 120,
            },
        })
    );
    let result = &done["result"];
    let usenet = &result["usenet"];
    assert_eq!(usenet["articlesRequested"], 3);
    assert_eq!(usenet["delivered"], 2);
    assert_eq!(usenet["missing"], 1);
    assert_eq!(usenet["failed"], 0);
    assert_eq!(usenet["decodedBytes"], 60_000.0);
    let decoded_bins: f64 = result["samples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|sample| sample["decodedBytes"].as_f64().unwrap())
        .sum();
    assert_eq!(decoded_bins, 60_000.0);
    assert_eq!(usenet["declaredBytes"], 93_000.0);
    assert_eq!(usenet["tls"], false);
    assert_eq!(usenet["tlsCipherSuite"], serde_json::Value::Null);
    let booked = h.metrics.bytes_downloaded.load(Ordering::Acquire) - before;
    assert_eq!(result["bytes"].as_f64().unwrap(), booked as f64);
    assert!(booked > 60_000);
    assert!(!h.handle.is_globally_paused());

    let response = h.execute("{speedTestRuns{id status}}").await;
    assert_no_errors(&response);
    assert_eq!(response_data(&response)["speedTestRuns"][0]["id"], id);
}

#[tokio::test]
async fn a_usenet_run_takes_an_uploaded_nzb_as_its_one_source() {
    let h = harness().await;
    let nntp = NntpFixture::start(
        vec![
            FixtureArticle::patterned("one@fixture", 30_000),
            FixtureArticle::patterned("two@fixture", 30_000),
        ],
        None,
        None,
    )
    .await;
    let server = add_server(&h, &nntp).await;
    let upload = nzb(&[("one@fixture", 31_000), ("two@fixture", 31_000)]);

    for (usenet, message) in [
        (
            serde_json::json!({"sabSize": "MB_100"}),
            "a Usenet test takes exactly one of sabSize, nzbUrl, nzbUpload and nzbRunId",
        ),
        (
            serde_json::json!({"nzbRunId": 1}),
            "a Usenet test takes exactly one of sabSize, nzbUrl, nzbUpload and nzbRunId",
        ),
    ] {
        let response = start_with_upload(&h, server, usenet, upload.as_bytes()).await;
        assert_eq!(refusal(&response), (message.into(), "INVALID_INPUT".into()));
    }
    let response = h
        .execute(&format!(
            "mutation{{startSpeedTest(input:{{kind:SERVER,id:{server},route:{DIRECT},usenet:{{}}}}){{id}}}}"
        ))
        .await;
    assert_eq!(refusal(&response).1, "INVALID_INPUT");
    let response = start_with_upload(&h, server, serde_json::json!({}), b"not an nzb").await;
    let (message, code) = refusal(&response);
    assert!(
        message.starts_with("the test NZB is not readable: "),
        "{message}"
    );
    assert_eq!(code, "INVALID_INPUT");

    let response = start_with_upload(&h, server, serde_json::json!({}), upload.as_bytes()).await;
    assert_no_errors(&response);
    let id = response_data(&response)["startSpeedTest"]["id"]
        .as_u64()
        .unwrap();
    let done = finished(&h, id).await;

    assert_eq!(done["status"], "COMPLETED", "{done}");
    assert_eq!(
        done["request"]["usenet"],
        serde_json::json!({
            "sabSize": null,
            "nzbUrl": null,
            "nzbFilename": "test.nzb",
            "nzbRunId": null,
            "connections": null,
            "maxDurationSeconds": 120,
        })
    );
    assert_eq!(done["result"]["usenet"]["delivered"], 2);
    assert_eq!(done["result"]["usenet"]["decodedBytes"], 60_000.0);
}

#[tokio::test]
async fn a_held_run_s_uploaded_nzb_runs_again_until_the_run_is_let_go() {
    let h = harness().await;
    let nntp = NntpFixture::start(
        vec![FixtureArticle::patterned("one@fixture", 20_000)],
        None,
        None,
    )
    .await;
    let server = add_server(&h, &nntp).await;
    let response = start_with_upload(
        &h,
        server,
        serde_json::json!({}),
        nzb(&[("one@fixture", 21_000)]).as_bytes(),
    )
    .await;
    assert_no_errors(&response);
    let uploaded = response_data(&response)["startSpeedTest"]["id"]
        .as_u64()
        .unwrap();
    assert_eq!(finished(&h, uploaded).await["status"], "COMPLETED");

    // A run started from a held run holds the same NZB, so it can be run
    // again in turn.
    let mut from = uploaded;
    for _ in 0..2 {
        let again = start(
            &h,
            &format!("{{kind:SERVER,id:{server},route:{DIRECT},usenet:{{nzbRunId:{from}}}}}"),
        )
        .await;
        let done = finished(&h, again["id"].as_u64().unwrap()).await;
        assert_eq!(done["status"], "COMPLETED", "{done}");
        assert_eq!(done["request"]["usenet"]["nzbRunId"], from);
        assert_eq!(done["request"]["usenet"]["nzbFilename"], "test.nzb");
        assert_eq!(done["result"]["usenet"]["delivered"], 1);
        from = again["id"].as_u64().unwrap();
    }

    // Runs without an upload have none to give, and enough of them push the
    // uploaded runs out of the ten held.
    let mut last = 0;
    for _ in 0..10 {
        let started = start(
            &h,
            &format!(
                r#"{{kind:SERVER,id:{server},route:{DIRECT},bandwidth:{{customUrl:"https://127.0.0.1:1/payload.bin"}}}}"#
            ),
        )
        .await;
        last = started["id"].as_u64().unwrap();
        finished(&h, last).await;
    }
    for held in [last, uploaded, from] {
        let response = h
            .execute(&format!(
                "mutation{{startSpeedTest(input:{{kind:SERVER,id:{server},route:{DIRECT},usenet:{{nzbRunId:{held}}}}}){{id}}}}"
            ))
            .await;
        assert_eq!(
            refusal(&response),
            (
                format!("run {held} holds no uploaded NZB"),
                "INVALID_INPUT".into()
            )
        );
    }
    let response = h.execute("{speedTestRuns{id}}").await;
    assert_eq!(
        response_data(&response)["speedTestRuns"]
            .as_array()
            .unwrap()
            .len(),
        10
    );
}

#[tokio::test]
async fn a_bandwidth_run_starts_on_its_source_alone_and_reports_it() {
    let h = harness().await;
    let nntp = NntpFixture::start(Vec::new(), None, None).await;
    let server = add_server(&h, &nntp).await;
    // Nothing listens here, so the run ends without moving a byte.
    let url = "https://127.0.0.1:1/payload.bin";
    let started = start(
        &h,
        &format!(r#"{{kind:SERVER,id:{server},route:{DIRECT},bandwidth:{{customUrl:"{url}"}}}}"#),
    )
    .await;
    assert_eq!(started["status"], "RUNNING");
    assert_eq!(started["kind"], "BANDWIDTH");
    let id = started["id"].as_u64().unwrap();
    let done = finished(&h, id).await;
    assert_eq!(
        done["request"],
        serde_json::json!({
            "bandwidth": {
                "providerId": null,
                "server": null,
                "customUrl": url,
            },
            "usenet": null,
        })
    );
    // One connection per leg up, and none added while nothing moves.
    assert_eq!(done["status"], "FAILED", "{done}");
    assert_eq!(done["result"]["bandwidth"]["connections"], 1);
    assert_eq!(done["result"]["bandwidth"]["url"], url);
    let connections = done["result"]["connections"].as_array().unwrap();
    assert_eq!(connections.len(), 1, "{done}");
    assert!(connections[0]["error"].is_string(), "{done}");
    // A refused connection has no kind of its own: its words stand.
    assert_eq!(done["failure"]["code"], "OTHER", "{done}");
    assert_eq!(
        done["failure"]["message"],
        format!(
            "no bytes were received: {}",
            connections[0]["error"].as_str().unwrap()
        ),
        "{done}"
    );
    assert_eq!(done["failure"]["params"], serde_json::json!({}), "{done}");
}

// Give egress 0 a download quota of `limit` bytes.
fn egress_quota(h: &TestHarness, limit: u64) {
    use weaver_nntp::transfer::{QuotaRuntimeConfig, ServerTransferConfig, StableServerId};

    h.handle
        .proxy_runtime()
        .unwrap()
        .network
        .egress_controls
        .configure(
            StableServerId(0),
            ServerTransferConfig {
                rate_bytes_per_sec: 0,
                quota: Some(QuotaRuntimeConfig {
                    limit_bytes: limit,
                    generation: 1,
                    retry_at: None,
                }),
            },
        );
}

#[tokio::test]
async fn a_bandwidth_run_over_https_completes_with_its_result() {
    let h = harness().await;
    let (acceptor, cert_der, _) = loopback_tls();
    let http = HttpFixture::start(String::new(), 200_000, Some(acceptor)).await;
    let service = SpeedTestService::for_handle(&h.handle, h.config.clone(), h.db.clone(), true);
    service.trust_roots_for_tests(vec![cert_der]);
    service.measure_until_stopped_for_tests();
    // Two GETs fit the quota and a third does not, so every connection ends
    // on it and the run ends with them.
    egress_quota(&h, 500_000);
    let url = format!("https://127.0.0.1:{}/blob", http.addr.port());

    let started = start(
        &h,
        &format!(r#"{{kind:RSS,id:1,route:{DIRECT},bandwidth:{{customUrl:"{url}"}}}}"#),
    )
    .await;
    assert_eq!(started["kind"], "BANDWIDTH");
    let done = finished(&h, started["id"].as_u64().unwrap()).await;

    assert_eq!(done["status"], "COMPLETED", "{done}");
    assert_eq!(done["failure"], serde_json::Value::Null);
    let result = &done["result"];
    assert_eq!(result["bytes"], 400_000.0);
    assert!(result["averageBps"].as_f64().unwrap() > 0.0, "{done}");
    assert_eq!(result["bandwidth"]["url"], url);
    assert_eq!(result["bandwidth"]["providerId"], serde_json::Value::Null);
    let connections = result["connections"].as_array().unwrap();
    assert!(!connections.is_empty());
    assert_eq!(result["bandwidth"]["connections"], connections.len());
    let moved: f64 = connections
        .iter()
        .map(|connection| connection["bytes"].as_f64().unwrap())
        .sum();
    assert_eq!(moved, 400_000.0);
    for connection in connections {
        assert_eq!(connection["leg"], 0, "{connection}");
        assert_eq!(connection["path"]["egressId"], 0, "{connection}");
        assert_eq!(
            connection["error"], "egress 0 has used its download quota",
            "{connection}"
        );
    }
    assert_eq!(result["legs"][0]["bytes"], 400_000.0);
}

#[tokio::test]
async fn a_bandwidth_run_a_provider_rate_limits_fails_as_rate_limited() {
    let h = harness().await;
    let (acceptor, cert_der, _) = loopback_tls();
    let http = HttpFixture::start(String::new(), 0, Some(acceptor)).await;
    let service = SpeedTestService::for_handle(&h.handle, h.config.clone(), h.db.clone(), true);
    service.trust_roots_for_tests(vec![cert_der]);
    let url = format!("https://127.0.0.1:{}/limited", http.addr.port());

    let started = start(
        &h,
        &format!(r#"{{kind:RSS,id:1,route:{DIRECT},bandwidth:{{customUrl:"{url}"}}}}"#),
    )
    .await;
    let done = finished(&h, started["id"].as_u64().unwrap()).await;

    assert_eq!(done["status"], "FAILED", "{done}");
    assert_eq!(
        done["failure"],
        serde_json::json!({
            "code": "RATE_LIMITED",
            "message": format!("GET {url} answered HTTP 429 Too Many Requests"),
            "params": { "url": url },
        })
    );
}

#[tokio::test]
async fn a_cancel_mid_run_ends_cancelled_with_the_bytes_moved_so_far() {
    let h = harness().await;
    let nntp = NntpFixture::start(
        (0..4)
            .map(|i| FixtureArticle::patterned(&format!("p{i}@fixture"), 20_000))
            .collect(),
        Some(1),
        None,
    )
    .await;
    let http = HttpFixture::start(
        nzb(&[
            ("p0@fixture", 20_000),
            ("p1@fixture", 20_000),
            ("p2@fixture", 20_000),
            ("p3@fixture", 20_000),
        ]),
        0,
        None,
    )
    .await;
    let server = add_server(&h, &nntp).await;
    let started = start(
        &h,
        &format!(
            r#"{{kind:SERVER,id:{server},route:{DIRECT},usenet:{{nzbUrl:"http://{}/test.nzb",connections:1}}}}"#,
            http.addr
        ),
    )
    .await;
    let id = started["id"].as_u64().unwrap();
    let mut served = nntp.served();
    served.wait_for(|served| *served == 1).await.unwrap();
    wait_until(&h, id, |run| {
        run.result
            .as_ref()
            .and_then(|r| r.usenet.as_ref())
            .is_some_and(|u| u.delivered == 1)
    })
    .await;

    let response = h
        .execute(&format!("mutation{{cancelSpeedTest(id:{id})}}"))
        .await;
    assert_no_errors(&response);
    assert_eq!(response_data(&response)["cancelSpeedTest"], true);
    let done = finished(&h, id).await;
    nntp.release();

    assert_eq!(done["status"], "CANCELLED", "{done}");
    assert_eq!(done["result"]["usenet"]["delivered"], 1);
    assert!(done["result"]["bytes"].as_f64().unwrap() > 20_000.0);
    assert!(!h.handle.is_globally_paused());
    let response = h
        .execute(&format!("mutation{{cancelSpeedTest(id:{id})}}"))
        .await;
    assert_eq!(response_data(&response)["cancelSpeedTest"], false);
}

#[tokio::test]
async fn a_downed_rung_is_absent_from_every_connection_path() {
    let h = harness().await;
    let response = h.execute(r#"mutation{saveProxyProfile(input:{name:"Gateway",kind:SOCKS5,resolver:PROXY,enabled:false,host:"127.0.0.1",port:1080}){id}}"#).await;
    assert_no_errors(&response);
    let proxy = response_data(&response)["saveProxyProfile"]["id"]
        .as_u64()
        .unwrap();
    let nntp = NntpFixture::start(
        vec![FixtureArticle::patterned("one@fixture", 10_000)],
        None,
        None,
    )
    .await;
    let http = HttpFixture::start(nzb(&[("one@fixture", 10_000)]), 0, None).await;
    let server = add_server(&h, &nntp).await;

    let started = start(
        &h,
        &format!(
            r#"{{kind:SERVER,id:{server},route:{{legs:[{{egressId:0,weight:100,path:{{ladder:{{rungs:[{{proxy:{proxy}}}],directFallback:true}}}}}}]}},overrides:{{downRungs:[{{leg:0,rung:0}}]}},usenet:{{nzbUrl:"http://{}/test.nzb"}}}}"#,
            http.addr
        ),
    )
    .await;
    let done = finished(&h, started["id"].as_u64().unwrap()).await;

    assert_eq!(done["status"], "COMPLETED", "{done}");
    assert_eq!(done["tested"]["overrides"]["downRungs"][0]["rung"], 0);
    assert_eq!(done["tested"]["legs"][0]["path"]["kind"], "LADDER");
    let connections = done["result"]["connections"].as_array().unwrap();
    assert!(!connections.is_empty());
    for connection in connections {
        assert_eq!(
            connection["path"]["proxyIds"],
            serde_json::json!([]),
            "{connection}"
        );
        assert_ne!(connection["path"]["rung"], 0, "{connection}");
    }
    assert!(
        done["tested"]["names"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["kind"] == "proxy" && n["name"] == "Gateway")
    );
}
