mod common;
use common::{TestHarness, assert_no_errors, response_data};
use std::sync::atomic::Ordering;
use weaver_server_core::proxies::ProxyRuntime;
use weaver_server_core::speed_test::SpeedTestStatus;
use weaver_server_core::speed_test::fixture::{FixtureArticle, HttpFixture, NntpFixture, nzb};

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
            r#"mutation{{addServer(input:{{host:"127.0.0.1",port:{},tls:false,connections:2,active:false}}){{id}}}}"#,
            nntp.addr.port()
        ))
        .await;
    assert_no_errors(&response);
    response_data(&response)["addServer"]["id"]
        .as_u64()
        .unwrap()
}

const DIRECT: &str = "{legs:[{egressId:0,weight:100,path:{direct:true}}]}";

const RUN_SHAPE: &str = "id consumer consumerName kind request{bandwidth{providerId server customUrl bytes streams} usenet{sabSize nzbUrl connections} maxDurationSeconds} status phase startedAt finishedAt error tested{legs{position egressId weight down downReason target path{kind}} failover overrides{downLegs downRungs{leg rung} downProxies downEgresses} names{kind id name}} result{bytes elapsedMs averageBps peakBps legs{position bytes connections sourceAddresses} connections{leg connectMs bytes error sourceAddress path{egressId rung poolId memberId proxyIds}} usenet{articlesRequested delivered missing failed declaredBytes decodedBytes pipelining tls tlsCipherSuite} bandwidth{url} samples{atMs leg bytes}}";

async fn start(h: &TestHarness, input: &str) -> serde_json::Value {
    let response = h
        .execute(&format!(
            "mutation{{startSpeedTest(input:{input}){{{RUN_SHAPE}}}}}"
        ))
        .await;
    assert_no_errors(&response);
    response_data(&response)["startSpeedTest"].clone()
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
async fn providers_are_listed_with_their_sizes() {
    let h = harness().await;
    let response = h
        .execute("{bandwidthProviders{id name fixedSizes anySize maxBytes needsServer}}")
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
    assert_eq!(providers[4]["needsServer"], true);
    assert_eq!(providers[0]["anySize"], true);
    assert_eq!(providers[0]["maxBytes"], 99_999_999.0);
    assert_eq!(providers[2]["maxBytes"], serde_json::Value::Null);
}

#[tokio::test]
async fn a_run_takes_exactly_one_of_bandwidth_and_usenet() {
    let h = harness().await;
    for body in [
        "",
        r#"bandwidth:{customUrl:"https://127.0.0.1/blob",bytes:1000},usenet:{sabSize:MB_100}"#,
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
            r#"mutation{{startSpeedTest(input:{{kind:RSS,id:1,route:{DIRECT},overrides:{{downLegs:[0]}},bandwidth:{{customUrl:"https://127.0.0.1/blob",bytes:1000}}}}){{id}}}}"#
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
                "connections": null,
            },
            "maxDurationSeconds": 120,
        })
    );
    let result = &done["result"];
    let usenet = &result["usenet"];
    assert_eq!(usenet["articlesRequested"], 3);
    assert_eq!(usenet["delivered"], 2);
    assert_eq!(usenet["missing"], 1);
    assert_eq!(usenet["failed"], 0);
    assert_eq!(usenet["decodedBytes"], 60_000.0);
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
async fn a_run_reports_the_bandwidth_request_it_accepted() {
    let h = harness().await;
    let nntp = NntpFixture::start(Vec::new(), None, None).await;
    let server = add_server(&h, &nntp).await;
    // Nothing listens here, so the run ends without moving a byte.
    let url = "https://127.0.0.1:1/payload.bin";
    let started = start(
        &h,
        &format!(
            r#"{{kind:SERVER,id:{server},route:{DIRECT},maxDurationSeconds:30,bandwidth:{{customUrl:"{url}",bytes:1048576,streams:3}}}}"#
        ),
    )
    .await;
    let id = started["id"].as_u64().unwrap();
    let done = finished(&h, id).await;
    assert_eq!(
        done["request"],
        serde_json::json!({
            "bandwidth": {
                "providerId": null,
                "server": null,
                "customUrl": url,
                "bytes": 1048576.0,
                "streams": 3,
            },
            "usenet": null,
            "maxDurationSeconds": 30,
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
    let response = h.execute(r#"mutation{saveProxyProfile(input:{name:"Gateway",kind:SOCKS5,enabled:false,host:"127.0.0.1",port:1080}){id}}"#).await;
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
