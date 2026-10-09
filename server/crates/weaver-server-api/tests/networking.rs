mod common;
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use weaver_server_api::auth::CallerScope;
use weaver_server_core::proxies::ProxyRuntime;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the frontend dependencies and Playwright runtime"]
async fn networking_browser_against_isolated_graphql() {
    use axum::{Json, Router, routing::post};
    use std::sync::Arc;
    let h = Arc::new(harness().await);
    let response = h.execute(r#"mutation {addServer(input:{host:"news.fixture.invalid",port:119,tls:false,connections:20,active:false}){id}}"#).await;
    assert_no_errors(&response);
    let id = response_data(&response)["addServer"]["id"]
        .as_u64()
        .unwrap() as u32;
    h.handle
        .proxy_runtime()
        .unwrap()
        .network
        .route(
            weaver_server_core::proxies::Consumer::Server(id),
            20,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
    let handler = h.clone();
    let subscriptions = h.clone();
    let app = Router::new().route(
        "/graphql",
        post(move |Json(request): Json<async_graphql::Request>| {
            let h = handler.clone();
            async move {
                Json(
                    h.schema
                        .execute(
                            request
                                .data(CallerScope::Local)
                                .data(weaver_server_api::auth::CallerIdentity::Local([7; 32])),
                        )
                        .await,
                )
            }
        })
        .get(move |ws: axum::extract::WebSocketUpgrade| {
            use async_graphql::futures_util::{SinkExt, StreamExt};
            let h = subscriptions.clone();
            async move {
                ws.protocols(["graphql-transport-ws"])
                    .on_upgrade(move |socket| async move {
                        let (mut sender, receiver) = socket.split();
                        let input = receiver.filter_map(|message| async move {
                            match message {
                                Ok(axum::extract::ws::Message::Text(text)) => Some(
                                    serde_json::from_str::<async_graphql::http::ClientMessage>(
                                        &text,
                                    ),
                                ),
                                _ => None,
                            }
                        });
                        let mut data = async_graphql::Data::default();
                        data.insert(CallerScope::Local);
                        data.insert(weaver_server_api::auth::CallerIdentity::Local([7; 32]));
                        let mut output = Box::pin(
                            async_graphql::http::WebSocket::from_message_stream(
                                h.schema.clone(),
                                input,
                                async_graphql::http::WebSocketProtocols::GraphQLWS,
                            )
                            .connection_data(data),
                        );
                        while let Some(message) = output.next().await {
                            match message {
                                async_graphql::http::WsMessage::Text(text) => {
                                    if sender
                                        .send(axum::extract::ws::Message::Text(text.into()))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                async_graphql::http::WsMessage::Close(_, _) => break,
                            }
                        }
                    })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("node")
            .current_dir(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../apps/weaver-web"),
            )
            .args(["--test", "tests/browser/networking.test.mjs"])
            .env("NETWORKING_FIXTURE_API", format!("http://{address}"))
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    let _ = stop.send(());
    serving.await.unwrap();
    h.handle.proxy_runtime().unwrap().stop_all().await;
    assert!(status.success());
}

async fn harness() -> TestHarness {
    let h = TestHarness::new().await;
    h.handle.set_proxy_runtime(
        ProxyRuntime::new(h.db.clone(), tokio::runtime::Handle::current()).unwrap(),
    );
    h
}
#[tokio::test]
async fn advanced_route_round_trip_refuses_lossy_legacy_writes_and_preserves_unrelated_edits() {
    let h = harness().await;
    let create = r#"mutation {createEgressInterface(input:{name:"Loopback source",bindingKind:SOURCE_ADDRESS,sourceAddress:"127.0.0.1"}){id}}"#;
    assert_has_errors(&h.execute_as(create, CallerScope::Control).await);
    let response = h.execute(create).await;
    assert_no_errors(&response);
    let egress = response_data(&response)["createEgressInterface"]["id"]
        .as_u64()
        .unwrap();
    let response=h.execute(&format!(r#"mutation {{addRssFeed(input:{{name:"Two paths",url:"https://feed.invalid/rss",enabled:false,route:{{failover:HOLD,legs:[{{egressId:0,weight:60,path:{{direct:true}}}},{{egressId:{egress},weight:40,path:{{direct:true}}}}]}}}}){{id route{{failover legs{{egressId weight path{{kind}}}}}} routing{{proxyIds allowDirect}}}}}}"#)).await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let feed = data["addRssFeed"]["id"].as_u64().unwrap();
    assert_eq!(
        data["addRssFeed"]["route"]["legs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(data["addRssFeed"]["routing"]["allowDirect"], true);
    assert_has_errors(
        &h.execute(&format!("mutation{{deleteEgressInterface(id:{egress})}}"))
            .await,
    );
    let before =
        h.db.proxy_routing_policy(weaver_server_core::proxies::Consumer::Rss(feed as u32))
            .unwrap();
    assert_has_errors(&h.execute(&format!(r#"mutation{{updateRssFeed(id:{feed},input:{{name:"Must not save",url:"https://feed.invalid/rss",enabled:false,routing:{{proxyIds:[],allowDirect:true}}}}){{id}}}}"#)).await);
    assert_eq!(
        h.db.get_rss_feed(feed as u32).unwrap().unwrap().name,
        "Two paths"
    );
    let response=h.execute(&format!(r#"mutation{{updateRssFeed(id:{feed},input:{{name:"Renamed",url:"https://feed.invalid/rss",enabled:false}}){{route{{failover legs{{weight}}}}}}}}"#)).await;
    assert_no_errors(&response);
    assert_eq!(
        h.db.proxy_routing_policy(weaver_server_core::proxies::Consumer::Rss(feed as u32))
            .unwrap(),
        before
    );
    assert_has_errors(&h.execute(&format!(r#"mutation{{updateRssFeed(id:{feed},input:{{name:"Must not save",url:"https://feed.invalid/rss",routing:{{proxyIds:[],allowDirect:true}},route:{{legs:[{{egressId:0,weight:100,path:{{direct:true}}}}]}}}}){{id}}}}"#)).await);
    let query=h.execute("{egressInterfaces{id name health} platformNetworking{egressBindingKinds} networkFlow{consumers{key id name kind cap route{failover legs{egressId weight}}} legs{consumer state} pools{poolId}}}").await;
    assert_no_errors(&query);
    let graph = response_data(&query);
    let consumer = &graph["networkFlow"]["consumers"][0];
    assert_eq!(consumer["key"], format!("rss:{feed}"));
    assert_eq!(consumer["name"], "Renamed");
    assert_eq!(consumer["route"]["failover"], "HOLD");
    assert_eq!(consumer["route"]["legs"].as_array().unwrap().len(), 2);
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn route_input_union_and_system_invariants_are_enforced() {
    let h = harness().await;
    for path in [
        "{direct:false}",
        "{direct:true,ladder:{rungs:[],directFallback:false}}",
        "{ladder:{rungs:[],directFallback:true}}",
    ] {
        assert_has_errors(&h.execute(&format!(r#"mutation{{addRssFeed(input:{{name:"Invalid",url:"https://feed.invalid/rss",enabled:false,route:{{legs:[{{egressId:0,weight:100,path:{path}}}]}}}}){{id}}}}"#)).await);
    }
    assert_has_errors(&h.execute("mutation{deleteEgressInterface(id:0)}").await);
    assert_has_errors(&h.execute(r#"mutation{updateEgressInterface(id:0,input:{name:"Renamed",bindingKind:SYSTEM}){id}}"#).await);
    assert_no_errors(&h.execute(r#"mutation{updateEgressInterface(id:0,input:{name:"System",bindingKind:SYSTEM,maxDownloadSpeed:1024}){id maxDownloadSpeed}}"#).await);
    assert!(h.db.list_rss_feeds().unwrap().is_empty());
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn pool_route_keeps_member_projection_and_prevents_pool_deletion() {
    let h = harness().await;
    let mut ids = Vec::new();
    for name in ["Primary", "Secondary"] {
        let result=h.execute(&format!(r#"mutation{{saveProxyProfile(input:{{name:"{name}",kind:SOCKS5,enabled:false,host:"127.0.0.1",port:1080,dnsServers:["192.0.2.53"]}}){{id}}}}"#)).await;
        assert_no_errors(&result);
        ids.push(
            response_data(&result)["saveProxyProfile"]["id"]
                .as_u64()
                .unwrap(),
        );
    }
    let result=h.execute(&format!(r#"mutation{{createProxyPool(input:{{name:"POP pool",kind:SOCKS5,memberIds:[{},{}]}}){{id}}}}"#,ids[0],ids[1])).await;
    assert_no_errors(&result);
    let pool = response_data(&result)["createProxyPool"]["id"]
        .as_u64()
        .unwrap();
    let result=h.execute(&format!(r#"mutation{{addRssFeed(input:{{name:"Pool feed",url:"https://feed.invalid/rss",enabled:false,route:{{legs:[{{egressId:0,weight:100,path:{{ladder:{{rungs:[{{pool:{pool}}}],directFallback:false}}}}}}]}}}}){{route{{legs{{path{{rungs{{kind poolId}}}}}}}} routing{{proxyIds allowDirect}}}}}}"#)).await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["addRssFeed"]["routing"]["proxyIds"],
        serde_json::json!(ids)
    );
    assert_has_errors(
        &h.execute(&format!("mutation{{deleteProxyPool(id:{pool})}}"))
            .await,
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn topology_queries_and_subscription_require_admin_scope() {
    use async_graphql::futures_util::StreamExt;
    let h = harness().await;
    let queries = [
        "{ egressInterfaces { id } }",
        "{ discoverNetworkInterfaces { name } }",
        "{ platformNetworking { egressBindingKinds } }",
        "{ proxyPools { id } }",
        "{ networkRoutes { consumer } }",
        "{ networkFlow { legs { consumer } } }",
    ];
    for scope in [
        CallerScope::Read,
        CallerScope::Control,
        CallerScope::Admin,
        CallerScope::Local,
    ] {
        for query in queries {
            let response = h.execute_as(query, scope).await;
            if matches!(scope, CallerScope::Read | CallerScope::Control) {
                assert!(
                    format!("{:?}", response.errors).contains("FORBIDDEN"),
                    "{query}: {:?}",
                    response.errors
                );
            } else {
                assert_no_errors(&response);
            }
        }
        let request =
            async_graphql::Request::new("subscription { networkFlow { legs { consumer } } }")
                .data(scope);
        let response = h.schema.execute_stream(request).next().await.unwrap();
        if matches!(scope, CallerScope::Read | CallerScope::Control) {
            assert!(format!("{:?}", response.errors).contains("FORBIDDEN"));
        } else {
            assert_no_errors(&response);
        }
    }
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn route_save_rejects_missing_consumers_without_persisting() {
    let h = harness().await;
    for kind in ["SERVER", "RSS"] {
        let response = h.execute(&format!("mutation {{ saveNetworkRoute(kind:{kind}, id:999, input:{{legs:[{{egressId:0,weight:100,path:{{direct:true}}}}]}}) {{ consumer }} }}")).await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("consumer no longer exists")),
            "{:?}",
            response.errors
        );
    }
    assert!(h.db.list_proxy_routes().unwrap().is_empty());
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

/// A server or a feed made behind a kill switch is kept switched off behind a
/// route nothing can take, and a route saved for it under Networking opens it.
#[tokio::test]
async fn kill_switch_keeps_a_new_consumer_blocked_until_it_is_given_a_route() {
    let h = harness().await;
    let shape = "id routing{proxyIds allowDirect} routingStatus{state} route{legs{egressId weight path{kind directFallback rungs{kind}}}}";
    // The interface recognises a kill switch by this leg: a ladder with no rung and no direct fallback.
    let closed = serde_json::json!([{
        "egressId": 0,
        "weight": 100,
        "path": {"kind": "LADDER", "directFallback": false, "rungs": []}
    }]);
    let response = h
        .execute(&format!(
            r#"mutation {{addServer(input:{{host:"news.fixture.invalid",port:563,tls:true,connections:20,active:false,routing:{{proxyIds:[],allowDirect:false}}}}){{active {shape}}}}}"#
        ))
        .await;
    assert_no_errors(&response);
    let server = response_data(&response)["addServer"].clone();
    let response = h
        .execute(&format!(
            r#"mutation {{addRssFeed(input:{{name:"Held feed",url:"https://feed.invalid/rss",enabled:false,routing:{{proxyIds:[],allowDirect:false}}}}){{enabled {shape}}}}}"#
        ))
        .await;
    assert_no_errors(&response);
    let feed = response_data(&response)["addRssFeed"].clone();
    assert_eq!(server["active"], false);
    assert_eq!(feed["enabled"], false);
    for consumer in [&server, &feed] {
        assert_eq!(
            consumer["routing"],
            serde_json::json!({"proxyIds": [], "allowDirect": false})
        );
        assert_eq!(consumer["routingStatus"]["state"], "BLOCKED");
        assert_eq!(consumer["route"]["legs"], closed);
    }

    // The flow draws both, switched off as they are, by the same leg.
    let response = h
        .execute("{networkFlow{legs{consumer egressId weight open path{kind directFallback rungs{kind}}}}}")
        .await;
    assert_no_errors(&response);
    let legs = response_data(&response)["networkFlow"]["legs"]
        .as_array()
        .unwrap()
        .clone();
    for key in [
        format!("server:{}", server["id"]),
        format!("rss:{}", feed["id"]),
    ] {
        let leg = legs.iter().find(|leg| leg["consumer"] == key.as_str());
        assert_eq!(
            leg,
            Some(&serde_json::json!({
                "consumer": key,
                "egressId": 0,
                "weight": 100,
                "open": 0,
                "path": {"kind": "LADDER", "directFallback": false, "rungs": []}
            }))
        );
    }

    // A connection test told of the kill switch finds no way out, where one told nothing goes direct.
    let response = h
        .execute(
            r#"mutation {testConnection(input:{host:"news.fixture.invalid",port:563,tls:true,connections:20,active:false,routing:{proxyIds:[],allowDirect:false}}){success message}}"#,
        )
        .await;
    assert_no_errors(&response);
    let test = &response_data(&response)["testConnection"];
    assert_eq!(test["success"], false);
    assert!(
        test["message"]
            .as_str()
            .unwrap()
            .contains("all rungs are unavailable"),
        "{test}"
    );

    for (kind, id) in [("SERVER", &server["id"]), ("RSS", &feed["id"])] {
        let response = h
            .execute(&format!(
                "mutation {{saveNetworkRoute(kind:{kind},id:{id},input:{{legs:[{{egressId:0,weight:100,path:{{direct:true}}}}]}}){{legs{{path{{kind}}}}}}}}"
            ))
            .await;
        assert_no_errors(&response);
        assert_eq!(
            response_data(&response)["saveNetworkRoute"]["legs"][0]["path"]["kind"],
            "DIRECT"
        );
    }
    let response = h
        .execute("{servers{routing{allowDirect}} rssFeeds{routing{allowDirect}}}")
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["servers"][0]["routing"]["allowDirect"], true);
    assert_eq!(data["rssFeeds"][0]["routing"]["allowDirect"], true);
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

async fn quota_harness() -> TestHarness {
    let h = TestHarness::new().await;
    h.handle.set_proxy_runtime(
        ProxyRuntime::with_quota_policy(
            h.db.clone(),
            tokio::runtime::Handle::current(),
            Some(h.server_transfer_policy.clone()),
        )
        .unwrap(),
    );
    h
}

const EGRESS_QUOTA_SHAPE: &str = "id health reason downloadQuota{enabled limitBytes period resetTimeMinutesLocal weeklyResetWeekday monthlyResetDay usedBytes} downloadQuotaUsage{usedBytes remainingBytes blocked}";

#[tokio::test]
async fn egress_download_quota_is_saved_on_create_and_kept_when_an_update_omits_it() {
    let h = quota_harness().await;
    let response = h
        .execute(&format!(
            r#"mutation {{createEgressInterface(input:{{name:"Metered link",bindingKind:SOURCE_ADDRESS,sourceAddress:"127.0.0.1",downloadQuota:{{enabled:true,limitBytes:5000000000,period:MONTHLY,monthlyResetDay:15,resetTimeMinutesLocal:120}}}}){{{EGRESS_QUOTA_SHAPE}}}}}"#
        ))
        .await;
    assert_no_errors(&response);
    let created = response_data(&response)["createEgressInterface"].clone();
    let id = created["id"].as_u64().unwrap();
    assert_eq!(
        created["downloadQuota"],
        serde_json::json!({
            "enabled": true,
            "limitBytes": 5_000_000_000u64,
            "period": "MONTHLY",
            "resetTimeMinutesLocal": 120,
            "weeklyResetWeekday": "MON",
            "monthlyResetDay": 15,
            "usedBytes": 0,
        })
    );
    assert_eq!(created["downloadQuotaUsage"]["usedBytes"], 0);
    assert_eq!(
        created["downloadQuotaUsage"]["remainingBytes"],
        5_000_000_000u64
    );

    // An update that leaves the quota out keeps the saved one.
    let response = h
        .execute(&format!(
            r#"mutation {{updateEgressInterface(id:{id},input:{{name:"Metered link renamed",bindingKind:SOURCE_ADDRESS,sourceAddress:"127.0.0.1",maxDownloadSpeed:2048}}){{name maxDownloadSpeed {EGRESS_QUOTA_SHAPE}}}}}"#
        ))
        .await;
    assert_no_errors(&response);
    let updated = response_data(&response)["updateEgressInterface"].clone();
    assert_eq!(updated["name"], "Metered link renamed");
    assert_eq!(updated["maxDownloadSpeed"], 2048);
    assert_eq!(updated["downloadQuota"], created["downloadQuota"]);
    let saved =
        h.db.list_egress_interfaces()
            .unwrap()
            .into_iter()
            .find(|egress| u64::from(egress.id) == id)
            .unwrap();
    assert!(saved.download_quota.enabled);
    assert_eq!(saved.download_quota.limit_bytes, 5_000_000_000);

    // An update that carries a quota replaces it.
    let response = h
        .execute(&format!(
            r#"mutation {{updateEgressInterface(id:{id},input:{{name:"Metered link renamed",bindingKind:SOURCE_ADDRESS,sourceAddress:"127.0.0.1",downloadQuota:{{enabled:false}}}}){{{EGRESS_QUOTA_SHAPE}}}}}"#
        ))
        .await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["updateEgressInterface"]["downloadQuota"]["enabled"],
        false
    );

    // A quota the server quota would refuse is refused here too.
    assert_has_errors(
        &h.execute(&format!(
            r#"mutation {{updateEgressInterface(id:{id},input:{{name:"Metered link renamed",bindingKind:SOURCE_ADDRESS,sourceAddress:"127.0.0.1",downloadQuota:{{enabled:true,limitBytes:0}}}}){{id}}}}"#
        ))
        .await,
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn egress_quota_usage_reports_recorded_bytes_and_a_used_up_quota_takes_the_egress_down() {
    let h = quota_harness().await;
    let response = h
        .execute(r#"mutation {createEgressInterface(input:{name:"Small allowance",bindingKind:SOURCE_ADDRESS,sourceAddress:"127.0.0.1",downloadQuota:{enabled:true,limitBytes:1048576,period:ONE_TIME}}){id}}"#)
        .await;
    assert_no_errors(&response);
    let id = response_data(&response)["createEgressInterface"]["id"]
        .as_u64()
        .unwrap() as u32;
    let control = h
        .server_transfer_policy
        .egress_transfer_registry()
        .control(weaver_nntp::transfer::StableServerId(id));
    let mut permit = control.try_reserve(700_000).unwrap();
    permit.record_blocking(700_000);
    permit.finish();

    let query = format!("{{egressInterfaces{{{EGRESS_QUOTA_SHAPE}}}}}");
    let response = h.execute(&query).await;
    assert_no_errors(&response);
    let egress = response_data(&response)["egressInterfaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|egress| egress["id"] == id)
        .unwrap()
        .clone();
    assert_eq!(egress["downloadQuota"]["usedBytes"], 700_000);
    assert_eq!(egress["downloadQuotaUsage"]["usedBytes"], 700_000);
    assert_eq!(egress["downloadQuotaUsage"]["remainingBytes"], 348_576);
    assert_eq!(egress["downloadQuotaUsage"]["blocked"], false);
    assert_ne!(egress["reason"], "Quota reached");

    // A body that no longer fits is turned away, and the egress reads Down.
    assert!(control.try_reserve(700_000).is_err());
    let response = h.execute(&query).await;
    assert_no_errors(&response);
    let egress = response_data(&response)["egressInterfaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|egress| egress["id"] == id)
        .unwrap()
        .clone();
    assert_eq!(egress["downloadQuotaUsage"]["blocked"], true);
    assert_eq!(egress["health"], "DOWN");
    assert_eq!(egress["reason"], "Quota reached");
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn download_block_names_the_egress_whose_quota_holds_downloads() {
    let h = quota_harness().await;
    h.shared_state
        .set_egress_quota_block(Some(weaver_server_core::EgressQuotaBlock {
            egress_id: 0,
            egress_name: "System".into(),
            used_bytes: 4_000,
            limit_bytes: 4_000,
            remaining_bytes: 0,
            window_starts_at_epoch_ms: Some(1_000.0),
            window_ends_at_epoch_ms: Some(2_000.0),
            timezone_name: "UTC".into(),
        }));
    // A server quota does not displace the egress that holds everything back.
    h.shared_state.set_server_quota_blocked(true);
    let query = "{downloadBlock{kind egressId egressName usedBytes limitBytes remainingBytes windowEndsAtEpochMs timezoneName}}";
    let response = h.execute(query).await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["downloadBlock"],
        serde_json::json!({
            "kind": "EGRESS_QUOTA",
            "egressId": 0,
            "egressName": "System",
            "usedBytes": 4_000,
            "limitBytes": 4_000,
            "remainingBytes": 0,
            "windowEndsAtEpochMs": 2_000.0,
            "timezoneName": "UTC",
        })
    );
    h.shared_state.set_egress_quota_block(None);
    let response = h.execute(query).await;
    assert_no_errors(&response);
    let block = &response_data(&response)["downloadBlock"];
    assert_eq!(block["kind"], "SERVER_QUOTA");
    assert_eq!(block["egressId"], serde_json::Value::Null);
    h.handle.proxy_runtime().unwrap().stop_all().await;
}
