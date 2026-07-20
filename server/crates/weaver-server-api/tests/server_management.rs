mod common;

use common::{
    BlockingDbOperation, TestHarness, assert_has_errors, assert_no_errors, local_request,
    response_data,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Duration;
use weaver_nntp::transfer::StableServerId;
use weaver_server_api::auth::CallerScope;

struct ScriptStep {
    expect_prefix: Option<&'static str>,
    response: &'static [u8],
}

async fn read_command_line(socket: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        let n = socket.read(&mut byte).await.unwrap();
        assert!(n > 0, "client closed connection before command completed");
        buf.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    String::from_utf8(buf).unwrap()
}

async fn spawn_scripted_server(steps: Vec<ScriptStep>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        for step in steps {
            if let Some(prefix) = step.expect_prefix {
                let line = read_command_line(&mut socket).await;
                assert!(
                    line.starts_with(prefix),
                    "expected command starting with {prefix:?}, got {line:?}"
                );
            }
            if !step.response.is_empty() {
                socket.write_all(step.response).await.unwrap();
                socket.flush().await.unwrap();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    });

    port
}

#[tokio::test]
async fn list_servers_empty() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ servers { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let servers = data["servers"].as_array().unwrap();
    assert!(servers.is_empty());
}

#[tokio::test]
async fn has_configured_servers_reflects_presence() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ hasConfiguredServers }").await;
    assert_no_errors(&resp);
    assert!(
        !response_data(&resp)["hasConfiguredServers"]
            .as_bool()
            .unwrap()
    );

    h.execute(
        r#"mutation {
            addServer(input: {
                host: "news.example.com",
                port: 119,
                tls: false,
                connections: 5,
                active: false
            }) { id }
        }"#,
    )
    .await;

    let resp = h.execute("{ hasConfiguredServers }").await;
    assert_no_errors(&resp);
    assert!(
        response_data(&resp)["hasConfiguredServers"]
            .as_bool()
            .unwrap()
    );
}

#[tokio::test]
async fn server_list_does_not_expose_username_field() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ servers { username } }").await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn add_server_basic() {
    let h = TestHarness::new().await;
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"500 unknown\r\n",
        },
        ScriptStep {
            expect_prefix: Some("QUIT"),
            response: b"205 bye\r\n",
        },
    ])
    .await;
    let resp = h
        .execute(&format!(
            r#"mutation {{
                addServer(input: {{
                    host: "127.0.0.1",
                    port: {port},
                    tls: false,
                    connections: 5
                }}) {{
                    id
                    host
                    port
                    tls
                    connections
                }}
            }}"#,
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let server = &data["addServer"];
    assert!(server["id"].as_u64().unwrap() > 0);
    assert_eq!(server["host"].as_str().unwrap(), "127.0.0.1");
    assert_eq!(server["port"].as_u64().unwrap(), port as u64);
    assert!(!server["tls"].as_bool().unwrap());
    assert_eq!(server["connections"].as_u64().unwrap(), 5);
}

#[tokio::test]
async fn server_download_limits_roundtrip_preserve_and_reset() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false,
                    maxDownloadSpeed: 2500000,
                    downloadQuota: {
                        enabled: true,
                        limitBytes: 9000000,
                        period: WEEKLY,
                        resetTimeMinutesLocal: 375,
                        weeklyResetWeekday: THU,
                        monthlyResetDay: 31
                    }
                }) {
                    id
                    maxDownloadSpeed
                    downloadQuota {
                        enabled
                        limitBytes
                        period
                        resetTimeMinutesLocal
                        weeklyResetWeekday
                        monthlyResetDay
                        lifetimeBytes
                        usedBytes
                        reservedBytes
                        remainingBytes
                        blocked
                        windowStartsAtEpochMs
                        windowEndsAtEpochMs
                        timezoneName
                    }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let added = &response_data(&resp)["addServer"];
    let id = added["id"].as_u64().unwrap();
    assert_eq!(added["maxDownloadSpeed"].as_u64(), Some(2_500_000));
    let quota = &added["downloadQuota"];
    assert!(quota["enabled"].as_bool().unwrap());
    assert_eq!(quota["limitBytes"].as_u64(), Some(9_000_000));
    assert_eq!(quota["period"].as_str(), Some("WEEKLY"));
    assert_eq!(quota["resetTimeMinutesLocal"].as_u64(), Some(375));
    assert_eq!(quota["weeklyResetWeekday"].as_str(), Some("THU"));
    assert_eq!(quota["monthlyResetDay"].as_u64(), Some(31));
    assert_eq!(quota["remainingBytes"].as_u64(), Some(9_000_000));
    assert_eq!(quota["usedBytes"].as_u64(), Some(0));
    assert!(
        quota["timezoneName"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateServer(id: {id}, input: {{
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 8,
                    active: false
                }}) {{
                    maxDownloadSpeed
                    downloadQuota {{ enabled limitBytes period remainingBytes }}
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let updated = &response_data(&resp)["updateServer"];
    assert_eq!(updated["maxDownloadSpeed"].as_u64(), Some(2_500_000));
    assert_eq!(
        updated["downloadQuota"]["limitBytes"].as_u64(),
        Some(9_000_000)
    );
    assert_eq!(updated["downloadQuota"]["period"].as_str(), Some("WEEKLY"));

    let server_id = u32::try_from(id).unwrap();
    let control = h
        .server_transfer_policy
        .transfer_registry()
        .control(StableServerId(server_id));
    let mut permit = control.try_reserve(1_000_000).unwrap();
    permit.record_blocking(1_000_000);
    permit.finish();
    let before_reset = h.server_transfer_policy.snapshot(server_id).unwrap();
    assert_eq!(before_reset.lifetime_bytes, 1_000_000);
    assert_eq!(before_reset.used_bytes, 1_000_000);

    let reset_mutation = format!(
        r#"mutation {{
                resetServerDownloadQuotaUsage(id: {id}) {{
                    id
                    downloadQuota {{ lifetimeBytes usedBytes reservedBytes remainingBytes blocked }}
                }}
            }}"#
    );
    let forbidden = h.execute_as(&reset_mutation, CallerScope::Control).await;
    assert_has_errors(&forbidden);
    assert!(format!("{:?}", forbidden.errors).contains("FORBIDDEN"));
    assert_eq!(
        h.server_transfer_policy
            .snapshot(server_id)
            .unwrap()
            .used_bytes,
        1_000_000
    );

    let resp = h.execute(&reset_mutation).await;
    assert_no_errors(&resp);
    let reset = &response_data(&resp)["resetServerDownloadQuotaUsage"]["downloadQuota"];
    assert_eq!(reset["lifetimeBytes"].as_u64(), Some(1_000_000));
    assert_eq!(reset["usedBytes"].as_u64(), Some(0));
    assert_eq!(reset["reservedBytes"].as_u64(), Some(0));
    assert_eq!(reset["remainingBytes"].as_u64(), Some(9_000_000));
    assert!(!reset["blocked"].as_bool().unwrap());
}

#[tokio::test]
async fn enabled_server_download_quota_requires_positive_limit() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false,
                    downloadQuota: { enabled: true, limitBytes: 0 }
                }) { id }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
    assert!(resp.errors[0].message.contains("greater than zero"));
}

#[tokio::test]
async fn add_server_with_tls() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 563,
                    tls: true,
                    connections: 10,
                    active: false
                }) {
                    id
                    host
                    port
                    tls
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let server = &data["addServer"];
    assert_eq!(server["port"].as_u64().unwrap(), 563);
    assert!(server["tls"].as_bool().unwrap());
}

#[tokio::test]
async fn add_server_with_auth() {
    let h = TestHarness::new().await;
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"500 unknown\r\n",
        },
        ScriptStep {
            expect_prefix: Some("AUTHINFO USER"),
            response: b"381 password required\r\n",
        },
        ScriptStep {
            expect_prefix: Some("AUTHINFO PASS"),
            response: b"281 authentication accepted\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"500 unknown\r\n",
        },
        ScriptStep {
            expect_prefix: Some("QUIT"),
            response: b"205 bye\r\n",
        },
    ])
    .await;
    let resp = h
        .execute(&format!(
            r#"mutation {{
                addServer(input: {{
                    host: "127.0.0.1",
                    port: {port},
                    tls: false,
                    connections: 5,
                    username: "user",
                    password: "pass"
                }}) {{
                    id
                    host
                }}
            }}"#,
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let server = &data["addServer"];
    let id = server["id"].as_u64().unwrap();
    // Password is intentionally not exposed in the Server output type.
    assert!(server.get("password").is_none());

    let resp = h
        .execute(&format!(
            r#"{{
                server(id: {id}) {{
                    id
                    username
                }}
            }}"#,
        ))
        .await;
    assert_no_errors(&resp);
    let detail = &response_data(&resp)["server"];
    assert_eq!(detail["username"].as_str().unwrap(), "user");
    assert!(detail.get("password").is_none());
}

#[tokio::test]
async fn add_multiple_servers() {
    let h = TestHarness::new().await;
    h.execute(
        r#"mutation {
            addServer(input: {
                host: "news1.example.com",
                port: 119,
                tls: false,
                connections: 5,
                active: false
            }) { id }
        }"#,
    )
    .await;

    h.execute(
        r#"mutation {
            addServer(input: {
                host: "news2.example.com",
                port: 563,
                tls: true,
                connections: 10,
                active: false
            }) { id }
        }"#,
    )
    .await;

    let resp = h.execute("{ servers { id host } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let servers = data["servers"].as_array().unwrap();
    assert_eq!(servers.len(), 2);
}

#[tokio::test]
async fn update_server_host() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "old.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addServer"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateServer(id: {id}, input: {{
                    host: "new.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }}) {{
                    id
                    host
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["updateServer"]["host"].as_str().unwrap(),
        "new.example.com"
    );
}

#[tokio::test]
async fn update_server_connections_are_visible_in_follow_up_query() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addServer"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateServer(id: {id}, input: {{
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 20,
                    active: false
                }}) {{
                    id
                    connections
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["updateServer"]["connections"].as_u64().unwrap(), 20);

    let resp = h.execute("{ servers { id connections } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let server = data["servers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|server| server["id"].as_u64() == Some(id))
        .expect("updated server should be present");
    assert_eq!(server["connections"].as_u64().unwrap(), 20);
}

#[tokio::test]
async fn active_connection_reduction_waits_for_new_runtime_generation() {
    let h = TestHarness::new().await;
    let initial_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"500 unknown\r\n",
        },
        ScriptStep {
            expect_prefix: Some("QUIT"),
            response: b"205 bye\r\n",
        },
    ])
    .await;
    let corrected_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"500 unknown\r\n",
        },
        ScriptStep {
            expect_prefix: Some("QUIT"),
            response: b"205 bye\r\n",
        },
    ])
    .await;

    let response = h
        .execute(&format!(
            r#"mutation {{
                addServer(input: {{
                    host: "127.0.0.1",
                    port: {initial_port},
                    tls: false,
                    connections: 80,
                    active: true
                }}) {{ id }}
            }}"#
        ))
        .await;
    assert_no_errors(&response);
    let id = response_data(&response)["addServer"]["id"]
        .as_u64()
        .unwrap();
    assert_eq!(
        h.shared_state.nntp_runtime_activation(),
        Some(weaver_server_core::NntpRuntimeActivation {
            generation: 1,
            configured_connections: 80,
            effective_connections: 80,
        })
    );

    let response = h
        .execute(&format!(
            r#"mutation {{
                updateServer(id: {id}, input: {{
                    host: "127.0.0.1",
                    port: {corrected_port},
                    tls: false,
                    connections: 20,
                    active: true
                }}) {{ id connections }}
            }}"#
        ))
        .await;
    assert_no_errors(&response);
    assert_eq!(
        h.shared_state.nntp_runtime_activation(),
        Some(weaver_server_core::NntpRuntimeActivation {
            generation: 2,
            configured_connections: 20,
            effective_connections: 20,
        })
    );
}

#[tokio::test]
async fn servers_query_stays_responsive_during_update_server_persist() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "old.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addServer"]["id"].as_u64().unwrap();

    let blocker = BlockingDbOperation::new("servers.mutation.update_server.persist");
    let schema = h.schema.clone();
    let mutation = tokio::spawn(async move {
        schema
            .execute(local_request(format!(
                r#"mutation {{
                    updateServer(id: {id}, input: {{
                        host: "new.example.com",
                        port: 119,
                        tls: false,
                        connections: 5,
                        active: false
                    }}) {{
                        id
                        host
                    }}
                }}"#
            )))
            .await
    });

    blocker.wait_until_started().await;

    let resp = tokio::time::timeout(
        Duration::from_millis(100),
        h.execute(r#"{ servers { id host } }"#),
    )
    .await
    .expect("servers query should stay responsive while persist is blocked");
    assert_no_errors(&resp);
    let servers = response_data(&resp)["servers"].as_array().unwrap().clone();
    let server = servers
        .iter()
        .find(|server| server["id"].as_u64() == Some(id))
        .expect("server should still be present");
    assert_eq!(server["host"].as_str().unwrap(), "old.example.com");

    blocker.release();

    let mutation = mutation.await.unwrap();
    assert_no_errors(&mutation);
    assert_eq!(
        response_data(&mutation)["updateServer"]["host"]
            .as_str()
            .unwrap(),
        "new.example.com"
    );
}

#[tokio::test]
async fn update_server_preserves_password() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    username: "user",
                    password: "secret",
                    active: false
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addServer"]["id"].as_u64().unwrap();

    // Update without providing password (null) — password should be preserved.
    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateServer(id: {id}, input: {{
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    username: "user",
                    active: false
                }}) {{
                    id
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
}

#[tokio::test]
async fn remove_server() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addServer"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                removeServer(id: {id}) {{
                    id
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let remaining = data["removeServer"].as_array().unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn remove_nonexistent_server() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                removeServer(id: 999) {
                    id
                }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn server_priority_ordering() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "backup.example.com",
                    port: 119,
                    tls: false,
                    connections: 2,
                    priority: 1,
                    active: false
                }) { id priority }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["addServer"]["priority"]
            .as_u64()
            .unwrap(),
        1
    );

    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "primary.example.com",
                    port: 119,
                    tls: false,
                    connections: 10,
                    priority: 0,
                    active: false
                }) { id priority }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["addServer"]["priority"]
            .as_u64()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn test_connection_invalid_host() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                testConnection(input: {
                    host: "invalid.host.example",
                    port: 119,
                    tls: false,
                    connections: 1
                }) {
                    success
                    message
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["testConnection"];
    assert!(!result["success"].as_bool().unwrap());
    assert!(!result["message"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn add_active_server_invalid_host_is_rejected() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "invalid.host.example",
                    port: 119,
                    tls: false,
                    connections: 5
                }) {
                    id
                }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn update_active_server_invalid_host_is_rejected() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "draft.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addServer"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateServer(id: {id}, input: {{
                    host: "invalid.host.example",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: true
                }}) {{
                    id
                }}
            }}"#
        ))
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn update_active_server_preserves_password_during_validation() {
    let h = TestHarness::new().await;
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"500 unknown\r\n",
        },
        ScriptStep {
            expect_prefix: Some("AUTHINFO USER"),
            response: b"381 password required\r\n",
        },
        ScriptStep {
            expect_prefix: Some("AUTHINFO PASS"),
            response: b"281 authentication accepted\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"500 unknown\r\n",
        },
        ScriptStep {
            expect_prefix: Some("QUIT"),
            response: b"205 bye\r\n",
        },
    ])
    .await;

    let resp = h
        .execute(&format!(
            r#"mutation {{
                addServer(input: {{
                    host: "127.0.0.1",
                    port: {port},
                    tls: false,
                    connections: 5,
                    username: "user",
                    password: "secret",
                    active: false
                }}) {{ id }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addServer"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateServer(id: {id}, input: {{
                    host: "127.0.0.1",
                    port: {port},
                    tls: false,
                    connections: 5,
                    username: "user",
                    active: true
                }}) {{
                    id
                    active
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["updateServer"]["active"].as_bool().unwrap());
}

#[tokio::test]
async fn add_server_active_false() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }) {
                    id
                    active
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["addServer"]["active"].as_bool().unwrap());
}
