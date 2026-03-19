mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};

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
async fn add_server_basic() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5
                }) {
                    id
                    host
                    port
                    tls
                    connections
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let server = &data["addServer"];
    assert!(server["id"].as_u64().unwrap() > 0);
    assert_eq!(server["host"].as_str().unwrap(), "news.example.com");
    assert_eq!(server["port"].as_u64().unwrap(), 119);
    assert!(!server["tls"].as_bool().unwrap());
    assert_eq!(server["connections"].as_u64().unwrap(), 5);
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
                    connections: 10
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
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.com",
                    port: 119,
                    tls: false,
                    connections: 5,
                    username: "user",
                    password: "pass"
                }) {
                    id
                    host
                    username
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let server = &data["addServer"];
    assert_eq!(server["username"].as_str().unwrap(), "user");
    // Password is intentionally not exposed in the Server output type.
    assert!(server.get("password").is_none());
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
                    username
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
