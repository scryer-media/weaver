use super::*;

#[test]
fn settings_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("foo", "bar").unwrap();
    assert_eq!(db.get_setting("foo").unwrap(), Some("bar".to_string()));
    assert_eq!(db.get_setting("missing").unwrap(), None);
}

#[test]
fn settings_upsert() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("key", "v1").unwrap();
    db.set_setting("key", "v2").unwrap();
    assert_eq!(db.get_setting("key").unwrap(), Some("v2".to_string()));
}

#[test]
fn server_crud() {
    let db = Database::open_in_memory().unwrap();

    let server = ServerConfig {
        id: 1,
        host: "news.example.com".to_string(),
        port: 443,
        tls: true,
        username: Some("user".to_string()),
        password: Some("pass".to_string()),
        connections: 10,
        active: true,
        supports_pipelining: false,
        priority: 0,
        tls_ca_cert: None,
    };

    db.insert_server(&server).unwrap();
    let servers = db.list_servers().unwrap();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].host, "news.example.com");
    assert_eq!(servers[0].port, 443);

    let mut updated = server.clone();
    updated.connections = 20;
    db.update_server(&updated).unwrap();
    let servers = db.list_servers().unwrap();
    assert_eq!(servers[0].connections, 20);

    assert!(db.delete_server(1).unwrap());
    assert!(!db.delete_server(1).unwrap());
    assert!(db.list_servers().unwrap().is_empty());
}

#[test]
fn next_server_id_empty() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(db.next_server_id().unwrap(), 1);
}

#[test]
fn config_roundtrip() {
    let db = Database::open_in_memory().unwrap();

    let config = Config {
        data_dir: "/tmp/weaver".to_string(),
        intermediate_dir: Some("/tmp/intermediate".to_string()),
        complete_dir: Some("/tmp/complete".to_string()),
        buffer_pool: None,
        tuner: None,
        servers: vec![ServerConfig {
            id: 1,
            host: "news.test.com".to_string(),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 5,
            active: true,
            supports_pipelining: true,
            priority: 0,
            tls_ca_cert: None,
        }],
        categories: vec![CategoryConfig {
            id: 1,
            name: "Movies".to_string(),
            dest_dir: Some("/media/movies".to_string()),
            aliases: "movie*, film*".to_string(),
        }],
        retry: None,
        max_download_speed: Some(1_000_000),
        isp_bandwidth_cap: None,
        cleanup_after_extract: Some(false),
        config_path: None,
    };

    db.save_config(&config).unwrap();
    let loaded = db.load_config().unwrap();

    assert_eq!(loaded.data_dir, "/tmp/weaver");
    assert_eq!(
        loaded.intermediate_dir,
        Some("/tmp/intermediate".to_string())
    );
    assert_eq!(loaded.complete_dir, Some("/tmp/complete".to_string()));
    assert_eq!(loaded.max_download_speed, Some(1_000_000));
    assert_eq!(loaded.cleanup_after_extract, Some(false));
    assert_eq!(loaded.servers.len(), 1);
    assert_eq!(loaded.servers[0].host, "news.test.com");
    assert!(loaded.servers[0].supports_pipelining);
    assert_eq!(loaded.categories.len(), 1);
    assert_eq!(loaded.categories[0].name, "Movies");
    assert_eq!(
        loaded.categories[0].dest_dir,
        Some("/media/movies".to_string())
    );
    assert_eq!(loaded.categories[0].aliases, "movie*, film*");
}

#[test]
fn category_crud() {
    let db = Database::open_in_memory().unwrap();

    let cat = CategoryConfig {
        id: 1,
        name: "TV".to_string(),
        dest_dir: None,
        aliases: "TV*, television".to_string(),
    };

    db.insert_category(&cat).unwrap();
    let cats = db.list_categories().unwrap();
    assert_eq!(cats.len(), 1);
    assert_eq!(cats[0].name, "TV");
    assert_eq!(cats[0].dest_dir, None);

    let mut updated = cat.clone();
    updated.dest_dir = Some("/media/tv".to_string());
    db.update_category(&updated).unwrap();
    let cats = db.list_categories().unwrap();
    assert_eq!(cats[0].dest_dir, Some("/media/tv".to_string()));

    assert!(db.delete_category(1).unwrap());
    assert!(!db.delete_category(1).unwrap());
    assert!(db.list_categories().unwrap().is_empty());
}

#[test]
fn next_category_id_empty() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(db.next_category_id().unwrap(), 1);
}
