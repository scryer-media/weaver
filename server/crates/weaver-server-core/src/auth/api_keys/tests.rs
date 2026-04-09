use super::*;

fn test_hash(seed: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = seed;
    h
}

#[test]
fn insert_and_lookup() {
    let db = Database::open_in_memory().unwrap();
    let hash = test_hash(1);
    let id = db.insert_api_key("scryer", &hash, "integration").unwrap();
    assert!(id > 0);

    let row = db.lookup_api_key(&hash).unwrap().unwrap();
    assert_eq!(row.id, id);
    assert_eq!(row.name, "scryer");
    assert_eq!(row.scope, "integration");
    assert!(row.last_used_at.is_none());
}

#[test]
fn lookup_missing_returns_none() {
    let db = Database::open_in_memory().unwrap();
    let hash = test_hash(99);
    assert!(db.lookup_api_key(&hash).unwrap().is_none());
}

#[test]
fn duplicate_hash_rejected() {
    let db = Database::open_in_memory().unwrap();
    let hash = test_hash(2);
    db.insert_api_key("first", &hash, "integration").unwrap();
    assert!(db.insert_api_key("second", &hash, "admin").is_err());
}

#[test]
fn list_and_delete() {
    let db = Database::open_in_memory().unwrap();
    let h1 = test_hash(10);
    let h2 = test_hash(11);
    let id1 = db.insert_api_key("key1", &h1, "integration").unwrap();
    let _id2 = db.insert_api_key("key2", &h2, "admin").unwrap();

    let keys = db.list_api_keys().unwrap();
    assert_eq!(keys.len(), 2);

    assert!(db.delete_api_key(id1).unwrap());
    assert!(!db.delete_api_key(id1).unwrap()); // already gone

    let keys = db.list_api_keys().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].name, "key2");
}

#[test]
fn touch_last_used() {
    let db = Database::open_in_memory().unwrap();
    let hash = test_hash(3);
    let id = db.insert_api_key("test", &hash, "integration").unwrap();

    db.touch_api_key_last_used(id, 1234567890).unwrap();
    let row = db.lookup_api_key(&hash).unwrap().unwrap();
    assert_eq!(row.last_used_at, Some(1234567890));
}
