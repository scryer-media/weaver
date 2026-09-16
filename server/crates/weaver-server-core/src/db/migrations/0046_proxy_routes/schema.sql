CREATE TABLE proxy_profiles (
    id BIGINT PRIMARY KEY,
    config TEXT NOT NULL,
    password TEXT
);
CREATE TABLE proxy_routes (
    consumer TEXT PRIMARY KEY,
    policy TEXT NOT NULL
);
