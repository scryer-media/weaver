-- Saved DNS presets. A server list names public DNS servers once, as a
-- versioned JSON list of at most ten; a profile says who answers a route's
-- names, or for a proxy name profile a proxy's own address, and, when Weaver
-- asks listed servers, which list, how they are reached and how their
-- answers are trusted. A profile's kind is set when it is created. A route
-- profile where the proxy answers says whether the host answers a stage
-- with no proxy. Both are kept
-- until the operator deletes them, and a save refuses more than 128 of each.
CREATE TABLE IF NOT EXISTS dns_server_lists (
    id BIGINT PRIMARY KEY CHECK (id > 0),
    name TEXT NOT NULL,
    servers TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dns_profiles (
    id BIGINT PRIMARY KEY CHECK (id > 0),
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'route',
    who TEXT NOT NULL,
    transit TEXT,
    trust TEXT NOT NULL,
    server_list_id BIGINT,
    system_fallback INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_dns_profiles_server_list
    ON dns_profiles (server_list_id);
-- The profile a route's names follow. 0 is the built-in System profile, the
-- host's own resolver, which every route had before.
ALTER TABLE proxy_routes ADD COLUMN dns_profile_id BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_proxy_routes_dns_profile
    ON proxy_routes (dns_profile_id);
-- The profile that finds a proxy's own address when it is a name. 0 is the
-- built-in System profile, which found every proxy before.
ALTER TABLE proxy_profiles ADD COLUMN dns_profile_id BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_proxy_profiles_dns_profile
    ON proxy_profiles (dns_profile_id);
