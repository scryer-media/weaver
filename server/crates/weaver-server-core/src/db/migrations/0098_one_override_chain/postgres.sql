-- One override chain for every torrent setting. A seeding policy no longer
-- holds an upload cap: each level keeps one cap of its own, NULL to
-- inherit, 'off' for none, or bytes per second.

-- A tracker rule's upload limit is its cap; 'unlimited' is 'off'. A cap its
-- seeding policy held moves to the rule's cap where the rule set none; where
-- both were set, the rule's own limit stands.
ALTER TABLE tracker_rules RENAME COLUMN upload_limit TO upload_cap;
UPDATE tracker_rules SET upload_cap = 'off' WHERE upload_cap = 'unlimited';
UPDATE tracker_rules
   SET upload_cap = seeding::jsonb ->> 'max_upload_bps'
 WHERE upload_cap IS NULL
   AND seeding IS NOT NULL
   AND jsonb_typeof(seeding::jsonb -> 'max_upload_bps') = 'number';
UPDATE tracker_rules SET seeding = (seeding::jsonb - 'max_upload_bps')::text
 WHERE seeding IS NOT NULL;

-- A category's cap moves out of its seeding policy.
ALTER TABLE categories ADD COLUMN upload_cap TEXT;
UPDATE categories
   SET upload_cap = seeding::jsonb ->> 'max_upload_bps'
 WHERE seeding IS NOT NULL
   AND jsonb_typeof(seeding::jsonb -> 'max_upload_bps') = 'number';
UPDATE categories SET seeding = (seeding::jsonb - 'max_upload_bps')::text
 WHERE seeding IS NOT NULL;

-- A torrent's own policy's cap joins its own upload limit, the tighter of
-- the two.
UPDATE torrents
   SET upload_limit = LEAST(
         COALESCE(upload_limit, (policy_override::jsonb ->> 'max_upload_bps')::BIGINT),
         (policy_override::jsonb ->> 'max_upload_bps')::BIGINT
       )
 WHERE policy_override IS NOT NULL
   AND jsonb_typeof(policy_override::jsonb -> 'max_upload_bps') = 'number';
UPDATE torrents SET policy_override = (policy_override::jsonb - 'max_upload_bps')::text
 WHERE policy_override IS NOT NULL;

-- The global cap is kept beside the global policy.
UPDATE settings SET key = 'seeding.upload_cap' WHERE key = 'seeding.max_upload_bps';

-- A torrent's unpack was both what was asked for it and what its job was
-- built with; the value kept is the one in force, and what is asked starts
-- inherited.
ALTER TABLE torrents RENAME COLUMN unpack TO unpack_in_force;
ALTER TABLE torrents ADD COLUMN unpack BOOLEAN;
