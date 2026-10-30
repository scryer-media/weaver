-- One override chain for every torrent setting. A seeding policy no longer
-- holds an upload cap: each level keeps one cap of its own, NULL to
-- inherit, 'off' for none, or bytes per second.

-- A tracker rule's upload limit is its cap; 'unlimited' is 'off'. A cap its
-- seeding policy held moves to the rule's cap where the rule set none; where
-- both were set, the rule's own limit stands.
ALTER TABLE tracker_rules RENAME COLUMN upload_limit TO upload_cap;
UPDATE tracker_rules SET upload_cap = 'off' WHERE upload_cap = 'unlimited';
UPDATE tracker_rules
   SET upload_cap = CAST(json_extract(seeding, '$.max_upload_bps') AS TEXT)
 WHERE upload_cap IS NULL
   AND json_valid(seeding)
   AND json_extract(seeding, '$.max_upload_bps') IS NOT NULL;
UPDATE tracker_rules SET seeding = json_remove(seeding, '$.max_upload_bps')
 WHERE json_valid(seeding);

-- A category's cap moves out of its seeding policy.
ALTER TABLE categories ADD COLUMN upload_cap TEXT;
UPDATE categories
   SET upload_cap = CAST(json_extract(seeding, '$.max_upload_bps') AS TEXT)
 WHERE json_valid(seeding)
   AND json_extract(seeding, '$.max_upload_bps') IS NOT NULL;
UPDATE categories SET seeding = json_remove(seeding, '$.max_upload_bps')
 WHERE json_valid(seeding);

-- A torrent's own policy's cap joins its own upload limit, the tighter of
-- the two.
UPDATE torrents
   SET upload_limit = CASE
         WHEN upload_limit IS NULL THEN json_extract(policy_override, '$.max_upload_bps')
         ELSE MIN(upload_limit, json_extract(policy_override, '$.max_upload_bps'))
       END
 WHERE json_valid(policy_override)
   AND json_extract(policy_override, '$.max_upload_bps') IS NOT NULL;
UPDATE torrents SET policy_override = json_remove(policy_override, '$.max_upload_bps')
 WHERE json_valid(policy_override);

-- The global cap is kept beside the global policy.
UPDATE settings SET key = 'seeding.upload_cap' WHERE key = 'seeding.max_upload_bps';

-- A torrent's unpack was both what was asked for it and what its job was
-- built with; the value kept is the one in force, and what is asked starts
-- inherited.
ALTER TABLE torrents RENAME COLUMN unpack TO unpack_in_force;
ALTER TABLE torrents ADD COLUMN unpack INTEGER;
