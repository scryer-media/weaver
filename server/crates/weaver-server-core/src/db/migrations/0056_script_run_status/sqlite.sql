ALTER TABLE script_outputs ADD COLUMN status TEXT NOT NULL DEFAULT '';
UPDATE script_outputs SET status = COALESCE(json_extract(result_json, '$.status'), '');
CREATE INDEX script_outputs_status_seq ON script_outputs(status, seq);
