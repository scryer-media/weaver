ALTER TABLE script_outputs ADD COLUMN status TEXT NOT NULL DEFAULT '';
UPDATE script_outputs SET status = COALESCE(result_json::jsonb ->> 'status', '');
CREATE INDEX script_outputs_status_seq ON script_outputs(status, seq);
