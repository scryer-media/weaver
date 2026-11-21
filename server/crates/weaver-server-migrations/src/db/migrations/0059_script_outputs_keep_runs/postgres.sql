ALTER TABLE script_outputs DROP CONSTRAINT script_outputs_instance_id_fkey;
ALTER TABLE script_outputs ADD CONSTRAINT script_outputs_instance_id_fkey
    FOREIGN KEY (instance_id) REFERENCES script_instances(id) ON DELETE SET NULL;
