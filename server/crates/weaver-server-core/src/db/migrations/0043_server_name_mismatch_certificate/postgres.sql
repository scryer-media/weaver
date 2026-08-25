ALTER TABLE servers ADD COLUMN IF NOT EXISTS tls_name_mismatch_certificate_der BYTEA;
