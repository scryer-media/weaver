-- How a name dialled straight out of an egress, with no proxy above it, is
-- resolved, as JSON: who answers, the servers asked and how their answers
-- are trusted. NULL is the host's own DNS, as every egress had before.
ALTER TABLE egress_interfaces ADD COLUMN dns TEXT;
