-- A member lifted into a namespace from the one below, a volume of a set
-- whose volumes were each packed alone in a container of their own: the
-- member it was there, and the member ids from the root down to it, the
-- position its name ranks at. Null for every other member, which is every
-- member written before this: nothing to backfill.
ALTER TABLE graph_members ADD COLUMN lifted_node_id BIGINT;
ALTER TABLE graph_members ADD COLUMN lifted_member_id BIGINT;
ALTER TABLE graph_members ADD COLUMN lifted_position TEXT;
