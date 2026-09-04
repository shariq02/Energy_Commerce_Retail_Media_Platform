-- CDC -- logical replication setup (run once, superuser)
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026
--
-- Prerequisites on the PostgreSQL server (postgresql.conf, needs a restart):
--   wal_level = logical
--   max_replication_slots >= 4
--   max_wal_senders       >= 4
--
-- And in pg_hba.conf, a replication line for the client host, e.g.:
--   host    replication    ecrmap_app    <wsl-client-cidr>    scram-sha-256
--
-- Run this file connected to database `ecrmap` as a superuser.

-- 1. The role Debezium connects as needs REPLICATION and read on the tables.
ALTER ROLE ecrmap_app WITH REPLICATION;

GRANT USAGE ON SCHEMA operational TO ecrmap_app;
GRANT SELECT ON ALL TABLES IN SCHEMA operational TO ecrmap_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA operational GRANT SELECT ON TABLES TO ecrmap_app;

-- 2. The publication is created by migration 0002. Verify it exists:
--   SELECT pubname FROM pg_publication WHERE pubname = 'ecrmap_cdc_pub';

-- 3. Create the logical replication slot Debezium will use (pgoutput plugin).
--    Debezium can also create this itself on first connect; creating it here
--    makes the slot an explicit, inspectable object from the start.
SELECT pg_create_logical_replication_slot('ecrmap_cdc_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ecrmap_cdc_slot'
);

-- 4. Inspect:
--   SELECT slot_name, plugin, slot_type, active, restart_lsn
--   FROM pg_replication_slots WHERE slot_name = 'ecrmap_cdc_slot';
