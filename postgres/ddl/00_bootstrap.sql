-- Operational PostgreSQL bootstrap
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026
--
-- Run once by a superuser on the local PostgreSQL instance, before any
-- migration. Creates the ecrmap database, the app role, and the schema, and
-- sets logical-replication readiness (Phase 6 / ADR-004 creates the Debezium
-- publication and slot; not here).

-- 1. Database (run from the `postgres` maintenance DB; cannot be in a txn block).
--    createdb ecrmap    -- or:
-- CREATE DATABASE ecrmap;

-- 2. Logical replication readiness -- server-level, needs a restart.
--    In postgresql.conf:  wal_level = logical
--    (max_replication_slots / max_wal_senders default 10 -- sufficient.)
-- ALTER SYSTEM SET wal_level = 'logical';   -- then restart the server

-- 3. App role. Password is set from the environment, not committed.
-- CREATE ROLE ecrmap_app WITH LOGIN PASSWORD :'app_password';
-- The role that Debezium connects as (Phase 6) needs REPLICATION:
-- ALTER ROLE ecrmap_app WITH REPLICATION;

-- 4. Schema (inside ecrmap). Also created by migration 0001 if absent.
CREATE SCHEMA IF NOT EXISTS operational;
-- GRANT USAGE, CREATE ON SCHEMA operational TO ecrmap_app;
-- ALTER ROLE ecrmap_app IN DATABASE ecrmap SET search_path = operational, public;

-- 5. Migration tracking (plain numbered SQL migrations; no Alembic).
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version     text        PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);
