-- Migration 0002 -- CDC publication for logical replication
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026
--
-- Applied inside database `ecrmap`. Creates the publication Debezium reads from,
-- covering all 10 operational tables. The replication slot and the role's
-- REPLICATION privilege are set separately by cdc/config/replication_setup.sql
-- (superuser, not tracked as a migration).

BEGIN;

DROP PUBLICATION IF EXISTS ecrmap_cdc_pub;

CREATE PUBLICATION ecrmap_cdc_pub FOR TABLE
    operational.tariffs,
    operational.products,
    operational.customers,
    operational.customer_contracts,
    operational.meters,
    operational.advertisers,
    operational.campaigns,
    operational.campaign_budgets,
    operational.orders,
    operational.order_items;

INSERT INTO public.schema_migrations (version) VALUES ('0002_cdc_publication');

COMMIT;
