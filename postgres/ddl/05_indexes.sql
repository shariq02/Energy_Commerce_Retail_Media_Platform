-- Operational schema -- secondary indexes
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026
--
-- Foreign-key columns and the few columns operational queries filter on.
-- UNIQUE constraints already provide the indexes for natural keys.

SET search_path = operational, public;

CREATE INDEX ix_customer_contracts_customer  ON operational.customer_contracts (customer_id);
CREATE INDEX ix_customer_contracts_tariff    ON operational.customer_contracts (tariff_id);
CREATE INDEX ix_meters_contract              ON operational.meters (contract_id);
CREATE INDEX ix_orders_customer              ON operational.orders (customer_id);
CREATE INDEX ix_orders_ordered_at            ON operational.orders (ordered_at);
CREATE INDEX ix_order_items_order            ON operational.order_items (order_id);
CREATE INDEX ix_order_items_product          ON operational.order_items (product_id);
CREATE INDEX ix_customers_status             ON operational.customers (status);
