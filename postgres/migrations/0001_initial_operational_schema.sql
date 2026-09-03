-- Migration 0001 -- initial operational schema
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026
--
-- Applied inside database `ecrmap`. Mirrors postgres/ddl/ as of this migration
-- (00_bootstrap runs first, by a superuser). Idempotent guards omitted -- a
-- migration runs once, tracked in public.schema_migrations.

BEGIN;

CREATE SCHEMA IF NOT EXISTS operational;
SET LOCAL search_path = operational, public;

-- ---- reference -----------------------------------------------------------

CREATE TABLE operational.tariffs (
    tariff_id                     uuid          PRIMARY KEY,
    tariff_code                   varchar(20)   NOT NULL UNIQUE,
    name                          varchar(120)  NOT NULL,
    energy_type                   varchar(20)   NOT NULL
                                  CHECK (energy_type IN ('electricity', 'gas')),
    unit_price_eur_per_kwh        numeric(8,5)  NOT NULL CHECK (unit_price_eur_per_kwh > 0),
    standing_charge_eur_per_month numeric(8,2)  NOT NULL CHECK (standing_charge_eur_per_month >= 0),
    contract_term_months          integer       NOT NULL CHECK (contract_term_months > 0),
    active                        boolean       NOT NULL DEFAULT true,
    valid_from                    date          NOT NULL,
    valid_to                      date,
    created_at                    timestamptz   NOT NULL,
    updated_at                    timestamptz   NOT NULL,
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE TABLE operational.products (
    product_id      uuid          PRIMARY KEY,
    sku             varchar(24)   NOT NULL UNIQUE,
    name            varchar(160)  NOT NULL,
    category        varchar(60)   NOT NULL,
    unit_price_eur  numeric(10,2) NOT NULL CHECK (unit_price_eur > 0),
    active          boolean       NOT NULL DEFAULT true,
    created_at      timestamptz   NOT NULL,
    updated_at      timestamptz   NOT NULL
);

-- ---- customer & metering ------------------------------------------------

CREATE TABLE operational.customers (
    customer_id      uuid          PRIMARY KEY,
    customer_number  varchar(12)   NOT NULL UNIQUE,
    first_name       varchar(80)   NOT NULL,
    last_name        varchar(80)   NOT NULL,
    email            varchar(200)  NOT NULL UNIQUE,
    phone            varchar(40),
    street           varchar(160)  NOT NULL,
    house_number     varchar(16)   NOT NULL,
    postal_code      varchar(5)    NOT NULL CHECK (postal_code ~ '^[0-9]{5}$'),
    city             varchar(120)  NOT NULL,
    country_code     char(2)       NOT NULL DEFAULT 'DE' CHECK (country_code = 'DE'),
    date_of_birth    date,
    signed_up_at     timestamptz   NOT NULL,
    status           varchar(20)   NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active', 'inactive', 'churned')),
    created_at       timestamptz   NOT NULL,
    updated_at       timestamptz   NOT NULL
);

CREATE TABLE operational.customer_contracts (
    contract_id      uuid         PRIMARY KEY,
    contract_number  varchar(16)  NOT NULL UNIQUE,
    customer_id      uuid         NOT NULL REFERENCES operational.customers (customer_id),
    tariff_id        uuid         NOT NULL REFERENCES operational.tariffs (tariff_id),
    start_date       date         NOT NULL,
    end_date         date,
    status           varchar(20)  NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active', 'pending', 'ended', 'cancelled')),
    billing_day      smallint     NOT NULL CHECK (billing_day BETWEEN 1 AND 28),
    created_at       timestamptz  NOT NULL,
    updated_at       timestamptz  NOT NULL,
    CHECK (end_date IS NULL OR end_date >= start_date)
);

CREATE TABLE operational.meters (
    meter_id       uuid         PRIMARY KEY,
    meter_serial   varchar(24)  NOT NULL UNIQUE,
    contract_id    uuid         NOT NULL REFERENCES operational.customer_contracts (contract_id),
    meter_type     varchar(20)  NOT NULL CHECK (meter_type IN ('electricity', 'gas')),
    melo_id        varchar(33),
    installed_on   date         NOT NULL,
    removed_on     date,
    status         varchar(20)  NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'removed', 'faulty')),
    created_at     timestamptz  NOT NULL,
    updated_at     timestamptz  NOT NULL,
    CHECK (removed_on IS NULL OR removed_on >= installed_on)
);

-- ---- commerce ---------------------------------------------------------

CREATE TABLE operational.orders (
    order_id            uuid          PRIMARY KEY,
    order_number        varchar(16)   NOT NULL UNIQUE,
    customer_id         uuid          NOT NULL REFERENCES operational.customers (customer_id),
    order_status        varchar(20)   NOT NULL DEFAULT 'placed'
                        CHECK (order_status IN
                               ('placed', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')),
    ordered_at          timestamptz   NOT NULL,
    currency            char(3)       NOT NULL DEFAULT 'EUR' CHECK (currency = 'EUR'),
    items_subtotal_eur  numeric(12,2) NOT NULL CHECK (items_subtotal_eur >= 0),
    shipping_fee_eur    numeric(8,2)  NOT NULL DEFAULT 0 CHECK (shipping_fee_eur >= 0),
    total_eur           numeric(12,2) NOT NULL CHECK (total_eur >= 0),
    created_at          timestamptz   NOT NULL,
    updated_at          timestamptz   NOT NULL,
    CHECK (total_eur = items_subtotal_eur + shipping_fee_eur)
);

CREATE TABLE operational.order_items (
    order_item_id   uuid          PRIMARY KEY,
    order_id        uuid          NOT NULL
                    REFERENCES operational.orders (order_id) ON DELETE CASCADE,
    product_id      uuid          NOT NULL REFERENCES operational.products (product_id),
    quantity        integer       NOT NULL CHECK (quantity > 0),
    unit_price_eur  numeric(10,2) NOT NULL CHECK (unit_price_eur > 0),
    line_total_eur  numeric(12,2) NOT NULL CHECK (line_total_eur >= 0),
    created_at      timestamptz   NOT NULL,
    UNIQUE (order_id, product_id),
    CHECK (line_total_eur = round(unit_price_eur * quantity, 2))
);

-- ---- retail media ----------------------------------------------------

CREATE TABLE operational.advertisers (
    advertiser_id    uuid          PRIMARY KEY,
    advertiser_name  varchar(160)  NOT NULL UNIQUE,
    industry         varchar(80),
    contact_email    varchar(200),
    onboarded_at     timestamptz   NOT NULL,
    status           varchar(20)   NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active', 'paused', 'offboarded')),
    created_at       timestamptz   NOT NULL,
    updated_at       timestamptz   NOT NULL
);

CREATE TABLE operational.campaigns (
    campaign_id    uuid          PRIMARY KEY,
    campaign_name  varchar(160)  NOT NULL,
    advertiser_id  uuid          NOT NULL REFERENCES operational.advertisers (advertiser_id),
    objective      varchar(30)   NOT NULL
                   CHECK (objective IN ('awareness', 'consideration', 'conversion')),
    start_date     date          NOT NULL,
    end_date       date          NOT NULL,
    status         varchar(20)   NOT NULL DEFAULT 'draft'
                   CHECK (status IN ('draft', 'running', 'paused', 'completed', 'cancelled')),
    created_at     timestamptz   NOT NULL,
    updated_at     timestamptz   NOT NULL,
    UNIQUE (advertiser_id, campaign_name),
    CHECK (end_date >= start_date)
);

CREATE TABLE operational.campaign_budgets (
    campaign_budget_id  uuid          PRIMARY KEY,
    campaign_id         uuid          NOT NULL
                        REFERENCES operational.campaigns (campaign_id) ON DELETE CASCADE,
    period_start        date          NOT NULL,
    period_end          date          NOT NULL,
    budget_eur          numeric(12,2) NOT NULL CHECK (budget_eur > 0),
    spent_eur           numeric(12,2) NOT NULL DEFAULT 0 CHECK (spent_eur >= 0),
    created_at          timestamptz   NOT NULL,
    updated_at          timestamptz   NOT NULL,
    UNIQUE (campaign_id, period_start),
    CHECK (period_end >= period_start)
);

-- ---- indexes --------------------------------------------------------

CREATE INDEX ix_customer_contracts_customer ON operational.customer_contracts (customer_id);
CREATE INDEX ix_customer_contracts_tariff   ON operational.customer_contracts (tariff_id);
CREATE INDEX ix_meters_contract             ON operational.meters (contract_id);
CREATE INDEX ix_orders_customer             ON operational.orders (customer_id);
CREATE INDEX ix_orders_ordered_at           ON operational.orders (ordered_at);
CREATE INDEX ix_order_items_order           ON operational.order_items (order_id);
CREATE INDEX ix_order_items_product         ON operational.order_items (product_id);
CREATE INDEX ix_campaigns_advertiser        ON operational.campaigns (advertiser_id);
CREATE INDEX ix_campaign_budgets_campaign   ON operational.campaign_budgets (campaign_id);
CREATE INDEX ix_customers_status            ON operational.customers (status);
CREATE INDEX ix_campaigns_status            ON operational.campaigns (status);

INSERT INTO public.schema_migrations (version) VALUES ('0001_initial_operational_schema');

COMMIT;
