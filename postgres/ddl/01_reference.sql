-- Operational schema -- reference tables (tariffs, products)
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026

CREATE SCHEMA IF NOT EXISTS operational;
SET search_path = operational, public;

-- Energy tariffs the retailer offers.
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

-- Physical products sold alongside energy (meters, smart-home devices, accessories).
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
