-- Operational schema -- customers, contracts, meters
-- Energy Commerce and Retail Media Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026

SET search_path = operational, public;

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
