-- Operational schema -- advertisers, campaigns, campaign_budgets
-- Energy Commerce and Retail Media Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026

SET search_path = operational, public;

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
