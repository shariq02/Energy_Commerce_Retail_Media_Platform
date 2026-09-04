-- shared_conformed.dim_weather_context -- conformed weather/environmental
-- regime attributes.
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: September 2026
--
-- NOT the full fact_weather (that stays Energy-local). This table carries
-- only the derived regime-level attributes other ecosystems may legitimately
-- reference as environmental context (a conformed dimension), published FROM
-- the Energy ecosystem's Weather domain once it exists.
--
-- STRUCTURE ONLY -- no data. Deriving this from the Energy ecosystem's
-- Weather domain requires that domain's own Silver/Gold to exist first,
-- which it does not yet. No row is guessed here.

CREATE TABLE shared_conformed.dim_weather_context (
    weather_context_key  varchar(40)  NOT NULL,   -- surrogate, e.g. {ags_code}:{date_key}
    ags_code             varchar(12)  NOT NULL,    -- references dim_geography.ags_code
    date_key             integer      NOT NULL,    -- references dim_date.date_key
    weather_regime        varchar(40),             -- e.g. 'cold_dry', 'mild_wet' -- classification rule defined later from real DWD Gold data
    avg_temperature_c      numeric(5,2),
    total_precipitation_mm numeric(6,2),
    source_system          varchar(60)  NOT NULL DEFAULT 'dwd',
    published_from         varchar(80)  NOT NULL DEFAULT 'energy.weather (Gold, not yet built)',
    created_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (weather_context_key),
    FOREIGN KEY (ags_code) REFERENCES shared_conformed.dim_geography (ags_code)
);

CREATE INDEX ix_dim_weather_context_date ON shared_conformed.dim_weather_context (date_key);
CREATE INDEX ix_dim_weather_context_geo  ON shared_conformed.dim_weather_context (ags_code);
