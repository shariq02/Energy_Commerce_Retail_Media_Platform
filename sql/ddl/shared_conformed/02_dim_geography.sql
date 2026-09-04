-- shared_conformed.dim_geography -- conformed German administrative geography
-- spine.
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: September 2026
--
-- The official German administrative hierarchy, one row per unit per level:
--   Nation (DE) -> Bundesland -> Regierungsbezirk (where it exists)
--     -> Kreis / kreisfreie Stadt -> Gemeinde
-- Authoritative key: Amtlicher Gemeindeschluessel (AGS) / Regionalschluessel
-- (ARS). NUTS codes are crosswalk attributes on these rows, not a level.
-- Postal codes (PLZ) are NOT here -- they are a separate many-to-many
-- crosswalk, geo_plz_gemeinde_xref (03_geo_plz_gemeinde_xref.sql).
--
-- Point/polygon/raster sources attach to this spine via a DERIVED
-- administrative attribution (point-in-polygon / area-overlap) -- they keep
-- their own native geometry and grain here is never merged into theirs
-- (the "non-collapse rule").
--
-- Seed data (seed/dim_geography_bundeslaender.csv): Nation + 16 Bundeslaender
-- only. Regierungsbezirk / Kreis / Gemeinde rows are NOT populated -- they
-- require an acquired official boundary dataset (BKG/Destatis), not guessed
-- here.

CREATE TABLE shared_conformed.dim_geography (
    ags_code          varchar(12)  NOT NULL,   -- Amtlicher Gemeindeschluessel (primary key)
    ars_code          varchar(12),             -- Regionalschluessel, where distinct from ags_code
    level             varchar(20)  NOT NULL,   -- nation | bundesland | regierungsbezirk | kreis | gemeinde
    name              varchar(120) NOT NULL,
    parent_ags_code   varchar(12),             -- NULL only for the nation row
    nuts_code         varchar(10),             -- crosswalk attribute, NOT a spine level
    valid_from        date,
    valid_to          date,                    -- NULL = currently valid
    source_system     varchar(60)  NOT NULL DEFAULT 'bkg_destatis_ags',
    created_at        timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (ags_code),
    CHECK (level IN ('nation', 'bundesland', 'regierungsbezirk', 'kreis', 'gemeinde')),
    FOREIGN KEY (parent_ags_code) REFERENCES shared_conformed.dim_geography (ags_code)
);

CREATE INDEX ix_dim_geography_parent ON shared_conformed.dim_geography (parent_ags_code);
CREATE INDEX ix_dim_geography_level  ON shared_conformed.dim_geography (level);

-- Derived administrative-attribution columns for point/polygon sources are
-- added to those sources' own tables at Silver, referencing
-- dim_geography.ags_code -- never stored redundantly here.
