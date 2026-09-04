-- shared_conformed.geo_plz_gemeinde_xref -- postal-code crosswalk.
-- ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
-- Author: Sharique Mohammad
-- Date: September 2026
--
-- PLZ (postal code) is explicitly NOT administrative and NOT a
-- dim_geography spine level. It is a separate
-- many-to-many crosswalk -- one PLZ can span multiple Gemeinden and vice
-- versa -- and every PLZ -> Gemeinde/Kreis lift through this table is
-- documented as approximate, never treated as exact administrative truth.
--
-- STRUCTURE ONLY -- no data. Populating this table requires an acquired
-- official PLZ-to-Gemeinde crosswalk dataset; none has been acquired yet, so
-- no row is guessed here.

CREATE TABLE shared_conformed.geo_plz_gemeinde_xref (
    plz               varchar(5)   NOT NULL,
    ags_code          varchar(12)  NOT NULL,   -- references dim_geography.ags_code (level = gemeinde)
    coverage_share    numeric(5,4),            -- approximate share of the PLZ area/population in this Gemeinde, where known
    source_system     varchar(60)  NOT NULL DEFAULT 'bkg_destatis_plz_xref',
    valid_from        date,
    valid_to          date,
    created_at        timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (plz, ags_code),
    FOREIGN KEY (ags_code) REFERENCES shared_conformed.dim_geography (ags_code)
);

CREATE INDEX ix_geo_plz_gemeinde_xref_plz ON shared_conformed.geo_plz_gemeinde_xref (plz);
