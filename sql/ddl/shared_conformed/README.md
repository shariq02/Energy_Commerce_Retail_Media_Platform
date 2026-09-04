# shared_conformed -- DDL for the conformed-dimension set

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Purpose: the DDL for the closed conformed-dimension set (`dim_date`,
`dim_time`, `dim_geography`, `dim_weather_context`) plus `dim_geography`'s
satellite crosswalk (`geo_plz_gemeinde_xref`). This is structural
implementation -- table definitions and, where the data is genuinely
ecosystem-independent, real reference data. It is not the Silver/Gold
implementation.

**Not yet deployed anywhere.** No BigQuery `shared_conformed` dataset and no
PostgreSQL serving database exist yet. This DDL is portable, ANSI-leaning
SQL, written so it can be applied to whichever store `shared_conformed` is
actually deployed to (BigQuery `shared_conformed` dataset, or the new
PostgreSQL serving database's `shared_conformed` schema) without rewriting
it -- there is one copy of dim_date, dim_time, dim_geography, and
dim_weather_context. Do not apply this DDL to the operational `ecrmap`
database (operational stays energy-only, never ecosystem-namespaced).

## Files

| File | Table | Status |
|---|---|---|
| `00_dim_date.sql` | `dim_date` | Structure + full data (`seed/dim_date.csv`) -- calendar dimension, no ecosystem evidence needed |
| `01_dim_time.sql` | `dim_time` | Structure + full data (`seed/dim_time.csv`) -- time-of-day dimension, no ecosystem evidence needed |
| `02_dim_geography.sql` | `dim_geography` | Structure + partial data (`seed/dim_geography_bundeslaender.csv`, Nation + 16 Bundesländer only). Regierungsbezirk / Kreis / Gemeinde rows require an acquired official boundary dataset (BKG/Destatis), not guessed here |
| `03_geo_plz_gemeinde_xref.sql` | `geo_plz_gemeinde_xref` | Structure only, no data -- requires an acquired PLZ crosswalk dataset |
| `04_dim_weather_context.sql` | `dim_weather_context` | Structure only, no data -- it is derived from the Energy ecosystem's Weather domain, and there is no Weather Gold yet to derive it from |

## What this is not

This DDL defines no ecosystem's entities, facts, or business dimensions. `dim_geography`'s Bundesland-level seed rows are the
16 German federal states plus the nation row -- fixed, official, unchanging
public administrative facts (the AGS 2-digit Land numbering), not evidence
requiring any specific source's acquisition. Everything below that level is
explicitly left empty rather than approximated.
