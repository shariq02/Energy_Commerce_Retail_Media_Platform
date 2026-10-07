---
unit_id: glossary.weather_terms
kind: glossary
ecosystem: energy
source:
- name: localisation_record
  sha256: 2f0d4f9a5a5abb5cafce8f44b7780ff525e3bc33125b76c3d801c71cbd85510b
version: 1
last_updated: '2026-10-07'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 272505e7f8292ff83b9a831fe4ec06225c72821f62b38c7817f97cfa32273a0e
---

# Glossary: DWD weather terms and conventions

Coded columns are codes, not measurements. They stay strings, are decoded to a label, and are never used in arithmetic. An unknown code is quarantined with its raw value, not turned into NULL.

- `QN_*`: DWD quality level. The scheme changed over the decades, so a record is decoded with the scheme valid for its era.
- `WW`: present weather, decoded with the WMO present-weather table. `WRTR` is the precipitation form.
- `V_N` and `V_S*_NS`: cloud cover in eighths (0 to 8). The value -1 means the sky is not recognisable and becomes NULL.
- Wind direction in degrees. The value 990 means variable and becomes NULL.
- Missing markers `-999`, `-999.0`, `-99.9` and an empty string are never real values. They become NULL before any statistic.

Units: temperature in degrees C, pressure in hPa, wind in m/s, precipitation in mm, humidity in percent, visibility in metres, sunshine in minutes. Solar radiation stays in J/cm2 and is not converted to W/m2. DWD solar `MESS_DATUM_WOZ` is true local solar time and is kept as it is.
