---
unit_id: glossary.units_and_conventions
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
  approved_content_hash: 6f58db247917c02a98ae1057319e2ccca92514959b92466d58ace632c8ce336d
---

# Glossary: canonical units and numeric conventions

Canonical units: electrical capacity and thermal output in kW; daily market energy in MWh; quarter-hourly market power in MW; redispatch power in MW and energy in MWh; electricity price in EUR/MWh; coordinates in decimal degrees (EPSG:4326); lengths in metres, area in m2, distance to coast in km. The MaStR capacity columns are read as kW and the power plant list capacity columns as MW. A daily and a quarter-hourly series of one metric are never blended.

Years such as commissioning year are integer years. They are not timestamps.

All timestamps are stored in UTC. MaStR, redispatch and SMARD are Europe/Berlin wall-clock time, including days with 23 or 25 hours.

Some sources use a German comma as the decimal separator. Values are cast explicitly. A value that fails to parse is quarantined, not coerced.

Negative values are valid for electricity price and residual load. They are an error for realised generation. A MaStR capacity below 1 kW is a real micro-generator.
