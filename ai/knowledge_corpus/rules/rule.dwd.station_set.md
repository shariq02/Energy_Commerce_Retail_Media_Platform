---
unit_id: rule.dwd.station_set
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/dwd.yml
  sha256: d5c2f3fd5ab18edbc7086ec69acdece5ec039d8325c6645cec90304c632fd91c
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 25ac1d9792fb2f337dc58c81355adf2b623de152ea4f7fb582174b5e585725ce
---

# Rule: station_set (dwd)

**Expression:** STATIONS_ID in the 28 curated numeric ids (scripts/download/download_dwd.py STATIONS); a non-listed value is a parser artefact

**Severity:** block
