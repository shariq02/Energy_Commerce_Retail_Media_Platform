---
unit_id: rule.dwd.metadata_trailer_and_header_rows
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/dwd.yml
  sha256: d5c2f3fd5ab18edbc7086ec69acdece5ec039d8325c6645cec90304c632fd91c
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: metadata_trailer_and_header_rows (dwd)

**Expression:** dwd_station_name_history, dwd_device_instrument and dwd_parameter_unit carry parser-trailer rows ('generiert: ... Deutscher Wetterdienst'), and station_name_history additionally carries a spurious 'Stations_ID' header row and parameter_unit a 'Legende:' row. NB02 detects these; stage_dwd.py does not yet strip them.

**Severity:** block
