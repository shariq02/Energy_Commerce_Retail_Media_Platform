---
unit_id: rule.dwd.station_geography_time_varying
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
  approved_content_hash: 56da081eb38029c723f90ab82460318bdbb1734b927045b28eb5d3a8e1001ed1
---

# Rule: station_geography_time_varying (dwd)

**Expression:** some stations have multiple geography rows (relocations > 5 km for 10 stations) -- join measurement rows on the von/bis validity window, not STATIONS_ID alone

**Severity:** block
