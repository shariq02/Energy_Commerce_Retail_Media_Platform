---
unit_id: rule.dwd.coordinate_quarantine
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

# Rule: coordinate_quarantine (dwd)

**Expression:** a station coordinate outside the Germany bounding box or at (0,0) is a quarantine class, not a silent NULL (0 observed in station_geography)

**Severity:** warn
