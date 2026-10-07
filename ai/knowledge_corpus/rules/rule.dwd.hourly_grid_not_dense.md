---
unit_id: rule.dwd.hourly_grid_not_dense
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

# Rule: hourly_grid_not_dense (dwd)

**Expression:** per-station hourly coverage 38-100% -- gaps are real, no dense-grid / uniform-completeness assumption

**Severity:** warn
