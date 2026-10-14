---
unit_id: rule.honda_iot.p_w_1to1
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/honda_iot.yml
  sha256: ecce2378681f571721ff13c94ef4488889694aecc7508f455237a1737e27b568
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: a8a0783748d472397d96d0c6d5c6e926cb3899d04d91a424e83e01757520ef2d
---

# Rule: p_w_1to1 (honda_iot)

**Expression:** each metric's _p and _w tables join 1:1 on (frequency, datetime_utc)

**Severity:** warn
