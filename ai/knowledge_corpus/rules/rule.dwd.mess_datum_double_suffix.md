---
unit_id: rule.dwd.mess_datum_double_suffix
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
  approved_content_hash: 86267ecc20099ef3936017b622f1db162aae34fb6eaddb249377fe046cae047e
---

# Rule: mess_datum_double_suffix (dwd)

**Expression:** MESS_DATUM may carry a trailing '.0' from a double-inferred column -- strip before parsing yyyyMMddHH

**Severity:** block
