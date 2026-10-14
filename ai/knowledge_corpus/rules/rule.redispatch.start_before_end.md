---
unit_id: rule.redispatch.start_before_end
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/redispatch.yml
  sha256: 99e2b9a9b512cfcc410bde881c833587a281acfd4d04fe495530fc8a8571e9cc
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: fd413342a9a6d6c2ad5a13e6972ec8dce5608814a86abf30c7446ec2fca896e5
---

# Rule: start_before_end (redispatch)

**Expression:** combined start timestamp <= combined end timestamp

**Severity:** warn
