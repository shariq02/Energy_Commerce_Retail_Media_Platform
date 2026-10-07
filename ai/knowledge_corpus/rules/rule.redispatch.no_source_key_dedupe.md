---
unit_id: rule.redispatch.no_source_key_dedupe
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
  approved_content_hash: f10c2039296a093f8c9f5f8d974afcb0bf622123618215ffdb649f81f877a8db
---

# Rule: no_source_key_dedupe (redispatch)

**Expression:** no measure id in the source; 568 exact full-row duplicates -- de-duplicate with distinct() at Silver before counting measures

**Severity:** block
