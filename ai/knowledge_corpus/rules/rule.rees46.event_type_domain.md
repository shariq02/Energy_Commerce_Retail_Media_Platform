---
unit_id: rule.rees46.event_type_domain
kind: rule
ecosystem: commerce
source:
- path: src/schemas/contracts/rees46.yml
  sha256: 22149137515ddc6f5a8dfa8c14480b56419bee5ef47a02812b3b49b91d80fad8
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 5144ab75c999108ee2ceae881005f5ad87c2a74ed953cab686105136226f0f4d
---

# Rule: event_type_domain (rees46)

**Expression:** event_type in {view, cart, purchase}

**Severity:** block
