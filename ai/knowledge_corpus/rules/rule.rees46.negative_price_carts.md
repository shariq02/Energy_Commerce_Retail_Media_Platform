---
unit_id: rule.rees46.negative_price_carts
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
  approved_content_hash: cf06128262da9ff2a66d914a663b3a372e0fd32bb051e07f182d3ac59cd8d59c
---

# Rule: negative_price_carts (rees46)

**Expression:** 4558 cart rows and 252203 view rows have price <= 0; 0 purchases do

**Severity:** warn
