---
unit_id: rule.rees46.session_grain
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
  approved_content_hash: 7fbc98d49453b06b7ddb4174fe1ce30aa29d464d1a3f539d1c130264d2ac6efc
---

# Rule: session_grain (rees46)

**Expression:** session identity is (user_id, user_session), never user_session alone

**Severity:** block
