---
unit_id: rule.rees46.bot_bursts
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
  approved_content_hash: 88dc116a63916f247e8492c2b3b92ace60c7cd27962001e447a23ad70c68c0f5
---

# Rule: bot_bursts (rees46)

**Expression:** 102 sessions show >20 events on a single event_time (likely bot / instrumentation)

**Severity:** warn
