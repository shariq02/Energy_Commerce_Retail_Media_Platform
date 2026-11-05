---
unit_id: model_card.lapse_ga4.return
kind: model_card
ecosystem: commerce
source:
- path: src/model_cards/commerce/lapse_ga4.return.md
  sha256: 6076a1a2ec82b06cbd0b6d0a6d764d9fe0c53eb6a0baa76e9b5b260e5ddefe86
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 01c79c4d183e6e9c421c8fe733451d82c5d351352a72272dd939e31a6b2f39ce
---

# Model card: lapse_ga4.return

_model `gbt_sklearn`, primary metric `pr_auc` (higher is better)_

## Decision
- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task
- paradigm: supervised; task type: classification
- target: `target_returned`
- baseline fallback: `base_rate`

## Intended use
- estimates the probability that a GA4 user returns
- target `target_returned`, dataset `lapse_ga4`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- pr_auc: gbt_sklearn held-out 0.1933; gbt_sklearn validation 0.2147; base_rate held-out 0.07814
- skill_primary: gbt_sklearn held-out 0.1151
- skill_primary_ci_high: gbt_sklearn held-out 0.1418
- skill_primary_ci_low: gbt_sklearn held-out 0.09327

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 3 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `base_rate`: validation 0.07863, held-out 0.07814 on `pr_auc`
