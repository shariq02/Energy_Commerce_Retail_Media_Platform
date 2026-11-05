---
unit_id: model_card.next_item_ga4.next_item
kind: model_card
ecosystem: commerce
source:
- path: src/model_cards/commerce/next_item_ga4.next_item.md
  sha256: 79a455ad0014058bbf4588f8b635c37cd7d42e2aa96ad6cb6e4213b02bea6ced
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: a3490c3e3cc5533a7b8f4672f887d029872fdacd0a982de5d5ffa63b29adcdde
---

# Model card: next_item_ga4.next_item

_model `sequence_transformer`, primary metric `ndcg_at_10` (higher is better)_

## Decision
- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task
- paradigm: ranking; task type: ranking
- target: `target_next_product_id`
- baseline fallback: `popularity`

## Intended use
- ranks products by the probability of being the next product in a GA4 session
- target `target_next_product_id`, dataset `next_item_ga4`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- ndcg_at_10: sequence_transformer held-out 0.4667; sequence_transformer validation 0.4684; popularity held-out 0.0749
- skill_primary: sequence_transformer held-out 0.3918
- skill_primary_ci_high: sequence_transformer held-out 0.4038
- skill_primary_ci_low: sequence_transformer held-out 0.3899

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- The only baseline is item popularity; there is no baseline that uses the items already seen in the session.
- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `popularity`: validation 0.07577, held-out 0.0749 on `ndcg_at_10`
