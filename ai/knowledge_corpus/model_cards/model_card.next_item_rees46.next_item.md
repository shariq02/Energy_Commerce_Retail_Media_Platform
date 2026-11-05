---
unit_id: model_card.next_item_rees46.next_item
kind: model_card
ecosystem: commerce
source:
- path: src/model_cards/commerce/next_item_rees46.next_item.md
  sha256: 0838ea9aa225e12a090b69e1c37f563a996ead721e17627ee175dc765dbcaf20
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 4a0456f97dbff310a5e5ea3aceb02ec8cb3f486bf8a2d8850ad25d1c9ad334b5
---

# Model card: next_item_rees46.next_item

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
- ranks products by the probability of being the next product in a REES46 session
- target `target_next_product_id`, dataset `next_item_rees46`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- ndcg_at_10: sequence_transformer held-out 0.09879; sequence_transformer validation 0.09651; popularity held-out 0.01693
- skill_primary: sequence_transformer held-out 0.08186
- skill_primary_ci_high: sequence_transformer held-out 0.08646
- skill_primary_ci_low: sequence_transformer held-out 0.07954

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `popularity`: validation 0.01624, held-out 0.01693 on `ndcg_at_10`
