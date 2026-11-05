---
unit_id: model_card.weak_supervision.labels
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/weak_supervision.labels.md
  sha256: 234e017f1e107e2b31f8dbb0cad425d66f693c3c0832d8af45ef04dfc45e89c7
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: bc5a6b95ef3cdc699bfa3898d82a511677bae1597d9a2dffea09cf7c7bca8e4e
---

# Model card: weak_supervision.labels

_model `label_model`, primary metric `coverage` (higher is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `diagnostic_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R02, R06, R10

## Purpose and task
- paradigm: weak_supervision; task type: label_model
- target: `lf_label`
- baseline fallback: `majority_vote`

## Intended use
- combines labelling functions into one label; there is no ground truth
- target `lf_label`, dataset `weak_supervision`
- restricted to diagnostic only (see the conditions below)

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- coverage: label_model held-out 1; label_model validation 1; majority_vote held-out 1
- skill_primary: label_model held-out 0
- skill_primary_ci_high: label_model held-out 0
- skill_primary_ci_low: label_model held-out 0

## Flags
- skill interval includes zero: interval 0 to 0
- no skill over the baseline: skill 0

## Conditions and known limits
- skill interval 0 to 0 includes zero
- no ground truth: diagnostic use only

## Disclosures
- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `majority_vote`: validation 1, held-out 1 on `coverage`
