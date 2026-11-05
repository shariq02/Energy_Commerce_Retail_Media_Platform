---
unit_id: model_card.capacity_additions.additions
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/capacity_additions.additions.md
  sha256: c403de65c5d3aba0d85d52975a8414b2ae1433005426445729ceae6f4b3ad70c
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: ae75199a4cf4ae4427161706f55954835f45c68cad9cfa444f7dda2623220839
---

# Model card: capacity_additions.additions

_model `tabpfn_25_in_context`, primary metric `mae` (lower is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04, R10, R13

## Purpose and task
- paradigm: supervised; task type: regression_count
- target: `target_capacity_added_mw`
- baseline fallback: `seasonal_mean`

## Intended use
- estimates the capacity in MW added per period
- target `target_capacity_added_mw`, dataset `capacity_additions`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: tabpfn_25_in_context held-out 31.64; tabpfn_25_in_context validation 14.1; seasonal_mean held-out 36.13
- skill_primary: tabpfn_25_in_context held-out 0.1243
- skill_primary_ci_high: tabpfn_25_in_context held-out 0.1953
- skill_primary_ci_low: tabpfn_25_in_context held-out 0.0513

## Flags
- validation-to-test degradation above the threshold: primary metric +124.5% worse than on validation
- stored model does not reproduce its validation result: relative gap 0.00031

## Conditions and known limits
- primary metric +124.5% worse than on validation; revalidate on newer data before relying on it
- contamination flag unresolved: the pretraining data of the model was not shown to be free of the evaluated data; revision 6c45f3a6d0d0; pretraining data per register: synthetic tables from the TabPFN prior; read the model card; task data names found: none

## Disclosures
- the held-out partition was read in at least 5 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged
- foundation model Prior-Labs/tabpfn_2_5 at revision 6c45f3a6d0d0, licence tabpfn-2.5-non-commercial, ran in context

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `seasonal_mean`: validation 20.47, held-out 36.13 on `mae`
