---
unit_id: model_card.ccpp.output
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/ccpp.output.md
  sha256: b506cbe64bf45323f9204381088b3253eea46dabb468b4e067f77c1d0c14a335
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: e583c8f854c74d470256b1e815e9ea618746ac1e1d81bee8714a79d15eef659d
---

# Model card: ccpp.output

_model `tabicl_v2_in_context`, primary metric `mae` (lower is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R10, R13

## Purpose and task
- paradigm: supervised; task type: regression
- target: `target_net_output_mw`
- baseline fallback: `train_mean`

## Intended use
- estimates the net electrical output in MW of a combined cycle power plant from its operating conditions
- target `target_net_output_mw`, dataset `ccpp`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: tabicl_v2_in_context held-out 1.882; tabicl_v2_in_context validation 1.927; train_mean held-out 15.03
- skill_primary: tabicl_v2_in_context held-out 0.8748
- skill_primary_ci_high: tabicl_v2_in_context held-out 0.8832
- skill_primary_ci_low: tabicl_v2_in_context held-out 0.8659

## Flags
- stored model does not reproduce its validation result: relative gap 0.000317

## Conditions and known limits
- contamination flag unresolved: the pretraining data of the model was not shown to be free of the evaluated data; revision 4dcd344ece2c; pretraining data per register: synthetic tables from the TabICL prior; read the model card; task data names found: none

## Disclosures
- the held-out partition was read in at least 5 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged
- foundation model jingang/TabICL at revision 4dcd344ece2c, licence bsd-3-clause, ran in context

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `train_mean`: validation 14.79, held-out 15.03 on `mae`
