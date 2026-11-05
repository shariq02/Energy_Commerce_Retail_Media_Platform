---
unit_id: model_card.rl_redispatch.policy
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/rl_redispatch.policy.md
  sha256: 09e1df5eff3d53a57e2a9b77d2ff7972dbca4b5fb8d2451f8ff8289f50f6bf3e
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: f282b386de643399aaf3e2eaf8c039d785d2371a44041a6d6774c72810e053fb
---

# Model card: rl_redispatch.policy

_model `reward_weighted_gbt_sklearn`, primary metric `action_agreement` (higher is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R09, R10

## Purpose and task
- paradigm: offline_rl; task type: policy
- target: `action_direction`
- baseline fallback: `majority_action`

## Intended use
- proposes the direction of a redispatch action; evaluated on logged data only
- target `action_direction`, dataset `rl_redispatch`
- restricted to offline only (see the conditions below)

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- action_agreement: reward_weighted_gbt_sklearn held-out 0.6285; reward_weighted_gbt_sklearn validation 0.6962; majority_action held-out 0.5428
- skill_primary: reward_weighted_gbt_sklearn held-out 0.08572
- skill_primary_ci_high: reward_weighted_gbt_sklearn held-out 0.105
- skill_primary_ci_low: reward_weighted_gbt_sklearn held-out 0.06498

## Flags
- none

## Conditions and known limits
- offline evidence only; operational feasibility is not shown

## Disclosures
- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `majority_action`: validation 0.527, held-out 0.5428 on `action_agreement`
