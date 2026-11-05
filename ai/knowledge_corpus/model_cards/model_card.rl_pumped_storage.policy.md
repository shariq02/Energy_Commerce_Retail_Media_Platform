---
unit_id: model_card.rl_pumped_storage.policy
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/rl_pumped_storage.policy.md
  sha256: 5b31db04f00930b81abc03d5043a6deda979fbd05ac6592c38f5e76e0a1fb1dc
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 7cc30e27f4eaba1d24706984af3c75d01378aebf8feb0e332fbb7b0576db7801
---

# Model card: rl_pumped_storage.policy

_model `conservative_q`, primary metric `reward_timing` (higher is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R09, R10

## Purpose and task
- paradigm: offline_rl; task type: policy
- target: `action_net_mwh`
- baseline fallback: `rule_policy`

## Intended use
- proposes the net pumped-storage action in MWh; evaluated on logged data only
- target `action_net_mwh`, dataset `rl_pumped_storage`
- restricted to offline only (see the conditions below)

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- reward_timing: conservative_q held-out 4.41e+04; conservative_q validation 2.459e+04; rule_policy held-out 4.159e+04
- skill_primary: conservative_q held-out 2513
- skill_primary_ci_high: conservative_q held-out 4160
- skill_primary_ci_low: conservative_q held-out 656.2

## Flags
- none

## Conditions and known limits
- offline evidence only; operational feasibility is not shown
- net energy per episode 4.446e+04 for the policy against 1.26e+04 in the logged data

## Disclosures
- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `rule_policy`: validation 1.308e+04, held-out 4.159e+04 on `reward_timing`
