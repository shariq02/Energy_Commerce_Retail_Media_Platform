# Model card: rl_redispatch.policy

_model `reward_weighted_gbt_sklearn`, dataset `rl_redispatch`, frozen version 2, primary metric `action_agreement` (higher is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R09

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

## Data

- frame: 6778 rows, columns 34, rows by partition {'test': 6778}
- held-out read: partition test, frozen_delta_version 2, rows 6778, groups 1026
- frame: 5207 rows, columns 34, rows by partition {'validation': 5207}

## Training

- model family: reward_weighted_gbt_sklearn; MLflow run `953f1c391ea3456c9356c1ac7e831d4a`
- parameters: {'row_fraction': 1.0}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | reward_weighted_gbt_sklearn held-out | reward_weighted_gbt_sklearn validation | majority_action held-out |
|---|---|---|---|
| action_agreement | 0.6285 | 0.6962 | 0.5428 |
| primary_change_relative | 0.09721 |  | -0.02999 |
| reward_weighted_agreement | 0.5883 | 0.6929 | 0.6312 |
| reward_when_agree | -1547 | -1678 | -1922 |
| reward_when_disagree | -1831 | -1704 | -1333 |
| skill_primary | 0.08572 |  |  |
| skill_primary_ci_high | 0.105 |  |  |
| skill_primary_ci_low | 0.06498 |  |  |

## Flags

- none

## Conditions and known limits

- offline evidence only; operational feasibility is not shown

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `majority_action`: validation 0.527, held-out 0.5428 on `action_agreement`
