# Model card: rl_redispatch.policy

_model `reward_weighted_gbt_lightgbm`, dataset `rl_redispatch`, frozen version 8, primary metric `action_agreement` (higher is better). Generated: 2026-11-23T13:07Z_

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

## Data

- frame: 6778 rows, columns 39, rows by partition {'test': 6778}
- held-out read: partition test, frozen_delta_version 8, rows 6778, groups 1026
- frame: 5207 rows, columns 39, rows by partition {'validation': 5207}

## Training

- model family: reward_weighted_gbt_lightgbm; MLflow run `f3238c574ed144cea481152acdad576f`
- parameters: {'row_fraction': 1.0}
- library versions: not recorded
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | reward_weighted_gbt_lightgbm held-out | reward_weighted_gbt_lightgbm validation | majority_action held-out |
|---|---|---|---|
| action_agreement | 0.6282 | 0.6941 | 0.5428 |
| primary_change_relative | 0.09489 |  | -0.02999 |
| reward_weighted_agreement | 0.5904 | 0.6938 | 0.6312 |
| reward_when_agree | -1553 | -1685 | -1922 |
| reward_when_disagree | -1821 | -1687 | -1333 |
| skill_primary | 0.08542 |  |  |
| skill_primary_ci_high | 0.1092 |  |  |
| skill_primary_ci_low | 0.06467 |  |  |

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
