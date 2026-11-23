# Model card: variant_high_impact.high_impact

_model `gbt_lightgbm`, dataset `variant_high_impact`, frozen version 51, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_is_high_impact`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a variant has a high predicted impact
- target `target_is_high_impact`, dataset `variant_high_impact`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 720434 rows, columns 411, rows by partition {'test': 720434}
- held-out read: partition test, frozen_delta_version 51, rows 720434, groups 2924
- frame: 126818 rows, columns 411, rows by partition {'validation': 126818}

## Training

- model family: gbt_lightgbm; MLflow run `79ac43fe66394cb6b041d8273c3e9151`
- parameters: {'row_fraction': 0.2, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.3627 | 0.3755 | 0.3627 |
| calibration_error | 4.165e-05 | 1.489e-07 | 0.003285 |
| log_loss | 0.0005341 | 1.489e-07 | 0.655 |
| pr_auc | 1 | 1 | 0.3627 |
| pr_auc_lift | 2.757 | 2.663 | 1 |
| primary_change_relative | 7.422e-08 |  | 0.03415 |
| skill_primary | 0.6373 |  |  |
| skill_primary_ci_high | 0.7541 |  |  |
| skill_primary_ci_low | 0.6098 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.3755, held-out 0.3627 on `pr_auc`
