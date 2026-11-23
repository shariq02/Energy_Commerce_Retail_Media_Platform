# Model card: variant_actionable_rare.actionable

_model `gbt_lightgbm`, dataset `variant_actionable_rare`, frozen version 51, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_is_clinically_actionable_rare_variant`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a rare variant is clinically actionable
- target `target_is_clinically_actionable_rare_variant`, dataset `variant_actionable_rare`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 323398 rows, columns 409, rows by partition {'test': 323398}
- held-out read: partition test, frozen_delta_version 51, rows 323398, groups 2795
- frame: 133628 rows, columns 409, rows by partition {'validation': 133628}

## Training

- model family: gbt_lightgbm; MLflow run `34256ce012af45b58ae17ee221e3ea54`
- parameters: {'row_fraction': 0.45, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.01487 | 0.01387 | 0.01487 |
| calibration_error | 1.138e-08 | 1.192e-08 | 0.0003993 |
| log_loss | 1.138e-08 | 1.192e-08 | 0.07733 |
| pr_auc | 1 | 1 | 0.01487 |
| pr_auc_lift | 67.26 | 72.11 | 1 |
| primary_change_relative | 0 |  | -0.07213 |
| skill_primary | 0.9851 |  |  |
| skill_primary_ci_high | 0.9903 |  |  |
| skill_primary_ci_low | 0.9856 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.01387, held-out 0.01487 on `pr_auc`
