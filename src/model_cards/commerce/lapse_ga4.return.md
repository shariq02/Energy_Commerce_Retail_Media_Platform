# Model card: lapse_ga4.return

_model `gbt_sklearn`, dataset `lapse_ga4`, frozen version 0, primary metric `pr_auc` (higher is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_returned`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a GA4 user returns
- target `target_returned`, dataset `lapse_ga4`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 6872 rows, columns 10, rows by partition {'test': 6872}
- held-out read: partition test, frozen_delta_version 0, rows 6872, groups 6872
- frame: 6524 rows, columns 10, rows by partition {'validation': 6524}

## Training

- model family: gbt_sklearn; MLflow run `a9f81375379840ab9a2826a8d57f723b`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.07814 | 0.07863 | 0.07814 |
| calibration_error | 0.00986 | 0.005187 | 0.02073 |
| log_loss | 0.2532 | 0.2508 | 0.2768 |
| pr_auc | 0.1933 | 0.2147 | 0.07814 |
| pr_auc_lift | 2.473 | 2.731 | 1 |
| primary_change_relative | 0.09988 |  | 0.006226 |
| skill_primary | 0.1151 |  |  |
| skill_primary_ci_high | 0.1418 |  |  |
| skill_primary_ci_low | 0.09327 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.07863, held-out 0.07814 on `pr_auc`
