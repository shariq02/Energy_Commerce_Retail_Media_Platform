# Model card: lapse_ga4.return

_model `gbt_sklearn`, dataset `lapse_ga4`, frozen version 8, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

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

- frame: 6872 rows, columns 11, rows by partition {'test': 6872}
- held-out read: partition test, frozen_delta_version 8, rows 6872, groups 6872
- frame: 6524 rows, columns 11, rows by partition {'validation': 6524}

## Training

- model family: gbt_sklearn; MLflow run `409c8edb21fc42648a8df8c84c3cfa58`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.07814 | 0.07863 | 0.07814 |
| calibration_error | 0.01058 | 0.007551 | 0.02073 |
| log_loss | 0.253 | 0.2516 | 0.2768 |
| pr_auc | 0.1943 | 0.2228 | 0.07814 |
| pr_auc_lift | 2.487 | 2.834 | 1 |
| primary_change_relative | 0.1279 |  | 0.006226 |
| skill_primary | 0.1162 |  |  |
| skill_primary_ci_high | 0.145 |  |  |
| skill_primary_ci_low | 0.09286 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.07863, held-out 0.07814 on `pr_auc`
