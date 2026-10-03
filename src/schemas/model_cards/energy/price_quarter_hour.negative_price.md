# Model card: price_quarter_hour.negative_price

_model `logistic`, dataset `price_quarter_hour`, frozen version 0, primary metric `pr_auc` (higher is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_is_negative_price`
- baseline fallback: `base_rate`

## Intended use

- estimates `target_is_negative_price` for the entities and period of dataset `price_quarter_hour`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 23804 rows, columns 44, rows by partition {'test': 23804}
- held-out read: partition test, frozen_delta_version 0, rows 23804, time_min 2026-01-01 00:00:00, time_max 2026-09-05 00:00:00, groups 248
- frame: 17476 rows, columns 44, rows by partition {'validation': 17476}

## Training

- model family: logistic; MLflow run `cdad4ea678a1436b9c3f730f3a634d3a`
- parameters: {'row_fraction': 1.0, 'C': 0.1}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | logistic held-out | logistic validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.07381 | 0.04252 | 0.07381 |
| bootstrap_blocks | 9 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 2.38e+04 |  |  |
| calibration_error | 0.03368 | 0.01277 | 0.008823 |
| log_loss | 0.1287 | 0.08613 | 0.264 |
| pr_auc | 0.7205 | 0.6168 | 0.07381 |
| pr_auc_lift | 9.761 | 14.51 | 1 |
| pred_finite_share | 1 |  | 1 |
| pred_mean | 0.1075 | 0.05497 | 0.06499 |
| pred_positive_share | 0.09406 | 0.03651 | 0 |
| pred_std | 0.2443 |  | 0 |
| primary_change_relative | -0.1681 |  | -0.7361 |
| reproduced_validation_value | 0.6168 |  | 0.04252 |
| reproduction_gap_relative | 0 |  | 0 |
| skill_primary | 0.6467 |  |  |
| skill_primary_ci_high | 0.735 |  |  |
| skill_primary_ci_low | 0.4814 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- held-out partition read 2 times (re-runs); scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.04252, held-out 0.07381 on `pr_auc`
