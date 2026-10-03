# Model card: redispatch.any_event

_model `logistic`, dataset `redispatch`, frozen version 2, primary metric `pr_auc` (higher is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_any_event`
- baseline fallback: `base_rate`

## Intended use

- estimates `target_any_event` for the entities and period of dataset `redispatch`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1464 rows, columns 32, rows by partition {'test': 1464}
- held-out read: partition test, frozen_delta_version 2, rows 1464, time_min 2020-01-01 00:00:00, time_max 2020-12-31 00:00:00, groups 366
- frame: 1452 rows, columns 32, rows by partition {'validation': 1452}

## Training

- model family: logistic; MLflow run `18a8365730ef481797230770cf89b5ec`
- parameters: {'row_fraction': 1.0, 'C': 1.0}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | logistic held-out | logistic validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.7008 | 0.6749 | 0.7008 |
| bootstrap_blocks | 12 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 1464 |  |  |
| calibration_error | 0.03913 | 0.04501 | 0.0589 |
| log_loss | 0.5004 | 0.5223 | 0.6179 |
| pr_auc | 0.8976 | 0.8828 | 0.7008 |
| pr_auc_lift | 1.281 | 1.308 | 1 |
| pred_finite_share | 1 |  | 1 |
| pred_mean | 0.6885 | 0.6763 | 0.6419 |
| pred_positive_share | 0.7732 | 0.7596 | 1 |
| pred_std | 0.2123 |  | 2.22e-16 |
| primary_change_relative | -0.01675 |  | -0.03836 |
| reproduced_validation_value | 0.8828 |  | 0.6749 |
| reproduction_gap_relative | 0 |  | 0 |
| skill_primary | 0.1968 |  |  |
| skill_primary_ci_high | 0.2322 |  |  |
| skill_primary_ci_low | 0.1526 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.6749, held-out 0.7008 on `pr_auc`
